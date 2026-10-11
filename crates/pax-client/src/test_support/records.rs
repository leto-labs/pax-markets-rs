//! Matched authenticated record streams for isolated, offline TLS experiments.
//! Session secrets exist only in process memory and are never serialized.
pub mod bounded;
pub mod unbuffered;
use super::TlsPeer;
use rustls::{ClientConnection, Connection, ServerConnection};
use std::{
    io::{self, Read, Write},
    sync::Arc,
};
type Error = Box<dyn std::error::Error + Send + Sync>;

/// An ordered batch of application records from one server write.
pub struct Batch {
    /// Generated ciphertext; usable exactly once at the matching receiver position.
    pub ciphertext: Vec<u8>,
    /// Expected authenticated plaintext bytes.
    pub plaintext_bytes: usize,
    /// TLS record count, parsed from generated record headers outside timing.
    pub records: usize,
}
/// Receiver and ciphertext prepared together. Repetitions require a fresh instance;
/// replaying ciphertext into advanced TLS sequence state is deliberately invalid.
pub struct Prepared {
    /// Receiver positioned immediately after handshake and ticket processing.
    pub receiver: Receiver,
    /// Ordered application data, with no production traffic or session keys.
    pub batches: Vec<Batch>,
    /// Negotiated suite name for benchmark provenance.
    pub cipher: String,
    /// Negotiated TLS version for benchmark provenance.
    pub version: String,
}
/// Owns evolving TLS authentication and record sequence state. Not cloneable.
pub struct Receiver(Connection);
impl Receiver {
    /// Authenticate all input and copy plaintext into caller-owned initialized memory.
    /// `chunk` bounds each simulated socket read. Handshake and sender encryption
    /// are excluded; record parsing, authentication, internal buffering and copying
    /// are included. Returns the exact number of plaintext bytes written.
    ///
    /// # Errors
    /// Rejects corruption, wrong record sequence, zero chunks, truncated output
    /// storage and TLS/protocol failures. Partial records remain in this receiver.
    pub fn receive(
        &mut self,
        encrypted: &[u8],
        output: &mut [u8],
        chunk: usize,
    ) -> Result<usize, Error> {
        self.receive_inner::<false>(encrypted, output, chunk)
            .map(|(written, _)| written)
    }
    /// Authenticate a separate component fixture and count actual read_tls calls.
    /// Use outside ordinary component timing to establish intrusive adapter cost.
    /// Counts refer to in-memory input operations, not network socket syscalls.
    /// The ordinary helper statically omits this counter.
    ///
    /// # Errors
    /// Returns the same authentication/input errors as `receive`, plus overflow.
    pub fn receive_counted(
        &mut self,
        encrypted: &[u8],
        output: &mut [u8],
        chunk: usize,
    ) -> Result<(usize, u64), Error> {
        self.receive_inner::<true>(encrypted, output, chunk)
    }
    fn receive_inner<const COUNT: bool>(
        &mut self,
        encrypted: &[u8],
        output: &mut [u8],
        chunk: usize,
    ) -> Result<(usize, u64), Error> {
        if chunk == 0 {
            return Err("TLS component read chunk must be positive".into());
        }
        let mut written = 0usize;
        let mut reads = 0u64;
        for mut input in encrypted.chunks(chunk) {
            while !input.is_empty() {
                if COUNT {
                    reads = reads.checked_add(1).ok_or("TLS read counter overflow")?;
                }
                if self.0.read_tls(&mut input)? == 0 {
                    return Err("TLS component made no progress".into());
                }
                let available = self.0.process_new_packets()?.plaintext_bytes_to_read();
                let end = written
                    .checked_add(available)
                    .ok_or("TLS plaintext size overflow")?;
                let destination = output
                    .get_mut(written..end)
                    .ok_or("TLS component output capacity exceeded")?;
                self.0.reader().read_exact(destination)?;
                written = end;
            }
        }
        Ok((written, reads))
    }
}
/// Perform a real in-memory verified handshake, then encrypt each plaintext batch.
/// Preparation and trust generation belong outside measurement. At most 16 handshake
/// flights are permitted, and application output is bounded by caller input.
///
/// # Errors
/// Returns TLS, certificate, invalid fragment-size, or record-format errors.
pub fn prepare(plaintext: &[&[u8]], fragment_size: usize) -> Result<Prepared, Error> {
    prepare_peer(plaintext, fragment_size, TlsPeer::new()?, false)
}
/// Generate a valid TLS 1.3 key-update control before each application batch.
/// Keys remain only in memory; this is an offline control/ancestry fixture.
///
/// # Errors
/// Returns fixture handshake, unsupported TLS key-update, or input errors.
pub fn prepare_with_key_updates(
    plaintext: &[&[u8]],
    fragment_size: usize,
) -> Result<Prepared, Error> {
    prepare_peer(plaintext, fragment_size, TlsPeer::new()?, true)
}
/// Select one supported AWS-LC TLS 1.3 suite for this verified fixture only.
/// Certificate, key exchange and signature validation remain unchanged; this
/// helper does not install a process-global provider or configure public feeds.
///
/// # Errors
/// Rejects unsupported or TLS 1.2 suites and preparation/handshake failures.
pub fn prepare_with_cipher(
    plaintext: &[&[u8]],
    fragment_size: usize,
    cipher: rustls::CipherSuite,
) -> Result<Prepared, Error> {
    let prepared = prepare_peer(plaintext, fragment_size, peer_with_cipher(cipher)?, false)?;
    if prepared.cipher != format!("{cipher:?}") || prepared.version != "TLSv1_3" {
        return Err("TLS component negotiated an unexpected suite/version".into());
    }
    Ok(prepared)
}
fn peer_with_cipher(cipher: rustls::CipherSuite) -> Result<TlsPeer, Error> {
    let mut provider = rustls::crypto::aws_lc_rs::default_provider();
    provider
        .cipher_suites
        .retain(|suite| suite.suite() == cipher && suite.tls13().is_some());
    if provider.cipher_suites.len() != 1 {
        return Err("TLS component requires one supported AWS-LC TLS 1.3 suite".into());
    }
    TlsPeer::with_provider(Arc::new(provider))
}
fn prepare_peer(
    plaintext: &[&[u8]],
    fragment_size: usize,
    peer: TlsPeer,
    key_updates: bool,
) -> Result<Prepared, Error> {
    let mut server_config = (*peer.server).clone();
    server_config.max_fragment_size = Some(fragment_size);
    server_config.send_tls13_tickets = 0;
    let mut client =
        Connection::Client(ClientConnection::new(peer.client, "localhost".try_into()?)?);
    let mut server = Connection::Server(ServerConnection::new(Arc::new(server_config))?);
    for _ in 0..16 {
        transfer(&mut client, &mut server)?;
        transfer(&mut server, &mut client)?;
        if !client.is_handshaking()
            && !server.is_handshaking()
            && !client.wants_write()
            && !server.wants_write()
        {
            break;
        }
    }
    if client.is_handshaking()
        || server.is_handshaking()
        || client.wants_write()
        || server.wants_write()
    {
        return Err("TLS component handshake did not finish".into());
    }
    let cipher = format!(
        "{:?}",
        client
            .negotiated_cipher_suite()
            .ok_or("missing cipher suite")?
            .suite()
    );
    let version = format!(
        "{:?}",
        client.protocol_version().ok_or("missing TLS version")?
    );
    let mut batches = Vec::with_capacity(plaintext.len());
    for plain in plaintext {
        if key_updates {
            server.refresh_traffic_keys()?;
        }
        // Bounded by this known input batch, rather than disabling TLS backpressure.
        server.set_buffer_limit(Some(plain.len().saturating_mul(2).saturating_add(65536)));
        server.writer().write_all(plain)?;
        let mut ciphertext = Vec::new();
        while server.wants_write() {
            server.write_tls(&mut ciphertext)?;
        }
        let records = record_count(&ciphertext)?;
        batches.push(Batch {
            ciphertext,
            plaintext_bytes: plain.len(),
            records,
        });
    }
    Ok(Prepared {
        receiver: Receiver(client),
        batches,
        cipher,
        version,
    })
}
fn transfer(sender: &mut Connection, receiver: &mut Connection) -> Result<(), Error> {
    let mut bytes = Vec::new();
    while sender.wants_write() {
        sender.write_tls(&mut bytes)?;
    }
    let mut input = bytes.as_slice();
    while !input.is_empty() {
        if receiver.read_tls(&mut input)? == 0 {
            return Err("handshake read made no progress".into());
        }
        receiver.process_new_packets()?;
    }
    Ok(())
}
fn record_count(mut bytes: &[u8]) -> io::Result<usize> {
    let mut count = 0;
    while !bytes.is_empty() {
        let header = bytes
            .get(..5)
            .ok_or_else(|| io::Error::other("partial generated TLS header"))?;
        let size = usize::from(u16::from_be_bytes([header[3], header[4]]));
        bytes = bytes
            .get(5 + size..)
            .ok_or_else(|| io::Error::other("partial generated TLS record"))?;
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn repeats_use_matching_state_and_fragmented_reads_preserve_plaintext() {
        let long = vec![b'x'; 131073];
        let input: &[&[u8]] = &[b"first", &long, b"last"];
        for chunk in [1, 17, 1024, 131072] {
            let mut prepared = prepare(input, 1024).unwrap();
            assert!(prepared.batches[1].records > 1);
            for (batch, expected) in prepared.batches.iter().zip(input) {
                let mut out = vec![0; expected.len()];
                assert_eq!(
                    prepared
                        .receiver
                        .receive(&batch.ciphertext, &mut out, chunk)
                        .unwrap(),
                    expected.len()
                );
                assert_eq!(&out, expected);
            }
        }
    }
    #[test]
    fn corruption_and_reuse_of_an_old_record_are_rejected() {
        let mut prepared = prepare(&[b"authenticated data"], 16384).unwrap();
        let record = &prepared.batches[0].ciphertext;
        let mut out = [0; 18];
        prepared.receiver.receive(record, &mut out, 65536).unwrap();
        assert!(prepared.receiver.receive(record, &mut out, 65536).is_err());
        let mut prepared = prepare(&[b"authenticated data"], 16384).unwrap();
        let record = &mut prepared.batches[0].ciphertext;
        *record.last_mut().unwrap() ^= 1;
        assert!(prepared.receiver.receive(record, &mut out, 65536).is_err());
    }
    #[test]
    fn explicit_suites_keep_verified_partial_streams_and_global_provider() {
        let _default = TlsPeer::new().unwrap();
        let before = rustls::crypto::CryptoProvider::get_default()
            .unwrap()
            .clone();
        let long = vec![b'x'; 2049];
        let input: &[&[u8]] = &[b"first", &long, b"last"];
        for cipher in [
            rustls::CipherSuite::TLS13_AES_128_GCM_SHA256,
            rustls::CipherSuite::TLS13_AES_256_GCM_SHA384,
            rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256,
        ] {
            for chunk in [1, 17, 1024, 131072] {
                let mut prepared = prepare_with_cipher(input, 1024, cipher).unwrap();
                assert_eq!(prepared.cipher, format!("{cipher:?}"));
                assert_eq!(prepared.version, "TLSv1_3");
                assert!(prepared.batches[1].records > 1);
                for (batch, expected) in prepared.batches.iter().zip(input) {
                    let mut out = vec![0; expected.len()];
                    assert_eq!(
                        prepared
                            .receiver
                            .receive(&batch.ciphertext, &mut out, chunk)
                            .unwrap(),
                        expected.len()
                    );
                    assert_eq!(&out, expected);
                }
            }
            let mut prepared = prepare_with_cipher(&[b"data"], 1024, cipher).unwrap();
            let record = &prepared.batches[0].ciphertext;
            prepared.receiver.receive(record, &mut [0; 4], 17).unwrap();
            assert!(prepared.receiver.receive(record, &mut [0; 4], 17).is_err());
            let mut prepared = prepare_with_cipher(&[b"data"], 1024, cipher).unwrap();
            *prepared.batches[0].ciphertext.last_mut().unwrap() ^= 1;
            assert!(
                prepared
                    .receiver
                    .receive(&prepared.batches[0].ciphertext, &mut [0; 4], 17)
                    .is_err()
            );
        }
        assert!(Arc::ptr_eq(
            &before,
            rustls::crypto::CryptoProvider::get_default().unwrap()
        ));
    }
    #[test]
    fn unsupported_and_tls12_suites_do_not_substitute_the_default() {
        for cipher in [
            rustls::CipherSuite::TLS13_AES_128_CCM_SHA256,
            rustls::CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
        ] {
            assert!(prepare_with_cipher(&[b"data"], 1024, cipher).is_err());
        }
    }
    #[test]
    fn counted_component_reads_preserve_bytes_and_match_single_byte_input() {
        let plain = vec![b'x'; 2049];
        for chunk in [1, 17, 1472, 65536] {
            let mut prepared = prepare(&[&plain], 256).unwrap();
            let ciphertext = &prepared.batches[0].ciphertext;
            let mut output = vec![0; plain.len()];
            let (written, reads) = prepared
                .receiver
                .receive_counted(ciphertext, &mut output, chunk)
                .unwrap();
            assert_eq!(written, plain.len());
            assert_eq!(output, plain);
            assert!(reads > 0);
            if chunk == 1 {
                assert_eq!(reads as usize, ciphertext.len());
            }
        }
    }
}
