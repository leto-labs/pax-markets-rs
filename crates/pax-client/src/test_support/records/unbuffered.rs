//! Bounded, one-direction authenticated fixture for the pinned unbuffered API.
//! This is not a network adapter. Post-handshake control output and closure are
//! refused when exposed. Peer key updates can defer replies until a later write;
//! this fixture does not qualify response delivery or a full-duplex adapter.
//! The pinned API still owns plaintext vectors, so this is not zero-copy.
use super::{Batch, TlsPeer, peer_with_cipher, record_count};
use rustls::{ServerConnection, client::UnbufferedClientConnection, unbuffered::ConnectionState};
use std::{io::Write, sync::Arc};
type PreparationError = Box<dyn std::error::Error + Send + Sync>;
/// Fixed initialized ciphertext capacity; independent of total stream size.
pub const INPUT_BYTES: usize = 131_072;
const OUTPUT_BYTES: usize = 65_536;
const PROGRESS_LIMIT: usize = 65_536;

/// A rejected fixed-record component stream cannot be resumed.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// TLS authentication, trust or protocol failure.
    #[error("unbuffered TLS: {0}")]
    Tls(#[from] rustls::Error),
    /// The bounded fixture contract was exceeded or unsupported progress occurred.
    #[error("unbuffered TLS fixture: {0}")]
    State(&'static str),
    /// Handshake output exceeded the bounded initialized encoding storage.
    #[error("unbuffered TLS encoding: {0}")]
    Encode(#[from] rustls::unbuffered::EncodeError),
}
/// Receiver and fresh matching ciphertext; record sequence state is not cloneable.
pub struct Prepared {
    /// Verified receiver after handshake, with no pending ciphertext.
    pub receiver: Receiver,
    /// Ordered ciphertext; each batch is usable once at its matching position.
    pub batches: Vec<Batch>,
    /// Actual negotiated suite.
    pub cipher: String,
    /// Actual negotiated TLS version.
    pub version: String,
}
/// Bounded record fixture, available only through `test-support`.
pub struct Receiver {
    connection: UnbufferedClientConnection,
    input: Vec<u8>,
    used: usize,
    poisoned: bool,
}
impl Receiver {
    fn new(peer: &TlsPeer, name: &'static str) -> Result<Self, PreparationError> {
        Ok(Self {
            connection: UnbufferedClientConnection::new(peer.client.clone(), name.try_into()?)?,
            input: vec![0; INPUT_BYTES],
            used: 0,
            poisoned: false,
        })
    }
    fn append(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let end = self
            .used
            .checked_add(bytes.len())
            .ok_or(Error::State("input size overflow"))?;
        self.input
            .get_mut(self.used..end)
            .ok_or(Error::State("input capacity exceeded"))?
            .copy_from_slice(bytes);
        self.used = end;
        Ok(())
    }
    // Follow UnbufferedStatus::discard + AppDataRecord::discard before re-entry.
    // https://docs.rs/rustls/0.23.45/rustls/unbuffered/struct.UnbufferedStatus.html
    fn pump(
        &mut self,
        output: &mut [u8],
        mut handshake: Option<(&mut [u8], &mut Vec<u8>)>,
    ) -> Result<usize, Error> {
        let mut written = 0usize;
        for _ in 0..PROGRESS_LIMIT {
            let status = self
                .connection
                .process_tls_records(&mut self.input[..self.used]);
            let mut discard = status.discard;
            let mut blocked = false;
            match status.state? {
                ConnectionState::ReadTraffic(mut traffic) => {
                    while let Some(record) = traffic.next_record() {
                        let record = record?;
                        let end = written
                            .checked_add(record.payload.len())
                            .ok_or(Error::State("plaintext size overflow"))?;
                        output
                            .get_mut(written..end)
                            .ok_or(Error::State("output capacity exceeded"))?
                            .copy_from_slice(record.payload);
                        written = end;
                        discard = discard
                            .checked_add(record.discard)
                            .ok_or(Error::State("discard overflow"))?;
                    }
                }
                ConnectionState::EncodeTlsData(mut encode) => {
                    let (scratch, outgoing) = handshake
                        .as_mut()
                        .ok_or(Error::State("post-handshake control output unsupported"))?;
                    let size = encode.encode(scratch)?;
                    if outgoing
                        .len()
                        .checked_add(size)
                        .is_none_or(|n| n > OUTPUT_BYTES)
                    {
                        return Err(Error::State("handshake output capacity exceeded"));
                    }
                    outgoing.extend_from_slice(&scratch[..size]);
                }
                ConnectionState::TransmitTlsData(transmit) => {
                    if handshake.is_none() {
                        return Err(Error::State("post-handshake transmit unsupported"));
                    }
                    transmit.done();
                }
                ConnectionState::WriteTraffic(_) | ConnectionState::BlockedHandshake => {
                    blocked = true
                }
                ConnectionState::PeerClosed | ConnectionState::Closed => {
                    return Err(Error::State("peer closed fixed-record fixture"));
                }
                _ => return Err(Error::State("unsupported TLS state")),
            }
            if discard > self.used {
                return Err(Error::State("discard exceeds input"));
            }
            if discard != 0 {
                self.input.copy_within(discard..self.used, 0);
                self.used -= discard;
            }
            if blocked {
                return Ok(written);
            }
        }
        Err(Error::State("bounded progress exhausted"))
    }
    /// Authenticate chunks and copy available plaintext into initialized output.
    /// Partial records remain in bounded input across calls. Handshake, encryption
    /// and output allocation are excluded by the benchmark; internal TLS buffering,
    /// authentication, input compaction and plaintext copying are included.
    ///
    /// # Errors
    /// Rejects zero chunks, exceeded capacities, invalid/reused ciphertext, control
    /// output and closed/unsupported state. Any failure poisons this fixture.
    pub fn receive(
        &mut self,
        encrypted: &[u8],
        output: &mut [u8],
        chunk: usize,
    ) -> Result<usize, Error> {
        if self.poisoned {
            return Err(Error::State("previously rejected stream"));
        }
        let result = self.receive_inner(encrypted, output, chunk);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn receive_inner(
        &mut self,
        encrypted: &[u8],
        output: &mut [u8],
        chunk: usize,
    ) -> Result<usize, Error> {
        if chunk == 0 {
            return Err(Error::State("read chunk must be positive"));
        }
        let mut written = 0usize;
        for part in encrypted.chunks(chunk) {
            self.append(part)?;
            let size = self.pump(&mut output[written..], None)?;
            written = written
                .checked_add(size)
                .ok_or(Error::State("plaintext total overflow"))?;
        }
        Ok(written)
    }
    /// Check the complete fixed-record endpoint without implying a network EOF.
    ///
    /// # Errors
    /// Rejects any prior failure or an incomplete final ciphertext record.
    pub fn finish(&self) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::State("previously rejected stream"));
        }
        if self.used != 0 {
            return Err(Error::State("incomplete final ciphertext"));
        }
        Ok(())
    }
}
/// Prepare fresh, verified default-provider state, outside component timing.
///
/// # Errors
/// Rejects trust, TLS, fragment bounds, handshake progress and record-format errors.
pub fn prepare(plaintext: &[&[u8]], fragment_size: usize) -> Result<Prepared, PreparationError> {
    prepare_peer(plaintext, fragment_size, TlsPeer::new()?, "localhost")
}
/// Prepare one explicitly supported fixture-local AWS-LC TLS 1.3 suite.
///
/// # Errors
/// Also rejects unsupported suites or an unexpected negotiated suite/version.
pub fn prepare_with_cipher(
    plaintext: &[&[u8]],
    fragment_size: usize,
    cipher: rustls::CipherSuite,
) -> Result<Prepared, PreparationError> {
    let prepared = prepare_peer(
        plaintext,
        fragment_size,
        peer_with_cipher(cipher)?,
        "localhost",
    )?;
    if prepared.cipher != format!("{cipher:?}") || prepared.version != "TLSv1_3" {
        return Err("TLS component negotiated an unexpected suite/version".into());
    }
    Ok(prepared)
}
fn handshake(
    receiver: &mut Receiver,
    server: &mut ServerConnection,
) -> Result<(), PreparationError> {
    let mut scratch = vec![0; OUTPUT_BYTES];
    for _ in 0..16 {
        let mut outgoing = Vec::new();
        if receiver.pump(&mut [], Some((&mut scratch, &mut outgoing)))? != 0 {
            return Err("unexpected handshake application data".into());
        }
        let mut input = outgoing.as_slice();
        while !input.is_empty() {
            if server.read_tls(&mut input)? == 0 {
                return Err("handshake read made no progress".into());
            }
            server.process_new_packets()?;
        }
        let mut incoming = Vec::new();
        while server.wants_write() {
            server.write_tls(&mut incoming)?;
        }
        receiver.append(&incoming)?;
        if !receiver.connection.is_handshaking()
            && !server.is_handshaking()
            && outgoing.is_empty()
            && incoming.is_empty()
        {
            receiver.finish()?;
            return Ok(());
        }
    }
    Err("TLS component handshake did not finish".into())
}
fn prepare_peer(
    plaintext: &[&[u8]],
    fragment_size: usize,
    peer: TlsPeer,
    name: &'static str,
) -> Result<Prepared, PreparationError> {
    let mut config = (*peer.server).clone();
    config.max_fragment_size = Some(fragment_size);
    config.send_tls13_tickets = 0;
    let mut server = ServerConnection::new(Arc::new(config))?;
    let mut receiver = Receiver::new(&peer, name)?;
    handshake(&mut receiver, &mut server)?;
    let cipher = format!(
        "{:?}",
        receiver
            .connection
            .negotiated_cipher_suite()
            .ok_or("missing cipher suite")?
            .suite()
    );
    let version = format!(
        "{:?}",
        receiver
            .connection
            .protocol_version()
            .ok_or("missing TLS version")?
    );
    let mut batches = Vec::with_capacity(plaintext.len());
    for plain in plaintext {
        server.set_buffer_limit(Some(plain.len().saturating_mul(2).saturating_add(65536)));
        server.writer().write_all(plain)?;
        let mut ciphertext = Vec::new();
        while server.wants_write() {
            server.write_tls(&mut ciphertext)?;
        }
        batches.push(Batch {
            records: record_count(&ciphertext)?,
            ciphertext,
            plaintext_bytes: plain.len(),
        });
    }
    Ok(Prepared {
        receiver,
        batches,
        cipher,
        version,
    })
}

#[cfg(test)]
mod tests;
