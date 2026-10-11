//! Differential qualification through Stream, which StreamOwned delegates to.
//! Wire completion remains untrusted until an upper read exposes plaintext.
use super::*;
use std::io::Write;

struct Wire {
    data: Vec<u8>,
    at: usize,
    available: usize,
    chunk: usize,
    state: wire::Limiter,
    complete: Vec<(u64, u64, u64)>,
    writes: Vec<u8>,
    block_writes: bool,
    write_chunk: usize,
}
impl Wire {
    fn new(data: Vec<u8>, chunk: usize) -> Self {
        Self {
            data,
            at: 0,
            available: 0,
            chunk,
            state: wire::Limiter::default(),
            complete: Vec::with_capacity(4096),
            writes: Vec::with_capacity(4096),
            block_writes: false,
            write_chunk: 4096,
        }
    }
}
impl Read for Wire {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.at == self.available {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let mut input = &self.data[self.at..self.available];
        let limit = out.len().min(self.chunk);
        let before = self.state.completed();
        let read = self.state.read(&mut input, &mut out[..limit])?;
        self.at += read;
        if let Some(record) = self
            .state
            .completed()
            .filter(|record| Some(*record) != before)
        {
            if self.complete.len() == 4096 {
                return Err(io::ErrorKind::OutOfMemory.into());
            }
            self.complete
                .push((record.ordinal, record.start, record.end));
        }
        Ok(read)
    }
}
impl Write for Wire {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.block_writes {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let bytes = &bytes[..bytes.len().min(self.write_chunk)];
        if self.writes.len() + bytes.len() > 4096 {
            return Err(io::ErrorKind::OutOfMemory.into());
        }
        self.writes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn raw(input: &[&[u8]], fragment: usize, keys: bool) -> super::super::Prepared {
    if keys {
        super::super::prepare_with_key_updates(input, fragment).unwrap()
    } else {
        super::super::prepare(input, fragment).unwrap()
    }
}
fn reference(input: &[&[u8]], fragment: usize, keys: bool) -> Vec<Record> {
    let mut fixture = if keys {
        prepare_with_key_updates(input, fragment, 4096).unwrap()
    } else {
        prepare(input, fragment, 4096).unwrap()
    };
    for batch in &fixture.batches {
        let mut output = vec![0; batch.plaintext_bytes];
        fixture
            .receiver
            .receive(&batch.ciphertext, &mut output, 17)
            .unwrap();
    }
    fixture.receiver.finish().unwrap();
    fixture.receiver.records().copied().collect()
}
fn client(prepared: super::super::Prepared, chunk: usize) -> (rustls::ClientConnection, Wire) {
    let rustls::Connection::Client(connection) = prepared.receiver.0 else {
        panic!("verified fixture must prepare a client connection")
    };
    let data = prepared
        .batches
        .into_iter()
        .flat_map(|batch| batch.ciphertext)
        .collect();
    (connection, Wire::new(data, chunk))
}

#[test]
fn stream_preserves_record_origins_through_partial_input_cached_plaintext_and_controls() {
    let long = vec![b'p'; 2049];
    let input: &[&[u8]] = &[b"first", &long, b"last"];
    let expected_plain = input.concat();
    let mut cached_reads = 0;
    let mut pending_reads = 0;
    for keys in [false, true] {
        for fragment in [64, 256, 16384] {
            let expected = reference(input, fragment, keys);
            for (gate, chunk, destination) in [
                (1, 17, 1),
                (17, 1, 17),
                (1472, 17, 4096),
                (65536, 65536, 17),
            ] {
                let (mut connection, mut wire) = client(raw(input, fragment, keys), chunk);
                let mut plain = Vec::with_capacity(expected_plain.len());
                let mut iterations = 0;
                while plain.len() < expected_plain.len() {
                    wire.available = (wire.available + gate).min(wire.data.len());
                    loop {
                        iterations += 1;
                        assert!(
                            iterations < 100_000,
                            "finite stream qualification must make progress"
                        );
                        let before = wire.at;
                        let mut out = vec![0; destination.min(expected_plain.len() - plain.len())];
                        let result = rustls::Stream::new(&mut connection, &mut wire).read(&mut out);
                        match result {
                            Ok(read) => {
                                assert!(read > 0);
                                let at = plain.len() as u64;
                                let record = expected
                                    .iter()
                                    .find(|r| r.plain_start <= at && at < r.plain_end)
                                    .unwrap();
                                assert!(
                                    at + read as u64 <= record.plain_end,
                                    "one upper read must not cross app records"
                                );
                                assert_eq!(
                                    wire.complete.last(),
                                    Some(&(record.ordinal, record.cipher_start, record.cipher_end))
                                );
                                if wire.at == before {
                                    cached_reads += 1;
                                }
                                plain.extend_from_slice(&out[..read]);
                                if plain.len() == expected_plain.len() {
                                    break;
                                }
                            }
                            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                                pending_reads += 1;
                                assert!(
                                    wire.available < wire.data.len(),
                                    "complete fixture may not lose plaintext"
                                );
                                break;
                            }
                            Err(error) => panic!("verified stream failed: {error}"),
                        }
                    }
                }
                assert_eq!(plain, expected_plain);
                assert_eq!(wire.at, wire.data.len());
                assert_eq!(
                    wire.complete,
                    expected
                        .iter()
                        .map(|r| (r.ordinal, r.cipher_start, r.cipher_end))
                        .collect::<Vec<_>>()
                );
                rustls::Stream::new(&mut connection, &mut wire)
                    .flush()
                    .unwrap();
                // Reply bytes remain only in memory. No peer delivery is claimed.
                assert!(wire.writes.len() <= 4096);
            }
        }
    }
    assert!(cached_reads > 0 && pending_reads > 0);
}

#[test]
fn empty_plaintext_read_can_prefetch_without_publishing_an_application_mapping() {
    let (mut connection, mut wire) = client(raw(&[b"buffered"], 256, false), 65536);
    wire.available = wire.data.len();
    assert_eq!(
        rustls::Stream::new(&mut connection, &mut wire)
            .read(&mut [])
            .unwrap(),
        0
    );
    assert_eq!(wire.complete.len(), 1);
    let cipher_end = wire.at;
    let mut output = [0; 8];
    assert_eq!(
        rustls::Stream::new(&mut connection, &mut wire)
            .read(&mut output)
            .unwrap(),
        8
    );
    assert_eq!(&output, b"buffered");
    assert_eq!(wire.at, cipher_end);
    assert_eq!(wire.complete.len(), 1);
}

#[test]
fn a_corrupt_wire_completion_never_becomes_a_successful_plaintext_origin() {
    let input: &[&[u8]] = &[b"first", b"last"];
    let expected = reference(input, 256, false);
    let (mut connection, mut wire) = client(raw(input, 256, false), 65536);
    *wire.data.last_mut().unwrap() ^= 1;
    wire.available = wire.data.len();
    let mut output = [0; 16];
    assert_eq!(
        rustls::Stream::new(&mut connection, &mut wire)
            .read(&mut output)
            .unwrap(),
        5
    );
    assert_eq!(&output[..5], b"first");
    assert_eq!(
        wire.complete.last(),
        Some(&(1, expected[0].cipher_start, expected[0].cipher_end))
    );
    assert!(
        rustls::Stream::new(&mut connection, &mut wire)
            .read(&mut output)
            .is_err()
    );
    // The ciphertext boundary exists, but successful upper reads exposed only first.
    assert_eq!(wire.complete.len(), 2);
    assert!(
        rustls::Stream::new(&mut connection, &mut wire)
            .read(&mut output)
            .is_err()
    );
}

#[test]
fn blocked_and_partial_prior_tls_writes_preserve_inbound_record_identity() {
    let (mut connection, mut wire) = client(raw(&[b"inbound"], 256, true), 17);
    wire.available = wire.data.len();
    connection.writer().write_all(b"outbound fixture").unwrap();
    assert!(connection.wants_write());
    wire.block_writes = true;
    let mut output = [0; 7];
    // complete_io can make inbound progress while prior writes remain blocked.
    // The upper operation succeeds without confusing outbound and inbound state.
    assert_eq!(
        rustls::Stream::new(&mut connection, &mut wire)
            .read(&mut output)
            .unwrap(),
        7
    );
    assert_eq!(&output, b"inbound");
    assert!(connection.wants_write());
    assert!(wire.writes.is_empty());
    assert_eq!(wire.complete.len(), 2);
    assert_eq!(wire.complete.last().unwrap().0, 2);
    let cipher_end = wire.at;

    wire.block_writes = false;
    wire.write_chunk = 3;
    rustls::Stream::new(&mut connection, &mut wire)
        .flush()
        .unwrap();
    assert!(!wire.writes.is_empty());
    assert!(!connection.wants_write());
    assert_eq!(cipher_end, wire.data.len());
    assert_eq!(wire.at, cipher_end);
    assert_eq!(wire.complete.len(), 2);
    // Outbound bytes prove attempted stream writes, not delivery to a peer.
}

#[test]
fn outbound_stream_operations_can_prefetch_the_later_plaintext_origin() {
    let (mut connection, mut wire) = client(raw(&[b"prefetched"], 256, false), 65536);
    wire.available = wire.data.len();
    wire.block_writes = true;
    // With writes blocked, complete_io can make inbound progress. Two writes
    // prefetch the header and payload before any upper plaintext read occurs.
    rustls::Stream::new(&mut connection, &mut wire)
        .write_all(b"one")
        .unwrap();
    rustls::Stream::new(&mut connection, &mut wire)
        .write_all(b"two")
        .unwrap();
    assert_eq!(
        connection.reader().into_first_chunk().unwrap(),
        b"prefetched"
    );
    assert_eq!(wire.complete.len(), 1);
    let before = wire.at;
    wire.block_writes = false;
    let mut output = [0; 10];
    assert_eq!(
        rustls::Stream::new(&mut connection, &mut wire)
            .read(&mut output)
            .unwrap(),
        10
    );
    assert_eq!(&output, b"prefetched");
    assert_eq!(wire.at, before);
    assert_eq!(wire.complete.last().unwrap().0, 1);
}
