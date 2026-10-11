//! Intrusive offline record intervals; ordinary TLS reads remain unchanged.
//!
//! The public rustls IoState describes queued plaintext, not its record origin.
//! Limit input to one record boundary and drain each result to make that relation
//! explicit. Offsets start after the prepared fixture handshake, not at TCP SYN.
use super::{Batch, Error};
use std::{
    collections::VecDeque,
    io::{self, Read},
};

pub mod wire;

/// One successfully processed wire record and its exposed plaintext interval.
/// An empty plaintext interval can represent a TLS control record. No decrypted
/// inner content type, timestamp or hardware origin is inferred.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Record {
    /// One-based record ordinal in this fixture's post-handshake input.
    pub ordinal: u64,
    /// Inclusive ciphertext start, including its five-byte wire header.
    pub cipher_start: u64,
    /// Exclusive ciphertext end.
    pub cipher_end: u64,
    /// Inclusive exposed plaintext start.
    pub plain_start: u64,
    /// Exclusive exposed plaintext end.
    pub plain_end: u64,
}

/// Exact operation counts; these are component Read calls, not socket syscalls.
#[derive(Clone, Copy, Debug, Default)]
pub struct Counts {
    /// Calls into rustls read_tls, including partial headers and bodies.
    pub reads: u64,
    /// Ciphertext bytes accepted.
    pub cipher_bytes: u64,
    /// Plaintext bytes exposed.
    pub plain_bytes: u64,
    /// Complete records successfully processed by rustls.
    pub records: u64,
    /// Records omitted at zero capacity or evicted from bounded retention.
    pub lost: u64,
}

/// Fixture converted to a distinct evolving receiver; no ordinary method can
/// insert untracked bytes into its record state.
pub struct Prepared {
    /// Record-bounded receiver positioned after the verified handshake.
    pub receiver: Receiver,
    /// Same generated ciphertext and expected plaintext as the original fixture.
    pub batches: Vec<Batch>,
    /// Negotiated cipher name.
    pub cipher: String,
    /// Negotiated TLS version.
    pub version: String,
}

/// Prepare a fresh verified fixture with bounded record intervals.
///
/// # Errors
/// Returns fixture/trust/input errors and rejects capacities above 4,096.
/// Zero explicitly omits metadata. No already-advanced receiver can be converted.
pub fn prepare(plaintext: &[&[u8]], fragment: usize, capacity: usize) -> Result<Prepared, Error> {
    observe(super::prepare(plaintext, fragment)?, capacity)
}
/// Prepare fresh verified sequence state with one supported TLS 1.3 suite.
///
/// # Errors
/// Returns unsupported-suite, fixture/trust/input and retention-capacity errors.
pub fn prepare_with_cipher(
    plaintext: &[&[u8]],
    fragment: usize,
    cipher: rustls::CipherSuite,
    capacity: usize,
) -> Result<Prepared, Error> {
    observe(
        super::prepare_with_cipher(plaintext, fragment, cipher)?,
        capacity,
    )
}
/// Prepare valid encrypted key-update controls before each application batch.
/// Successful decryption does not qualify delivery of deferred control replies.
///
/// # Errors
/// Returns fixture/trust/control/input and retention-capacity errors.
pub fn prepare_with_key_updates(
    plaintext: &[&[u8]],
    fragment: usize,
    capacity: usize,
) -> Result<Prepared, Error> {
    observe(
        super::prepare_with_key_updates(plaintext, fragment)?,
        capacity,
    )
}
// Only private fresh preparation and module tests can convert the ordinary fixture.
// Its public evolving receiver cannot prove that sequence state is still unused.
fn observe(prepared: super::Prepared, capacity: usize) -> Result<Prepared, Error> {
    if capacity > 4096 {
        return Err("record retention capacity exceeds 4096".into());
    }
    Ok(Prepared {
        receiver: Receiver {
            connection: prepared.receiver.0,
            state: State::default(),
            counts: Counts::default(),
            records: VecDeque::with_capacity(capacity),
            capacity,
            failed: false,
        },
        batches: prepared.batches,
        cipher: prepared.cipher,
        version: prepared.version,
    })
}

/// Bounded diagnostic state; no ciphertext or plaintext payload is retained here.
pub struct Receiver {
    connection: rustls::Connection,
    state: State,
    counts: Counts,
    records: VecDeque<Record>,
    capacity: usize,
    failed: bool,
}
impl Receiver {
    /// Cumulative component work and explicit retention loss.
    pub fn counts(&self) -> Counts {
        self.counts
    }
    /// Recent processed record intervals in stream order; earlier loss is counted.
    pub fn records(&self) -> impl ExactSizeIterator<Item = &Record> {
        self.records.iter()
    }
    /// Ciphertext has an incomplete wire header or payload across input calls.
    pub fn has_partial_record(&self) -> bool {
        self.state.offset != self.state.start
    }
    /// Check that all supplied fixture input ended at a complete record boundary.
    /// This does not prove close-notify delivery or a finished network session.
    ///
    /// # Errors
    /// Rejects previously failed state and a truncated final wire record.
    pub fn finish(&self) -> Result<(), Error> {
        if self.failed {
            return Err("record-bounded receiver failed previously".into());
        }
        if self.has_partial_record() {
            return Err("record-bounded input ended in a partial record".into());
        }
        Ok(())
    }
    /// Feed partial or complete ciphertext and drain exposed plaintext into output.
    /// Record state persists across calls; `chunk` only bounds simulated input reads.
    /// Extra read calls and record bookkeeping are intrusive diagnostic costs.
    ///
    /// # Errors
    /// Rejects zero chunks, unavailable output, overflow, lack of input progress,
    /// authentication/protocol failures and use after a mutating error. A failure
    /// does not publish that record and permanently invalidates this receiver.
    pub fn receive(
        &mut self,
        encrypted: &[u8],
        output: &mut [u8],
        chunk: usize,
    ) -> Result<usize, Error> {
        if self.failed {
            return Err("record-bounded receiver failed previously".into());
        }
        if chunk == 0 {
            return Err("record input chunk must be positive".into());
        }
        let result = self.receive_inner(encrypted, output, chunk);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn receive_inner(
        &mut self,
        encrypted: &[u8],
        output: &mut [u8],
        chunk: usize,
    ) -> Result<usize, Error> {
        let mut written = 0usize;
        for mut bytes in encrypted.chunks(chunk) {
            while !bytes.is_empty() {
                let mut input = Limited {
                    input: &mut bytes,
                    state: &mut self.state,
                };
                self.counts.reads = self
                    .counts
                    .reads
                    .checked_add(1)
                    .ok_or("record read counter overflow")?;
                let received = self.connection.read_tls(&mut input)?;
                if received == 0 {
                    return Err("record input made no progress".into());
                }
                self.counts.cipher_bytes = self.state.offset;
                let available = self
                    .connection
                    .process_new_packets()?
                    .plaintext_bytes_to_read();
                let complete = self.state.complete.take();
                if available != 0 && complete.is_none() {
                    return Err("plaintext exposed without a completing record".into());
                }
                let end = written
                    .checked_add(available)
                    .ok_or("record plaintext size overflow")?;
                self.connection.reader().read_exact(
                    output
                        .get_mut(written..end)
                        .ok_or("record plaintext output capacity exceeded")?,
                )?;
                written = end;
                if let Some((ordinal, cipher_start, cipher_end)) = complete {
                    let plain_start = self.counts.plain_bytes;
                    let plain_end = plain_start
                        .checked_add(available.try_into()?)
                        .ok_or("record plaintext counter overflow")?;
                    self.counts.plain_bytes = plain_end;
                    self.counts.records = self
                        .counts
                        .records
                        .checked_add(1)
                        .ok_or("record counter overflow")?;
                    if self.records.len() == self.capacity {
                        self.records.pop_front();
                        self.counts.lost = self
                            .counts
                            .lost
                            .checked_add(1)
                            .ok_or("record loss counter overflow")?;
                    }
                    if self.capacity != 0 {
                        self.records.push_back(Record {
                            ordinal,
                            cipher_start,
                            cipher_end,
                            plain_start,
                            plain_end,
                        });
                    }
                }
            }
        }
        Ok(written)
    }
}

#[derive(Default)]
struct State {
    header: [u8; 5],
    header_used: usize,
    remaining: usize,
    offset: u64,
    start: u64,
    ordinal: u64,
    complete: Option<(u64, u64, u64)>,
}
struct Limited<'a, R> {
    input: R,
    state: &'a mut State,
}
impl<R: Read> Read for Limited<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || self.state.complete.is_some() {
            return Ok(0);
        }
        let header = self.state.header_used < 5;
        let limit = output.len().min(if header {
            5 - self.state.header_used
        } else {
            self.state.remaining
        });
        let read = self.input.read(&mut output[..limit])?;
        if read > limit {
            return Err(io::ErrorKind::InvalidData.into());
        }
        if read == 0 {
            return Ok(0);
        }
        self.state.offset = self
            .state
            .offset
            .checked_add(read as u64)
            .ok_or(io::ErrorKind::InvalidData)?;
        if header {
            let used = self.state.header_used;
            self.state.header[used..used + read].copy_from_slice(&output[..read]);
            self.state.header_used += read;
            if self.state.header_used == 5 {
                // Bound only by the encoded wire length. Rustls validates type,
                // version, size, padding and authentication; this is not a TLS validator.
                self.state.remaining = usize::from(u16::from_be_bytes([
                    self.state.header[3],
                    self.state.header[4],
                ]));
            }
        } else {
            self.state.remaining -= read;
        }
        if self.state.header_used == 5 && self.state.remaining == 0 {
            self.state.ordinal = self
                .state
                .ordinal
                .checked_add(1)
                .ok_or(io::ErrorKind::InvalidData)?;
            self.state.complete = Some((self.state.ordinal, self.state.start, self.state.offset));
            self.state.start = self.state.offset;
            self.state.header_used = 0;
        }
        Ok(read)
    }
}

#[cfg(test)]
#[path = "bounded/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "bounded/stream_tests.rs"]
mod stream_tests;
