//! Safe wire framing shared by offline and native diagnostics.
//! This bounds reads; rustls remains the TLS validator and authenticator.
use super::{Limited, State};
use std::io::{self, Read, Write};

/// Untrusted completed wire interval, relative to limiter construction.
/// It includes the five-byte header and may be a handshake or control record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Record {
    /// One-based completed wire-record ordinal.
    pub ordinal: u64,
    /// Inclusive wire offset, including the header.
    pub start: u64,
    /// Exclusive wire offset.
    pub end: u64,
}

/// Allocation-free checked record state for an exclusively owned lower input.
/// Every successful lower read must pass through this limiter from construction.
/// Do not reuse it for another connection or read around it. It retains only the
/// latest completion, which is not evidence of authenticated application data.
#[derive(Default)]
pub struct Limiter {
    state: State,
    completed: Option<Record>,
    failed: bool,
}
impl Limiter {
    /// Limit one input call to a partial header or the remaining record payload.
    /// Empty output does not call input. WouldBlock/Interrupted preserve state.
    ///
    /// # Errors
    /// Forwards input errors. Other errors make this mutable state terminal;
    /// invalid lengths or overflowing offsets return InvalidData.
    pub fn read<R: Read>(&mut self, input: &mut R, output: &mut [u8]) -> io::Result<usize> {
        if self.failed {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let result = Limited {
            input,
            state: &mut self.state,
        }
        .read(output);
        match result {
            Ok(read) => {
                if let Some((ordinal, start, end)) = self.state.complete.take() {
                    self.completed = Some(Record {
                        ordinal,
                        start,
                        end,
                    });
                }
                Ok(read)
            }
            Err(error) => {
                if !matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) {
                    self.failed = true;
                }
                Err(error)
            }
        }
    }
    /// Successful wire bytes since construction, never a TCP sequence number.
    pub fn offset(&self) -> u64 {
        self.state.offset
    }
    /// Latest untrusted completion, unavailable after invalid mutable state.
    pub fn completed(&self) -> Option<Record> {
        (!self.failed).then_some(self.completed).flatten()
    }
    /// Verify final input ended between records, without authenticating it.
    ///
    /// # Errors
    /// Returns InvalidData after terminal failure or UnexpectedEof for a partial
    /// header/body. This read-only check does not reset or advance state.
    pub fn finish(&self) -> io::Result<()> {
        if self.failed {
            Err(io::ErrorKind::InvalidData.into())
        } else if self.state.header_used != 0 {
            Err(io::ErrorKind::UnexpectedEof.into())
        } else {
            Ok(())
        }
    }
}

/// Owned record-limited input with unchanged write forwarding.
/// No mutable lower-input access is exposed; consuming it ends attribution.
pub struct Input<R> {
    inner: R,
    limiter: Limiter,
}
impl<R> Input<R> {
    /// Start at byte zero on a fresh, exclusively owned stream input.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            limiter: Limiter::default(),
        }
    }
    /// Inspect the lower input without permitting a bypass read.
    pub fn get_ref(&self) -> &R {
        &self.inner
    }
    /// End attribution and consume the wrapper.
    pub fn into_inner(self) -> R {
        self.inner
    }
    /// Successful ciphertext bytes since construction.
    pub fn offset(&self) -> u64 {
        self.limiter.offset()
    }
    /// Latest completed untrusted wire interval.
    pub fn completed(&self) -> Option<Record> {
        self.limiter.completed()
    }
    /// Check final wire completeness. Does not authenticate a record.
    ///
    /// # Errors
    /// Returns terminal framing failure or UnexpectedEof for partial input.
    pub fn finish(&self) -> io::Result<()> {
        self.limiter.finish()
    }
}
impl<R: Read> Read for Input<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.limiter.read(&mut self.inner, output)
    }
}
impl<R: Write> Write for Input<R> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.inner.write(input)
    }
    fn write_vectored(&mut self, input: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.inner.write_vectored(input)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests;
