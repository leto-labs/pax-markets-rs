//! Select the fixture outside timing; each inner receive loop is monomorphized.
#[path = "tls/ancestry.rs"]
mod ancestry;
use clap::ValueEnum;
use pax_client::test_support::records;
use pax_lab::Error;
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, ValueEnum, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Api {
    Buffered,
    Unbuffered,
    RecordBounded,
}
#[derive(Clone, Copy, Debug, ValueEnum, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Cipher {
    Default,
    Aes128,
    Aes256,
    Chacha20,
}
impl Cipher {
    fn suite(self) -> Option<tokio_rustls::rustls::CipherSuite> {
        use tokio_rustls::rustls::CipherSuite;
        match self {
            Self::Default => None,
            Self::Aes128 => Some(CipherSuite::TLS13_AES_128_GCM_SHA256),
            Self::Aes256 => Some(CipherSuite::TLS13_AES_256_GCM_SHA384),
            Self::Chacha20 => Some(CipherSuite::TLS13_CHACHA20_POLY1305_SHA256),
        }
    }
}
pub enum Prepared {
    Buffered(records::Prepared),
    Unbuffered(records::unbuffered::Prepared),
    RecordBounded {
        prepared: records::bounded::Prepared,
        capacity: usize,
    },
}
impl Prepared {
    pub fn new(
        api: Api,
        cipher: Cipher,
        plaintext: &[&[u8]],
        fragment: usize,
        capacity: usize,
    ) -> Result<Self, Error> {
        Ok(match api {
            Api::Buffered => Self::Buffered(buffered(cipher, plaintext, fragment)?),
            Api::Unbuffered => Self::Unbuffered(match cipher.suite() {
                Some(suite) => {
                    records::unbuffered::prepare_with_cipher(plaintext, fragment, suite)?
                }
                None => records::unbuffered::prepare(plaintext, fragment)?,
            }),
            Api::RecordBounded => Self::RecordBounded {
                prepared: match cipher.suite() {
                    Some(suite) => {
                        records::bounded::prepare_with_cipher(plaintext, fragment, suite, capacity)?
                    }
                    None => records::bounded::prepare(plaintext, fragment, capacity)?,
                },
                capacity,
            },
        })
    }
    pub fn metadata(&self) -> (&str, &str, &[records::Batch]) {
        match self {
            Self::Buffered(p) => (&p.cipher, &p.version, &p.batches),
            Self::Unbuffered(p) => (&p.cipher, &p.version, &p.batches),
            Self::RecordBounded { prepared: p, .. } => (&p.cipher, &p.version, &p.batches),
        }
    }
    /// Validate suffix intervals and measure ordinary read counts outside timing.
    pub fn ancestry(
        &self,
        cipher: Cipher,
        plaintext: &[&[u8]],
        fragment: usize,
        chunk: usize,
    ) -> Result<Option<serde_json::Value>, Error> {
        match self {
            Self::RecordBounded { prepared, capacity } => Ok(Some(ancestry::report(
                prepared,
                *capacity,
                buffered(cipher, plaintext, fragment)?,
                plaintext,
                chunk,
            )?)),
            _ => Ok(None),
        }
    }
    pub fn measure(
        &mut self,
        decrypted: &mut [Vec<u8>],
        chunk: usize,
        samples: &mut Vec<Duration>,
    ) -> Result<(), Error> {
        match self {
            Self::Buffered(p) => measure(
                &p.batches,
                decrypted,
                chunk,
                samples,
                |input, out, chunk| p.receiver.receive(input, out, chunk),
            ),
            Self::Unbuffered(p) => {
                measure(
                    &p.batches,
                    decrypted,
                    chunk,
                    samples,
                    |input, out, chunk| Ok(p.receiver.receive(input, out, chunk)?),
                )?;
                p.receiver.finish()?;
                Ok(())
            }
            Self::RecordBounded { prepared: p, .. } => {
                measure(
                    &p.batches,
                    decrypted,
                    chunk,
                    samples,
                    |input, out, chunk| p.receiver.receive(input, out, chunk),
                )?;
                p.receiver.finish()?;
                Ok(())
            }
        }
    }
}
fn buffered(
    cipher: Cipher,
    plaintext: &[&[u8]],
    fragment: usize,
) -> Result<records::Prepared, Error> {
    match cipher.suite() {
        Some(suite) => records::prepare_with_cipher(plaintext, fragment, suite),
        None => records::prepare(plaintext, fragment),
    }
}
fn measure(
    batches: &[records::Batch],
    outputs: &mut [Vec<u8>],
    chunk: usize,
    samples: &mut Vec<Duration>,
    mut receive: impl FnMut(&[u8], &mut [u8], usize) -> Result<usize, Error>,
) -> Result<(), Error> {
    assert_eq!(batches.len(), outputs.len());
    for (batch, output) in batches.iter().zip(outputs) {
        let start = Instant::now();
        let size = receive(black_box(&batch.ciphertext), output, chunk)?;
        samples.push(start.elapsed());
        assert_eq!(size, batch.plaintext_bytes);
    }
    Ok(())
}
