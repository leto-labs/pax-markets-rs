//! Validate an entire journal before exposing records as a complete replay.
use crate::capture::{CaptureError, MAGIC, Manifest, RECORD_HEADER, Record, RecordKind};
use bytes::Bytes;
use std::{
    fs::File,
    io::{BufReader, Read, Seek},
    path::Path,
};

/// Verified capture reader. No network access or live API is required.
pub struct Replay {
    manifest: Manifest,
    reader: BufReader<File>,
    consumed_hash: blake3::Hasher,
    consumed_records: u64,
    finished: bool,
}
impl Replay {
    /// Check the manifest, digest, framing, counts, metadata, and monotonic offsets.
    /// The caller sets a total input bound; each record is capped at 64 MiB.
    pub fn open(directory: &Path, max_bytes: u64) -> Result<Self, CaptureError> {
        let manifest_path = directory.join("manifest.json");
        if std::fs::metadata(&manifest_path)?.len() > 1024 * 1024 {
            return Err(CaptureError::Invalid("manifest too large"));
        }
        let mut manifest_bytes = Vec::new();
        File::open(manifest_path)?
            .take(1024 * 1024 + 1)
            .read_to_end(&mut manifest_bytes)?;
        if manifest_bytes.len() > 1024 * 1024 {
            return Err(CaptureError::Invalid("manifest too large"));
        }
        // Ignored JSON fields must not bypass the manifest's UTF-8 contract.
        let text = std::str::from_utf8(&manifest_bytes)
            .map_err(|_| CaptureError::Invalid("manifest is not valid UTF-8"))?;
        let manifest: Manifest = serde_json::from_str(text)?;
        if manifest.version != 1 || !manifest.complete {
            return Err(CaptureError::Invalid("unsupported or incomplete manifest"));
        }
        manifest.metadata.validate()?;
        if manifest.markets.is_empty() {
            return Err(CaptureError::Invalid("no selected markets"));
        }
        let mut unique = manifest.markets.clone();
        unique.sort_unstable();
        unique.dedup();
        if unique.len() != manifest.markets.len() {
            return Err(CaptureError::Invalid("duplicate selected market"));
        }
        for id in &manifest.markets {
            manifest.metadata.resolve(*id)?;
        }
        if manifest.max_record_bytes == 0 || manifest.max_record_bytes > 64 * 1024 * 1024 {
            return Err(CaptureError::Invalid("record allocation bound"));
        }
        if blake3::hash(&serde_json::to_vec(&manifest.metadata)?)
            .to_hex()
            .as_str()
            != manifest.metadata_digest
        {
            return Err(CaptureError::Invalid("metadata digest mismatch"));
        }
        let mut file = File::open(directory.join("events.pax"))?;
        let length = file.metadata()?.len();
        if length != manifest.bytes || length > max_bytes {
            return Err(CaptureError::Invalid(
                "journal size mismatch or limit exceeded",
            ));
        }
        let mut hash = blake3::Hasher::new();
        let mut chunk = [0u8; 64 * 1024];
        let mut hashed_bytes = 0u64;
        loop {
            let n = file.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            hashed_bytes += n as u64;
            if hashed_bytes > length {
                return Err(CaptureError::Invalid("journal grew during verification"));
            }
            hash.update(&chunk[..n]);
        }
        if hash.finalize().to_hex().as_str() != manifest.digest {
            return Err(CaptureError::Invalid("journal digest mismatch"));
        }
        file.rewind()?;
        let mut reader = BufReader::new(file);
        check_magic(&mut reader)?;
        let mut records = 0u64;
        let mut messages = 0u64;
        let mut last = 0;
        let mut generation = 0;
        let mut connected = false;
        while let Some(record) = read_record(&mut reader, manifest.max_record_bytes)? {
            if record.offset_ns < last || record.offset_ns > manifest.end_offset_ns {
                return Err(CaptureError::Invalid("record time order"));
            }
            last = record.offset_ns;
            records += 1;
            if records > manifest.records {
                return Err(CaptureError::Invalid("journal grew during validation"));
            }
            match record.kind {
                RecordKind::Connected => {
                    if connected || record.generation <= generation {
                        return Err(CaptureError::Invalid("invalid connection generation"));
                    }
                    generation = record.generation;
                    connected = true;
                }
                RecordKind::Message(_) => {
                    if !connected || record.generation != generation {
                        return Err(CaptureError::Invalid("message outside connection"));
                    }
                    messages += 1;
                }
                RecordKind::Disconnected { lossy, .. } => {
                    if !connected || lossy || record.generation != generation {
                        return Err(CaptureError::Invalid("lossy or inconsistent boundary"));
                    }
                    connected = false;
                }
            }
        }
        if records != manifest.records || messages != manifest.messages {
            return Err(CaptureError::Invalid("record count mismatch"));
        }
        reader.rewind()?;
        let mut consumed_hash = blake3::Hasher::new();
        check_magic(&mut HashReader {
            reader: &mut reader,
            hash: &mut consumed_hash,
        })?;
        Ok(Self {
            manifest,
            reader,
            consumed_hash,
            consumed_records: 0,
            finished: false,
        })
    }
    /// Verified manifest, including exact metadata and source identity.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    /// Read the next record, hashing the bytes actually consumed a second time.
    /// `Ok(None)` confirms final integrity, including changes after opening.
    /// Consumers must reach that result before claiming a completed replay.
    pub fn next_record(&mut self) -> Result<Option<Record>, CaptureError> {
        if self.finished {
            return Ok(None);
        }
        let record = read_record(
            &mut HashReader {
                reader: &mut self.reader,
                hash: &mut self.consumed_hash,
            },
            self.manifest.max_record_bytes,
        )?;
        if record.is_some() {
            self.consumed_records += 1;
            if self.consumed_records > self.manifest.records {
                return Err(CaptureError::Invalid("journal grew during replay"));
            }
        } else {
            if self.consumed_hash.finalize().to_hex().as_str() != self.manifest.digest
                || self.consumed_records != self.manifest.records
            {
                return Err(CaptureError::Invalid("journal changed during replay"));
            }
            self.finished = true;
        }
        Ok(record)
    }
}

struct HashReader<'a> {
    reader: &'a mut BufReader<File>,
    hash: &'a mut blake3::Hasher,
}
impl Read for HashReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let count = self.reader.read(bytes)?;
        self.hash.update(&bytes[..count]);
        Ok(count)
    }
}
fn check_magic(reader: &mut impl Read) -> Result<(), CaptureError> {
    let mut magic = [0u8; 8];
    reader.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(CaptureError::Invalid("journal magic/version"));
    }
    Ok(())
}
fn read_record(reader: &mut impl Read, bound: usize) -> Result<Option<Record>, CaptureError> {
    let mut prefix = [0u8; 4];
    let first = reader.read(&mut prefix[..1])?;
    if first == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut prefix[1..])?;
    let length = u32::from_le_bytes(prefix) as usize;
    if length < RECORD_HEADER || length - RECORD_HEADER > bound {
        return Err(CaptureError::Invalid("record length exceeds bound"));
    }
    let mut header = [0u8; RECORD_HEADER];
    reader.read_exact(&mut header)?;
    let offset_ns = u64::from_le_bytes(header[..8].try_into().unwrap());
    let generation = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let mut payload = vec![0u8; length - RECORD_HEADER];
    reader.read_exact(&mut payload)?;
    let kind = match header[16] {
        1 if payload.is_empty() => RecordKind::Connected,
        2 if !payload.is_empty() => RecordKind::Message(Bytes::from(payload)),
        3 if !payload.is_empty() && payload[0] <= 1 => RecordKind::Disconnected {
            lossy: payload[0] == 1,
            reason: std::str::from_utf8(&payload[1..])
                .map_err(|_| CaptureError::Invalid("invalid boundary text"))?
                .into(),
        },
        _ => return Err(CaptureError::Invalid("record kind or payload")),
    };
    Ok(Some(Record {
        offset_ns,
        generation,
        kind,
    }))
}
