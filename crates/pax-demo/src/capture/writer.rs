//! Framed writer and crash-safe completion manifest.
use super::{CaptureError, MAGIC, Manifest, RECORD_HEADER, RecordKind, WriterInput};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::Receiver,
    },
};

pub(super) fn manifest(
    directory: &Path,
    value: &Manifest,
    finalize: bool,
) -> Result<(), CaptureError> {
    let path = directory.join(if finalize {
        "manifest.tmp"
    } else {
        "manifest.json"
    });
    let encoded = serde_json::to_vec_pretty(value)?;
    if encoded.len() + 1 > 1024 * 1024 {
        return Err(CaptureError::Invalid("manifest exceeds 1 MiB"));
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)?;
    file.write_all(&encoded)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    if finalize {
        std::fs::rename(path, directory.join("manifest.json"))?;
    }
    #[cfg(unix)]
    File::open(directory)?.sync_all()?;
    Ok(())
}

pub(super) fn run(
    directory: PathBuf,
    state: Manifest,
    receiver: Receiver<WriterInput>,
    failed: Arc<AtomicBool>,
) -> Result<Manifest, CaptureError> {
    let result = write_records(directory, state, receiver, &failed);
    if result.is_err() {
        failed.store(true, Ordering::Release);
    }
    result
}

fn write_records(
    directory: PathBuf,
    mut state: Manifest,
    receiver: Receiver<WriterInput>,
    failed: &AtomicBool,
) -> Result<Manifest, CaptureError> {
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(directory.join("events.pax"))?;
    let mut writer = BufWriter::with_capacity(128 * 1024, file);
    let mut hash = blake3::Hasher::new();
    write(&mut writer, &mut hash, MAGIC)?;
    state.bytes = 8;
    let mut finished = false;
    for input in receiver {
        match input {
            WriterInput::Finish { offset_ns, reason } => {
                state.end_offset_ns = offset_ns;
                state.stop_reason = reason;
                finished = true;
                break;
            }
            WriterInput::Record(record) => {
                let length = record.encoded_len();
                let mut header = [0u8; 4 + RECORD_HEADER];
                header[..4].copy_from_slice(
                    &u32::try_from(length - 4)
                        .map_err(|_| CaptureError::Invalid("record length"))?
                        .to_le_bytes(),
                );
                header[4..12].copy_from_slice(&record.offset_ns.to_le_bytes());
                header[12..20].copy_from_slice(&record.generation.to_le_bytes());
                header[20] = match record.kind {
                    RecordKind::Connected => 1,
                    RecordKind::Message(_) => 2,
                    RecordKind::Disconnected { .. } => 3,
                };
                write(&mut writer, &mut hash, &header)?;
                match record.kind {
                    RecordKind::Connected => {}
                    RecordKind::Message(bytes) => {
                        write(&mut writer, &mut hash, &bytes)?;
                        state.messages += 1;
                    }
                    RecordKind::Disconnected { reason, lossy } => {
                        write(&mut writer, &mut hash, &[u8::from(lossy)])?;
                        write(&mut writer, &mut hash, reason.as_bytes())?;
                        if lossy {
                            failed.store(true, Ordering::Release);
                        }
                    }
                }
                state.records += 1;
                state.bytes += length as u64;
                state.end_offset_ns = record.offset_ns;
            }
        }
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    state.digest = hash.finalize().to_hex().to_string();
    state.complete = finished && !failed.load(Ordering::Acquire);
    if !state.complete {
        state.stop_reason = "input loss, abandoned recording, or writer failure".into();
    }
    manifest(&directory, &state, true)?;
    if !state.complete {
        return Err(CaptureError::Incomplete(state.stop_reason));
    }
    Ok(state)
}
fn write(
    writer: &mut BufWriter<File>,
    hash: &mut blake3::Hasher,
    bytes: &[u8],
) -> Result<(), CaptureError> {
    writer.write_all(bytes)?;
    hash.update(bytes);
    Ok(())
}
