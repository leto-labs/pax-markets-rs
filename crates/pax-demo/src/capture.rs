//! Versioned, bounded recording of exact public inputs and session boundaries.
mod writer;

use bytes::Bytes;
use pax_types::{market::ExchangeInfo, units::MarketId};
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, SyncSender},
    },
    thread::JoinHandle,
};

pub(crate) const MAGIC: &[u8; 8] = b"PAXCAP01";
pub(crate) const RECORD_HEADER: usize = 17;

/// Recording or replay failure; incomplete input is never a successful replay.
#[derive(Debug, thiserror::Error)]
pub enum CaptureError {
    /// Filesystem failure.
    #[error("capture I/O: {0}")]
    Io(#[from] std::io::Error),
    /// Invalid manifest JSON.
    #[error("capture JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// Invalid exact units/references.
    #[error(transparent)]
    Metadata(#[from] pax_types::market::MetadataError),
    /// Format or integrity validation failed.
    #[error("invalid capture: {0}")]
    Invalid(&'static str),
    /// Recording ended without preserving every accepted input.
    #[error("incomplete capture: {0}")]
    Incomplete(String),
}

/// Build/source identity attached to a recording or performance report.
pub type BuildInfo =
    pax_client::measurement::build_info::BuildInfo<crate::measurement::capabilities::Capabilities>;

/// Embed this package's identity; never consult Git during runtime processing.
pub fn current_build_info() -> BuildInfo {
    pax_client::build_info!(crate::measurement::capabilities::CAPABILITIES)
}

/// Integrity and provenance for one finite recording.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Manifest {
    /// On-disk schema version.
    pub version: u32,
    /// True only after writer drain, data sync, and manifest finalization.
    pub complete: bool,
    /// Public venue endpoint or explicitly labeled synthetic source.
    pub endpoint: String,
    /// Selected markets in this segment.
    pub markets: Vec<MarketId>,
    /// Exact metadata used for decoding and reports.
    pub metadata: ExchangeInfo,
    /// Digest of canonical serialized metadata.
    pub metadata_digest: String,
    /// UTC start time in Unix seconds.
    pub started_unix_seconds: u64,
    /// Monotonic stop offset from capture start.
    pub end_offset_ns: u64,
    /// Application source/compiler identity.
    pub identity: BuildInfo,
    /// Number of framed input records.
    pub records: u64,
    /// Number of complete application messages.
    pub messages: u64,
    /// Total journal file bytes, including magic and framing.
    pub bytes: u64,
    /// BLAKE3 of the exact journal bytes.
    pub digest: String,
    /// Maximum payload permitted in this recording.
    pub max_record_bytes: usize,
    /// Failure or deliberate stopping condition.
    pub stop_reason: String,
}
impl Manifest {
    /// Validate metadata and construct an initially incomplete capture manifest.
    pub fn new(
        metadata: ExchangeInfo,
        markets: Vec<MarketId>,
        endpoint: String,
    ) -> Result<Self, CaptureError> {
        metadata.validate()?;
        if markets.is_empty() {
            return Err(CaptureError::Invalid("no selected markets"));
        }
        for market in &markets {
            metadata.resolve(*market)?;
        }
        let mut unique = markets.clone();
        unique.sort_unstable();
        unique.dedup();
        if unique.len() != markets.len() {
            return Err(CaptureError::Invalid("duplicate market"));
        }
        let metadata_digest = blake3::hash(&serde_json::to_vec(&metadata)?)
            .to_hex()
            .to_string();
        Ok(Self {
            version: 1,
            complete: false,
            endpoint,
            markets,
            metadata,
            metadata_digest,
            started_unix_seconds: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            end_offset_ns: 0,
            identity: crate::capture::current_build_info(),
            records: 0,
            messages: 0,
            bytes: 0,
            digest: String::new(),
            max_record_bytes: 0,
            stop_reason: "recording".into(),
        })
    }
}

/// Ordered recording input. Payload bytes remain unchanged.
#[derive(Clone, Debug)]
pub struct Record {
    /// Offset from a single local monotonic origin.
    pub offset_ns: u64,
    /// Connection identity, also shared by all selected books.
    pub generation: u64,
    /// Data or lifecycle boundary.
    pub kind: RecordKind,
}
/// Record payload.
#[derive(Clone, Debug)]
pub enum RecordKind {
    /// A new connection; every book must obtain a fresh snapshot.
    Connected,
    /// Exact complete application-message bytes.
    Message(Bytes),
    /// Connection ended, possibly with missing local inputs.
    Disconnected {
        /// Cause supplied by the transport.
        reason: String,
        /// Local queue/receive loss occurred.
        lossy: bool,
    },
}
impl Record {
    /// Encoded journal bytes including frame prefix.
    pub fn encoded_len(&self) -> usize {
        4 + RECORD_HEADER
            + match &self.kind {
                RecordKind::Connected => 0,
                RecordKind::Message(bytes) => bytes.len(),
                RecordKind::Disconnected { reason, .. } => 1 + reason.len(),
            }
    }
}

/// Finite queue, message, and disk budgets.
#[derive(Clone, Debug)]
pub struct CaptureConfig {
    /// A new directory; existing paths are never overwritten.
    pub directory: PathBuf,
    /// Maximum queued records.
    pub queue_capacity: usize,
    /// Maximum journal bytes, including framing.
    pub max_bytes: u64,
    /// Maximum application bytes or diagnostic payload.
    pub max_record_bytes: usize,
}
/// Whether a record was accepted or the deliberate byte limit was reached.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordStatus {
    /// Accepted into the finite writer queue.
    Recorded,
    /// Closed cleanly before exceeding the configured byte budget.
    LimitReached,
}

pub(crate) enum WriterInput {
    Record(Record),
    Finish { offset_ns: u64, reason: String },
}

/// Nonblocking ingestion-side handle to a dedicated bounded writer thread.
pub struct Recorder {
    sender: Option<SyncSender<WriterInput>>,
    task: Option<JoinHandle<Result<Manifest, CaptureError>>>,
    failed: Arc<AtomicBool>,
    config: CaptureConfig,
    offered_bytes: u64,
    last_offset: u64,
    limited: bool,
}
impl Recorder {
    /// Create a new recording. An initial incomplete manifest survives failures.
    pub fn start(config: CaptureConfig, mut manifest: Manifest) -> Result<Self, CaptureError> {
        if config.queue_capacity == 0
            || config.queue_capacity > 65536
            || config.max_bytes < 8
            || config.max_record_bytes == 0
            || config.max_record_bytes > 64 * 1024 * 1024
        {
            return Err(CaptureError::Invalid("recording resource bounds"));
        }
        manifest.max_record_bytes = config.max_record_bytes;
        if let Some(parent) = config.directory.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::create_dir(&config.directory)?;
        writer::manifest(&config.directory, &manifest, false)?;
        let (sender, receiver) = mpsc::sync_channel(config.queue_capacity);
        let failed = Arc::new(AtomicBool::new(false));
        let status = Arc::clone(&failed);
        let directory = config.directory.clone();
        let background_cpu = crate::worker::target();
        let task = std::thread::Builder::new()
            .name("pax-capture".into())
            .spawn(move || {
                if let Err(error) = crate::worker::pin(background_cpu) {
                    status.store(true, Ordering::Release);
                    return Err(error.into());
                }
                writer::run(directory, manifest, receiver, status)
            })?;
        Ok(Self {
            sender: Some(sender),
            task: Some(task),
            failed,
            config,
            offered_bytes: 8,
            last_offset: 0,
            limited: false,
        })
    }
    /// Try to enqueue without waiting for disk or capacity. Saturation permanently
    /// marks the recording incomplete; ingestion can continue independently.
    pub fn record(&mut self, record: Record) -> Result<RecordStatus, CaptureError> {
        if self.limited {
            return Ok(RecordStatus::LimitReached);
        }
        if record.offset_ns < self.last_offset {
            self.failed.store(true, Ordering::Release);
            return Err(CaptureError::Invalid("backwards receive offset"));
        }
        let length = record.encoded_len();
        if length - 4 - RECORD_HEADER > self.config.max_record_bytes {
            self.failed.store(true, Ordering::Release);
            return Err(CaptureError::Invalid("oversized record"));
        }
        if length as u64 > self.config.max_bytes - self.offered_bytes {
            self.limited = true;
            return Ok(RecordStatus::LimitReached);
        }
        let sender = self
            .sender
            .as_ref()
            .ok_or(CaptureError::Invalid("recorder closed"))?;
        let offset = record.offset_ns;
        if let Err(error) = sender.try_send(WriterInput::Record(record)) {
            self.failed.store(true, Ordering::Release);
            self.sender.take();
            return Err(CaptureError::Incomplete(error.to_string()));
        }
        self.offered_bytes += length as u64;
        self.last_offset = offset;
        Ok(RecordStatus::Recorded)
    }
    /// Mark external input loss or application failure without blocking ingestion.
    pub fn invalidate(&self) {
        self.failed.store(true, Ordering::Release);
    }
    /// Observe writer/input failure without waiting for another feed message or
    /// blocking on the writer. Finalization retains the detailed error.
    pub fn has_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
            || self.task.as_ref().is_some_and(JoinHandle::is_finished)
    }
    /// Drain and sync the writer outside ingestion. A byte-limited capture ends at
    /// its last included event, not at a later unrecorded observation time.
    pub fn finish(mut self, end_offset_ns: u64, reason: &str) -> Result<Manifest, CaptureError> {
        if end_offset_ns < self.last_offset {
            self.failed.store(true, Ordering::Release);
        }
        if let Some(sender) = self.sender.take() {
            let offset_ns = if self.limited {
                self.last_offset
            } else {
                end_offset_ns
            };
            let reason = if self.limited { "byte limit" } else { reason }.to_owned();
            if sender
                .send(WriterInput::Finish { offset_ns, reason })
                .is_err()
            {
                self.failed.store(true, Ordering::Release);
            }
        }
        self.task
            .take()
            .expect("writer handle")
            .join()
            .map_err(|_| CaptureError::Incomplete("writer panicked".into()))?
    }
}
impl Drop for Recorder {
    fn drop(&mut self) {
        if self.task.is_some() {
            self.failed.store(true, Ordering::Release);
            self.sender.take();
        }
    }
}

#[cfg(test)]
mod tests;
