//! Opt-in receive diagnostics. Timings describe read/poll wall time, not CPU time.
//!
//! TLS adapter work includes record authentication, buffering and plaintext copying.
//! It is not a measurement of cryptographic instructions alone. Encrypted reads
//! are shared windows: TCP, TLS records and application messages are many-to-many.
mod association;
pub mod clock;
mod collector;
mod config;
pub mod control;
pub(crate) mod dispatch;
mod io;
pub mod kernel;
pub mod policy;
mod work;

pub(crate) use collector::Collector;
pub use config::{PhcCalibration, ReceiveConfig};
#[cfg(feature = "diagnostic-handoff")]
pub use dispatch::DiagnosticReceiver;
pub use dispatch::Observation;
pub(crate) use io::{PlainIo, SocketIo};
use serde::Serialize;
use std::time::Instant;
pub use work::{ProbeWork, ReadDetail, ReceiveSelection, ReceiveWork};

/// Bounded identity range; IDs are connection-local, and never packet numbers.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct BatchRange {
    /// First observed batch in the adapter-read window. Earlier buffered TLS bytes may exist.
    #[serde(serialize_with = "integer")]
    pub first: u64,
    /// Last observed batch before plaintext was exposed by the adapter; read-ahead may be included.
    #[serde(serialize_with = "integer")]
    pub last: u64,
}
fn integer<S: serde::Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}
/// Qualified metadata for a complete application message. All `Instant` values
/// belong to the caller's monotonic clock. Serialize derived durations in reports.
#[derive(Clone, Copy, Debug)]
pub struct ReceiveTiming {
    /// Cumulative independent selections at this message boundary. A dropped
    /// metadata tail can leave these counters stale; inspect queue coverage too.
    pub selection: ReceiveSelection,
    /// Policy revision for operation work; initial configuration is revision zero.
    pub revision: u64,
    /// False when a live update interrupted this delivery's observation window.
    pub complete_policy_window: bool,
    /// Effective probe policy. Each rate has a separate operation denominator.
    pub policy: policy::ReceivePolicy,
    /// Latest new adapter read, with no claim that its bytes belong to this message.
    pub latest_read: Option<ReadObservation>,
    /// Metadata for the first observed socket read in the associated adapter
    /// window. This is not proven first contributing packet coverage: buffered
    /// TLS input may predate the window and ancillary packet position is unknown.
    /// Unselected initial timestamps remain unavailable rather than using later ones.
    pub first_observed_read: Option<ReadObservation>,
    /// Session identity; prevents association across reconnects.
    pub generation: u64,
    /// Retained plaintext byte-range association, or an explicit loss below.
    pub plaintext: Option<BatchRange>,
    /// Observed socket read window (ciphertext for TLS), shared with buffered plaintext.
    /// Earlier partial TLS record bytes may precede this window.
    pub socket: Option<BatchRange>,
    /// First socket read observed in this adapter window, not first contributing input.
    pub first_read: Option<Instant>,
    /// Most recent observed socket read before authenticated plaintext exposure.
    /// A batch boundary, never an exact TLS record or TCP packet boundary.
    pub last_read: Option<Instant>,
    /// Several frames/messages share at least one plaintext read batch.
    pub shared: bool,
    /// This receive operation needed no new socket read.
    pub buffered: bool,
    /// Clear granularity or reason association is unavailable.
    pub coverage: &'static str,
    /// Work performed once by this receive operation; never duplicated for buffering.
    pub work: ReceiveWork,
    /// Number of metadata entries lost since this connection began.
    pub lost: u64,
    /// Sampling denominator for application messages.
    pub sample_every: u64,
    /// Optional software timestamp result for the completion read batch.
    pub software: kernel::Stamp,
    /// Optional hardware timestamp result for the completion read batch.
    pub hardware: kernel::Stamp,
}

/// A new read observed during this delivery, independent of frame association.
#[derive(Clone, Copy, Debug)]
pub struct ReadObservation {
    /// Connection-local successful read number.
    pub batch: u64,
    /// Monotonic read completion boundary, not packet arrival.
    pub ended: Instant,
    /// Qualified software timestamp for this read batch.
    pub software: kernel::Stamp,
    /// Qualified hardware timestamp for this read batch.
    pub hardware: kernel::Stamp,
}
