//! Writer status is independent of successful JSON/file output.
use super::*;
#[derive(Default)]
pub(super) struct Progress {
    pub alive: AtomicBool,
    pub failed: AtomicBool,
    pub records: AtomicU64,
    pub last_write_ns: AtomicU64,
    pub eligible: AtomicU64,
    pub selected: AtomicU64,
    pub byte_drops: AtomicU64,
    pub complete: AtomicBool,
}
/// Cloneable status handle, intended to be retained before starting observation.
#[derive(Clone)]
pub struct Status {
    pub(super) origin: std::time::Instant,
    pub(super) drops: Arc<AtomicU64>,
    pub(super) source: Arc<AtomicU64>,
    pub(super) progress: Arc<Progress>,
}
/// Lock-free writer counters; concurrent reads are approximate snapshots.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct Snapshot {
    /// Writer thread still exists. It can be alive but stalled.
    pub worker_alive: bool,
    /// I/O error, panic or bounded finalization timeout observed.
    pub failed: bool,
    /// Receive samples considered by the independent file-export policy.
    pub eligible: u64,
    /// Receive samples selected for attempted writer publication.
    pub selected: u64,
    /// Samples appended to the worker's output buffer; flush happens at finish.
    pub records: u64,
    /// Age of the latest buffered write, not durable-file flush age.
    pub last_buffered_age_ns: Option<u64>,
    /// Writer-queue publication failures.
    pub queue_drops: u64,
    /// Loss before the file writer; final cumulative source coverage.
    pub source_drops: u64,
    /// Samples omitted due to the finite file-size limit.
    pub byte_drops: u64,
    /// True only after successful final flush of a complete diagnostic stream.
    pub complete: bool,
}
impl Status {
    /// Read without waiting on a file, a queue, or the worker thread.
    pub fn snapshot(&self) -> Snapshot {
        let records = self.progress.records.load(Ordering::Acquire);
        Snapshot {
            worker_alive: self.progress.alive.load(Ordering::Acquire),
            failed: self.progress.failed.load(Ordering::Acquire),
            records,
            last_buffered_age_ns: (records != 0).then(|| {
                crate::pipeline::ns(self.origin.elapsed())
                    .saturating_sub(self.progress.last_write_ns.load(Ordering::Relaxed))
            }),
            eligible: self.progress.eligible.load(Ordering::Relaxed),
            selected: self.progress.selected.load(Ordering::Relaxed),
            queue_drops: self.drops.load(Ordering::Relaxed),
            source_drops: self.source.load(Ordering::Relaxed),
            byte_drops: self.progress.byte_drops.load(Ordering::Relaxed),
            complete: self.progress.complete.load(Ordering::Acquire),
        }
    }
}
pub(super) struct Guard(pub Arc<Progress>);
impl Drop for Guard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.failed.store(true, Ordering::Release);
        }
        self.0.alive.store(false, Ordering::Release);
    }
}
