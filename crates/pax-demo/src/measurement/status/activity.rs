//! Report liveness and cadence counters have no dependency on report publication.
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
#[derive(Clone)]
pub(crate) struct Activity {
    state: Arc<State>,
}
struct State {
    alive: AtomicBool,
    failed: AtomicBool,
    skipped: AtomicU64,
    published: AtomicU64,
    last_ns: AtomicU64,
    mode: &'static str,
}
/// Independent report-owner status. Counters are approximate during concurrent updates.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct ReportStatus {
    /// `worker` or the explicit diagnostic `ingestion` baseline.
    pub mode: &'static str,
    /// Owner still exists, independently of the sample stream.
    pub alive: bool,
    /// The worker panicked or a final drain timed out.
    pub failed: bool,
    /// Periodic reports discarded because the queue was full or closed.
    pub skipped: u64,
    /// Completed report publications.
    pub publications: u64,
    /// Time since the last successful publication, separate from input age.
    pub publication_age_ns: Option<u64>,
}
impl Activity {
    pub fn new(mode: &'static str) -> Self {
        Self {
            state: Arc::new(State {
                alive: AtomicBool::new(true),
                failed: AtomicBool::new(false),
                skipped: AtomicU64::new(0),
                published: AtomicU64::new(0),
                last_ns: AtomicU64::new(0),
                mode,
            }),
        }
    }
    pub fn snapshot(&self, now_ns: u64) -> ReportStatus {
        let s = &self.state;
        let count = s.published.load(Ordering::Acquire);
        ReportStatus {
            mode: s.mode,
            alive: s.alive.load(Ordering::Acquire),
            failed: s.failed.load(Ordering::Acquire),
            skipped: s.skipped.load(Ordering::Relaxed),
            publications: count,
            publication_age_ns: (count != 0)
                .then(|| now_ns.saturating_sub(s.last_ns.load(Ordering::Relaxed))),
        }
    }
    pub fn skipped(&self, count: u64) {
        self.state.skipped.store(count, Ordering::Relaxed);
    }
    pub fn published(&self, at: u64) {
        self.state.last_ns.store(at, Ordering::Relaxed);
        self.state.published.fetch_add(1, Ordering::Release);
    }
    pub fn failed(&self) {
        self.state.failed.store(true, Ordering::Release);
    }
    pub fn stopped(&self) {
        self.state.alive.store(false, Ordering::Release);
    }
}
pub(crate) struct Guard(pub Activity);
impl Drop for Guard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.failed();
        }
        self.0.stopped();
    }
}
