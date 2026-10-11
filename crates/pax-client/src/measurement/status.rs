//! Wire-compatible diagnostic status, independent of compiled producers.
use serde::Serialize;
/// Monotonic counters sampled without a lock. Loads are not an atomic transaction;
/// transient pending counts are approximate, while final quiescent counts are exact.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct QueueStatus {
    /// Fixed item count allocated and touched before ingestion.
    pub capacity: usize,
    /// Samples accepted into the queue.
    pub accepted: u64,
    /// Samples discarded because finite storage was occupied.
    pub full_drops: u64,
    /// Samples discarded after the consumer endpoint was dropped.
    pub closed_drops: u64,
    /// Samples taken by the consumer, not necessarily persisted.
    pub consumed: u64,
    /// Approximate accepted samples not yet taken; exact after both owners stop.
    pub pending: u64,
    /// The input owner can still publish samples.
    pub producer_alive: bool,
    /// The consumer endpoint still exists; activity/freshness is reported separately.
    pub consumer_alive: bool,
}
