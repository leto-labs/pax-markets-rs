//! Measure local processing of an already assembled application message.

use std::time::{Duration, Instant};

/// Time exactly one synchronous processing callback using a local monotonic clock.
///
/// The caller supplies a complete WebSocket application message. The measured
/// interval excludes network receipt, TLS, frame assembly, fixture loading,
/// result formatting, and percentile calculation. Results, including errors,
/// are returned unchanged so failed messages can be classified separately.
///
/// Timestamp overhead is included; measure that overhead before interpreting
/// very small durations. See <https://doc.rust-lang.org/std/time/struct.Instant.html>.
pub fn measure_message<T>(message: &[u8], process: impl FnOnce(&[u8]) -> T) -> (T, Duration) {
    let started = Instant::now();
    let result = process(message);
    (result, started.elapsed())
}

/// Nearest-rank latency percentiles for one explicitly identified workload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TurnoverSummary {
    /// Number of measured messages.
    pub samples: usize,
    /// Smallest measured duration.
    pub min: Duration,
    /// Nearest-rank 50th percentile.
    pub p50: Duration,
    /// Nearest-rank 95th percentile.
    pub p95: Duration,
    /// Nearest-rank 99th percentile.
    pub p99: Duration,
    /// Largest measured duration.
    pub max: Duration,
}

impl TurnoverSummary {
    /// Summarize samples in place, outside the timed processing path.
    ///
    /// Returns `None` for an empty sample. Sorting changes the input order.
    /// Percentiles use rank `ceil(sample_count * percentile / 100)`.
    pub fn from_samples(samples: &mut [Duration]) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        samples.sort_unstable();
        let count = samples.len();
        let percentile = |percent: usize| {
            // Split the product to avoid overflowing for a large sample count.
            let rank = (count / 100) * percent + ((count % 100) * percent).div_ceil(100);
            samples[rank - 1]
        };
        Some(Self {
            samples: count,
            min: samples[0],
            p50: percentile(50),
            p95: percentile(95),
            p99: percentile(99),
            max: samples[count - 1],
        })
    }
}

#[cfg(test)]
mod tests;
