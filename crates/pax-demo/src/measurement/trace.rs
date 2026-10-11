//! Bounded sampled local traces. Every timestamp belongs to one monotonic domain.
use pax_types::units::{MarketId, Sequence};
use serde::{Serialize, Serializer};
use std::{collections::VecDeque, fmt};

/// Shared exact generation/message identity.
pub use pax_client::measurement::schema::ObservationId as TraceId;

fn integer<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}
fn optional_integer<S: Serializer, T: fmt::Display>(
    value: &Option<T>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(value) => serializer.collect_str(value),
        None => serializer.serialize_none(),
    }
}

/// One completed message, with exclusive stage durations in nanoseconds.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Trace {
    /// Policy revision at the input boundary.
    pub policy_revision: u64,
    /// Requested domain rates, retained with this sample across updates.
    pub policy: crate::measurement::policy::ProcessingPolicy,
    /// Unique within a process; generation/market/sequence are included separately.
    pub id: TraceId,
    /// Connection generation, serialized without JavaScript integer loss.
    #[serde(serialize_with = "integer")]
    pub generation: u64,
    /// Market identifier when available.
    #[serde(serialize_with = "optional_integer")]
    pub market: Option<MarketId>,
    /// Committed sequence on success (including snapshot reconciliation),
    /// otherwise the input sequence when available.
    #[serde(serialize_with = "optional_integer")]
    pub sequence: Option<Sequence>,
    /// Committed snapshot/event, buffered, duplicate, control or rejected input.
    /// Populations are never mixed for percentiles.
    pub population: &'static str,
    /// Monotonic application receive offset.
    pub received_ns: u64,
    /// Receive-to-decode wait, including caller dispatch, excluded from processing.
    pub queue_ns: Option<u64>,
    /// Exclusive attribution of the receive-to-decode interval, when instrumented.
    pub delivery: Option<DeliveryStages>,
    /// Bookkeeping after the processing finish marker, through trace insertion.
    pub bookkeeping_ns: Option<u64>,
    /// Previous sampled callback end to this decode start, only for consecutive
    /// messages already available at that end. Overlaps the queue interval.
    pub pending_gap_ns: Option<u64>,
    /// Protocol decoding.
    pub decode_ns: Option<u64>,
    /// Market dispatch, elapsed-liquidity accounting, sequence checks and exact
    /// preflight (sampled events only).
    pub preflight_ns: Option<u64>,
    /// Ladder mutation (sampled events only).
    pub mutation_ns: Option<u64>,
    /// Post-state verification (sampled events only).
    pub validation_ns: Option<u64>,
    /// Remaining book work, or snapshot load/reconciliation, or rejected processing.
    pub other_ns: Option<u64>,
    /// Complete decode-through-disposition callback duration; only successful
    /// snapshot/event populations reach a validated book commit.
    pub processing_ns: Option<u64>,
}
impl Trace {
    // Nonzero string pointer means filling this value writes physical storage;
    // unlike a zero-only allocation, it cannot be satisfied by lazy zero pages.
    const EMPTY: Self = Self {
        policy_revision: 0,
        policy: crate::measurement::policy::ProcessingPolicy::OFF,
        id: TraceId {
            generation: 0,
            message: 0,
        },
        generation: 0,
        market: None,
        sequence: None,
        population: "",
        received_ns: 0,
        queue_ns: None,
        delivery: None,
        bookkeeping_ns: None,
        pending_gap_ns: None,
        decode_ns: None,
        preflight_ns: None,
        mutation_ns: None,
        validation_ns: None,
        other_ns: None,
        processing_ns: None,
    };
}
/// Exclusive receive-to-decode stages; these sum to the trace's queue interval.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct DeliveryStages {
    /// Complete WebSocket message to completion of transport control checks.
    pub transport_ns: u64,
    /// Transport prepared to consumer delivery; includes send and task scheduling.
    pub handoff_ns: u64,
    /// Consumer delivery to decode start, including capture and pipeline setup.
    pub preparation_ns: u64,
    /// Remaining inputs observed at delivery, not a maximum or enqueue-time depth.
    pub pending: usize,
}
/// Statistics for one trace population; percentiles are not fabricated stage traces.
#[derive(Clone, Debug, Serialize)]
pub struct Population {
    /// Shared typed decode-through-commit or noncommit disposition metadata.
    pub observation: pax_client::measurement::schema::Observation,
    /// Committed snapshot/event, buffered, duplicate, control or rejected input.
    pub name: &'static str,
    /// Retained samples in the active window.
    pub samples: usize,
    /// Policy revisions represented by timed samples in this retained population.
    pub policy_revisions: Vec<u64>,
    /// Whole-callback median nanoseconds.
    pub p50_ns: u64,
    /// Whole-callback 95th percentile nanoseconds.
    pub p95_ns: u64,
    /// Whole-callback 99th percentile nanoseconds.
    pub p99_ns: u64,
}
/// Reporting snapshot of a finite sample window.
#[derive(Clone, Debug, Serialize)]
pub struct TraceWindow {
    /// Version of shared cold metadata; payload stage versions remain explicit.
    pub measurement_schema: u16,
    /// Deferred receive worker coverage; absent for embedded or omitted collection.
    pub diagnostics: Option<super::worker::DiagnosticStatus>,
    /// Resolved groups; absent observations are not implicitly enabled.
    pub capabilities: super::capabilities::Capabilities,
    /// Effective independent processing policy; absent for standalone trace stores.
    pub processing: Option<super::policy::ProcessingStatus>,
    /// Complete message traces, oldest first.
    pub traces: Vec<Trace>,
    /// Qualified receive observations, joined by trace ID; empty when disabled.
    pub receive: Vec<super::receive::ReceiveTrace>,
    /// Lifetime receive-window capacity evictions, separate from client association loss.
    pub receive_capacity_drops: u64,
    /// Receive samples expired by age, separate from loss and capacity eviction.
    pub receive_age_expirations: u64,
    /// Separate sampled workload summaries.
    pub populations: Vec<Population>,
    /// Window duration.
    pub window_ns: u64,
    /// One message in this many is sampled; errors are always retained.
    pub sample_every: u64,
    /// Lifetime number displaced by capacity, distinct from age expiry.
    pub capacity_drops: u64,
    /// Processing samples expired by age, including entries filtered in this view.
    pub age_expirations: u64,
    /// No samples within the active window.
    pub stale: bool,
}
/// Fixed-capacity sampled trace storage. Formatting happens only on publication.
pub struct Traces {
    entries: VecDeque<Trace>,
    capacity: usize,
    window_ns: u64,
    sample_every: u64,
    capacity_drops: u64,
    age_expirations: u64,
}
impl Traces {
    /// Validate explicit finite bounds.
    pub fn new(capacity: usize, window_ns: u64, sample_every: u64) -> Result<Self, &'static str> {
        if capacity == 0 || capacity > 65536 || window_ns == 0 {
            return Err("invalid trace bounds");
        }
        Ok(Self {
            entries: if cfg!(any(
                feature = "processing-traces",
                feature = "delivery-traces"
            )) {
                VecDeque::with_capacity(capacity)
            } else {
                VecDeque::new()
            },
            capacity,
            window_ns,
            sample_every,
            capacity_drops: 0,
            age_expirations: 0,
        })
    }
    /// Allocate and touch the bounded storage before input processing begins.
    /// This shifts ordinary first-touch page faults to initialization. It does
    /// not lock pages, prevent later eviction, or make a tail-latency guarantee.
    pub fn new_prefaulted(
        capacity: usize,
        window_ns: u64,
        sample_every: u64,
    ) -> Result<Self, &'static str> {
        let mut traces = Self::new(capacity, window_ns, sample_every)?;
        traces.entries.resize(capacity, Trace::EMPTY);
        // Make initialized storage observable before clearing logical entries;
        // prevent dead-store removal. All memory access remains safe Rust.
        std::hint::black_box(&mut traces.entries);
        traces.entries.clear();
        Ok(traces)
    }
    /// Sampling decision made before stage clock reads.
    pub fn samples(&self, message: u64) -> bool {
        self.sample_every != 0 && message.is_multiple_of(self.sample_every)
    }
    /// Retain one trace; evict expired entries before counting capacity displacement.
    pub fn push(&mut self, trace: Trace) {
        while self.entries.front().is_some_and(|first| {
            trace.received_ns.saturating_sub(first.received_ns) > self.window_ns
        }) {
            self.entries.pop_front();
            self.age_expirations = self.age_expirations.saturating_add(1);
        }
        if self.entries.len() == self.capacity {
            self.entries.pop_front();
            self.capacity_drops = self.capacity_drops.saturating_add(1);
        }
        self.entries.push_back(trace);
    }
    /// Fill the just-inserted trace's callback tail without another allocation.
    pub(crate) fn bookkeeping(&mut self, id: TraceId, duration_ns: u64) {
        if let Some(trace) = self.entries.back_mut().filter(|trace| trace.id == id) {
            trace.bookkeeping_ns = Some(duration_ns);
        }
    }
    /// Copy and summarize an immutable window outside the processing interval.
    pub fn report(&self, now_ns: u64) -> TraceWindow {
        self.snapshot(now_ns).summarize()
    }
    /// Copy a bounded numeric window; percentile work belongs to its consumer.
    pub(crate) fn snapshot(&self, now_ns: u64) -> TraceWindow {
        let traces: Vec<_> = self
            .entries
            .iter()
            .filter(|t| now_ns.saturating_sub(t.received_ns) <= self.window_ns)
            .copied()
            .collect();
        TraceWindow {
            measurement_schema: pax_client::measurement::schema::VERSION,
            diagnostics: None,
            capabilities: super::capabilities::CAPABILITIES,
            processing: None,
            stale: traces.is_empty(),
            traces,
            populations: Vec::new(),
            receive: Vec::new(),
            receive_capacity_drops: 0,
            receive_age_expirations: 0,
            window_ns: self.window_ns,
            sample_every: self.sample_every,
            capacity_drops: self.capacity_drops,
            age_expirations: self.age_expirations.saturating_add(
                self.entries
                    .iter()
                    .filter(|t| now_ns.saturating_sub(t.received_ns) > self.window_ns)
                    .count() as u64,
            ),
        }
    }
}
impl TraceWindow {
    pub(crate) fn summarize(mut self) -> Self {
        let populations = [
            "snapshot",
            "event",
            "buffered",
            "duplicate",
            "control",
            "rejected",
        ]
        .into_iter()
        .filter_map(|name| {
            let mut times: Vec<_> = self
                .traces
                .iter()
                .filter(|t| t.population == name)
                .filter_map(|t| t.processing_ns)
                .collect();
            if times.is_empty() {
                return None;
            }
            times.sort_unstable();
            let n = times.len();
            let mut policy_revisions: Vec<_> = self
                .traces
                .iter()
                .filter(|t| t.population == name && t.processing_ns.is_some())
                .map(|t| t.policy_revision)
                .collect();
            policy_revisions.sort_unstable();
            policy_revisions.dedup();
            let kind = match name {
                "event" => pax_client::measurement::schema::Population::Event,
                "snapshot" => pax_client::measurement::schema::Population::Snapshot,
                "control" => pax_client::measurement::schema::Population::Control,
                "buffered" => pax_client::measurement::schema::Population::Buffered,
                "duplicate" => pax_client::measurement::schema::Population::Duplicate,
                _ => pax_client::measurement::schema::Population::Rejected,
            };
            let mut observation = pax_client::measurement::schema::Observation::new(
                if matches!(
                    kind,
                    pax_client::measurement::schema::Population::Control
                        | pax_client::measurement::schema::Population::Buffered
                        | pax_client::measurement::schema::Population::Duplicate
                        | pax_client::measurement::schema::Population::Rejected
                ) {
                    pax_client::measurement::schema::Boundary::DecodeToDisposition
                } else {
                    pax_client::measurement::schema::Boundary::DecodeToBook
                },
                kind,
                pax_client::measurement::schema::Clock::same(
                    pax_client::measurement::schema::ClockDomain::ReceiverMonotonic,
                ),
                pax_client::measurement::schema::Coverage {
                    eligible: None,
                    retained: n as u64,
                    lost: None, // Window-wide evictions are reported separately, not attributed to this population.
                    completed: !self.stale,
                },
            );
            observation
                .probes
                .push(pax_client::measurement::schema::Probe {
                    domain: "trace_retention".into(),
                    rate: pax_client::measurement::SampleRate::every(self.sample_every),
                });
            Some(Population {
                observation,
                name,
                policy_revisions,
                samples: n,
                p50_ns: times[(n * 50).div_ceil(100) - 1],
                p95_ns: times[(n * 95).div_ceil(100) - 1],
                p99_ns: times[(n * 99).div_ceil(100) - 1],
            })
        })
        .collect();
        self.populations = populations;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn trace(time: u64, population: &'static str) -> Trace {
        Trace {
            policy_revision: 0,
            policy: crate::measurement::policy::ProcessingPolicy::every(1),
            id: TraceId {
                generation: 1,
                message: time,
            },
            generation: 1,
            market: Some(MarketId(1)),
            sequence: Some(Sequence(time)),
            population,
            received_ns: time,
            queue_ns: Some(1),
            delivery: None,
            bookkeeping_ns: None,
            pending_gap_ns: None,
            decode_ns: Some(1),
            preflight_ns: Some(2),
            mutation_ns: Some(3),
            validation_ns: Some(4),
            other_ns: None,
            processing_ns: Some(10),
        }
    }
    #[test]
    fn numeric_trace_storage_preserves_exact_string_json_contract() {
        let mut sample = trace(u64::MAX, "event");
        sample.generation = u64::MAX;
        sample.market = Some(MarketId(u64::MAX));
        let value = serde_json::to_value(sample).unwrap();
        assert_eq!(value["id"], format!("1:{}", u64::MAX));
        for field in ["generation", "market", "sequence"] {
            assert_eq!(value[field], u64::MAX.to_string());
        }
        sample.sequence = None;
        sample.market = None;
        let value = serde_json::to_value(sample).unwrap();
        assert!(value["market"].is_null() && value["sequence"].is_null());
    }
    #[test]
    fn prefaulted_storage_starts_empty_and_preserves_wraparound() {
        let mut lazy = Traces::new(3, 100, 1).unwrap();
        let mut touched = Traces::new_prefaulted(3, 100, 1).unwrap();
        assert!(touched.report(0).stale);
        for time in 0..10 {
            lazy.push(trace(time, "event"));
            touched.push(trace(time, "event"));
        }
        assert_eq!(
            serde_json::to_value(lazy.report(10)).unwrap(),
            serde_json::to_value(touched.report(10)).unwrap()
        );
        assert_eq!(touched.report(10).capacity_drops, 7);
    }
    #[test]
    fn saturation_and_age_expiry_are_distinct_and_do_not_merge_populations() {
        let mut traces = Traces::new(2, 100, 4).unwrap();
        assert!(!traces.samples(1));
        assert!(traces.samples(4));
        traces.push(trace(0, "event"));
        traces.push(trace(1, "snapshot"));
        traces.push(trace(2, "rejected"));
        let report = traces.report(2);
        assert_eq!(report.capacity_drops, 1);
        assert_eq!(report.populations.len(), 2);
        traces.push(trace(200, "event"));
        let report = traces.report(200);
        assert_eq!(report.capacity_drops, 1);
        assert_eq!(report.traces.len(), 1);
        assert!(!report.stale);
        assert_eq!(report.age_expirations, 2);
        assert!(traces.report(301).stale);
        assert_eq!(traces.report(301).age_expirations, 3);
        // Reporting expiry is pure; physically removing the same entries later
        // must not count them twice.
        traces.push(trace(302, "event"));
        assert_eq!(traces.report(302).age_expirations, 3);
        assert_eq!(traces.report(302).capacity_drops, 1);
    }
    #[test]
    #[allow(clippy::manual_is_multiple_of)] // Keep the remainder operator as an independent oracle.
    fn sampling_matches_remainder_across_periods_and_full_width_counters() {
        let periods = (1..=257)
            .chain((0..64).map(|bit| 1u64 << bit))
            .chain([u64::MAX, u64::MAX - 1]);
        let mut random = 0x9e3779b97f4a7c15u64;
        for period in periods {
            let traces = Traces::new(1, 1, period).unwrap();
            for message in (0..1024).chain([
                period - 1,
                period,
                period.saturating_add(1),
                u64::MAX - 1,
                u64::MAX,
            ]) {
                assert_eq!(
                    traces.samples(message),
                    message % period == 0,
                    "period {period}, counter {message}"
                );
            }
            for _ in 0..1024 {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                assert_eq!(
                    traces.samples(random),
                    random % period == 0,
                    "period {period}, counter {random}"
                );
            }
        }
    }
}
