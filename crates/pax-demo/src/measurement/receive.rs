//! Qualified receive metadata kept outside the compact processing trace ring.
use crate::{measurement::trace::TraceId, pipeline::ns};
use pax_client::measurement::{
    SampleRate,
    schema::{Boundary, Clock, ClockDomain, Coverage, Mapping, Observation, Population, Probe},
};
use pax_client::websocket::receive::{BatchRange, ReceiveTiming, ReceiveWork, kernel::Stamp};
use serde::Serialize;
use std::time::Instant;

/// Numeric receive-stage sample associated with one existing processing trace.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct ReceiveTrace {
    /// Selection counts at original receipt, possibly stale after diagnostic loss.
    pub selection: pax_client::websocket::receive::ReceiveSelection,
    /// Original policy revision; never replaced with exporter state.
    pub revision: u64,
    /// False if this observation was interrupted by a policy update.
    pub complete_policy_window: bool,
    /// Version of this additive diagnostic schema.
    pub schema_version: u32,
    /// Effective receive domains and their independent operation rates.
    pub policy: pax_client::websocket::receive::policy::ReceivePolicy,
    /// Latest new read observed during delivery, without a message association.
    pub latest_read: Option<ReadTrace>,
    /// First observed socket read in the associated adapter window, retaining
    /// unavailable initial source metadata separately from the completion read.
    /// Earlier buffered TLS input and ancillary packet position are not known.
    pub first_observed_read: Option<ReadTrace>,
    /// Same generation/message identity as the processing trace.
    pub id: TraceId,
    /// Original complete-message receive offset; used for bounded-window expiry.
    pub received_ns: u64,
    /// Plaintext byte-range read batches; never packet IDs.
    pub plaintext: Option<BatchRange>,
    /// Observed socket read window, not an exact TLS record association.
    pub socket: Option<BatchRange>,
    /// Time from first observed read in the window to complete message availability.
    /// Earlier buffered TLS bytes may exist, so this is not first-packet latency.
    pub observed_first_read_to_message_ns: Option<u64>,
    /// Time from last observed read in the window to complete message availability.
    /// Read-ahead can include later records; this is a batch boundary.
    pub observed_last_read_to_message_ns: Option<u64>,
    /// Measured active work performed once by the receive operation; may be shared.
    pub work: ReceiveWork,
    /// More than one frame shares a plaintext batch.
    pub shared: bool,
    /// No new socket read in this operation.
    pub buffered: bool,
    /// Association granularity or concrete unavailable reason.
    pub coverage: &'static str,
    /// Connection-local association entries lost; canonical bytes are unaffected.
    pub lost: u64,
    /// Receive emission sampling denominator (distinct from processing sampling).
    pub sample_every: u64,
    /// Qualified kernel-software completion-read observation.
    pub software: TimestampTrace,
    /// Qualified hardware completion-read observation.
    pub hardware: TimestampTrace,
}
/// A batch arrival estimate. No quantity here estimates exchange origination.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct TimestampTrace {
    /// Typed domain and alignment; missing uncertainty never implies exact time.
    pub clock: Clock,
    /// Timestamp source and original clock domain.
    pub source: &'static str,
    /// Why this observation is available or withheld.
    pub status: &'static str,
    /// Source-clock nanoseconds serialized exactly; absent if no stamp arrived.
    #[serde(serialize_with = "optional_integer")]
    pub raw_ns: Option<i128>,
    /// Aligned batch timestamp to message receipt; withheld on invalid alignment.
    pub batch_to_message_ns: Option<u64>,
    /// Alignment bracket and drift uncertainty, never a claimed exact offset.
    pub uncertainty_ns: Option<u64>,
    /// Offset-only model details; legacy or detail-free observations omit this field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qualification: Option<pax_client::websocket::receive::kernel::ClockQualification>,
}
fn optional_integer<S: serde::Serializer>(
    value: &Option<i128>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(v) => serializer.collect_str(v),
        None => serializer.serialize_none(),
    }
}
impl TimestampTrace {
    fn new(stamp: Stamp, received: Instant, domain: ClockDomain) -> Self {
        let stamp = stamp.resolve();
        let duration = stamp
            .at
            .and_then(|at| received.checked_duration_since(at))
            .map(ns);
        Self {
            clock: Clock {
                start: domain,
                end: ClockDomain::ReceiverMonotonic,
                mapping: duration
                    .and(stamp.uncertainty_ns)
                    .map_or(Mapping::Unavailable, |uncertainty_ns| Mapping::Aligned {
                        uncertainty_ns,
                    }),
            },
            source: stamp.source,
            status: if stamp.at.is_some() && duration.is_none() {
                "timestamp follows message; interval withheld"
            } else {
                stamp.status
            },
            raw_ns: stamp.raw_ns,
            batch_to_message_ns: duration,
            uncertainty_ns: stamp.uncertainty_ns,
            qualification: stamp.qualification(),
        }
    }
}
impl ReceiveTrace {
    /// Construct common export metadata on the report/file worker, not ingestion.
    pub fn observation(&self) -> Observation {
        let mut observation = Observation::new(
            Boundary::ReceiveOperation,
            Population::Receive,
            Clock::same(ClockDomain::ReceiverMonotonic),
            Coverage {
                eligible: Some(self.selection.emission.eligible),
                retained: 1,
                lost: Some(self.lost),
                completed: self.complete_policy_window,
            },
        );
        // Legacy synthetic fixtures can omit counters; absence stays unknown.
        if self.selection.emission.eligible == 0 {
            observation.coverage.eligible = None;
        }
        observation.id = Some(self.id);
        observation.probes = [
            ("timestamps", self.policy.timestamps),
            ("socket", self.policy.socket),
            ("read_detail", self.policy.read_detail),
            ("tls", self.policy.tls),
            ("websocket", self.policy.websocket),
            ("pending", self.policy.pending),
            ("association", self.policy.association),
            ("emission", SampleRate::every(self.sample_every)),
        ]
        .into_iter()
        .map(|(domain, rate)| Probe {
            domain: domain.into(),
            rate,
        })
        .collect();
        observation
    }

    /// Reject cross-generation metadata and preserve every unavailable boundary.
    pub fn new(
        id: TraceId,
        received_ns: u64,
        received: Instant,
        timing: ReceiveTiming,
    ) -> Option<Self> {
        if timing.generation != id.generation {
            return None;
        }
        Some(Self {
            schema_version: 3,
            selection: timing.selection,
            revision: timing.revision,
            complete_policy_window: timing.complete_policy_window,
            policy: timing.policy,
            latest_read: timing.latest_read.map(|r| ReadTrace::new(r, received)),
            first_observed_read: timing
                .first_observed_read
                .filter(|read| {
                    timing.complete_policy_window
                        && timing.socket.is_some_and(|range| range.first == read.batch)
                        && timing.first_read == Some(read.ended)
                })
                .map(|r| ReadTrace::new(r, received)),
            id,
            received_ns,
            plaintext: timing.plaintext,
            socket: timing.socket,
            observed_first_read_to_message_ns: timing
                .first_read
                .and_then(|at| received.checked_duration_since(at))
                .map(ns),
            observed_last_read_to_message_ns: timing
                .last_read
                .and_then(|at| received.checked_duration_since(at))
                .map(ns),
            work: timing.work,
            shared: timing.shared,
            buffered: timing.buffered,
            coverage: timing.coverage,
            lost: timing.lost,
            sample_every: timing.sample_every,
            software: TimestampTrace::new(timing.software, received, ClockDomain::KernelRealtime),
            hardware: TimestampTrace::new(timing.hardware, received, ClockDomain::Phc),
        })
    }
}

/// Read-level observation without a proven application-frame association.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct ReadTrace {
    /// Connection-local read batch number.
    #[serde(serialize_with = "batch_integer")]
    pub batch: u64,
    /// Local read completion to message availability, not byte-specific latency.
    pub ended_to_message_ns: Option<u64>,
    /// Software read-batch timestamp.
    pub software: TimestampTrace,
    /// Hardware read-batch timestamp.
    pub hardware: TimestampTrace,
}
impl ReadTrace {
    fn new(read: pax_client::websocket::receive::ReadObservation, received: Instant) -> Self {
        Self {
            batch: read.batch,
            ended_to_message_ns: received.checked_duration_since(read.ended).map(ns),
            software: TimestampTrace::new(read.software, received, ClockDomain::KernelRealtime),
            hardware: TimestampTrace::new(read.hardware, received, ClockDomain::Phc),
        }
    }
}
fn batch_integer<S: serde::Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

/// Allocate and touch a worker-owned receive window before starting transport.
pub(crate) fn prefaulted_window(capacity: usize) -> std::collections::VecDeque<ReceiveTrace> {
    let stamp = TimestampTrace {
        clock: Clock {
            start: ClockDomain::KernelRealtime,
            end: ClockDomain::ReceiverMonotonic,
            mapping: Mapping::Unavailable,
        },
        source: "unavailable",
        status: "prefault seed; never published",
        raw_ns: None,
        batch_to_message_ns: None,
        uncertainty_ns: None,
        qualification: None,
    };
    let seed = ReceiveTrace {
        selection: Default::default(),
        revision: 0,
        complete_policy_window: false,
        schema_version: 3,
        policy: pax_client::websocket::receive::policy::ReceivePolicy::OFF,
        latest_read: None,
        first_observed_read: None,
        id: TraceId {
            generation: 0,
            message: 0,
        },
        received_ns: 0,
        plaintext: None,
        socket: None,
        observed_first_read_to_message_ns: None,
        observed_last_read_to_message_ns: None,
        work: ReceiveWork::default(),
        shared: false,
        buffered: true,
        coverage: "prefault seed; never published",
        lost: 0,
        sample_every: 1,
        software: stamp,
        hardware: stamp,
    };
    let mut window = std::collections::VecDeque::with_capacity(capacity);
    window.resize(capacity, seed);
    std::hint::black_box(&mut window);
    window.clear();
    window
}
