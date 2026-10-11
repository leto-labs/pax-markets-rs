//! Cold observation contracts. These types add no probes to the receive path.
use super::SampleRate;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt;

/// Common current metadata format; legacy readers retain their original labels.
pub const VERSION: u16 = 1;

/// Opaque process-session label initialized only by cold report construction.
/// It qualifies generation/message ordinals; it is not a synchronized timestamp.
pub fn process_session() -> &'static str {
    static SESSION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    SESSION.get_or_init(|| {
        format!(
            "{}:{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |time| time.as_nanos())
        )
    })
}

/// Allocation-free message identity, serialized without JavaScript integer loss.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservationId {
    /// Connection generation within one recording or process session.
    pub generation: u64,
    /// Monotonic message ordinal within that session.
    pub message: u64,
}
impl fmt::Display for ObservationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.generation, self.message)
    }
}
impl Serialize for ObservationId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}
impl<'de> Deserialize<'de> for ObservationId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let (generation, message) = text
            .split_once(':')
            .ok_or_else(|| serde::de::Error::custom("expected generation:message"))?;
        Ok(Self {
            generation: generation.parse().map_err(serde::de::Error::custom)?,
            message: message.parse().map_err(serde::de::Error::custom)?,
        })
    }
}

/// Exact start/end definitions; proxies cannot become packet-specific by renaming.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Boundary {
    /// Decoder entry through a noncommit disposition, including controls/rejections.
    DecodeToDisposition,
    /// Decoder entry through validated commit, excluding prior dispatch.
    DecodeToBook,
    /// Complete text-message observation through validated commit.
    CompleteMessageToBook,
    /// First observed software-timestamped read window through commit.
    FirstSoftwareReadWindowToBook,
    /// First observed hardware-timestamped read window through commit.
    FirstHardwareReadWindowToBook,
    /// Completion read's software timestamp through commit.
    SoftwareReadBatchToBook,
    /// Completion read's hardware timestamp through commit.
    HardwareReadBatchToBook,
    /// Receiver's software TCP-sequence callback through commit, not NIC ingress.
    NativeSoftwarePacketToBook,
    /// Source preparation through that same source's ACK read completion.
    SourcePreparationToAck,
    /// A sender and receiver sharing one process monotonic origin.
    LocalSenderToBook,
    /// Receive-operation observations; no book commit is implied.
    ReceiveOperation,
}
impl Boundary {
    /// Current phase-boundary version; native v1 ended frame work at decode start.
    pub const fn version(self) -> u16 {
        match self {
            Self::NativeSoftwarePacketToBook => 2,
            _ => 1,
        }
    }
}

/// Distinct populations must not be combined into one latency percentile.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Population {
    /// State-changing event.
    Event,
    /// Snapshot reconciliation.
    Snapshot,
    /// Transport/protocol control.
    Control,
    /// Input retained until snapshot reconciliation, without a current commit.
    Buffered,
    /// An already committed sequence, without a new book commit.
    Duplicate,
    /// Rejected input, never a successful commit.
    Rejected,
    /// Explicit mixed committed snapshot/event workload.
    Committed,
    /// Receive observations without a commit classification.
    Receive,
}
/// Clock domain, including the owning process for monotonic counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClockDomain {
    /// Receiver process monotonic origin.
    ReceiverMonotonic,
    /// Controlled source process monotonic origin.
    SourceMonotonic,
    /// Kernel realtime timestamp.
    KernelRealtime,
    /// Hardware clock exposed as a PHC.
    Phc,
    /// Device raw counter without a qualified PHC conversion.
    DeviceRaw,
}
/// Clock conversion status; absence never means zero uncertainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Mapping {
    /// Both endpoints use one clock origin.
    SameDomain,
    /// Qualified bracket/drift model; uncertainty retained with the interval.
    Aligned {
        /// Conservative conversion uncertainty in nanoseconds.
        uncertainty_ns: u64,
    },
    /// Raw counters may be retained, but no duration across clocks is valid.
    Unavailable,
}
/// Typed clock pair for one interval; construction alone does not qualify it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Clock {
    /// Ingress/start clock.
    pub start: ClockDomain,
    /// Completion/end clock.
    pub end: ClockDomain,
    /// Conversion evidence.
    pub mapping: Mapping,
}
impl Clock {
    /// One owner clock, with no cross-host subtraction.
    pub const fn same(domain: ClockDomain) -> Self {
        Self {
            start: domain,
            end: domain,
            mapping: Mapping::SameDomain,
        }
    }
    /// Whether a duration is allowed by this clock contract.
    pub fn qualified(self) -> bool {
        match self.mapping {
            Mapping::SameDomain => self.start == self.end,
            Mapping::Aligned { .. } => {
                matches!(self.start, ClockDomain::KernelRealtime | ClockDomain::Phc)
                    && self.end == ClockDomain::ReceiverMonotonic
            }
            Mapping::Unavailable => false,
        }
    }
}
/// Coverage of a retained population. Unknown loss remains explicitly unknown.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Coverage {
    /// Total eligible observations, when the producer counts this population.
    pub eligible: Option<u64>,
    /// Observations represented in this report.
    pub retained: u64,
    /// Capacity/queue/association loss, excluding intentional sampling.
    pub lost: Option<u64>,
    /// Whether the owner/exporter finished its declared collection window.
    pub completed: bool,
}
/// One independent clock/metadata/output selection rule.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Probe {
    /// Stable domain name; describes eligible operations rather than messages by default.
    pub domain: String,
    /// Actual resolved independent sampling policy.
    pub rate: SampleRate,
}
/// Current common metadata attached by report/export workers after ingestion.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    /// Owning process/recording session; ordinals from another session cannot join.
    pub session: String,
    /// Common metadata format.
    pub schema_version: u16,
    /// Exact measurement endpoints.
    pub boundary: Boundary,
    /// Version of phase placement, separate from envelope format.
    pub boundary_version: u16,
    /// Successful/rejected/snapshot/control selection.
    pub population: Population,
    /// Individual matched event; absent for aggregate windows.
    pub id: Option<ObservationId>,
    /// Clock-domain and alignment proof.
    pub clock: Clock,
    /// Retention and loss evidence.
    pub coverage: Coverage,
    /// Independent effective rates; build capabilities remain in BuildInfo.
    pub probes: Vec<Probe>,
    /// True only for a declared ordinary comparison; diagnostics remain false.
    pub ordinary_performance: bool,
}
impl Observation {
    /// Construct a cold report contract; validate before consuming external data.
    pub fn new(
        boundary: Boundary,
        population: Population,
        clock: Clock,
        coverage: Coverage,
    ) -> Self {
        Self {
            session: process_session().into(),
            schema_version: VERSION,
            boundary,
            boundary_version: boundary.version(),
            population,
            id: None,
            clock,
            coverage,
            probes: Vec::new(),
            ordinary_performance: false,
        }
    }
    /// Verify format, clock/boundary pairing, sampling and coverage consistency.
    ///
    /// # Errors
    /// Unknown versions, impossible clock evidence, duplicate probes or invalid counts.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.session.is_empty() || self.session.len() > 128 || self.probes.len() > 32 {
            return Err("invalid observation session identity or probe bound");
        }
        if self.schema_version != VERSION || self.boundary_version != self.boundary.version() {
            return Err("unsupported observation boundary/schema version");
        }
        if self
            .coverage
            .eligible
            .is_some_and(|n| n < self.coverage.retained)
        {
            return Err("retained observations exceed eligible population");
        }
        if self.clock.mapping != Mapping::Unavailable && !self.clock.qualified() {
            return Err("incompatible clock domains or mapping");
        }
        let valid_population = match self.boundary {
            Boundary::ReceiveOperation => self.population == Population::Receive,
            Boundary::DecodeToDisposition => {
                matches!(
                    self.population,
                    Population::Control
                        | Population::Buffered
                        | Population::Duplicate
                        | Population::Rejected
                )
            }
            Boundary::SourcePreparationToAck => self.population == Population::Event,
            _ => matches!(
                self.population,
                Population::Event | Population::Snapshot | Population::Committed
            ),
        };
        if !valid_population {
            return Err("population does not reach the declared boundary");
        }
        let expected = match self.boundary {
            Boundary::FirstSoftwareReadWindowToBook | Boundary::SoftwareReadBatchToBook => {
                ClockDomain::KernelRealtime
            }
            Boundary::FirstHardwareReadWindowToBook | Boundary::HardwareReadBatchToBook => {
                ClockDomain::Phc
            }
            Boundary::SourcePreparationToAck => ClockDomain::SourceMonotonic,
            _ => ClockDomain::ReceiverMonotonic,
        };
        let expected_end = if self.boundary == Boundary::SourcePreparationToAck {
            ClockDomain::SourceMonotonic
        } else {
            ClockDomain::ReceiverMonotonic
        };
        if self.clock.start != expected || self.clock.end != expected_end {
            return Err("clock does not match measurement boundary");
        }
        for (index, probe) in self.probes.iter().enumerate() {
            probe.rate.validate()?;
            if probe.domain.is_empty()
                || probe.domain.len() > 64
                || self.probes[..index]
                    .iter()
                    .any(|old| old.domain == probe.domain)
            {
                return Err("empty or duplicate probe domain");
            }
        }
        Ok(())
    }
    /// Refuse joining phases from different messages, clocks, boundaries or populations.
    ///
    /// # Errors
    /// Missing identity, incompatible metadata or unavailable conversion.
    pub fn compatible_event(&self, other: &Self) -> Result<(), &'static str> {
        self.validate()?;
        other.validate()?;
        if self.session != other.session
            || self.id.is_none()
            || self.id != other.id
            || self.boundary != other.boundary
            || self.population != other.population
            || self.clock != other.clock
            || !self.clock.qualified()
            || self.probes != other.probes
            || self.ordinary_performance != other.ordinary_performance
        {
            return Err("incompatible observation event or instrumentation");
        }
        Ok(())
    }
}

/// Matched-event percentages, never ratios of independent stage percentiles.
///
/// # Errors
/// Missing phases, overflow or a phase sum different from the measured total.
pub fn matched_percentages(
    total_ns: u64,
    phases: &[Option<u64>],
) -> Result<Vec<f64>, &'static str> {
    let values: Option<Vec<u64>> = phases.iter().copied().collect();
    let values = values.ok_or("unobserved phase")?;
    let sum = values
        .iter()
        .try_fold(0u64, |sum, value| sum.checked_add(*value))
        .ok_or("phase sum overflow")?;
    if sum != total_ns {
        return Err("phases do not match the same event total");
    }
    Ok(values
        .iter()
        .map(|value| {
            if total_ns == 0 {
                0.0
            } else {
                100.0 * *value as f64 / total_ns as f64
            }
        })
        .collect())
}

#[cfg(test)]
#[path = "schema/tests.rs"]
mod tests;
