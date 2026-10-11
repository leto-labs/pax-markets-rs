//! Independently selected processing work, unaffected by export/display cadence.
use pax_client::measurement::{SampleRate, Sampler};
use serde::{Deserialize, Serialize};

/// Optional application probe domains. Each denominator counts complete messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookPolicy {
    /// Validation before mutation.
    pub preflight: SampleRate,
    /// Level updates after successful preflight.
    pub mutation: SampleRate,
    /// Post-mutation top-of-book consistency checks.
    pub validation: SampleRate,
}
impl BookPolicy {
    /// Select each substage independently at the same initial rate.
    pub const fn every(every: u64) -> Self {
        Self {
            preflight: SampleRate::every(every),
            mutation: SampleRate::every(every),
            validation: SampleRate::every(every),
        }
    }
}
/// Optional application probes. All selection denominators count complete messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessingPolicy {
    /// Decode-through-commit interval and receive-to-callback delay.
    pub callback: SampleRate,
    /// JSON/protocol decoder interval.
    pub decode: SampleRate,
    /// Deterministic book preflight, mutation and validation boundaries.
    pub book: BookPolicy,
    /// Post-commit callback bookkeeping and next already-ready callback gap.
    pub bookkeeping: SampleRate,
}
impl ProcessingPolicy {
    /// Disable all optional processing clocks.
    pub const OFF: Self = Self {
        callback: SampleRate::OFF,
        decode: SampleRate::OFF,
        book: BookPolicy::every(0),
        bookkeeping: SampleRate::OFF,
    };
    /// Preserve the legacy common-rate preset. Zero disables optional probes.
    pub const fn every(every: u64) -> Self {
        Self {
            callback: SampleRate::every(every),
            decode: SampleRate::every(every),
            book: BookPolicy::every(every),
            bookkeeping: SampleRate::every(every),
        }
    }
    /// Validate the entire update before changing any domain.
    pub fn validate(self) -> Result<(), &'static str> {
        for rate in [
            self.callback,
            self.decode,
            self.book.preflight,
            self.book.mutation,
            self.book.validation,
            self.bookkeeping,
        ] {
            rate.validate()?;
        }
        if !cfg!(feature = "processing-traces") && self != Self::OFF {
            return Err("processing-traces probes compiled out");
        }
        Ok(())
    }
}
impl Default for ProcessingPolicy {
    fn default() -> Self {
        if cfg!(feature = "processing-traces") {
            Self::every(1)
        } else {
            Self::OFF
        }
    }
}
/// Successful update boundary. Earlier retained samples keep their revision.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct ProcessingApplied {
    /// Monotonic pipeline policy revision; startup is zero.
    pub revision: u64,
    /// Count of complete messages processed before the new policy became active.
    pub after_message: u64,
}
pub(crate) struct Probes {
    pub policy: ProcessingPolicy,
    pub revision: u64,
    callback: Sampler,
    decode: Sampler,
    preflight: Sampler,
    mutation: Sampler,
    validation: Sampler,
    bookkeeping: Sampler,
}
pub(crate) struct Selection {
    pub callback: bool,
    pub decode: bool,
    pub preflight: bool,
    pub mutation: bool,
    pub validation: bool,
    pub bookkeeping: bool,
}
impl Selection {
    pub fn book(&self) -> bool {
        self.preflight || self.mutation || self.validation
    }
    pub fn any(&self) -> bool {
        self.callback || self.decode || self.book() || self.bookkeeping
    }
}
impl Probes {
    pub fn new(policy: ProcessingPolicy) -> Result<Self, &'static str> {
        policy.validate()?;
        Ok(Self {
            policy,
            revision: 0,
            callback: Sampler::new(policy.callback)?,
            decode: Sampler::new(policy.decode)?,
            preflight: Sampler::new(policy.book.preflight)?,
            mutation: Sampler::new(policy.book.mutation)?,
            validation: Sampler::new(policy.book.validation)?,
            bookkeeping: Sampler::new(policy.bookkeeping)?,
        })
    }
    pub fn configure(&mut self, policy: ProcessingPolicy) -> Result<u64, &'static str> {
        policy.validate()?;
        let revision = self
            .revision
            .checked_add(1)
            .ok_or("processing policy revision exhausted")?;
        self.callback.configure(policy.callback)?;
        self.decode.configure(policy.decode)?;
        self.preflight.configure(policy.book.preflight)?;
        self.mutation.configure(policy.book.mutation)?;
        self.validation.configure(policy.book.validation)?;
        self.bookkeeping.configure(policy.bookkeeping)?;
        self.policy = policy;
        self.revision = revision;
        Ok(revision)
    }
    pub fn select(&mut self) -> Selection {
        if !cfg!(feature = "processing-traces") {
            return Selection {
                callback: false,
                decode: false,
                preflight: false,
                mutation: false,
                validation: false,
                bookkeeping: false,
            };
        }
        Selection {
            callback: self.callback.observe(),
            decode: self.decode.observe(),
            preflight: self.preflight.observe(),
            mutation: self.mutation.observe(),
            validation: self.validation.observe(),
            bookkeeping: self.bookkeeping.observe(),
        }
    }

    /// Receive emission restarts at each connection. Restart processing selection
    /// at the same boundary, preserving policy revision and global trace identity.
    pub fn restart_connection(&mut self) -> Result<(), &'static str> {
        let mut next = Self::new(self.policy)?;
        next.revision = self.revision;
        *self = next;
        Ok(())
    }
}

/// Selection counts since this domain's last rate change or connection. A selected message can
/// still lack a book substage (for example, a control or a rejected preflight).
#[derive(Clone, Copy, Debug, Serialize)]
pub struct DomainCount {
    /// Complete messages considered by this domain.
    pub eligible: u64,
    /// Messages selected before optional markers were read.
    pub selected: u64,
}
impl From<&Sampler> for DomainCount {
    fn from(s: &Sampler) -> Self {
        Self {
            eligible: s.eligible(),
            selected: s.selected(),
        }
    }
}
/// Independent effective policy and selection denominators.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct ProcessingStatus {
    /// Current pipeline policy revision.
    pub revision: u64,
    /// Current per-domain rates.
    pub policy: ProcessingPolicy,
    /// Callback selections.
    pub callback: DomainCount,
    /// Decoder selections.
    pub decode: DomainCount,
    /// Preflight selections.
    pub preflight: DomainCount,
    /// Mutation selections.
    pub mutation: DomainCount,
    /// Post-mutation validation selections.
    pub validation: DomainCount,
    /// Callback-tail selections.
    pub bookkeeping: DomainCount,
}
impl Probes {
    pub fn status(&self) -> ProcessingStatus {
        ProcessingStatus {
            revision: self.revision,
            policy: self.policy,
            callback: (&self.callback).into(),
            decode: (&self.decode).into(),
            preflight: (&self.preflight).into(),
            mutation: (&self.mutation).into(),
            validation: (&self.validation).into(),
            bookkeeping: (&self.bookkeeping).into(),
        }
    }
}
