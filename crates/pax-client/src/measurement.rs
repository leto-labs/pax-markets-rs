//! Allocation-free sampling primitives shared by transport and application probes.
pub mod build_info;
pub mod capabilities;
#[cfg(feature = "diagnostic-handoff")]
pub mod queue;
pub mod schema;
pub mod status;
use serde::{Deserialize, Serialize};

/// Select a finite window of ordinal positions within a repeating period.
/// Ordinals are one-based. `every(8)` selects 8, 16, 24, ...; zero disables work.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SampleRate {
    /// Eligible operations per period, zero when disabled.
    pub every: u64,
    /// Consecutive positions selected per period; zero when disabled.
    pub width: u64,
    /// First selected position in the period, less than `every`.
    pub phase: u64,
}
impl SampleRate {
    /// No optional observation work.
    pub const OFF: Self = Self {
        every: 0,
        width: 0,
        phase: 0,
    };
    /// Observe every eligible operation.
    pub const ALL: Self = Self {
        every: 1,
        width: 1,
        phase: 0,
    };
    /// Observe one operation per period; zero returns the disabled policy.
    pub const fn every(every: u64) -> Self {
        Self {
            every,
            width: if every == 0 { 0 } else { 1 },
            phase: 0,
        }
    }
    /// Validate bounded settings before entering the input loop.
    pub fn validate(self) -> Result<(), &'static str> {
        if self == Self::OFF {
            return Ok(());
        }
        if self.every == 0
            || self.every > 1_000_000_000
            || self.width == 0
            || self.width > self.every
            || self.phase > self.every - self.width
        {
            return Err("invalid measurement period, width or phase");
        }
        Ok(())
    }
    /// Whether the domain requests observations.
    pub const fn enabled(self) -> bool {
        self.every != 0
    }
    /// Stateless selection for an existing canonical ordinal.
    pub fn selects(self, ordinal: u64) -> bool {
        if self.every == 0 {
            return false;
        }
        let position = ordinal % self.every;
        position >= self.phase && position - self.phase < self.width
    }
}

/// Single-owner selection state. Policy and counters contain no locks or clocks.
#[derive(Clone, Debug)]
pub struct Sampler {
    rate: SampleRate,
    position: u64,
    eligible: u64,
    selected: u64,
}
impl Sampler {
    /// Construct after validating an externally supplied policy.
    pub fn new(rate: SampleRate) -> Result<Self, &'static str> {
        rate.validate()?;
        Ok(Self {
            rate,
            position: 0,
            eligible: 0,
            selected: 0,
        })
    }
    /// Decide before reading any optional clock. Uses no division in the loop.
    pub fn observe(&mut self) -> bool {
        self.eligible = self.eligible.saturating_add(1);
        if !self.rate.enabled() {
            return false;
        }
        self.position += 1;
        if self.position == self.rate.every {
            self.position = 0;
        }
        let sampled =
            self.position >= self.rate.phase && self.position - self.rate.phase < self.rate.width;
        self.selected = self.selected.saturating_add(u64::from(sampled));
        sampled
    }
    /// Change only this domain. An unchanged policy preserves its sample phase.
    /// Changed policies reset their own counters at the caller's revision boundary.
    pub fn configure(&mut self, rate: SampleRate) -> Result<(), &'static str> {
        if self.rate != rate {
            *self = Self::new(rate)?;
        }
        Ok(())
    }
    /// Effective selection policy.
    pub const fn rate(&self) -> SampleRate {
        self.rate
    }
    /// Eligible operations since the current policy became effective.
    pub const fn eligible(&self) -> u64 {
        self.eligible
    }
    /// Selected operations since the current policy became effective.
    pub const fn selected(&self) -> u64 {
        self.selected
    }
}

/// Independent selection counts; no extrapolation from an emitted sample window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct SelectionCounts {
    /// Operations considered by this domain since its own policy changed.
    pub eligible: u64,
    /// Operations selected by this domain in that same population.
    pub selected: u64,
}
impl Sampler {
    /// A numeric copy, requiring no optional clock, lock or allocation.
    pub const fn counts(&self) -> SelectionCounts {
        SelectionCounts {
            eligible: self.eligible,
            selected: self.selected,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn windows_and_independent_updates_preserve_other_domains() {
        for rate in [
            SampleRate::OFF,
            SampleRate::ALL,
            SampleRate::every(8),
            SampleRate {
                every: 11,
                width: 3,
                phase: 4,
            },
        ] {
            let mut sampler = Sampler::new(rate).unwrap();
            for ordinal in 1..=1000 {
                assert_eq!(sampler.observe(), rate.selects(ordinal));
            }
            assert_eq!(sampler.eligible(), 1000);
        }
        let mut first = Sampler::new(SampleRate::every(2)).unwrap();
        let mut second = Sampler::new(SampleRate::every(3)).unwrap();
        assert!(!first.observe());
        assert!(!second.observe());
        first.configure(SampleRate::OFF).unwrap();
        second.configure(SampleRate::every(3)).unwrap();
        assert!(!first.observe());
        assert!(!second.observe());
        assert!(second.observe());
        assert_eq!(second.selected(), 1);
    }
    #[test]
    fn invalid_policy_does_not_modify_effective_sampling() {
        let mut sampler = Sampler::new(SampleRate::every(2)).unwrap();
        assert!(!sampler.observe());
        assert!(
            sampler
                .configure(SampleRate {
                    every: 2,
                    width: 2,
                    phase: 1
                })
                .is_err()
        );
        assert_eq!(sampler.rate(), SampleRate::every(2));
        assert!(sampler.observe());
    }
}
