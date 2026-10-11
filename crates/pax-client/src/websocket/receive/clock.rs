//! Bracketed cross-clock calibration. No clock is adjusted by this module.
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// A single bounded affine offset model (rate one), with explicit uncertainty.
#[derive(Clone, Copy, Debug)]
pub struct Calibration {
    source_ns: i128,
    monotonic: Instant,
    uncertainty_ns: u64,
    valid: bool,
}
/// Rejection reason; local monotonic measurements remain valid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidClock {
    /// Calibration brackets were reversed or too wide.
    Bracket,
    /// Alignment is older than the caller's finite freshness budget.
    Stale,
    /// A new sample disagrees with the preceding offset beyond the tolerance.
    Step,
    /// Observation or alignment would produce a negative interval.
    Future,
    /// Value cannot be represented by the local monotonic clock.
    Range,
    /// The device rejected a clock request with EBUSY; no fresh sample exists.
    Busy,
    /// Requested background-worker CPU placement was refused.
    WorkerAffinity,
    /// Optional collector exited; its previous model cannot qualify new reads.
    CollectorStopped,
    /// A newer model attempt was not available in the bounded publication ring.
    PublicationLost,
    /// No post-start or post-policy-transition model has been published yet.
    Warming,
    /// A source clock read failed or no calibration has been established.
    Unavailable,
}
impl Calibration {
    /// Construct from a source reading bracketed by same-domain monotonic reads.
    pub fn bracket(source_ns: i128, before: Instant, after: Instant) -> Result<Self, InvalidClock> {
        let span = after
            .checked_duration_since(before)
            .ok_or(InvalidClock::Bracket)?;
        if span > Duration::from_millis(1) {
            return Err(InvalidClock::Bracket);
        }
        Ok(Self {
            source_ns,
            monotonic: before + span / 2,
            uncertainty_ns: span.as_nanos().div_ceil(2) as u64,
            valid: true,
        })
    }
    /// Add an independent clock-domain anchor's conservative uncertainty.
    #[cfg(all(target_os = "linux", feature = "phc-background"))]
    pub(crate) fn with_added_uncertainty(mut self, extra_ns: u64) -> Self {
        self.uncertainty_ns = self.uncertainty_ns.saturating_add(extra_ns);
        self
    }
    #[cfg(all(target_os = "linux", feature = "clock-model-details"))]
    pub(crate) fn details(&self, now: Instant) -> Option<(u64, u64)> {
        Some((
            u64::try_from(now.checked_duration_since(self.monotonic)?.as_nanos()).ok()?,
            self.uncertainty_ns,
        ))
    }
    /// Sample CLOCK_REALTIME through Rust's system clock, without clock mutation.
    pub fn system() -> Result<Self, InvalidClock> {
        let before = Instant::now();
        let source = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| InvalidClock::Range)?
            .as_nanos() as i128;
        Self::bracket(source, before, Instant::now())
    }
    /// Check sequential samples for a step. A detected step invalidates this
    /// update; the caller must obtain a subsequent stable pair before publishing.
    pub fn update(&mut self, mut next: Self, tolerance: Duration) -> Result<(), InvalidClock> {
        let Some(elapsed) = next.monotonic.checked_duration_since(self.monotonic) else {
            self.valid = false;
            return Err(InvalidClock::Step);
        };
        let Some(delta) = next
            .source_ns
            .checked_sub(self.source_ns)
            .and_then(|delta| delta.checked_sub(elapsed.as_nanos() as i128))
        else {
            self.valid = false;
            return Err(InvalidClock::Range);
        };
        let delta = delta.unsigned_abs();
        if delta > u128::from(self.uncertainty_ns + next.uncertainty_ns) + tolerance.as_nanos() {
            next.valid = false;
            *self = next;
            return Err(InvalidClock::Step);
        }
        *self = next;
        Ok(())
    }
    /// Align a timestamp only within a fresh, stable calibration. Uncertainty
    /// includes the bracket and a conservative caller-supplied rate-error budget.
    pub fn align(
        &self,
        source_ns: i128,
        now: Instant,
        max_age: Duration,
        drift_ppm: u64,
    ) -> Result<(Instant, u64), InvalidClock> {
        if !self.valid {
            return Err(InvalidClock::Step);
        }
        let age = now
            .checked_duration_since(self.monotonic)
            .ok_or(InvalidClock::Future)?;
        if age > max_age {
            return Err(InvalidClock::Stale);
        }
        let delta = source_ns
            .checked_sub(self.source_ns)
            .ok_or(InvalidClock::Range)?;
        let magnitude = u64::try_from(delta.unsigned_abs()).map_err(|_| InvalidClock::Range)?;
        if magnitude > max_age.as_nanos().min(u128::from(u64::MAX)) as u64 {
            return Err(InvalidClock::Stale);
        }
        let mapped = if delta < 0 {
            self.monotonic.checked_sub(Duration::from_nanos(magnitude))
        } else {
            self.monotonic.checked_add(Duration::from_nanos(magnitude))
        }
        .ok_or(InvalidClock::Range)?;
        if mapped > now {
            return Err(InvalidClock::Future);
        }
        let drift = age
            .as_nanos()
            .max(u128::from(magnitude))
            .saturating_mul(u128::from(drift_ppm))
            .div_ceil(1_000_000);
        Ok((
            mapped,
            self.uncertainty_ns
                .saturating_add(u64::try_from(drift).unwrap_or(u64::MAX)),
        ))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn out_of_range_source_arithmetic_returns_error_and_invalidates_the_model() {
        let now = Instant::now();
        let mut model = Calibration::bracket(i128::MIN, now, now).unwrap();
        assert_eq!(
            model.align(i128::MAX, now, Duration::from_secs(2), 100),
            Err(InvalidClock::Range)
        );
        assert_eq!(
            model.update(
                Calibration::bracket(i128::MAX, now, now).unwrap(),
                Duration::ZERO
            ),
            Err(InvalidClock::Range)
        );
        assert_eq!(
            model.align(i128::MIN, now, Duration::from_secs(2), 100),
            Err(InvalidClock::Step)
        );
    }
    #[test]
    fn bracket_freshness_steps_and_negative_intervals() {
        let origin = Instant::now();
        let mut c = Calibration::bracket(1000, origin, origin + Duration::from_nanos(20)).unwrap();
        let (mapped, error) = c
            .align(
                990,
                origin + Duration::from_nanos(30),
                Duration::from_secs(1),
                100,
            )
            .unwrap();
        assert_eq!(mapped, origin);
        assert!(error >= 10);
        assert_eq!(
            c.align(2000, origin, Duration::from_secs(1), 100),
            Err(InvalidClock::Future)
        );
        assert_eq!(
            c.align(
                1000,
                origin + Duration::from_secs(2),
                Duration::from_secs(1),
                100
            ),
            Err(InvalidClock::Stale)
        );
        let step = Calibration::bracket(
            1_000_000_000,
            origin + Duration::from_nanos(20),
            origin + Duration::from_nanos(40),
        )
        .unwrap();
        assert_eq!(
            c.update(step, Duration::from_nanos(50)),
            Err(InvalidClock::Step)
        );
        assert_eq!(
            c.align(
                1_000_000_000,
                origin + Duration::from_nanos(50),
                Duration::from_secs(1),
                100
            ),
            Err(InvalidClock::Step)
        );
        let stable = Calibration::bracket(
            1_000_000_020,
            origin + Duration::from_nanos(40),
            origin + Duration::from_nanos(60),
        )
        .unwrap();
        c.update(stable, Duration::from_nanos(50)).unwrap();
        assert!(
            c.align(
                1_000_000_010,
                origin + Duration::from_nanos(70),
                Duration::from_secs(1),
                100
            )
            .is_ok()
        );
        assert_eq!(
            c.update(step, Duration::from_nanos(50)),
            Err(InvalidClock::Step)
        );
        assert_eq!(
            Calibration::bracket(0, origin + Duration::from_nanos(1), origin).err(),
            Some(InvalidClock::Bracket)
        );
    }
}
