//! Connection-local, bounded-rate PHC calibration. Failed reads are never cached
//! as successful alignment. Optional worker ownership never changes the NIC.
#[cfg(feature = "phc-background")]
mod extended;
#[cfg(feature = "phc-background")]
mod worker;
#[cfg(test)]
use super::clock::align_stamp;
use super::clock::{Identity, align_stamp_with_identity, device_sample};
use super::{Calibration, Duration, File, Instant, InvalidClock, Stamp, TimeSpec};

pub(super) struct PhcClock(Mode);
enum Mode {
    Sampled(SampledClock),
    #[cfg(feature = "phc-background")]
    Background(worker::Worker),
}
impl PhcClock {
    pub(super) fn new(file: File, interval: Duration) -> Self {
        Self(Mode::Sampled(SampledClock::new(file, interval)))
    }
    #[cfg(feature = "phc-background")]
    pub(super) fn background(
        file: File,
        interval: Duration,
        cpu: Option<usize>,
        deferred: bool,
        active: bool,
    ) -> Self {
        Self(Mode::Background(worker::Worker::new(
            file, interval, cpu, deferred, active,
        )))
    }
    pub(super) fn configure(&mut self, active: bool) {
        #[cfg(feature = "phc-background")]
        if let Mode::Background(worker) = &mut self.0 {
            worker.configure(active);
        }
        #[cfg(not(feature = "phc-background"))]
        let _ = active;
    }
    pub(super) fn stamp(&mut self, time: TimeSpec, read: Instant) -> Stamp {
        match &mut self.0 {
            Mode::Sampled(clock) => clock.stamp(time, read),
            #[cfg(feature = "phc-background")]
            Mode::Background(worker) => worker.stamp(time, read),
        }
    }
}

struct SampledClock {
    file: File,
    model: Option<Calibration>,
    refresh: Refresh,
}
impl SampledClock {
    fn new(file: File, interval: Duration) -> Self {
        Self {
            file,
            model: None,
            refresh: Refresh {
                interval,
                attempted: None,
                failure: None,
                identity: Identity::new(),
            },
        }
    }
    fn stamp(&mut self, time: TimeSpec, read: Instant) -> Stamp {
        // Missing or malformed ancillary data requires no device request.
        let next = if time.tv_sec() < 0
            || !(0..1_000_000_000).contains(&time.tv_nsec())
            || (time.tv_sec() == 0 && time.tv_nsec() == 0)
        {
            Err(InvalidClock::Unavailable)
        } else {
            self.refresh.sample(read, || device_sample(&self.file))
        };
        align_stamp_with_identity(
            time,
            "nic-hardware/phc",
            &mut self.model,
            next,
            read,
            Some(&self.refresh.identity),
        )
    }
}

struct Refresh {
    interval: Duration,
    attempted: Option<Instant>,
    failure: Option<InvalidClock>,
    identity: Identity,
}
impl Refresh {
    fn sample(
        &mut self,
        now: Instant,
        read: impl FnOnce() -> Result<Calibration, InvalidClock>,
    ) -> Result<Option<Calibration>, InvalidClock> {
        if self.attempted.is_some_and(|previous| {
            now.checked_duration_since(previous)
                .is_some_and(|age| age < self.interval)
        }) {
            return self.failure.map_or(Ok(None), Err);
        }
        self.attempted = Some(now);
        self.identity.attempted();
        let next = read();
        self.failure = next.as_ref().err().copied();
        next.map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[cfg(feature = "clock-model-details")]
    #[test]
    fn local_model_details_match_alignment_and_actual_attempts_without_extra_requests() {
        let base = Instant::now() - Duration::from_millis(100);
        let calls = Cell::new(0);
        let mut refresh = Refresh {
            interval: Duration::from_millis(20),
            attempted: None,
            failure: None,
            identity: Identity::new(),
        };
        let mut model = None;
        let calibration =
            Calibration::bracket(1_000_000_000, base - Duration::from_nanos(20), base).unwrap();
        let next = refresh
            .sample(base, || {
                calls.set(calls.get() + 1);
                Ok(calibration)
            })
            .unwrap();
        fn stamp(
            model: &mut Option<Calibration>,
            next: Result<Option<Calibration>, InvalidClock>,
            read: Instant,
            identity: &Identity,
        ) -> Stamp {
            align_stamp_with_identity(
                TimeSpec::new(1, 0),
                "nic-hardware/phc",
                model,
                next,
                read,
                Some(identity),
            )
        }
        let first = stamp(&mut model, Ok(next), base, &refresh.identity);
        let details = first.qualification().unwrap();
        assert_eq!(details.epoch, 1);
        assert_eq!(details.bracket_uncertainty_ns, 10);
        assert_eq!(details.assumed_drift_ppm, 100);
        assert!(!details.rate_validated && !details.mapping_deferred);
        assert_eq!(
            first.uncertainty_ns.unwrap(),
            10 + details.age_ns.saturating_mul(100).div_ceil(1_000_000)
        );
        let cached = refresh.sample(base + Duration::from_millis(1), || {
            panic!("cached annotation must not request a clock")
        });
        let cached = stamp(&mut model, cached, base, &refresh.identity);
        let cached_details = cached.qualification().unwrap();
        assert_eq!(cached.at, first.at);
        assert_eq!(cached_details.generation, details.generation);
        assert_eq!(cached_details.epoch, details.epoch);
        assert!(cached_details.age_ns >= details.age_ns);
        let failed = refresh.sample(base + Duration::from_millis(21), || {
            calls.set(calls.get() + 1);
            Err(InvalidClock::Busy)
        });
        let failed = stamp(&mut model, failed, base, &refresh.identity);
        assert!(failed.at.is_none() && failed.qualification().is_none());
        let cached_failure = refresh.sample(base + Duration::from_millis(22), || {
            panic!("cached failed attempt must not request a clock")
        });
        assert!(
            stamp(&mut model, cached_failure, base, &refresh.identity)
                .qualification()
                .is_none()
        );
        let retry = refresh.sample(base + Duration::from_millis(41), || {
            calls.set(calls.get() + 1);
            Ok(calibration)
        });
        // Reusing the identical offset at the identical anchor is stable; the
        // attempted-read epoch advances independently of cached observations.
        let retried = stamp(&mut model, retry, base, &refresh.identity);
        assert_eq!(retried.qualification().unwrap().epoch, 3);
        assert_eq!(calls.get(), 3);
    }

    #[test]
    fn clock_requests_are_bounded_and_failed_reads_stay_unavailable_until_retry() {
        let base = Instant::now();
        let calls = Cell::new(0);
        let mut refresh = Refresh {
            interval: Duration::from_millis(20),
            attempted: None,
            failure: None,
            identity: Identity::new(),
        };
        let mut sample = |at, result| {
            refresh.sample(at, || {
                calls.set(calls.get() + 1);
                result
            })
        };
        let calibration = Calibration::bracket(1_000_000, base, base).unwrap();
        assert!(sample(base, Ok(calibration)).unwrap().is_some());
        for i in 1..20 {
            assert!(
                sample(base + Duration::from_millis(i), Err(InvalidClock::Busy))
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(calls.get(), 1);
        assert_eq!(
            sample(base + Duration::from_millis(20), Err(InvalidClock::Busy)).err(),
            Some(InvalidClock::Busy)
        );
        assert_eq!(
            sample(base + Duration::from_millis(39), Ok(calibration)).err(),
            Some(InvalidClock::Busy)
        );
        assert_eq!(calls.get(), 2);
        assert!(
            sample(base + Duration::from_millis(40), Ok(calibration))
                .unwrap()
                .is_some()
        );
        assert_eq!(calls.get(), 3);
        assert_eq!(
            sample(
                base + Duration::from_millis(60),
                Err(InvalidClock::Unavailable)
            )
            .err(),
            Some(InvalidClock::Unavailable)
        );
        assert_eq!(
            sample(base + Duration::from_millis(61), Ok(calibration)).err(),
            Some(InvalidClock::Unavailable)
        );
    }

    #[test]
    fn zero_interval_keeps_the_explicit_per_read_experiment() {
        let base = Instant::now();
        let mut refresh = Refresh {
            interval: Duration::ZERO,
            attempted: None,
            failure: None,
            identity: Identity::new(),
        };
        for _ in 0..10 {
            assert!(
                refresh
                    .sample(base, || Calibration::bracket(100, base, base))
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[test]
    fn initial_busy_clock_can_retry_and_missing_metadata_does_not_read_device() {
        let now = Instant::now();
        let mut clock =
            SampledClock::new(File::open("/dev/null").unwrap(), Duration::from_millis(20));
        let absent = clock.stamp(TimeSpec::new(0, 0), now);
        assert!(absent.at.is_none() && clock.refresh.attempted.is_none());
        assert!(clock.stamp(TimeSpec::new(-1, 0), now).at.is_none());
        assert!(clock.refresh.attempted.is_none());
        let failed = clock.stamp(TimeSpec::new(1, 0), now);
        assert_eq!(clock.refresh.failure, Some(InvalidClock::Unavailable));
        assert!(failed.at.is_none());
        let mut refresh = Refresh {
            interval: Duration::from_millis(20),
            attempted: None,
            failure: None,
            identity: Identity::new(),
        };
        assert_eq!(
            refresh.sample(now, || Err(InvalidClock::Busy)).err(),
            Some(InvalidClock::Busy)
        );
        let later = now + Duration::from_millis(20);
        assert!(
            refresh
                .sample(later, || Calibration::bracket(1_020_000_000, later, later))
                .unwrap()
                .is_some()
        );
        assert!(refresh.failure.is_none());
    }

    #[test]
    fn cached_alignment_has_drift_uncertainty_and_rejects_absent_stale_steps_and_future() {
        let now = Instant::now();
        let base = now - Duration::from_millis(20);
        let calibration = Calibration::bracket(1_000_000_000, base, base).unwrap();
        let mut model = Some(calibration);
        let time = TimeSpec::new(1, 1_000_000);
        let cached = align_stamp(time, "nic-hardware/phc", &mut model, Ok(None), now);
        assert_eq!(cached.at, Some(base + Duration::from_millis(1)));
        assert!(cached.uncertainty_ns.unwrap() >= 2000);
        assert!(cached.status.contains("cached PHC alignment"));
        assert!(
            align_stamp(time, "test", &mut None, Ok(None), now)
                .at
                .is_none()
        );
        let stale = Calibration::bracket(
            1_000_000_000,
            now - Duration::from_secs(3),
            now - Duration::from_secs(3),
        )
        .unwrap();
        assert!(
            align_stamp(time, "test", &mut Some(stale), Ok(None), now)
                .status
                .contains("stale")
        );
        assert!(
            align_stamp(
                TimeSpec::new(1, 30_000_000),
                "test",
                &mut model,
                Ok(None),
                now
            )
            .status
            .contains("future")
        );
        let step = Calibration::bracket(2_000_000_000, now, now).unwrap();
        assert!(
            align_stamp(time, "test", &mut model, Ok(Some(step)), now)
                .status
                .contains("discontinuity")
        );
        assert!(
            align_stamp(time, "test", &mut model, Ok(None), now)
                .status
                .contains("discontinuity")
        );
        assert!(
            align_stamp(time, "test", &mut model, Err(InvalidClock::Busy), now)
                .status
                .contains("EBUSY")
        );
        assert!(
            align_stamp(
                time,
                "test",
                &mut model,
                Err(InvalidClock::Unavailable),
                now
            )
            .at
            .is_none()
        );
    }
}
