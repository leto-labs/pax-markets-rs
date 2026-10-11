//! Connection-owned calibration worker with bounded nonblocking publication.
use super::super::clock::align_stamp;
use super::super::{Calibration, Duration, File, Instant, InvalidClock, Stamp, TimeSpec};
use super::extended::{Method, Sample, Sampler};
use rtrb::{Consumer, RingBuffer};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread::{self, JoinHandle};

struct Signal {
    stop: AtomicBool,
    active: AtomicBool,
    alive: AtomicBool,
    latest: AtomicU64,
}
struct Publication {
    epoch: u64,
    sample: Result<Sample, InvalidClock>,
}

pub(super) struct Worker {
    consumer: Consumer<Publication>,
    signal: Arc<Signal>,
    thread: Option<JoinHandle<()>>,
    deferred: bool,
    active: bool,
    generation: u64,
    epoch: u64,
    minimum_epoch: u64,
    latest: Result<Sample, InvalidClock>,
}
impl Worker {
    pub fn new(
        file: File,
        interval: Duration,
        cpu: Option<usize>,
        deferred: bool,
        active: bool,
    ) -> Self {
        let mut sampler = Sampler::new();
        let mut initialized = false;
        let mut worker = Self::spawn(interval, 128, active, move || {
            if !initialized {
                if cpu.is_some_and(|id| {
                    let mut mask = nix::sched::CpuSet::new();
                    mask.set(id).is_err()
                        || nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &mask)
                            .is_err()
                }) {
                    return Err(InvalidClock::WorkerAffinity);
                }
                initialized = true;
            }
            sampler.sample(&file)
        });
        worker.deferred = deferred;
        worker
    }
    fn spawn(
        interval: Duration,
        capacity: usize,
        active: bool,
        mut sample: impl FnMut() -> Result<Sample, InvalidClock> + Send + 'static,
    ) -> Self {
        let (mut producer, consumer) = RingBuffer::new(capacity);
        let signal = Arc::new(Signal {
            stop: AtomicBool::new(false),
            active: AtomicBool::new(active),
            alive: AtomicBool::new(true),
            latest: AtomicU64::new(0),
        });
        let worker = signal.clone();
        let generation = super::super::clock::Identity::new().generation();
        let handle = thread::Builder::new()
            .name("pax-phc".into())
            .spawn(move || {
                struct Alive(Arc<Signal>);
                impl Drop for Alive {
                    fn drop(&mut self) {
                        self.0.alive.store(false, Ordering::Release);
                    }
                }
                let _alive = Alive(worker.clone());
                let mut model: Option<Calibration> = None;
                let mut epoch = 0u64;
                while !worker.stop.load(Ordering::Acquire) {
                    if !worker.active.load(Ordering::Acquire) {
                        thread::park_timeout(interval);
                        continue;
                    }
                    let began = Instant::now();
                    let result = sample().and_then(|mut next| {
                        if let Some(previous) = &mut model {
                            previous.update(next.calibration, Duration::from_micros(10))?;
                            next.calibration = *previous;
                        } else {
                            model = Some(next.calibration);
                        }
                        Ok(next)
                    });
                    epoch = epoch.saturating_add(1);
                    // An omitted/full publication is explicitly detectable. Ingestion
                    // never treats an older success as the newest failed/stepped model.
                    let _ = producer.push(Publication {
                        epoch,
                        sample: result,
                    });
                    worker.latest.store(epoch, Ordering::Release);
                    if let Some(remaining) = interval.checked_sub(began.elapsed()) {
                        thread::park_timeout(remaining);
                    }
                }
            })
            .ok();
        if handle.is_none() {
            signal.alive.store(false, Ordering::Release);
        }
        Self {
            deferred: false,
            active,
            consumer,
            signal,
            thread: handle,
            generation,
            epoch: 0,
            minimum_epoch: 1,
            latest: Err(InvalidClock::Unavailable),
        }
    }
    pub fn configure(&mut self, active: bool) {
        self.active = active;
        self.signal.active.store(active, Ordering::Release);
        // A policy transition never resurrects an older queued model. Wait for
        // a subsequent calibration attempt; no device operation occurs here.
        self.minimum_epoch = self.signal.latest.load(Ordering::Acquire).saturating_add(1);
        self.latest = Err(InvalidClock::Unavailable);
    }
    fn poll(&mut self) -> Result<Sample, InvalidClock> {
        // The 128-slot ring covers the two-second freshness window at the
        // minimum 20 ms cadence. Take only the newest immutable publication and
        // commit the chunk without work per entry (Publication has no Drop).
        // This is bounded and nonblocking; no collector mutex or device syscall.
        let available = self.consumer.slots();
        if available != 0
            && let Ok(chunk) = self.consumer.read_chunk(available)
        {
            let (first, second) = chunk.as_slices();
            if let Some(publication) = second.last().or_else(|| first.last()) {
                self.epoch = publication.epoch;
                self.latest = publication.sample;
            }
            chunk.commit_all();
        }
        if !self.signal.alive.load(Ordering::Acquire) {
            return Err(InvalidClock::CollectorStopped);
        }
        if !self.active || self.epoch < self.minimum_epoch {
            return Err(InvalidClock::Warming);
        }
        if self.signal.latest.load(Ordering::Acquire) != self.epoch {
            return Err(InvalidClock::PublicationLost);
        }
        self.latest
    }
    pub fn stamp(&mut self, time: TimeSpec, read: Instant) -> Stamp {
        let sample = self.poll();
        let mut model = sample.ok().map(|s| s.calibration);
        let mut stamp = if self.deferred && sample.is_ok() {
            let mut stamp =
                Stamp::unavailable("PHC mapping deferred; original read/model retained");
            stamp.source = "nic-hardware/phc";
            if time.tv_sec() >= 0
                && (0..1_000_000_000).contains(&time.tv_nsec())
                && (time.tv_sec() != 0 || time.tv_nsec() != 0)
            {
                stamp.raw_ns =
                    Some(i128::from(time.tv_sec()) * 1_000_000_000 + i128::from(time.tv_nsec()));
            } else {
                stamp.status = "requested timestamp absent or invalid in ancillary data";
            }
            stamp
        } else {
            align_stamp(
                time,
                "nic-hardware/phc",
                &mut model,
                sample.map(|_| None),
                read,
            )
        };
        if let Ok(sample) = sample
            && stamp.at.is_some()
        {
            stamp.status = match sample.method {
                Method::Driver => {
                    "read-batch timestamp; background driver/monotonic alignment; packet position unspecified"
                }
                Method::User => {
                    "read-batch timestamp; background user-bracket alignment; packet position unspecified"
                }
            };
        }
        // The generation is local to this connection/descriptor and never reused
        // by a reconnect. Exposing qualification metadata is handled separately.
        if let Ok(sample) = sample
            && let Some((age_ns, bracket_uncertainty_ns)) = sample.calibration.details(read)
        {
            stamp.qualification = Some(super::super::super::ClockQualification {
                generation: self.generation,
                epoch: self.epoch,
                method: match sample.method {
                    Method::Driver => super::super::super::ClockMethod::DriverExtended,
                    Method::User => super::super::super::ClockMethod::UserBracket,
                },
                age_ns,
                bracket_uncertainty_ns,
                assumed_drift_ppm: 100,
                mapping_deferred: self.deferred,
                rate_validated: false,
                model: self.deferred.then_some(sample.calibration),
                read: self.deferred.then_some(read),
            });
        } else if self.deferred && sample.is_ok() && stamp.raw_ns.is_some() {
            stamp.status = "PHC model newer than original read; interval withheld";
        }
        stamp
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.signal.stop.store(true, Ordering::Release);
        if let Some(handle) = self.thread.take() {
            handle.thread().unpark();
            // Do not join a device operation on the receive owner's teardown.
            // The worker owns the File until that operation returns and then exits.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn await_epoch(worker: &Worker, epoch: u64) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while worker.signal.latest.load(Ordering::Acquire) < epoch {
            assert!(Instant::now() < deadline, "worker missed deadline");
            thread::sleep(Duration::from_millis(1));
        }
    }
    fn good() -> Result<Sample, InvalidClock> {
        let now = Instant::now();
        Ok(Sample {
            calibration: Calibration::bracket(1_000_000_000, now, now).unwrap(),
            method: Method::Driver,
        })
    }
    #[test]
    fn invalid_cpu_is_reported_without_touching_the_phc() {
        let mut worker = Worker::new(
            File::open("/dev/null").unwrap(),
            Duration::from_millis(100),
            Some(usize::MAX),
            false,
            true,
        );
        await_epoch(&worker, 1);
        assert_eq!(worker.poll().err(), Some(InvalidClock::WorkerAffinity));
    }
    #[test]
    fn worker_keeps_latest_calibration_and_original_read_boundary() {
        let base = Instant::now();
        let mut worker = Worker::spawn(Duration::from_millis(20), 128, true, move || {
            let at = Instant::now();
            let source =
                1_000_000_000 + i128::try_from(at.duration_since(base).as_nanos()).unwrap();
            Ok(Sample {
                calibration: Calibration::bracket(source, at, at).unwrap(),
                method: Method::Driver,
            })
        });
        await_epoch(&worker, 3);
        let read = Instant::now();
        let elapsed = read
            .duration_since(base)
            .saturating_sub(Duration::from_micros(200));
        let time = TimeSpec::new(
            1 + i64::try_from(elapsed.as_secs()).unwrap(),
            i64::from(elapsed.subsec_nanos()),
        );
        let stamp = worker.stamp(time, read);
        assert_eq!(stamp.at, Some(base + elapsed));
        assert_eq!(
            stamp.qualification.unwrap().method,
            super::super::super::super::ClockMethod::DriverExtended
        );
        assert!(stamp.qualification.unwrap().epoch >= 3);
        assert!(read.checked_duration_since(stamp.at.unwrap()).is_some());
        assert_eq!(
            stamp.raw_ns,
            Some(1_000_000_000 + i128::try_from(elapsed.as_nanos()).unwrap())
        );
    }
    #[test]
    fn full_publication_never_reuses_a_superseded_success_or_failure() {
        let mut worker = Worker::spawn(Duration::from_millis(20), 1, true, good);
        await_epoch(&worker, 2);
        assert_eq!(worker.poll().err(), Some(InvalidClock::PublicationLost));
        await_epoch(&worker, 3);
        // Fixed synthetic PHC time steps while Instant advances, so step failure
        // must be preserved rather than returning the old successful model.
        assert_eq!(worker.poll().err(), Some(InvalidClock::Step));
    }
    #[test]
    fn stop_and_reconnect_have_distinct_ownership_without_joining_ingestion() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = Worker::spawn(Duration::from_millis(100), 128, true, move || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            good()
        });
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let signal = worker.signal.clone();
        let generation = worker.generation;
        let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
        let teardown = thread::spawn(move || {
            drop(worker);
            dropped_tx.send(()).unwrap();
        });
        // Teardown must finish while the simulated device read is still blocked.
        // This checks ownership/cancellation, not a scheduler-sensitive latency.
        let dropped = dropped_rx.recv_timeout(Duration::from_secs(1));
        release_tx.send(()).unwrap();
        teardown.join().unwrap();
        dropped.unwrap();
        let next = Worker::spawn(Duration::from_millis(100), 128, true, good);
        assert_ne!(generation, next.generation);
        let deadline = Instant::now() + Duration::from_secs(1);
        while signal.alive.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
    }
    #[test]
    fn failed_clock_read_and_stopped_worker_are_explicitly_unavailable() {
        let mut worker = Worker::spawn(Duration::from_millis(100), 128, true, || {
            Err(InvalidClock::Busy)
        });
        await_epoch(&worker, 1);
        assert_eq!(worker.poll().err(), Some(InvalidClock::Busy));
        worker.signal.alive.store(false, Ordering::Release);
        assert_eq!(worker.poll().err(), Some(InvalidClock::CollectorStopped));
    }
    #[test]
    fn disabled_timestamp_policy_does_not_sample_or_reuse_queued_models() {
        let calls = Arc::new(AtomicU64::new(0));
        let sampled = calls.clone();
        let mut worker = Worker::spawn(Duration::from_millis(20), 128, false, move || {
            sampled.fetch_add(1, Ordering::Relaxed);
            good()
        });
        thread::sleep(Duration::from_millis(60));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        worker.configure(true);
        await_epoch(&worker, 1);
        assert!(worker.poll().is_ok());
        worker.configure(false);
        assert_eq!(worker.poll().err(), Some(InvalidClock::Warming));
        // A read already in flight may finish; after two ticks no new request
        // should start until the timestamp domain is enabled again.
        thread::sleep(Duration::from_millis(60));
        let paused = calls.load(Ordering::Relaxed);
        thread::sleep(Duration::from_millis(60));
        assert_eq!(calls.load(Ordering::Relaxed), paused);
        worker.configure(true);
        assert_eq!(worker.poll().err(), Some(InvalidClock::Warming));
        await_epoch(&worker, paused + 1);
        assert!(worker.poll().is_err()); // Fixed synthetic source detects a step.
    }
}
