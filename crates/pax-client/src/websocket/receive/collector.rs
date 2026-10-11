use super::{
    BatchRange, ProbeWork, ReadObservation, ReceiveConfig, ReceiveTiming, ReceiveWork,
    association::{Association, Observer},
    kernel::Stamp,
};
use crate::measurement::Sampler;
use std::time::Instant;

pub(crate) struct Collector {
    pub config: ReceiveConfig,
    pub selection: super::ReceiveSelection,
    emitted: u64,
    pub generation: u64,
    revision: u64,
    complete_policy_window: bool,
    pub work: ReceiveWork,
    pub stream_total: ProbeWork,
    websocket: Sampler,
    pending_sampler: Sampler,
    latest_read: Option<ReadObservation>,
    #[cfg(all(test, feature = "receive-association"))]
    reads: u64,
    plain: u64,
    pub first: Option<ReadObservation>,
    pub last: Option<(u64, Instant)>,
    window: Option<Association>,
    observer: Option<Observer>,
    ordinal: u64,
    association_lost: u64,
    pending: Option<Option<Instant>>,
    pub software: Stamp,
    pub hardware: Stamp,
}
impl Collector {
    #[cfg(all(test, feature = "receive-association"))]
    pub fn new(config: ReceiveConfig, generation: u64) -> Self {
        Self::with_revision(config, generation, 0)
    }
    pub fn with_revision(config: ReceiveConfig, generation: u64, revision: u64) -> Self {
        Self {
            selection: Default::default(),
            emitted: 0,
            observer: (cfg!(feature = "receive-association")
                && config.policy.association.enabled())
            .then(|| Observer::with_rate(config.capacity, config.policy.association)),
            websocket: Sampler::new(config.policy.websocket).expect("validated policy"),
            pending_sampler: Sampler::new(config.policy.pending).expect("validated policy"),
            latest_read: None,
            config,
            generation,
            revision,
            complete_policy_window: true,
            work: ReceiveWork::default(),
            stream_total: ProbeWork::default(),
            #[cfg(all(test, feature = "receive-association"))]
            reads: 0,
            plain: 0,
            first: None,
            last: None,
            window: None,
            ordinal: 0,
            association_lost: 0,
            pending: None,
            software: Stamp::unavailable("not requested"),
            hardware: Stamp::unavailable("not requested"),
        }
    }
    #[cfg(all(test, feature = "receive-association"))]
    pub fn read(&mut self, now: Instant) {
        self.reads = self.reads.saturating_add(1);
        self.work.reads += 1;
        self.first.get_or_insert(ReadObservation {
            batch: self.reads,
            ended: now,
            software: self.software,
            hardware: self.hardware,
        });
        self.last = Some((self.reads, now));
    }
    pub fn plaintext(&mut self, bytes: &[u8]) {
        if bytes.is_empty() || self.observer.is_none() {
            return;
        }
        self.plain = self.plain.saturating_add(1);
        let mut batch = if let (Some(first), Some(last)) = (self.first.take(), self.last) {
            Association {
                revision: self.revision,
                complete_policy_window: self.complete_policy_window,
                plain: BatchRange {
                    first: self.plain,
                    last: self.plain,
                },
                socket: Some(BatchRange {
                    first: first.batch,
                    last: last.0,
                }),
                first: Some(first.ended),
                first_observed_read: Some(first),
                last: Some(last.1),
                shared: false,
                software: self.software,
                hardware: self.hardware,
            }
        } else if let Some(mut previous) = self.window {
            // Rustls can expose further authenticated plaintext without a new read.
            // Preserve its earlier read window and explicitly mark it shared.
            previous.shared = true;
            previous
        } else {
            Association {
                revision: self.revision,
                complete_policy_window: self.complete_policy_window,
                plain: BatchRange {
                    first: self.plain,
                    last: self.plain,
                },
                socket: None,
                first: None,
                first_observed_read: None,
                last: None,
                shared: false,
                software: self.software,
                hardware: self.hardware,
            }
        };
        batch.plain = BatchRange {
            first: self.plain,
            last: self.plain,
        };
        self.window = Some(batch);
        self.observer
            .as_mut()
            .expect("enabled observer")
            .bytes(bytes, batch);
    }
    pub fn read_observation(&mut self, observation: ReadObservation) {
        self.latest_read = Some(observation);
    }
    pub fn poll_start(&mut self) -> PollProbe {
        let selected = cfg!(feature = "receive-stages") && self.websocket.observe();
        let now = (selected || self.pending.is_some_and(|p| p.is_some())).then(Instant::now);
        self.poll_start_at(selected, now)
    }
    fn poll_start_at(&mut self, selected: bool, now: Option<Instant>) -> PollProbe {
        if let Some(pending) = self.pending.take() {
            let wait = pending
                .zip(now)
                .map(|(start, end)| ns(end.saturating_duration_since(start)));
            self.work.pending.record(wait, wait);
        }
        self.work.polls = self.work.polls.saturating_add(1);
        PollProbe {
            start: selected.then_some(now).flatten(),
            stream_before: self.stream_total,
        }
    }
    pub fn poll_end(&mut self, probe: PollProbe, pending: bool) {
        let wait_selected =
            cfg!(feature = "receive-stages") && pending && self.pending_sampler.observe();
        let now = (probe.start.is_some() || wait_selected).then(Instant::now);
        self.poll_end_at(probe, pending, wait_selected, now);
    }
    fn poll_end_at(
        &mut self,
        probe: PollProbe,
        pending: bool,
        wait_selected: bool,
        now: Option<Instant>,
    ) {
        let elapsed = probe
            .start
            .zip(now)
            .map(|(start, end)| ns(end.saturating_duration_since(start)));
        let exclusive = elapsed
            .zip(self.stream_total.inclusive_since(probe.stream_before))
            .and_then(|(outer, nested)| outer.checked_sub(nested));
        self.work.websocket.record(elapsed, exclusive);
        if pending {
            self.pending = Some(wait_selected.then_some(now).flatten());
        }
    }
    pub fn protocol_message(&mut self, application: bool) -> Option<ReceiveTiming> {
        let work = std::mem::take(&mut self.work).qualify(self.config.policy);
        let latest_read = self.latest_read.take();
        self.pending = None;
        if !application {
            return None;
        }
        self.ordinal = self.ordinal.saturating_add(1);
        let association = self
            .observer
            .as_mut()
            .and_then(|o| o.take(self.ordinal))
            .filter(|a| a.revision == self.revision && a.complete_policy_window);
        let complete_policy_window = std::mem::replace(&mut self.complete_policy_window, true);
        if !self.config.policy.enabled()
            || self.config.sample_every == 0
            || !self.ordinal.is_multiple_of(self.config.sample_every)
        {
            return None;
        }
        self.emitted = self.emitted.saturating_add(1);
        self.selection.websocket =
            cfg!(feature = "receive-stages").then(|| self.websocket.counts());
        self.selection.pending =
            cfg!(feature = "receive-stages").then(|| self.pending_sampler.counts());
        self.selection.association = self.observer.as_ref().map(Observer::counts);
        self.selection.emission = crate::measurement::SelectionCounts {
            eligible: self.ordinal,
            selected: self.emitted,
        };
        Some(ReceiveTiming {
            selection: self.selection,
            revision: self.revision,
            complete_policy_window,
            policy: self.config.policy,
            latest_read,
            first_observed_read: association.and_then(|a| {
                a.first_observed_read
                    .filter(|read| a.socket.is_some_and(|range| range.first == read.batch))
            }),
            generation: self.generation,
            plaintext: association.map(|a| a.plain),
            socket: association.and_then(|a| a.socket),
            first_read: association.and_then(|a| a.first),
            last_read: association.and_then(|a| a.last),
            shared: association.is_some_and(|a| a.shared),
            buffered: work.reads == 0,
            coverage: if self.observer.is_none() {
                "frame association disabled"
            } else if association.is_none()
                && self.config.policy.association != crate::measurement::SampleRate::ALL
            {
                "frame association unsampled or unavailable in this policy window"
            } else if self.observer.as_ref().is_some_and(|o| o.invalid) {
                "frame observer unavailable"
            } else if association.is_none() {
                "association unsampled, lost or outside current policy window"
            } else {
                "plaintext byte ranges; observed adapter-read window; prior TLS bytes may be buffered"
            },
            work,
            lost: self
                .association_lost
                .saturating_add(self.observer.as_ref().map_or(0, |o| o.lost)),
            sample_every: self.config.sample_every,
            software: association.map_or(Stamp::unavailable("association unavailable"), |a| {
                a.software
            }),
            hardware: association.map_or(Stamp::unavailable("association unavailable"), |a| {
                a.hardware
            }),
        })
    }
    pub fn configure(&mut self, config: ReceiveConfig, revision: u64) -> bool {
        let partial = self.work.polls != 0 || self.pending.is_some() || self.first.is_some();
        self.websocket
            .configure(config.policy.websocket)
            .expect("validated policy");
        self.pending_sampler
            .configure(config.policy.pending)
            .expect("validated policy");
        if config.policy.association.enabled() {
            if let Some(observer) = &mut self.observer {
                observer.configure(config.policy.association);
            }
        } else if let Some(observer) = self.observer.take() {
            self.association_lost = self
                .association_lost
                .saturating_add(observer.lost)
                .saturating_add(observer.ready.len() as u64);
        }
        self.work = ReceiveWork::default();
        self.pending = None;
        self.latest_read = None;
        self.window = None;
        self.first = None;
        self.last = None;
        self.software = Stamp::unavailable("policy boundary");
        self.hardware = Stamp::unavailable("policy boundary");
        self.complete_policy_window = !partial;
        self.revision = revision;
        self.config = config;
        partial
    }
    pub fn handshake_done(&mut self) {
        self.work = ReceiveWork::default();
        self.pending = None;
        self.latest_read = None;
    }
}
pub(crate) struct PollProbe {
    start: Option<Instant>,
    stream_before: ProbeWork,
}
pub(super) fn ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(all(test, feature = "receive-association"))]
mod tests {
    use super::*;
    #[test]
    fn several_reads_before_plaintext_preserve_initial_unselected_metadata() {
        let mut c = Collector::new(ReceiveConfig::default(), 3);
        c.read(Instant::now());
        c.plaintext(b"HTTP/1.1 101\r\n\r\n");
        c.handshake_done();
        c.software = Stamp::unavailable("initial read not selected");
        let initial = Instant::now();
        c.read(initial);
        c.software = Stamp {
            at: Some(initial),
            raw_ns: Some(99),
            uncertainty_ns: Some(1),
            #[cfg(feature = "clock-model-details")]
            qualification: None,
            source: "kernel-software/realtime",
            status: "later selected read",
        };
        c.read(Instant::now());
        c.plaintext(b"\x81\x01a");
        let timing = c.protocol_message(true).unwrap();
        let first = timing.first_observed_read.unwrap();
        assert_eq!(first.batch, 2);
        assert_eq!(first.ended, initial);
        assert!(first.software.at.is_none());
        assert_eq!(first.software.status, "initial read not selected");
        assert_eq!(timing.software.raw_ns, Some(99));
        assert_eq!(timing.socket.unwrap().last, 3);
        c.configure(ReceiveConfig::default(), 1);
        assert!(
            c.protocol_message(true)
                .unwrap()
                .first_observed_read
                .is_none()
        );
    }
    #[test]
    fn sampling_and_generation_are_separate_from_byte_observation() {
        let config = ReceiveConfig {
            sample_every: 2,
            capacity: 1,
            ..ReceiveConfig::default()
        };
        let mut c = Collector::new(config.clone(), 7);
        c.read(Instant::now());
        c.plaintext(b"HTTP/1.1 101\r\n\r\n\x81\x01a\x81\x01b");
        assert!(c.protocol_message(true).is_none());
        let t = c.protocol_message(true).unwrap();
        assert_eq!(t.generation, 7);
        assert_eq!(
            t.selection.emission,
            crate::measurement::SelectionCounts {
                eligible: 2,
                selected: 1
            }
        );
        assert_eq!(
            t.selection.association.unwrap(),
            crate::measurement::SelectionCounts {
                eligible: 2,
                selected: 2
            }
        );
        assert_eq!(t.lost, 1);
        assert!(t.buffered);
        assert_eq!(t.work.reads, 0);
        assert_eq!(t.plaintext.unwrap().first, 1);
        let mut other = Collector::new(config, 8);
        assert!(other.protocol_message(true).is_none());
        let t = other.protocol_message(true).unwrap();
        assert_eq!(t.generation, 8);
        assert!(t.plaintext.is_none());
        assert_eq!(t.lost, 0);
    }
}

#[cfg(all(test, feature = "receive-stages"))]
mod poll_tests {
    use super::super::policy::ReceivePolicy;
    use super::*;
    use std::time::Duration;
    #[test]
    fn zero_emission_keeps_layer_sampling_and_resets_work_before_reenable() {
        let mut c = Collector::new(
            ReceiveConfig {
                sample_every: 0,
                ..Default::default()
            },
            1,
        );
        c.config.validate().unwrap();
        let probe = c.poll_start();
        assert!(probe.start.is_some()); // Layer clocks remain selected independently.
        c.work.socket.record(Some(5), Some(5));
        c.poll_end(probe, false);
        assert!(c.protocol_message(true).is_none());
        assert_eq!(c.ordinal, 1);
        assert_eq!(c.emitted, 0);
        assert_eq!(c.work.polls, 0);
        assert_eq!(c.websocket.counts().selected, 1);
        let config = ReceiveConfig {
            sample_every: 1,
            ..c.config.clone()
        };
        assert!(!c.configure(config, 7));
        let timing = c.protocol_message(true).unwrap();
        assert_eq!(timing.revision, 7);
        assert_eq!(timing.selection.emission.eligible, 2);
        assert_eq!(timing.selection.emission.selected, 1);
        assert_eq!(timing.work.polls, 0);
        assert!(timing.latest_read.is_none());
        assert!(timing.first_observed_read.is_none());
    }

    #[test]
    fn active_nested_work_and_suspension_are_separate() {
        let mut c = Collector::new(ReceiveConfig::default(), 1);
        let start = Instant::now();
        let probe = c.poll_start_at(true, Some(start));
        c.stream_total.record(Some(20), Some(15));
        c.work.socket.record(Some(5), Some(5));
        c.work.tls.record(Some(20), Some(15));
        c.poll_end_at(probe, true, true, Some(start + Duration::from_nanos(30)));
        let probe = c.poll_start_at(true, Some(start + Duration::from_nanos(1030)));
        c.poll_end_at(
            probe,
            false,
            false,
            Some(start + Duration::from_nanos(1040)),
        );
        let trace = c.protocol_message(true).unwrap();
        assert_eq!(trace.work.pending_ns, Some(1000));
        assert_eq!(
            trace.work.socket_ns.unwrap()
                + trace.work.tls_ns.unwrap()
                + trace.work.websocket_ns.unwrap(),
            40
        );
        let buffered = c.protocol_message(true).unwrap();
        assert_eq!(buffered.work.polls, 0);
        assert_eq!(buffered.work.tls_ns, Some(0));
    }
    #[test]
    fn disabled_probes_do_not_request_clocks_or_allocate_an_observer() {
        let mut c = Collector::new(
            ReceiveConfig {
                policy: ReceivePolicy::OFF,
                ..ReceiveConfig::default()
            },
            1,
        );
        assert!(c.observer.is_none());
        let probe = c.poll_start();
        assert!(probe.start.is_none());
        c.poll_end(probe, true);
        assert_eq!(c.pending, Some(None));
        let probe = c.poll_start();
        c.poll_end(probe, false);
        assert_eq!(c.work.websocket.samples, 0);
        assert!(c.protocol_message(true).is_none());
    }
    #[test]
    fn missing_inner_probe_retains_inclusive_work_without_fabricated_exclusive_time() {
        let mut c = Collector::new(ReceiveConfig::default(), 1);
        let now = Instant::now();
        let probe = c.poll_start_at(true, Some(now));
        c.stream_total.record(None, None);
        c.poll_end_at(probe, false, false, Some(now + Duration::from_nanos(20)));
        let work = c.protocol_message(true).unwrap().work;
        assert_eq!(work.websocket.sampled_ns, 20);
        assert_eq!(work.websocket_ns, None);
    }
}
