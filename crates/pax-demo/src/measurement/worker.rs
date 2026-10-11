//! Receive joins and finite observation windows owned by report preparation.
use super::{receive::ReceiveTrace, trace::TraceWindow};
#[cfg(feature = "telemetry-export")]
use pax_client::websocket::receive::DiagnosticReceiver;
use serde::Serialize;
use std::{collections::VecDeque, time::Instant};

/// Qualified coverage of deferred receive observations; never canonical capture.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct DiagnosticStatus {
    /// Independent producer/consumer counters. Full and failed workers stay visible.
    pub queue: pax_client::measurement::status::QueueStatus,
    /// Samples displaced by the report worker's finite retained window.
    pub window_evictions: u64,
    /// Samples expired from the display window; export may still contain them.
    pub window_age_expirations: u64,
    /// Samples rejected because their original clock boundary preceded this run.
    pub invalid_clock: u64,
    /// Original message receipt to latest worker consumption. Not event latency.
    pub last_handoff_ns: Option<u64>,
    /// Time since the latest consumed observation's original receipt, at publication.
    pub observation_age_ns: Option<u64>,
}
pub(crate) struct ReceiveWorker {
    #[cfg(feature = "telemetry-export")]
    receiver: Option<DiagnosticReceiver>,
    window: VecDeque<ReceiveTrace>,
    #[cfg(feature = "telemetry-export")]
    origin: Instant,
    evictions: u64,
    age_expirations: u64,
    #[cfg(feature = "telemetry-export")]
    invalid_clock: u64,
    #[cfg(feature = "telemetry-export")]
    latest: Option<Instant>,
    #[cfg(feature = "telemetry-export")]
    last_handoff_ns: Option<u64>,
}
impl ReceiveWorker {
    pub fn disabled(_origin: Instant) -> Self {
        Self {
            #[cfg(feature = "telemetry-export")]
            receiver: None,
            window: VecDeque::new(),
            evictions: 0,
            age_expirations: 0,
            #[cfg(feature = "telemetry-export")]
            origin: _origin,
            #[cfg(feature = "telemetry-export")]
            invalid_clock: 0,
            #[cfg(feature = "telemetry-export")]
            latest: None,
            #[cfg(feature = "telemetry-export")]
            last_handoff_ns: None,
        }
    }
    #[cfg(feature = "telemetry-export")]
    pub fn prepared(origin: Instant) -> Self {
        Self {
            window: super::receive::prefaulted_window(2048),
            ..Self::disabled(origin)
        }
    }
    #[cfg(feature = "telemetry-export")]
    pub fn connect(&mut self, receiver: DiagnosticReceiver) {
        self.receiver = Some(receiver);
    }
    pub fn register(&self, _status: &super::status::Diagnostics) {
        #[cfg(feature = "telemetry-export")]
        if let Some(receiver) = &self.receiver {
            _status.receive(receiver.status());
        }
    }
    pub fn enabled(&self) -> bool {
        #[cfg(feature = "telemetry-export")]
        {
            self.receiver.is_some()
        }
        #[cfg(not(feature = "telemetry-export"))]
        {
            false
        }
    }
    /// Bound every worker turn; no timestamp here replaces an input timestamp.
    /// The callback is exclusively worker-owned (for optional export).
    pub fn drain(&mut self, mut export: impl FnMut(ReceiveTrace)) -> usize {
        #[cfg(feature = "telemetry-export")]
        if let Some(receiver) = &mut self.receiver {
            let mut consumed = 0;
            for _ in 0..256 {
                let Some(observation) = receiver.try_pop() else {
                    break;
                };
                consumed += 1;
                let Some(offset) = observation.received.checked_duration_since(self.origin) else {
                    self.invalid_clock += 1;
                    continue;
                };
                let id = super::trace::TraceId {
                    generation: observation.timing.generation,
                    message: observation.message,
                };
                let Some(trace) = ReceiveTrace::new(
                    id,
                    crate::pipeline::ns(offset),
                    observation.received,
                    observation.timing,
                ) else {
                    continue;
                };
                self.latest = Some(observation.received);
                // Lag is a separate worker statistic, never a transport boundary.
                self.last_handoff_ns = Some(crate::pipeline::ns(observation.received.elapsed()));
                while self.window.front().is_some_and(|old| {
                    trace.received_ns.saturating_sub(old.received_ns) > 10_000_000_000
                }) {
                    self.window.pop_front();
                    self.age_expirations = self.age_expirations.saturating_add(1);
                }
                if self.window.len() == 2048 {
                    self.window.pop_front();
                    self.evictions = self.evictions.saturating_add(1);
                }
                self.window.push_back(trace);
                export(trace);
            }
            return consumed;
        }
        let _ = &mut export;
        0
    }
    pub fn status(&self) -> Option<DiagnosticStatus> {
        #[cfg(feature = "telemetry-export")]
        if let Some(receiver) = &self.receiver {
            return Some(DiagnosticStatus {
                queue: receiver.status().snapshot(),
                window_evictions: self.evictions,
                window_age_expirations: self.age_expirations.saturating_add(
                    self.window
                        .iter()
                        .filter(|t| {
                            crate::pipeline::ns(self.origin.elapsed()).saturating_sub(t.received_ns)
                                > 10_000_000_000
                        })
                        .count() as u64,
                ),
                invalid_clock: self.invalid_clock,
                last_handoff_ns: self.last_handoff_ns,
                observation_age_ns: self.latest.map(|at| crate::pipeline::ns(at.elapsed())),
            });
        }
        None
    }
    pub fn attach(&self, latency: &mut TraceWindow, observed_ns: u64, messages: u64) {
        if !self.enabled() {
            return;
        }
        latency.receive = self
            .window
            .iter()
            .filter(|r| {
                r.id.message <= messages
                    && r.received_ns <= observed_ns
                    && observed_ns - r.received_ns <= latency.window_ns
            })
            .copied()
            .collect();
        latency.receive_capacity_drops = self.evictions;
        latency.receive_age_expirations = self.age_expirations.saturating_add(
            self.window
                .iter()
                .filter(|t| observed_ns.saturating_sub(t.received_ns) > latency.window_ns)
                .count() as u64,
        );
        latency.diagnostics = self.status();
    }
}
