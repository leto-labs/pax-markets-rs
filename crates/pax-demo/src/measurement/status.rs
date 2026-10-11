//! Status handles shared by HTTP and callers, independently of lossy report output.
mod activity;
use super::{capabilities, sidecar};
pub use activity::ReportStatus;
pub(crate) use activity::{Activity, Guard};
use pax_client::websocket::{
    Health,
    receive::{control::MeasurementControl, policy::ReceivePolicy},
};
use serde::Serialize;
use std::{
    sync::{Arc, OnceLock},
    time::Instant,
};
use tokio::sync::watch;

struct Registry {
    origin: Instant,
    settings: OnceLock<super::settings::Settings>,
    reports: OnceLock<Activity>,
    transport: OnceLock<watch::Receiver<Health>>,
    policy: OnceLock<MeasurementControl>,
    sidecar: OnceLock<sidecar::status::Status>,
    #[cfg(feature = "telemetry-export")]
    receive: OnceLock<pax_client::measurement::queue::Status>,
}
/// One run's independent status registry. Clone handles before starting observers;
/// producers update atomics or existing health, without taking a registry lock.
#[derive(Clone)]
pub struct Diagnostics {
    inner: Arc<Registry>,
}
impl std::fmt::Debug for Diagnostics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Diagnostics").finish_non_exhaustive()
    }
}
/// Current status; serialization occurs on its caller, never on ingestion.
#[derive(Serialize)]
pub struct Snapshot {
    /// Startup settings, independently of any later per-owner policy revisions.
    pub settings: Option<super::settings::Settings>,
    /// Actual compiled groups.
    pub capabilities: capabilities::Capabilities,
    /// Report worker or explicitly selected owner baseline.
    pub reports: Option<ReportStatus>,
    /// Diagnostic queue remains readable when its consumer cannot export.
    pub receive_queue: Option<pax_client::measurement::status::QueueStatus>,
    /// File writer failure/loss independent of its own output.
    pub sidecar: Option<sidecar::status::Snapshot>,
    /// Mandatory transport health independent of the report worker.
    pub transport: Option<Transport>,
    /// Last acknowledged receive policy independent of sample export.
    pub policy: Option<Policy>,
}
/// Fresh transport status; no claim about whether the displayed book is current.
#[derive(Serialize)]
pub struct Transport {
    /// Current connection generation, exact in browser JSON.
    pub generation: String,
    /// Current transport connection state.
    pub connected: bool,
    /// Original disconnect/failure reason.
    pub reason: Option<String>,
}
/// Effective client policy, excluding local device configuration and credentials.
#[derive(Serialize)]
pub struct Policy {
    /// Last acknowledged runtime revision.
    pub revision: u64,
    /// Selected receive domains, or none for ordinary transport.
    pub receive: Option<ReceivePolicy>,
    /// Separate complete-message receive emission denominator.
    pub receive_emit_every: Option<u64>,
    /// Complete-message delivery marker denominator.
    pub delivery_every: u64,
}
impl Diagnostics {
    /// Create one registry per session; a new run should use a fresh channel/session.
    pub fn new(origin: Instant) -> Self {
        Self {
            inner: Arc::new(Registry {
                origin,
                settings: OnceLock::new(),
                reports: OnceLock::new(),
                transport: OnceLock::new(),
                policy: OnceLock::new(),
                sidecar: OnceLock::new(),
                #[cfg(feature = "telemetry-export")]
                receive: OnceLock::new(),
            }),
        }
    }
    /// Snapshot without waiting for optional sample/report/file output.
    pub fn snapshot(&self) -> Snapshot {
        let now = crate::pipeline::ns(self.inner.origin.elapsed());
        let receive_queue = {
            #[cfg(feature = "telemetry-export")]
            {
                self.inner.receive.get().map(|s| s.snapshot())
            }
            #[cfg(not(feature = "telemetry-export"))]
            {
                None
            }
        };
        Snapshot {
            settings: self.inner.settings.get().copied(),
            capabilities: capabilities::CAPABILITIES,
            reports: self.inner.reports.get().map(|r| r.snapshot(now)),
            receive_queue,
            sidecar: self.inner.sidecar.get().map(|s| s.snapshot()),
            transport: self.inner.transport.get().map(|h| {
                let h = h.borrow().clone();
                Transport {
                    generation: h.generation.to_string(),
                    connected: h.connected,
                    reason: h.reason,
                }
            }),
            policy: self.inner.policy.get().map(|p| {
                let p = p.effective();
                Policy {
                    revision: p.revision,
                    receive: p.settings.receive.as_ref().map(|c| c.policy),
                    receive_emit_every: p.settings.receive.as_ref().map(|c| c.sample_every),
                    delivery_every: p.settings.delivery_sample_every,
                }
            }),
        }
    }
    pub(crate) fn settings(&self, settings: super::settings::Settings) {
        let _ = self.inner.settings.set(settings);
    }
    /// Obtain the bounded transport policy controller after the runner starts.
    /// Responses require the runner to continue polling its feed.
    pub fn transport_control(&self) -> Option<MeasurementControl> {
        self.inner.policy.get().cloned()
    }
    pub(crate) fn report_owner(&self, activity: Activity, health: watch::Receiver<Health>) {
        let _ = self.inner.reports.set(activity);
        let _ = self.inner.transport.set(health);
    }
    pub(crate) fn policy(&self, policy: MeasurementControl) {
        let _ = self.inner.policy.set(policy);
    }
    pub(crate) fn sidecar(&self, status: sidecar::status::Status) {
        let _ = self.inner.sidecar.set(status);
    }
    #[cfg(feature = "telemetry-export")]
    pub(crate) fn receive(&self, status: pax_client::measurement::queue::Status) {
        let _ = self.inner.receive.set(status);
    }
}
