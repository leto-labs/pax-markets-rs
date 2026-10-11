//! Bounded report preparation on a managed thread, outside the ingestion owner.
mod render;
use super::{Error, Published};
use crate::pipeline::{Pipeline, ns, snapshot::Snapshot};
use pax_client::websocket::Health;
use pax_types::units::Quantity;
use serde::Serialize;
use std::{sync::Arc, thread::JoinHandle, time::Instant};
use tokio::sync::{mpsc, watch};

/// Report costs, separate from per-message processing and display coalescing.
#[derive(Clone, Debug, Serialize)]
pub struct ReportWork {
    /// Local time when numeric snapshot extraction started.
    pub captured_ns: u64,
    /// Numeric snapshot extraction on the ingestion owner.
    pub snapshot_ns: u64,
    /// Snapshot completion to worker start, including delivery and scheduling.
    pub handoff_ns: u64,
    /// Formatting, percentiles and health qualification on the report worker.
    pub preparation_ns: u64,
    /// Lifetime periodic reports skipped because the worker queue was occupied.
    pub skipped: u64,
}
struct Request {
    snapshot: Snapshot,
    kind: Kind,
    revision: u64,
    capture_error: Option<String>,
    captured_ns: u64,
    snapshot_ns: u64,
    ready: Instant,
    skipped: u64,
}
#[derive(Clone, Copy)]
enum Kind {
    Live,
    Final,
}
pub(super) struct Reports {
    sender: Option<mpsc::Sender<Request>>,
    owner: Option<render::Renderer>,
    thread: Option<JoinHandle<()>>,
    origin: Instant,
    skipped: u64,
    activity: crate::measurement::status::Activity,
}
impl Reports {
    #[cfg(test)]
    pub fn new(
        output: watch::Sender<Arc<Published>>,
        health: watch::Receiver<Health>,
        origin: Instant,
    ) -> Result<Self, Error> {
        Self::start(output, health, origin, || {})
    }
    /// Diagnostic baseline: identical preparation on the ingestion owner.
    pub fn on_owner(
        output: watch::Sender<Arc<Published>>,
        health: watch::Receiver<Health>,
        origin: Instant,
    ) -> Self {
        let owner = render::Renderer::new(output, health, origin, "ingestion");
        let activity = owner.activity.clone();
        Self {
            activity,
            sender: None,
            thread: None,
            owner: Some(owner),
            origin,
            skipped: 0,
        }
    }
    #[cfg(test)]
    fn start(
        output: watch::Sender<Arc<Published>>,
        health: watch::Receiver<Health>,
        origin: Instant,
        before_render: impl FnMut() + Send + 'static,
    ) -> Result<Self, Error> {
        Self::start_worker(
            output,
            health,
            origin,
            crate::measurement::worker::ReceiveWorker::disabled(origin),
            2,
            None,
            before_render,
        )
    }
    pub(crate) fn with_receive(
        output: watch::Sender<Arc<Published>>,
        health: watch::Receiver<Health>,
        origin: Instant,
        receive: crate::measurement::worker::ReceiveWorker,
        export_poll_ms: u64,
        writer: Option<crate::measurement::sidecar::Sidecar>,
    ) -> Result<Self, Error> {
        Self::start_worker(
            output,
            health,
            origin,
            receive,
            export_poll_ms,
            writer,
            || {},
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn start_worker(
        output: watch::Sender<Arc<Published>>,
        health: watch::Receiver<Health>,
        origin: Instant,
        receive: crate::measurement::worker::ReceiveWorker,
        export_poll_ms: u64,
        writer: Option<crate::measurement::sidecar::Sidecar>,
        mut before_render: impl FnMut() + Send + 'static,
    ) -> Result<Self, Error> {
        let (sender, mut receiver) = mpsc::channel::<Request>(1);
        let mut renderer = render::Renderer::new(output, health, origin, "worker");
        let activity = renderer.activity.clone();
        renderer.receive = receive;
        let background_cpu = crate::worker::target();
        let thread = std::thread::Builder::new()
            .name("pax-reports".into())
            .spawn(move || {
                let _guard = crate::measurement::status::Guard(renderer.activity.clone());
                if let Err(error) = crate::worker::pin(background_cpu) {
                    renderer.activity.failed();
                    eprintln!("report worker affinity failed: {error}");
                    return;
                }
                let mut writer = writer;
                let mut finalized = false;
                loop {
                    let drained = if renderer.receive.enabled() {
                        renderer.receive.drain(|trace| {
                            if let Some(writer) = &mut writer {
                                writer.record(trace);
                            }
                        })
                    } else {
                        0
                    };
                    let request = if renderer.receive.enabled() {
                        match receiver.try_recv() {
                            Ok(request) => Some(request),
                            Err(mpsc::error::TryRecvError::Disconnected) => None,
                            Err(mpsc::error::TryRecvError::Empty) => {
                                // No per-sample wakeups. Each turn drains at most 256
                                // observations, then uses its independent idle cadence.
                                if drained == 256 {
                                    // A full batch may leave work ready. Check control
                                    // each turn and yield without adding an idle delay.
                                    std::thread::yield_now();
                                } else {
                                    std::thread::park_timeout(std::time::Duration::from_millis(
                                        export_poll_ms,
                                    ));
                                }
                                continue;
                            }
                        }
                    } else {
                        receiver.blocking_recv()
                    };
                    let Some(request) = request else {
                        break;
                    };
                    if matches!(request.kind, Kind::Final) {
                        // The feed has stopped. A finite optional drain cannot hold
                        // canonical capture finalization or cancellation indefinitely.
                        let deadline = Instant::now() + std::time::Duration::from_millis(250);
                        while Instant::now() < deadline
                            && renderer.receive.drain(|trace| {
                                if let Some(writer) = &mut writer {
                                    writer.record(trace);
                                }
                            }) != 0
                        {}
                        finalized = true;
                    }
                    before_render();
                    renderer.publish(request);
                }
                if finalized && let Some(writer) = writer {
                    if let Some(status) = renderer.receive.status() {
                        writer.source_loss(
                            status
                                .queue
                                .full_drops
                                .saturating_add(status.queue.closed_drops)
                                .saturating_add(status.queue.pending)
                                .saturating_add(status.invalid_clock),
                        );
                    }
                    match writer.finish() {
                        Ok(coverage) => eprintln!(
                            "receive sidecar coverage: {}",
                            serde_json::to_string(&coverage).unwrap_or_default()
                        ),
                        Err(error) => eprintln!("receive sidecar incomplete: {error}"),
                    }
                }
            })?;
        Ok(Self {
            activity,
            sender: Some(sender),
            owner: None,
            thread: Some(thread),
            origin,
            skipped: 0,
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub fn publish(
        &mut self,
        pipeline: &Pipeline,
        revision: u64,
        observed_ns: u64,
        depth: usize,
        quantity: Quantity,
        capture_error: &Option<String>,
    ) -> Result<bool, Error> {
        if let Some(owner) = &self.owner {
            owner.publish(Self::request(
                pipeline,
                Kind::Live,
                revision,
                observed_ns,
                depth,
                quantity,
                capture_error.clone(),
                self.origin,
                0,
            ));
            return Ok(true);
        }
        let permit = match self
            .sender
            .as_ref()
            .expect("report owner active")
            .try_reserve()
        {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.skipped = self.skipped.saturating_add(1);
                self.activity.skipped(self.skipped);
                return Ok(false);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.skipped = self.skipped.saturating_add(1);
                self.activity.skipped(self.skipped);
                return Ok(false);
            }
        };
        // Reserve before copying: a slow viewer cannot accumulate report work.
        permit.send(Self::request(
            pipeline,
            Kind::Live,
            revision,
            observed_ns,
            depth,
            quantity,
            capture_error.clone(),
            self.origin,
            self.skipped,
        ));
        if let Some(thread) = &self.thread {
            thread.thread().unpark();
        }
        Ok(true)
    }
    #[allow(clippy::too_many_arguments)]
    fn request(
        pipeline: &Pipeline,
        kind: Kind,
        revision: u64,
        observed_ns: u64,
        depth: usize,
        quantity: Quantity,
        capture_error: Option<String>,
        origin: Instant,
        skipped: u64,
    ) -> Request {
        let start = Instant::now();
        let snapshot = pipeline.snapshot(observed_ns, depth, quantity);
        let ready = Instant::now();
        Request {
            snapshot,
            kind,
            revision,
            capture_error,
            captured_ns: ns(start.duration_since(origin)),
            snapshot_ns: ns(ready.duration_since(start)),
            ready,
            skipped,
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn finish(
        mut self,
        pipeline: &Pipeline,
        revision: u64,
        observed_ns: u64,
        depth: usize,
        quantity: Quantity,
        capture_error: Option<String>,
    ) -> Result<(), Error> {
        if let Some(owner) = self.owner.take() {
            owner.publish(Self::request(
                pipeline,
                Kind::Final,
                revision,
                observed_ns,
                depth,
                quantity,
                capture_error,
                self.origin,
                0,
            ));
            self.activity.stopped();
            return Ok(());
        }
        // Only shutdown waits for report capacity; normal ingestion never does.
        let sender = self.sender.take().expect("report owner active");
        let permit = match tokio::time::timeout(std::time::Duration::from_secs(2), sender.reserve())
            .await
        {
            Ok(Ok(permit)) => permit,
            _ => {
                self.activity.failed();
                eprintln!(
                    "final diagnostic report unavailable: worker closed or drain deadline exceeded"
                );
                return Ok(());
            }
        };
        permit.send(Self::request(
            pipeline,
            Kind::Final,
            revision,
            observed_ns,
            depth,
            quantity,
            capture_error,
            self.origin,
            self.skipped,
        ));
        drop(sender);
        let thread = self.thread.take().expect("report thread active");
        thread.thread().unpark();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        while !thread.is_finished() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        if thread.is_finished() {
            if thread.join().is_err() {
                eprintln!("diagnostic report worker panicked");
            }
        } else {
            self.activity.failed();
            eprintln!("diagnostic worker drain deadline exceeded; detached optional worker");
        }
        Ok(())
    }
}
impl Drop for Reports {
    fn drop(&mut self) {
        if self.owner.is_some() {
            self.activity.stopped();
        }
        self.sender.take();
        // Cancellation never joins an optional worker. Closing the report sender
        // eventually stops it; the independent queue status exposes abandonment.
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
        }
    }
}

#[cfg(test)]
mod tests;
