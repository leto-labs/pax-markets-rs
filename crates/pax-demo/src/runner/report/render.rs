//! Shared preparation for production worker and diagnostic owner baseline.
use super::*;
pub(super) struct Renderer {
    output: watch::Sender<Arc<Published>>,
    health: watch::Receiver<Health>,
    origin: Instant,
    session: String,
    diagnostics: crate::measurement::status::Diagnostics,
    pub(super) activity: crate::measurement::status::Activity,
    pub(super) receive: crate::measurement::worker::ReceiveWorker,
}
impl Renderer {
    pub(super) fn new(
        output: watch::Sender<Arc<Published>>,
        health: watch::Receiver<Health>,
        origin: Instant,
        mode: &'static str,
    ) -> Self {
        let session = output.borrow().session.clone();
        let diagnostics = output.borrow().diagnostics.clone();
        let activity = crate::measurement::status::Activity::new(mode);
        diagnostics.report_owner(activity.clone(), health.clone());
        Self {
            output,
            diagnostics,
            activity,
            health,
            origin,
            session,
            receive: crate::measurement::worker::ReceiveWorker::disabled(origin),
        }
    }
    pub(super) fn publish(&self, mut request: Request) {
        request.snapshot.attach_receive(&self.receive);
        let start = Instant::now();
        let mut view = request.snapshot.render(match request.kind {
            Kind::Live => "live",
            Kind::Final => "completed observation",
        });
        let mut state = self.health.borrow().clone();
        match request.kind {
            Kind::Live => view.qualify_transport(state.connected, state.generation),
            Kind::Final => {
                // Retain the historical final book for observation and
                // replay comparison, while explicitly marking it stopped.
                view.connected = false;
                state.connected = false;
                state.reason = Some("observation stopped".into());
            }
        }
        let published = Instant::now();
        self.activity
            .published(ns(published.duration_since(self.origin)));
        self.output.send_replace(Arc::new(Published {
            diagnostics: self.diagnostics.clone(),
            session: self.session.clone(),
            update_id: request.revision.to_string(),
            published_ns: ns(published.duration_since(self.origin)),
            heartbeat_rtt_ns: state.round_trip.map(ns),
            heartbeat_age_ns: state
                .last_pong
                .map(|at| ns(published.saturating_duration_since(at))),
            transport_connected: state.connected,
            transport_error: state.reason,
            capture_error: request.capture_error,
            view,
            report_work: Some(ReportWork {
                captured_ns: request.captured_ns,
                snapshot_ns: request.snapshot_ns,
                handoff_ns: ns(start.duration_since(request.ready)),
                preparation_ns: ns(published.duration_since(start)),
                skipped: request.skipped,
            }),
        }));
    }
}
