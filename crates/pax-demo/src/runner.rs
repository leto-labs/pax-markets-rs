//! Async live input orchestration with coalesced, read-only reporting snapshots.
pub mod offline;
pub mod placement;
pub mod report;
use crate::{
    capture::{CaptureConfig, Manifest, Record, RecordKind, RecordStatus, Recorder},
    pipeline::{Pipeline, ns, view::View},
};
use pax_client::websocket::{DeliveryTiming, Feed, FeedConfig, Input, receive::ReceiveTiming};
use pax_types::{
    market::ExchangeInfo,
    units::{MarketId, Quantity},
};
use serde::Serialize;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::watch;

/// Application-boundary error with its original source preserved.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
/// Finite runtime and presentation settings.
pub struct RunConfig {
    /// Independent startup collection/output settings; defaults retain legacy behavior.
    pub measurements: crate::measurement::settings::Settings,
    /// Public endpoint and explicit selected markets.
    pub feed: FeedConfig,
    /// Validated metadata for this environment.
    pub metadata: ExchangeInfo,
    /// Optional journal; observation remains usable without storage.
    pub capture: Option<CaptureConfig>,
    /// Independent bounded diagnostic sidecar; absent performs no diagnostic I/O.
    pub receive_sidecar: Option<crate::measurement::sidecar::SidecarConfig>,
    /// Stop after this duration, or observe until canceled.
    pub duration: Option<Duration>,
    /// Bounded display depth.
    pub depth: usize,
    /// Execution-estimate size in base native units.
    pub quantity: Quantity,
    /// Sample one message in this many for detailed timing.
    pub sample_every: u64,
    /// Receive and reconstruct in one task; false retains background queued delivery.
    pub inline_delivery: bool,
    /// Maximum queued inputs per control-loop turn; 1..=256, exactly one inline.
    pub queue_drain: usize,
    /// Diagnostic benchmark baseline; production callers should use false.
    pub report_on_ingestion: bool,
}
/// Coalesced application state sent to headless output or HTTP readers.
#[derive(Clone, Debug, Serialize)]
pub struct Published {
    /// Independently readable liveness/loss, not serialized through this lossy report.
    #[serde(skip)]
    pub diagnostics: crate::measurement::status::Diagnostics,
    /// Unique process identifier used to invalidate browser clock alignment.
    pub session: String,
    /// Presentation revision, independent of exchange sequence and sampled trace ID.
    pub update_id: String,
    /// Local publication time from the server monotonic origin.
    pub published_ns: u64,
    /// Latest public transport heartbeat round trip, never a one-way latency.
    pub heartbeat_rtt_ns: Option<u64>,
    /// Age of the last application pong.
    pub heartbeat_age_ns: Option<u64>,
    /// Immediate transport health (also visible during queue saturation).
    pub transport_connected: bool,
    /// Most recent transport failure.
    pub transport_error: Option<String>,
    /// Capture failure, if recording was unable to retain an input.
    pub capture_error: Option<String>,
    /// Live numeric snapshot and independent report-worker costs.
    pub report_work: Option<report::ReportWork>,
    /// Reconstruction and processing telemetry.
    pub view: View,
}
/// Initial view and the process monotonic origin shared with the HTTP clock API.
pub fn channel(
    metadata: &ExchangeInfo,
    markets: &[MarketId],
    mode: &str,
) -> Result<(Instant, watch::Sender<Arc<Published>>), Error> {
    let origin = Instant::now();
    let pipeline = Pipeline::new(
        metadata,
        markets,
        u64::from(cfg!(feature = "processing-traces")),
    )?;
    let session = format!(
        "{:x}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    );
    let initial = Published {
        diagnostics: crate::measurement::status::Diagnostics::new(origin),
        session,
        update_id: "0".into(),
        published_ns: 0,
        heartbeat_rtt_ns: None,
        heartbeat_age_ns: None,
        transport_connected: false,
        transport_error: None,
        capture_error: None,
        report_work: None,
        view: pipeline.view(mode, 0, 20, Quantity(1)),
    };
    Ok((origin, watch::channel(Arc::new(initial)).0))
}
/// Run public ingestion until canceled or bounded duration/bytes are reached.
/// A failed recorder is finalized as incomplete while live books keep updating.
pub async fn live(
    config: RunConfig,
    origin: Instant,
    output: watch::Sender<Arc<Published>>,
    stop: watch::Receiver<bool>,
) -> Result<String, Error> {
    live_observed(config, origin, output, stop, |_| {}).await
}
/// Run the identical live pipeline with a synchronous benchmark commit observer.
/// The observer owns its timing/storage independently of optional telemetry and
/// must return promptly. Normal live observation uses an inlined no-op hook.
pub async fn live_observed(
    mut config: RunConfig,
    origin: Instant,
    output: watch::Sender<Arc<Published>>,
    mut stop: watch::Receiver<bool>,
    mut observer: impl FnMut(crate::pipeline::CommitPoint) + Send,
) -> Result<String, Error> {
    config.measurements.apply_feed(&mut config.feed)?;
    output.borrow().diagnostics.settings(config.measurements);
    if !(1..=256).contains(&config.queue_drain)
        || (config.inline_delivery && config.queue_drain != 1)
    {
        return Err("queue drain must be 1..=256 and exactly one with inline delivery".into());
    }
    let mut diagnostic_writer = config
        .receive_sidecar
        .map(|mut c| {
            c.sample_rate = config.measurements.sidecar;
            crate::measurement::sidecar::Sidecar::start(c)
        })
        .transpose()?;
    if let Some(writer) = &diagnostic_writer {
        output.borrow().diagnostics.sidecar(writer.status());
    }
    let mut last_diagnostic = None;
    let mut pipeline = Pipeline::new(&config.metadata, &config.feed.markets, config.sample_every)?;
    if let Some(policy) = config.measurements.processing {
        pipeline.configure_measurements(policy)?;
    }
    let receive_capabilities = crate::measurement::capabilities::CAPABILITIES.client;
    let receive_available = receive_capabilities.receive_timestamps
        || receive_capabilities.receive_stages
        || receive_capabilities.association();
    let asynchronous_receive = cfg!(feature = "telemetry-export")
        && receive_available
        && config.feed.receive.is_some()
        && !config.report_on_ingestion;
    if receive_available && config.feed.receive.is_some() && !asynchronous_receive {
        pipeline.reserve_receive_diagnostics();
    }
    let mut recorder = config
        .capture
        .map(|capture| {
            Recorder::start(
                capture,
                Manifest::new(
                    config.metadata.clone(),
                    config.feed.markets.clone(),
                    config.feed.url.clone(),
                )?,
            )
        })
        .transpose()?;
    let (mut feed, receive_worker) = start_feed(
        config.feed,
        config.inline_delivery,
        asynchronous_receive,
        origin,
    )?;
    let diagnostics = output.borrow().diagnostics.clone();
    diagnostics.policy(feed.measurement_control());
    receive_worker.register(&diagnostics);
    let health = feed.health();
    let mut reports = if config.report_on_ingestion {
        report::Reports::on_owner(output.clone(), health, origin)
    } else {
        report::Reports::with_receive(
            output.clone(),
            health,
            origin,
            receive_worker,
            config.measurements.export_poll_ms,
            if asynchronous_receive {
                diagnostic_writer.take()
            } else {
                None
            },
        )?
    };
    let mut interval =
        tokio::time::interval(Duration::from_millis(config.measurements.publication_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let deadline = async {
        match config.duration {
            Some(duration) => tokio::time::sleep(duration).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(deadline);
    let mut revision = 0u64;
    let mut capture_error = None;
    let mut limited = false;
    let mut stopping = *stop.borrow();
    let mut requested_recovery = None;
    let mut last_offset = 0;
    let mut stop_offset = 0;
    if stopping {
        feed.stop();
    }
    loop {
        tokio::select! {
            _ = &mut deadline, if !stopping => { stopping = true; stop_offset = ns(origin.elapsed()); feed.stop(); }
            result = stop.changed(), if !stopping => {
                if result.is_err() || *stop.borrow() { stopping = true; stop_offset = ns(origin.elapsed()); feed.stop(); }
            }
            _ = interval.tick(), if !stopping => {
                if capture_error.is_none() && recorder.as_ref().is_some_and(Recorder::has_failed) {
                    capture_error = Some("capture writer failed; recording is incomplete".into());
                }
                revision += 1;
                let now = ns(origin.elapsed());
                reports.publish(&pipeline, revision, now, config.depth, config.quantity, &capture_error)?;
            }
            input = feed.recv() => {
                let Some(input) = input else { break; };
                let mut next = Some(input);
                for index in 0..config.queue_drain {
                    let Some(input) = next.take() else { break; };
                    let (record, received, timing, receive) = record(input, origin);
                    last_offset = record.offset_ns;
                    if let Some(writer) = recorder.as_mut().filter(|_| capture_error.is_none()) {
                      match writer.record(record.clone()) {
                          Ok(RecordStatus::Recorded) => {},
                          Ok(RecordStatus::LimitReached) => { limited = true; stopping = true; stop_offset = record.offset_ns; feed.stop(); },
                          Err(error) => { capture_error = Some(error.to_string()); writer.invalidate(); },
                      }
                    }
                    let result = pipeline.process_observed(&record, received, timing, receive, &mut observer)?;
                    if let (Some(writer),Some(trace))=(&mut diagnostic_writer,pipeline.latest_receive()) && last_diagnostic!=Some(trace.id) {writer.record(trace);last_diagnostic=Some(trace.id);}
                    if result.recover && requested_recovery != Some(record.generation) {
                      feed.request_recovery(); requested_recovery = Some(record.generation);
                    }
                    if index + 1 < config.queue_drain { next = feed.try_recv(); }
                }
            }
        }
    }
    feed.shutdown().await;
    // Cancellation drains all inputs already received. A concurrent final input
    // can be newer than the cancellation request, so include it in the boundary.
    let end = stop_offset.max(last_offset);
    pipeline.finish(end)?;
    if let Some(writer) = recorder {
        let reason = if limited {
            "byte limit"
        } else {
            "duration or cancellation"
        };
        match tokio::task::spawn_blocking(move || writer.finish(end, reason)).await? {
            Ok(manifest) => eprintln!(
                "capture complete: {} messages, {} bytes, {}",
                manifest.messages, manifest.bytes, manifest.digest
            ),
            Err(error) => {
                capture_error = Some(error.to_string());
            }
        }
    }
    if let Some(writer) = diagnostic_writer {
        match tokio::task::spawn_blocking(move || writer.finish()).await? {
            Ok(coverage) => eprintln!(
                "receive sidecar coverage: {}",
                serde_json::to_string(&coverage)?
            ),
            Err(error) => eprintln!("receive sidecar incomplete: {error}"),
        }
    }
    revision += 1;
    reports
        .finish(
            &pipeline,
            revision,
            end,
            config.depth,
            config.quantity,
            capture_error.clone(),
        )
        .await?;
    if let Some(error) = capture_error {
        return Err(error.into());
    }
    Ok(pipeline.digest())
}
fn record(
    input: Input,
    origin: Instant,
) -> (
    Record,
    Instant,
    Option<DeliveryTiming>,
    Option<ReceiveTiming>,
) {
    let (timing, receive) = match &input {
        Input::Message {
            timing, receive, ..
        } => (*timing, receive.as_deref().copied()),
        _ => (None, None),
    };
    let (generation, at, kind) = match input {
        Input::Connected { at, generation } => (generation, at, RecordKind::Connected),
        Input::Message {
            generation,
            received,
            bytes,
            ..
        } => (generation, received, RecordKind::Message(bytes)),
        Input::Disconnected {
            at,
            generation,
            reason,
            lossy,
        } => (generation, at, RecordKind::Disconnected { reason, lossy }),
    };
    (
        Record {
            generation,
            offset_ns: ns(at.duration_since(origin)),
            kind,
        },
        at,
        timing,
        receive,
    )
}

fn start_feed(
    config: FeedConfig,
    inline: bool,
    asynchronous: bool,
    origin: Instant,
) -> Result<(Feed, crate::measurement::worker::ReceiveWorker), Error> {
    #[cfg(feature = "telemetry-export")]
    if asynchronous {
        let mut worker = crate::measurement::worker::ReceiveWorker::prepared(origin);
        let (feed, receiver) = if inline {
            Feed::inline_with_diagnostics(config, 4096)
        } else {
            Feed::spawn_with_diagnostics(config, 4096)
        }?;
        worker.connect(receiver);
        return Ok((feed, worker));
    }
    let _ = asynchronous;
    let feed = if inline {
        Feed::inline(config)
    } else {
        Feed::spawn(config)
    }?;
    Ok((
        feed,
        crate::measurement::worker::ReceiveWorker::disabled(origin),
    ))
}
