//! Explicitly labeled offline sources for a repeatable, credential-free demo.
use super::{Error, Published};
use crate::{
    capture::{Record, RecordKind},
    pipeline::{Pipeline, ns},
    replay::Replay,
};
use bytes::Bytes;
use pax_types::{
    market::ExchangeInfo,
    units::{MarketId, Quantity},
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::watch;

/// Metadata retained with the bundled public fixture. Never substitutes live discovery.
pub fn fixture_metadata() -> Result<ExchangeInfo, Error> {
    Ok(serde_json::from_slice(include_bytes!(
        "../../../pax-types/tests/fixtures/metadata.json"
    ))?)
}
/// Cycle a small retained public stream at an artificial display-friendly cadence.
/// It is always labeled fixture replay, never current venue liquidity.
pub async fn fixture(
    origin: Instant,
    output: watch::Sender<Arc<Published>>,
    stop: watch::Receiver<bool>,
    quantity: Quantity,
    sample_every: u64,
) -> Result<(), Error> {
    fixture_with_settings(
        origin,
        output,
        stop,
        quantity,
        sample_every,
        Default::default(),
    )
    .await
}
/// Fixture replay with independent processing and publication policies. The artificial
/// input cadence remains 250 ms regardless of publication or browser cadence.
pub async fn fixture_with_settings(
    origin: Instant,
    output: watch::Sender<Arc<Published>>,
    mut stop: watch::Receiver<bool>,
    quantity: Quantity,
    sample_every: u64,
    settings: crate::measurement::settings::Settings,
) -> Result<(), Error> {
    settings.validate()?;
    output.borrow().diagnostics.settings(settings);
    let metadata = fixture_metadata()?;
    let mut pipeline = Pipeline::new(&metadata, &[MarketId(1)], sample_every)?;
    if let Some(policy) = settings.processing {
        pipeline.configure_measurements(policy)?;
    }
    let messages: Vec<_> = include_bytes!("../../../pax-book/tests/fixtures/public-active.jsonl")
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(Bytes::from_static)
        .collect();
    let mut generation = 1;
    let mut index = 0;
    let mut connected = false;
    let mut revision = 0;
    let mut interval = tokio::time::interval(Duration::from_millis(settings.publication_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut input_interval = tokio::time::interval(Duration::from_millis(250));
    input_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while !*stop.borrow() {
        tokio::select! {
            result = stop.changed() => { if result.is_err() { break; } },
            _ = input_interval.tick() => {
                let now = ns(origin.elapsed());
                if !connected {
                    pipeline.process(&Record { offset_ns: now, generation, kind: RecordKind::Connected }, 0)?;
                    connected = true;
                }
                if index < messages.len() {
                    pipeline.process(&Record { offset_ns: now, generation, kind: RecordKind::Message(messages[index].clone()) }, 0)?;
                    index += 1;
                } else {
                    pipeline.process(&Record { offset_ns: now, generation, kind: RecordKind::Disconnected { reason: "fixture loop boundary".into(), lossy: false } }, 0)?;
                    generation += 1; index = 0; connected = false;
                }
            }
            _ = interval.tick() => {
                revision += 1;
                let now = ns(origin.elapsed());
                publish(&pipeline, &output, origin, revision, now, "fixture replay · public sample 2026-10-07 · artificial cadence", quantity);
            }
        }
    }
    Ok(())
}
/// Pace a verified recording using its original monotonic offsets. Timings show
/// local reprocessing; original receive offsets are never exchange timestamps.
pub async fn replay(
    replay: Replay,
    origin: Instant,
    output: watch::Sender<Arc<Published>>,
    stop: watch::Receiver<bool>,
    quantity: Quantity,
    sample_every: u64,
) -> Result<(), Error> {
    replay_with_settings(
        replay,
        origin,
        output,
        stop,
        quantity,
        sample_every,
        Default::default(),
    )
    .await
}
/// Recorded input pacing with independently configurable observation/publication.
pub async fn replay_with_settings(
    mut replay: Replay,
    origin: Instant,
    output: watch::Sender<Arc<Published>>,
    mut stop: watch::Receiver<bool>,
    quantity: Quantity,
    sample_every: u64,
    settings: crate::measurement::settings::Settings,
) -> Result<(), Error> {
    settings.validate()?;
    output.borrow().diagnostics.settings(settings);
    let mut pipeline = Pipeline::new(
        &replay.manifest().metadata,
        &replay.manifest().markets,
        sample_every,
    )?;
    if let Some(policy) = settings.processing {
        pipeline.configure_measurements(policy)?;
    }
    let mut next = replay.next_record()?;
    let mut revision = 0;
    let end = replay.manifest().end_offset_ns;
    let mut interval = tokio::time::interval(Duration::from_millis(settings.publication_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while !*stop.borrow() {
        let offset = next.as_ref().map_or(end, |r| r.offset_ns);
        let deadline = tokio::time::Instant::from_std(origin + Duration::from_nanos(offset));
        tokio::select! {
            result = stop.changed() => { if result.is_err() { break; } },
            _ = tokio::time::sleep_until(deadline), if next.is_some() => {
                pipeline.process(&next.take().unwrap(), 0)?;
                next = replay.next_record()?;
                if next.is_none() { pipeline.finish(end)?; }
            },
            _ = interval.tick() => {
                revision += 1;
                let now = ns(origin.elapsed()).min(end);
                publish(&pipeline, &output, origin, revision, now, if next.is_some() { "capture replay · recorded cadence · local reprocessing" } else { "completed capture replay · historical liquidity" }, quantity);
            }
        }
    }
    Ok(())
}
fn publish(
    pipeline: &Pipeline,
    output: &watch::Sender<Arc<Published>>,
    origin: Instant,
    revision: u64,
    offset: u64,
    mode: &str,
    quantity: Quantity,
) {
    let session = output.borrow().session.clone();
    let view = pipeline.view(mode, offset, 20, quantity);
    let diagnostics = output.borrow().diagnostics.clone();
    output.send_replace(Arc::new(Published {
        diagnostics,
        session,
        update_id: revision.to_string(),
        published_ns: ns(origin.elapsed()),
        heartbeat_rtt_ns: None,
        heartbeat_age_ns: None,
        transport_connected: false,
        transport_error: Some("offline source; no venue connection".into()),
        capture_error: None,
        report_work: None,
        view,
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn fixture_honors_requested_quantity_and_sample_interval() {
        let metadata = fixture_metadata().unwrap();
        let (origin, output) =
            crate::runner::channel(&metadata, &[MarketId(1)], "fixture").unwrap();
        let mut reports = output.subscribe();
        let (stop, receiver) = watch::channel(false);
        let sample_every = if cfg!(feature = "processing-traces") {
            7
        } else {
            0
        };
        let task = tokio::spawn(fixture(
            origin,
            output,
            receiver,
            Quantity(257_000),
            sample_every,
        ));
        let report = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                reports.changed().await.unwrap();
                let report = reports.borrow_and_update().clone();
                if !report.view.markets[0].estimates.is_empty() {
                    break report;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(report.view.latency.sample_every, sample_every);
        assert!(
            report.view.markets[0]
                .estimates
                .iter()
                .all(|estimate| estimate.requested == "257000")
        );
        stop.send(true).unwrap();
        task.await.unwrap().unwrap();
    }
}
