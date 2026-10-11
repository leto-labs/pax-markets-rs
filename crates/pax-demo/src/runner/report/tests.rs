use super::*;
use crate::runner::{channel, offline::fixture_metadata};
use pax_types::units::MarketId;
use std::time::Duration;

#[tokio::test]
async fn final_report_preserves_the_historical_book_after_transport_stops() {
    use crate::capture::{Record, RecordKind};
    let metadata = fixture_metadata().unwrap();
    let (origin, output) = channel(&metadata, &[MarketId(1)], "test").unwrap();
    let (_health, health) = watch::channel(Health::default());
    let reports = Reports::new(output.clone(), health, origin).unwrap();
    let mut pipeline = Pipeline::new(
        &metadata,
        &[MarketId(1)],
        u64::from(cfg!(feature = "processing-traces")),
    )
    .unwrap();
    pipeline
        .process(
            &Record {
                generation: 1,
                offset_ns: 0,
                kind: RecordKind::Connected,
            },
            0,
        )
        .unwrap();
    pipeline
        .process(
            &Record {
                generation: 1,
                offset_ns: 1,
                kind: RecordKind::Message(bytes::Bytes::from_static(include_bytes!(
                    "../../../../pax-types/tests/fixtures/snapshot.json"
                ))),
            },
            0,
        )
        .unwrap();
    let expected = pipeline.view("expected", 1, 20, Quantity(1000)).markets;
    assert!(!expected[0].asks.is_empty());
    reports
        .finish(&pipeline, 1, 1, 20, Quantity(1000), None)
        .await
        .unwrap();
    let report = output.borrow();
    assert!(!report.transport_connected && !report.view.connected);
    assert_eq!(report.view.mode, "completed observation");
    assert_eq!(
        serde_json::to_value(&report.view.markets).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
}

#[tokio::test]
async fn slow_preparation_is_bounded_and_shutdown_publishes_final_state() {
    let metadata = fixture_metadata().unwrap();
    let (origin, output) = channel(&metadata, &[MarketId(1)], "test").unwrap();
    let (_health, health) = watch::channel(Health::default());
    let diagnostics = output.borrow().diagnostics.clone();
    let (entered, waiting) = std::sync::mpsc::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let mut first = true;
    let mut reports = Reports::start(output.clone(), health, origin, move || {
        if first {
            first = false;
            entered.send(()).unwrap();
            blocked.recv().unwrap();
        }
    })
    .unwrap();
    let pipeline = Pipeline::new(
        &metadata,
        &[MarketId(1)],
        u64::from(cfg!(feature = "processing-traces")),
    )
    .unwrap();
    assert!(
        reports
            .publish(&pipeline, 1, 0, 20, Quantity(1), &None)
            .unwrap()
    );
    waiting.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(
        reports
            .publish(&pipeline, 2, 0, 20, Quantity(1), &None)
            .unwrap()
    );
    assert!(
        !reports
            .publish(&pipeline, 3, 0, 20, Quantity(1), &None)
            .unwrap()
    );
    assert_eq!(reports.skipped, 1);
    // The blocked worker has published nothing, but independent status exposes
    // liveness and report loss without requiring a successful report first.
    let status = diagnostics.snapshot().reports.unwrap();
    assert!(status.alive && !status.failed);
    assert_eq!(status.publications, 0);
    assert_eq!(status.skipped, 1);
    assert_eq!(status.publication_age_ns, None);
    release.send(()).unwrap();
    reports
        .finish(&pipeline, 4, 0, 20, Quantity(1), None)
        .await
        .unwrap();
    let report = output.borrow();
    assert_eq!(report.update_id, "4");
    assert_eq!(report.view.mode, "completed observation");
    let status = diagnostics.snapshot().reports.unwrap();
    assert!(!status.alive && !status.failed);
    assert!(status.publications > 0 && status.publication_age_ns.is_some());
    let work = report.report_work.as_ref().unwrap();
    assert_eq!(work.skipped, 1);
    assert!(
        work.captured_ns + work.snapshot_ns + work.handoff_ns + work.preparation_ns
            <= report.published_ns
    );
}

#[tokio::test]
async fn canceling_a_stalled_report_does_not_join_the_worker() {
    let metadata = fixture_metadata().unwrap();
    let (origin, output) = channel(&metadata, &[MarketId(1)], "test").unwrap();
    let (_health, health) = watch::channel(Health::default());
    let (entered, waiting) = std::sync::mpsc::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let mut reports = Reports::start(output, health, origin, move || {
        entered.send(()).unwrap();
        blocked.recv().unwrap();
    })
    .unwrap();
    let pipeline = Pipeline::new(
        &metadata,
        &[MarketId(1)],
        u64::from(cfg!(feature = "processing-traces")),
    )
    .unwrap();
    reports
        .publish(&pipeline, 1, 0, 20, Quantity(1), &None)
        .unwrap();
    waiting.recv_timeout(Duration::from_secs(2)).unwrap();
    let dropped = tokio::task::spawn_blocking(move || drop(reports));
    let result = tokio::time::timeout(Duration::from_millis(500), dropped).await;
    release.send(()).unwrap();
    assert!(result.is_ok(), "optional worker must not hold cancellation");
}
#[tokio::test]
async fn failed_optional_report_worker_does_not_fail_publication_calls() {
    let metadata = fixture_metadata().unwrap();
    let (origin, output) = channel(&metadata, &[MarketId(1)], "test").unwrap();
    let (_health, health) = watch::channel(Health::default());
    let diagnostics = output.borrow().diagnostics.clone();
    let mut reports =
        Reports::start(output, health, origin, || panic!("injected report failure")).unwrap();
    let pipeline = Pipeline::new(
        &metadata,
        &[MarketId(1)],
        u64::from(cfg!(feature = "processing-traces")),
    )
    .unwrap();
    reports
        .publish(&pipeline, 1, 0, 20, Quantity(1), &None)
        .unwrap();
    for _ in 0..100 {
        if reports.thread.as_ref().unwrap().is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(reports.thread.as_ref().unwrap().is_finished());
    assert!(
        !reports
            .publish(&pipeline, 2, 0, 20, Quantity(1), &None)
            .unwrap()
    );
    assert_eq!(reports.skipped, 1);
    let status = diagnostics.snapshot().reports.unwrap();
    assert!(status.failed && !status.alive);
    assert_eq!(status.publications, 0);
    assert_eq!(status.skipped, 1);
    reports
        .finish(&pipeline, 3, 0, 20, Quantity(1), None)
        .await
        .unwrap();
}
