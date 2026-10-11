//! Local transport-to-book recovery and captured replay equivalence, without Pax.
use futures_util::{SinkExt, StreamExt};
use pax_client::websocket::FeedConfig;
use pax_demo::{
    capture::CaptureConfig,
    pipeline::Pipeline,
    replay::Replay,
    runner::{
        self, RunConfig,
        placement::{self, Placement},
    },
};
use pax_types::units::{MarketId, Quantity};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn gap_forces_new_snapshot_and_capture_replays_identically() {
    tokio::time::timeout(
        Duration::from_secs(5),
        scenario(Placement::Shared, false, false),
    )
    .await
    .unwrap();
}
#[tokio::test]
async fn dedicated_gap_recovery_and_capture_replay_are_identical() {
    tokio::time::timeout(
        Duration::from_secs(5),
        scenario(Placement::Dedicated { cpu: None }, false, false),
    )
    .await
    .unwrap();
}
#[tokio::test]
async fn inline_gap_recovery_and_capture_replay_are_identical() {
    for placement in [Placement::Shared, Placement::Dedicated { cpu: None }] {
        tokio::time::timeout(Duration::from_secs(5), scenario(placement, true, false))
            .await
            .unwrap();
    }
}
async fn scenario(placement: Placement, inline_delivery: bool, diagnostics: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for generation in 1..=2 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            let subscribe = ws.next().await.unwrap().unwrap();
            assert!(subscribe.to_text().unwrap().contains("subscribe"));
            ws.send(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
                .await
                .unwrap();
            ws.send(Message::text(include_str!(
                "../../pax-types/tests/fixtures/snapshot.json"
            )))
            .await
            .unwrap();
            let event = include_str!("../../pax-types/tests/fixtures/xa.json");
            let event = if generation == 1 {
                event.replace("\"seq_num\":\"3\"", "\"seq_num\":\"4\"")
            } else {
                event.into()
            };
            ws.send(Message::text(event)).await.unwrap();
            while let Some(Ok(message)) = ws.next().await {
                if message.is_close() {
                    break;
                }
                if message.to_text().is_ok_and(|s| s.contains("ping")) {
                    let _ = ws.send(Message::text(r#"{"type":"pong"}"#)).await;
                }
            }
        }
    });
    let metadata = serde_json::from_slice(include_bytes!(
        "../../pax-types/tests/fixtures/metadata.json"
    ))
    .unwrap();
    let (origin, output) = runner::channel(&metadata, &[MarketId(1)], "fixture").unwrap();
    let mut updates = output.subscribe();
    let path = std::env::temp_dir().join(format!(
        "pax-live-replay-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut feed = FeedConfig::public(vec![MarketId(1)]);
    feed.url = format!("ws://{address}");
    feed.retry_min = Duration::from_millis(1);
    feed.receive = diagnostics.then(pax_client::websocket::receive::ReceiveConfig::default);
    let config = RunConfig {
        measurements: Default::default(),
        feed,
        metadata,
        capture: Some(CaptureConfig {
            directory: path.clone(),
            queue_capacity: 128,
            max_bytes: 1_000_000,
            max_record_bytes: 65536,
        }),
        duration: None,
        depth: 20,
        quantity: Quantity(1000),
        sample_every: 1,
        inline_delivery,
        queue_drain: if inline_delivery { 1 } else { 8 },
        report_on_ingestion: false,
        receive_sidecar: diagnostics.then(|| pax_demo::measurement::sidecar::SidecarConfig {
            sample_rate: pax_client::measurement::SampleRate::ALL,
            path: path.with_extension("receive.jsonl"),
            capacity: 128,
            max_bytes: 1_000_000,
        }),
    };
    let (stop, receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(placement::run(
        placement,
        runner::live(config, origin, output, receiver),
    ));
    loop {
        updates.changed().await.unwrap();
        let report = updates.borrow_and_update();
        let market = &report.view.markets[0];
        if market.generation == "2"
            && market.sequence.as_deref() == Some("3")
            && market.status == "live"
        {
            break;
        }
    }
    stop.send(true).unwrap();
    let digest = task.await.unwrap().unwrap();
    if diagnostics {
        let report = updates.borrow();
        let status = report
            .view
            .latency
            .diagnostics
            .as_ref()
            .expect("deferred worker status");
        assert_eq!(status.queue.full_drops, 0);
        assert_eq!(status.queue.closed_drops, 0);
        assert_eq!(status.queue.pending, 0);
        assert_eq!(status.invalid_clock, 0);
        assert!(!report.view.latency.receive.is_empty());
        assert!(
            report
                .view
                .latency
                .receive
                .iter()
                .any(|r| r.id.generation == 2)
        );
        let sidecar = path.with_extension("receive.jsonl");
        let inspected = pax_demo::measurement::sidecar::inspect(&sidecar, 1_000_000)
            .unwrap()
            .unwrap();
        assert_eq!(inspected["complete"], true);
        assert_eq!(inspected["version"], 3);
        std::fs::remove_file(sidecar).unwrap();
    }
    server.await.unwrap();
    let mut replay = Replay::open(&path, 1_000_000).unwrap();
    let mut book =
        Pipeline::new(&replay.manifest().metadata, &replay.manifest().markets, 1).unwrap();
    while let Some(record) = replay.next_record().unwrap() {
        book.process(&record, 0).unwrap();
    }
    book.finish(replay.manifest().end_offset_ns).unwrap();
    assert_eq!(book.digest(), digest);
    assert_eq!(
        book.view(
            "replay",
            replay.manifest().end_offset_ns,
            20,
            Quantity(1000)
        )
        .rejected,
        "1"
    );
    std::fs::remove_dir_all(path).unwrap();
}

// A continuously ready socket/queue must still allow the runner deadline and
// report task to run on a single-thread runtime.
#[tokio::test]
async fn bounded_drains_and_inline_frames_allow_control_progress() {
    for (inline_delivery, queue_drain) in [(false, 256), (true, 1)] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            ws.next().await.unwrap().unwrap();
            ws.send(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
                .await
                .unwrap();
            while ws.send(Message::text(r#"{"type":"pong"}"#)).await.is_ok() {}
        });
        let metadata = runner::offline::fixture_metadata().unwrap();
        let (origin, output) = runner::channel(&metadata, &[MarketId(1)], "live").unwrap();
        let updates = output.subscribe();
        let mut feed = FeedConfig::public(vec![MarketId(1)]);
        feed.url = format!("ws://{address}");
        let config = RunConfig {
            measurements: Default::default(),
            feed,
            metadata,
            capture: None,
            duration: Some(Duration::from_millis(50)),
            depth: 20,
            quantity: Quantity(1),
            sample_every: 64,
            inline_delivery,
            queue_drain,
            report_on_ingestion: false,
            receive_sidecar: None,
        };
        let (_stop, receiver) = tokio::sync::watch::channel(false);
        tokio::time::timeout(
            Duration::from_secs(2),
            runner::live(config, origin, output, receiver),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(updates.borrow().view.mode, "completed observation");
        assert_ne!(updates.borrow().view.messages, "0");
        server.abort();
    }
}

#[cfg(feature = "telemetry-export")]
#[tokio::test]
async fn deferred_receive_and_sidecar_preserve_recovery_and_canonical_capture() {
    for inline in [false, true] {
        tokio::time::timeout(
            Duration::from_secs(5),
            scenario(Placement::Shared, inline, true),
        )
        .await
        .unwrap();
    }
}
