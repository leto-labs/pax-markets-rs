//! Minimal reconstruction and explicit requests for omitted measurement groups.
use pax_client::test_support::fixture::Fixture;
use pax_demo::{
    capture::{Record, RecordKind},
    measurement::{capabilities::CAPABILITIES, policy::ProcessingPolicy},
    pipeline::Pipeline,
};
use pax_types::{
    market::ExchangeInfo,
    units::{MarketId, Quantity},
};
#[tokio::test]
async fn compiled_out_receive_has_no_diagnostic_queue_even_when_export_is_compiled() {
    if CAPABILITIES.client.receive_stages
        || CAPABILITIES.client.receive_timestamps
        || CAPABILITIES.client.association()
    {
        return;
    }
    use futures_util::{SinkExt, StreamExt};
    use pax_demo::runner::{self, RunConfig};
    use std::time::Duration;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let fixture = Fixture::synthetic(10, 20).unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        for message in fixture.messages {
            ws.send(tokio_tungstenite::tungstenite::Message::text(
                std::str::from_utf8(&message).unwrap(),
            ))
            .await
            .unwrap();
        }
        while let Some(Ok(message)) = ws.next().await {
            if message.is_close() {
                break;
            }
        }
    });
    let metadata = runner::offline::fixture_metadata().unwrap();
    let (origin, output) = runner::channel(&metadata, &[MarketId(1)], "live").unwrap();
    let reports = output.subscribe();
    let mut feed = pax_client::websocket::FeedConfig::public(vec![MarketId(1)]);
    feed.url = format!("ws://{address}");
    let config = RunConfig {
        measurements: pax_demo::measurement::settings::Settings::off(),
        feed,
        metadata,
        capture: None,
        receive_sidecar: None,
        duration: Some(Duration::from_millis(100)),
        depth: 20,
        quantity: Quantity(1),
        sample_every: 0,
        inline_delivery: true,
        report_on_ingestion: false,
        queue_drain: 1,
    };
    let (_stop, receiver) = tokio::sync::watch::channel(false);
    runner::live(config, origin, output, receiver)
        .await
        .unwrap();
    let report = reports.borrow();
    assert_eq!(report.view.applied, "11");
    assert!(report.view.latency.receive.is_empty());
    assert!(report.diagnostics.snapshot().receive_queue.is_none());
    drop(report);
    server.await.unwrap();
}
#[test]
fn resolved_groups_preserve_reconstruction_and_reject_omitted_processing() {
    let metadata: ExchangeInfo = serde_json::from_slice(include_bytes!(
        "../../pax-types/tests/fixtures/metadata.json"
    ))
    .unwrap();
    let mut pipeline = Pipeline::new(&metadata, &[MarketId(1)], 0).unwrap();
    assert_eq!(
        pipeline
            .configure_measurements(ProcessingPolicy::every(1))
            .is_ok(),
        CAPABILITIES.processing_traces
    );
    pipeline
        .configure_measurements(ProcessingPolicy::OFF)
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
    let fixture = Fixture::synthetic(1000, 20).unwrap();
    for (i, bytes) in fixture.messages.into_iter().enumerate() {
        let result = pipeline
            .process(
                &Record {
                    generation: 1,
                    offset_ns: i as u64 + 1,
                    kind: RecordKind::Message(bytes),
                },
                0,
            )
            .unwrap();
        assert!(!result.recover);
    }
    let view = pipeline.view("test", 1002, 20, Quantity(1));
    assert_eq!(view.applied, "1001");
    assert_eq!(view.markets[0].sequence.as_deref(), Some("1000"));
    assert!(view.latency.traces.is_empty());
    assert!(view.latency.receive.is_empty());
    assert_eq!(view.latency.capabilities, CAPABILITIES);
}
#[test]
fn omitted_export_returns_an_error_before_creating_a_file() {
    if CAPABILITIES.telemetry_export {
        return;
    }
    let path = std::env::temp_dir().join(format!("pax-compiled-out-{}", std::process::id()));
    let result = pax_demo::measurement::sidecar::Sidecar::start(
        pax_demo::measurement::sidecar::SidecarConfig {
            sample_rate: pax_client::measurement::SampleRate::ALL,
            path: path.clone(),
            max_bytes: 4096,
            capacity: 1,
        },
    );
    assert!(matches!(result,Err(e) if e.kind()==std::io::ErrorKind::Unsupported));
    assert!(!path.exists());
}

#[test]
fn delivery_collection_is_independent_of_processing_probes() {
    use pax_client::websocket::DeliveryTiming;
    let metadata: ExchangeInfo = serde_json::from_slice(include_bytes!(
        "../../pax-types/tests/fixtures/metadata.json"
    ))
    .unwrap();
    let mut p = Pipeline::new(&metadata, &[MarketId(1)], 0).unwrap();
    p.process(
        &Record {
            generation: 1,
            offset_ns: 0,
            kind: RecordKind::Connected,
        },
        0,
    )
    .unwrap();
    let at = std::time::Instant::now();
    p.process_delivered(
        &Record {
            generation: 1,
            offset_ns: 1,
            kind: RecordKind::Message(bytes::Bytes::from_static(br#"{"type":"pong"}"#)),
        },
        at,
        Some(DeliveryTiming {
            prepared: at,
            delivered: Some(at),
            pending: Some(0),
        }),
    )
    .unwrap();
    let view = p.view("test", 1, 20, Quantity(1));
    assert_eq!(
        view.latency.traces.len(),
        usize::from(CAPABILITIES.client.delivery_traces)
    );
    if let Some(t) = view.latency.traces.first() {
        assert!(t.delivery.is_some() && t.queue_ns.is_some());
        assert!(
            t.processing_ns.is_none()
                && t.decode_ns.is_none()
                && t.preflight_ns.is_none()
                && t.mutation_ns.is_none()
                && t.validation_ns.is_none()
        );
    }
}
