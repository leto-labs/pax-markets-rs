//! This target runs under minimal, individual and mixed feature selections.
use futures_util::{SinkExt, StreamExt};
use pax_client::{
    measurement::{SampleRate, capabilities::CAPABILITIES},
    websocket::{
        Feed, FeedConfig, Input,
        receive::{ReceiveConfig, policy::ReceivePolicy},
    },
};
use pax_types::units::MarketId;
#[test]
fn compiled_out_requests_are_explicit_and_available_defaults_validate() {
    ReceiveConfig::default().validate().unwrap();
    let requested = ReceiveConfig {
        policy: ReceivePolicy::TIMESTAMPS,
        ..ReceiveConfig::default()
    };
    assert_eq!(
        requested.validate().is_ok(),
        CAPABILITIES.receive_timestamps
    );
    let requested = ReceiveConfig {
        policy: ReceivePolicy {
            socket: SampleRate::ALL,
            ..ReceivePolicy::OFF
        },
        ..ReceiveConfig::default()
    };
    assert_eq!(requested.validate().is_ok(), CAPABILITIES.receive_stages);
    let requested = ReceiveConfig {
        policy: ReceivePolicy::INGRESS,
        ..ReceiveConfig::default()
    };
    assert_eq!(
        requested.validate().is_ok(),
        CAPABILITIES.receive_timestamps && CAPABILITIES.association()
    );
    let mut config = FeedConfig::public(vec![MarketId(1)]);
    config.delivery_sample_every = 1;
    assert_eq!(config.validate().is_ok(), CAPABILITIES.delivery_traces);
    config.delivery_sample_every = 0;
    config.socket_readbacks = true;
    assert_eq!(
        config.validate().is_ok(),
        CAPABILITIES.socket_readbacks == Some(true)
    );
}

#[test]
fn legacy_capability_metadata_remains_absent_and_recognizes_stage_association() {
    use pax_client::measurement::capabilities::Capabilities;
    for stages in [false, true] {
        let value = serde_json::json!({"receive_timestamps":true,
            "receive_stages":stages,"delivery_traces":false});
        let legacy: Capabilities = serde_json::from_value(value).unwrap();
        assert_eq!(legacy.receive_association, None);
        assert_eq!(legacy.clock_model_details, None);
        assert_eq!(legacy.socket_readbacks, None);
        assert!(
            serde_json::to_value(legacy)
                .unwrap()
                .get("socket_readbacks")
                .is_none()
        );
        assert!(!legacy.clock_details());
        assert!(
            serde_json::to_value(legacy)
                .unwrap()
                .get("clock_model_details")
                .is_none()
        );
        assert_eq!(legacy.association(), stages);
        assert!(
            serde_json::to_value(legacy)
                .unwrap()
                .get("receive_association")
                .is_none()
        );
    }
    let legacy_worker: Capabilities = serde_json::from_value(serde_json::json!({"receive_timestamps":true,"phc_background":true,"receive_stages":false,"delivery_traces":false})).unwrap();
    assert!(legacy_worker.clock_details());
    assert_eq!(
        CAPABILITIES.clock_model_details,
        Some(cfg!(feature = "clock-model-details"))
    );
    assert_eq!(
        CAPABILITIES.receive_association,
        Some(cfg!(feature = "receive-association"))
    );
}

#[cfg(all(target_os = "linux", feature = "receive-association"))]
#[tokio::test]
async fn ingress_only_tls_fragments_controls_and_recovery_preserve_first_read() {
    use pax_client::test_support::TlsPeer;
    use std::time::{Duration, Instant};
    use tokio_rustls::TlsAcceptor;
    use tokio_tungstenite::tungstenite::{
        Message,
        protocol::frame::{
            Frame,
            coding::{Data, OpCode},
        },
    };
    for inline in [true, false] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut peer = TlsPeer::new().unwrap();
        std::sync::Arc::get_mut(&mut peer.server)
            .unwrap()
            .max_fragment_size = Some(64);
        let mut config = FeedConfig::public(vec![MarketId(1)]);
        config.url = format!("wss://localhost:{}/", listener.local_addr().unwrap().port());
        config.tls_config = Some(peer.client);
        config.read_buffer_bytes = 1024;
        config.receive = Some(ReceiveConfig {
            policy: if cfg!(feature = "receive-timestamps") {
                ReceivePolicy::INGRESS
            } else {
                ReceivePolicy {
                    association: SampleRate::ALL,
                    ..ReceivePolicy::OFF
                }
            },
            software_timestamps: cfg!(feature = "receive-timestamps"),
            ..ReceiveConfig::default()
        });
        let expected = format!(r#"{{"type":"snapshot","padding":"{}"}}"#, "x".repeat(8192));
        let payload = expected.clone();
        let server = tokio::spawn(async move {
            let acceptor = TlsAcceptor::from(peer.server);
            for _ in 0..2 {
                let (socket, _) = listener.accept().await.unwrap();
                let mut ws =
                    tokio_tungstenite::accept_async(acceptor.accept(socket).await.unwrap())
                        .await
                        .unwrap();
                ws.next().await.unwrap().unwrap();
                ws.feed(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
                    .await
                    .unwrap();
                let split = payload.len() / 2;
                ws.feed(Message::Frame(Frame::message(
                    payload.as_bytes()[..split].to_vec(),
                    OpCode::Data(Data::Text),
                    false,
                )))
                .await
                .unwrap();
                ws.feed(Message::Ping(bytes::Bytes::from_static(b"ingress-control")))
                    .await
                    .unwrap();
                ws.feed(Message::Frame(Frame::message(
                    payload.as_bytes()[split..].to_vec(),
                    OpCode::Data(Data::Continue),
                    true,
                )))
                .await
                .unwrap();
                ws.flush().await.unwrap();
                while ws.next().await.is_some() {}
            }
        });
        let mut feed = if inline {
            Feed::inline(config)
        } else {
            Feed::spawn(config)
        }
        .unwrap();
        let mut previous_clock = None;
        for generation in 1..=2 {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut saw_message = false;
            while !saw_message {
                let input = tokio::time::timeout_at(deadline.into(), feed.recv())
                    .await
                    .unwrap()
                    .unwrap();
                if let Input::Message { bytes, receive, .. } = input
                    && bytes.as_ref() == expected.as_bytes()
                {
                    let timing = receive.unwrap();
                    assert_eq!(timing.generation, generation);
                    assert_eq!(timing.lost, 0);
                    let first = timing.first_observed_read.unwrap();
                    if !cfg!(feature = "receive-timestamps") {
                        assert!(first.software.at.is_none() && first.hardware.at.is_none());
                    }
                    if cfg!(feature = "clock-model-details") && first.software.at.is_some() {
                        let model = first.software.qualification().unwrap();
                        assert_eq!(
                            model.method,
                            pax_client::websocket::receive::kernel::ClockMethod::UserBracket
                        );
                        assert!(!model.rate_validated && !model.mapping_deferred);
                        assert!(model.generation > 0 && model.epoch > 0);
                        assert_ne!(previous_clock, Some(model.generation));
                        previous_clock = Some(model.generation);
                    } else {
                        assert!(first.software.qualification().is_none());
                    }
                    if cfg!(feature = "clock-model-details") {
                        assert!(
                            first.software.at.is_some(),
                            "loopback fixture must exercise qualified clock annotations: {:?}",
                            first.software
                        );
                    }
                    assert_eq!(first.batch, timing.socket.unwrap().first);
                    assert_eq!(Some(first.ended), timing.first_read);
                    assert!(Some(first.ended) <= timing.last_read);
                    assert!(timing.plaintext.unwrap().last > timing.plaintext.unwrap().first);
                    assert!(
                        timing.work.socket_ns.is_none()
                            && timing.work.tls_ns.is_none()
                            && timing.work.websocket_ns.is_none()
                            && timing.work.pending_ns.is_none()
                    );
                    assert_eq!(
                        timing.work.socket.samples
                            + timing.work.tls.samples
                            + timing.work.websocket.samples,
                        0
                    );
                    saw_message = true;
                }
            }
            if generation == 1 {
                feed.request_recovery();
            }
        }
        feed.shutdown().await;
        server.abort();
    }
}

#[test]
fn hardware_clock_refresh_is_finite_before_connection() {
    let mut config = ReceiveConfig::default();
    for interval in [
        std::time::Duration::ZERO,
        std::time::Duration::from_millis(100),
    ] {
        config.phc_refresh_interval = interval;
        config.validate().unwrap();
    }
    config.phc_refresh_interval = std::time::Duration::from_millis(101);
    assert_eq!(
        config.validate().unwrap_err(),
        "PHC refresh interval must be at most 100 ms"
    );
}
#[tokio::test]
async fn canonical_bytes_timestamps_and_recovery_survive_without_optional_groups() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = FeedConfig::public(vec![MarketId(1)]);
    config.url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            ws.next().await.unwrap().unwrap();
            ws.send(tokio_tungstenite::tungstenite::Message::text(
                r#"{"type":"subscribed","market_ids":[1]}"#,
            ))
            .await
            .unwrap();
            ws.send(tokio_tungstenite::tungstenite::Message::text(
                r#"{"type":"snapshot","data":{}}"#,
            ))
            .await
            .unwrap();
            while ws.next().await.is_some() {}
        }
    });
    let mut feed = Feed::inline(config).unwrap();
    for generation in 1..=2 {
        assert!(
            matches!(feed.recv().await,Some(Input::Connected {generation:g,..}) if g==generation)
        );
        assert!(matches!(feed.recv().await, Some(Input::Message { .. })));
        let Some(Input::Message {
            bytes,
            received,
            timing,
            receive,
            ..
        }) = feed.recv().await
        else {
            panic!()
        };
        assert_eq!(bytes.as_ref(), br#"{"type":"snapshot","data":{}}"#);
        assert!(received <= std::time::Instant::now());
        assert!(timing.is_none() && receive.is_none());
        if generation == 1 {
            feed.request_recovery();
            assert!(matches!(
                feed.recv().await,
                Some(Input::Disconnected { .. })
            ));
        }
    }
    feed.shutdown().await;
    server.abort();
}
