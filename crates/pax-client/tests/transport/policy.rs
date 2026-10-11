//! Policy changes must not disturb an in-progress encrypted frame or its bytes.
#![cfg(any(
    all(feature = "receive-stages", feature = "delivery-traces"),
    all(
        feature = "phc-background",
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ),
    all(
        feature = "diagnostic-handoff",
        any(feature = "receive-timestamps", feature = "receive-association")
    ),
))]
use super::*;
#[cfg(any(
    all(feature = "receive-stages", feature = "delivery-traces"),
    all(
        feature = "phc-background",
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    )
))]
async fn acknowledgment(
    feed: &mut Feed,
    mut reply: tokio::sync::oneshot::Receiver<
        Result<
            pax_client::websocket::receive::control::Applied,
            pax_client::websocket::receive::control::UpdateError,
        >,
    >,
) -> Result<
    pax_client::websocket::receive::control::Applied,
    pax_client::websocket::receive::control::UpdateError,
> {
    timeout(Duration::from_secs(2), async {
        loop {
            tokio::select! {
                result=&mut reply => return result.unwrap(),
                input=feed.recv()=> assert!(input.is_some(),"feed stopped during update"),
            }
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
#[cfg(all(
    feature = "receive-stages",
    feature = "receive-timestamps",
    feature = "delivery-traces"
))]
async fn live_updates_preserve_partial_tls_input_and_report_atomic_rejections() {
    use pax_client::{
        measurement::SampleRate,
        test_support::TlsPeer,
        websocket::receive::{ReceiveConfig, control::UpdateError},
    };
    use tokio::{io::AsyncWriteExt, sync::oneshot};
    for inline in [false, true] {
        let (listener, mut config) = fixture().await;
        config.receive = Some(ReceiveConfig::default());
        config.ping_interval = Duration::from_secs(10);
        let peer = TlsPeer::new().unwrap();
        config.url = config.url.replacen("ws:", "wss:", 1);
        config.tls_config = Some(peer.client);
        let (complete, proceed) = oneshot::channel();
        let payload = br#"{"type":"snapshot","data":{}}"#;
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let tls = tokio_rustls::TlsAcceptor::from(peer.server)
                .accept(socket)
                .await
                .unwrap();
            let mut ws = accept_async(tls).await.unwrap();
            ws.next().await.unwrap().unwrap();
            ws.send(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
                .await
                .unwrap();
            ws.get_mut()
                .write_all(&[0x81, payload.len() as u8])
                .await
                .unwrap();
            ws.get_mut().write_all(&payload[..10]).await.unwrap();
            ws.get_mut().flush().await.unwrap();
            proceed.await.unwrap();
            ws.get_mut().write_all(&payload[10..]).await.unwrap();
            ws.get_mut().flush().await.unwrap();
            while let Some(Ok(message)) = ws.next().await {
                if message.is_text() && message.to_text().unwrap().contains("ping") {
                    ws.send(Message::text(r#"{"type":"pong"}"#)).await.unwrap();
                }
            }
        });
        let mut feed = if inline {
            Feed::inline(config)
        } else {
            Feed::spawn(config)
        }
        .unwrap();
        assert!(matches!(next(&mut feed).await, Input::Connected { .. }));
        assert!(matches!(next(&mut feed).await, Input::Message { .. }));
        assert!(
            timeout(Duration::from_millis(20), feed.recv())
                .await
                .is_err()
        );
        let control = feed.measurement_control();
        let mut update = control.effective().settings;
        update.receive.as_mut().unwrap().policy.socket = SampleRate::OFF;
        update.delivery_sample_every = 1;
        let receipt = acknowledgment(&mut feed, control.try_update(update.clone()).unwrap())
            .await
            .unwrap();
        assert_eq!(receipt.revision, 1);
        assert_eq!(receipt.generation, 1);
        assert_eq!(receipt.after_message, 1);
        assert!(receipt.partial_observation_discarded);
        complete.send(()).unwrap();
        let Input::Message {
            bytes,
            receive: Some(trace),
            timing: Some(_),
            ..
        } = next(&mut feed).await
        else {
            panic!()
        };
        assert_eq!(bytes.as_ref(), payload);
        assert_eq!(trace.revision, 1);
        assert!(!trace.complete_policy_window);
        assert!(
            trace.plaintext.is_none(),
            "partial frame must not combine policy revisions"
        );
        assert!(trace.software.at.is_none());
        let mut unavailable = update.clone();
        unavailable.receive.as_mut().unwrap().software_timestamps = true;
        let result = acknowledgment(&mut feed, control.try_update(unavailable).unwrap()).await;
        assert!(matches!(result, Err(UpdateError::ReconnectRequired(_))));
        assert_eq!(control.effective().revision, 1);
        assert!(
            !control
                .effective()
                .settings
                .receive
                .unwrap()
                .software_timestamps
        );
        let mut changed_clock = update.clone();
        changed_clock.receive.as_mut().unwrap().phc_refresh_interval = Duration::from_millis(40);
        assert!(matches!(
            acknowledgment(&mut feed, control.try_update(changed_clock).unwrap()).await,
            Err(UpdateError::ReconnectRequired(_))
        ));
        assert_eq!(control.effective().revision, 1);
        assert_eq!(
            control
                .effective()
                .settings
                .receive
                .unwrap()
                .phc_refresh_interval,
            Duration::from_millis(20)
        );
        update.receive.as_mut().unwrap().policy.association = SampleRate::OFF;
        assert_eq!(
            acknowledgment(&mut feed, control.try_update(update.clone()).unwrap())
                .await
                .unwrap()
                .revision,
            2
        );
        update.receive.as_mut().unwrap().policy.association = SampleRate::ALL;
        assert!(matches!(
            acknowledgment(&mut feed, control.try_update(update).unwrap()).await,
            Err(UpdateError::ReconnectRequired(_))
        ));
        assert_eq!(control.effective().revision, 2);
        feed.shutdown().await;
        server.abort();
    }
}

#[cfg(all(
    feature = "phc-background",
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[tokio::test]
async fn connection_owned_clock_changes_require_reconnect_without_publishing_settings() {
    use pax_client::websocket::receive::{
        PhcCalibration, ReceiveConfig, control::UpdateError, kernel::HardwareConfig,
    };
    for inline in [false, true] {
        let (listener, mut config) = fixture().await;
        config.ping_interval = Duration::from_secs(10);
        config.receive = Some(ReceiveConfig {
            hardware: Some(HardwareConfig {
                // Deliberately unavailable: this checks source ownership without
                // opening a PHC device or requiring installed NIC capabilities.
                interface: "pax-no-interface".into(),
                clock: "/dev/pax-no-clock".into(),
            }),
            phc_calibration: PhcCalibration::BackgroundDriver,
            ..ReceiveConfig::default()
        });
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            ws.next().await.unwrap().unwrap();
            ws.send(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
                .await
                .unwrap();
            while let Some(Ok(_)) = ws.next().await {}
        });
        let mut feed = if inline {
            Feed::inline(config)
        } else {
            Feed::spawn(config)
        }
        .unwrap();
        assert!(matches!(next(&mut feed).await, Input::Connected { .. }));
        assert!(matches!(next(&mut feed).await, Input::Message { .. }));
        let control = feed.measurement_control();
        let original = control.effective().settings;
        for field in 0..4 {
            let mut requested = original.clone();
            let receive = requested.receive.as_mut().unwrap();
            match field {
                0 => receive.phc_calibration = PhcCalibration::SampledRead,
                1 => receive.phc_worker_cpu = Some(0),
                2 => receive.phc_deferred_mapping = true,
                3 => receive.phc_refresh_interval = Duration::from_millis(40),
                _ => unreachable!(),
            }
            let reply = control.try_update(requested).unwrap();
            assert!(
                matches!(
                    acknowledgment(&mut feed, reply).await,
                    Err(UpdateError::ReconnectRequired(_))
                ),
                "field {field}, inline {inline}"
            );
            let current = control.effective();
            assert_eq!(current.revision, 0);
            let current = current.settings.receive.unwrap();
            let expected = original.receive.as_ref().unwrap();
            assert_eq!(current.phc_calibration, expected.phc_calibration);
            assert_eq!(current.phc_worker_cpu, expected.phc_worker_cpu);
            assert_eq!(current.phc_deferred_mapping, expected.phc_deferred_mapping);
            assert_eq!(current.phc_refresh_interval, expected.phc_refresh_interval);
            assert_eq!(current.policy, expected.policy);
        }
        feed.shutdown().await;
        timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
#[cfg(all(feature = "receive-stages", feature = "delivery-traces"))]
async fn busy_delivery_and_reconnect_retain_accepted_revision() {
    use pax_client::{measurement::SampleRate, websocket::receive::ReceiveConfig};
    let (listener, mut config) = fixture().await;
    config.receive = Some(ReceiveConfig::default());
    config.ping_interval = Duration::from_secs(10);
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            ws.next().await.unwrap().unwrap();
            ws.send(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
                .await
                .unwrap();
            for _ in 0..100_000 {
                if ws
                    .send(Message::text(r#"{"type":"md_event_detail","data":{}}"#))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });
    let mut feed = Feed::inline(config).unwrap();
    let control = feed.measurement_control();
    assert!(matches!(next(&mut feed).await, Input::Connected { .. }));
    assert!(matches!(next(&mut feed).await, Input::Message { .. }));
    let mut update = control.effective().settings;
    update.delivery_sample_every = 7;
    update.receive.as_mut().unwrap().policy.tls = SampleRate::every(3);
    let receipt = acknowledgment(&mut feed, control.try_update(update).unwrap())
        .await
        .unwrap();
    assert_eq!(receipt.revision, 1);
    feed.request_recovery();
    disconnected(&mut feed).await;
    assert!(matches!(
        next(&mut feed).await,
        Input::Connected { generation: 2, .. }
    ));
    let Input::Message {
        receive: Some(trace),
        ..
    } = next(&mut feed).await
    else {
        panic!()
    };
    assert_eq!(trace.revision, 1);
    assert_eq!(trace.policy.tls, SampleRate::every(3));
    feed.shutdown().await;
    server.abort();
}

#[cfg(all(
    feature = "diagnostic-handoff",
    any(feature = "receive-timestamps", feature = "receive-association")
))]
#[tokio::test]
async fn stalled_and_dropped_diagnostic_consumers_do_not_block_tls_input_or_heartbeat() {
    use pax_client::{test_support::TlsPeer, websocket::receive::ReceiveConfig};
    for inline in [false, true] {
        let (listener, mut config) = fixture().await;
        config.receive = Some(ReceiveConfig {
            ..Default::default()
        });
        let peer = TlsPeer::new().unwrap();
        config.url = config.url.replacen("ws:", "wss:", 1);
        config.tls_config = Some(peer.client);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let tls = tokio_rustls::TlsAcceptor::from(peer.server)
                .accept(socket)
                .await
                .unwrap();
            let mut ws = accept_async(tls).await.unwrap();
            ws.next().await.unwrap().unwrap();
            ws.send(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
                .await
                .unwrap();
            for i in 0..200 {
                ws.send(Message::text(format!(
                    r#"{{"type":"md_event_detail","test":{i}}}"#
                )))
                .await
                .unwrap();
            }
            while let Some(Ok(message)) = ws.next().await {
                if message.is_text() && message.to_text().unwrap().contains("ping") {
                    ws.send(Message::text(r#"{"type":"pong"}"#)).await.unwrap();
                }
            }
        });
        let (mut feed, diagnostics) = if inline {
            Feed::inline_with_diagnostics(config, 1)
        } else {
            Feed::spawn_with_diagnostics(config, 1)
        }
        .unwrap();
        let status = diagnostics.status();
        assert!(matches!(next(&mut feed).await, Input::Connected { .. }));
        assert!(matches!(
            next(&mut feed).await,
            Input::Message { receive: None, .. }
        ));
        for i in 0..200 {
            let Input::Message {
                bytes,
                receive,
                received,
                ..
            } = next(&mut feed).await
            else {
                panic!()
            };
            assert_eq!(
                bytes.as_ref(),
                format!(r#"{{"type":"md_event_detail","test":{i}}}"#).as_bytes()
            );
            assert!(receive.is_none());
            assert!(received <= std::time::Instant::now());
        }
        let covered = status.snapshot();
        assert_eq!(covered.accepted, 1);
        assert_eq!(covered.full_drops, 200);
        assert_eq!(covered.pending, 1);
        drop(diagnostics);
        let Input::Message {
            bytes,
            receive: None,
            ..
        } = next(&mut feed).await
        else {
            panic!()
        };
        assert_eq!(bytes.as_ref(), br#"{"type":"pong"}"#);
        assert!(feed.health().borrow().round_trip.is_some());
        assert!(!status.snapshot().consumer_alive);
        assert!(status.snapshot().closed_drops > 0);
        feed.shutdown().await;
        assert!(!status.snapshot().producer_alive);
        server.abort();
    }
}
