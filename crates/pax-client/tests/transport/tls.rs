//! Real encrypted peers with independent generated trust and bounded failures.
use super::*;
use pax_client::{test_support::TlsPeer, websocket::receive::ReceiveConfig};
use tokio::io::AsyncWriteExt;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::protocol::frame::{
    Frame,
    coding::{Data, OpCode},
};

#[tokio::test]
async fn tls_fragments_batches_controls_and_recovery_preserve_bytes() {
    for (inline, diagnostics) in [(false, false), (true, false), (false, true), (true, true)]
        .into_iter()
        .filter(|(_, diagnostics)| {
            !diagnostics
                || cfg!(all(
                    feature = "receive-timestamps",
                    feature = "receive-stages"
                ))
        })
    {
        let (listener, mut config) = fixture().await;
        config.receive = diagnostics.then(|| ReceiveConfig {
            software_timestamps: true,
            ..ReceiveConfig::default()
        });
        let mut peer = TlsPeer::new().unwrap();
        // Force many TLS records independently of WebSocket message boundaries.
        std::sync::Arc::get_mut(&mut peer.server)
            .unwrap()
            .max_fragment_size = Some(64);
        config.url = config.url.replacen("ws:", "wss:", 1);
        config.tls_config = Some(peer.client);
        config.read_buffer_bytes = 1024;
        config.ping_interval = Duration::from_secs(10);
        let payload = format!(r#"{{"type":"snapshot","padding":"{}"}}"#, "x".repeat(16384));
        let expected = payload.clone();
        let server = tokio::spawn(async move {
            let acceptor = TlsAcceptor::from(peer.server);
            for _ in 0..2 {
                let (socket, _) = listener.accept().await.unwrap();
                let tls = acceptor.accept(socket).await.unwrap();
                let mut ws = accept_async(tls).await.unwrap();
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
                ws.feed(Message::Ping(bytes::Bytes::from_static(b"control")))
                    .await
                    .unwrap();
                ws.feed(Message::Frame(Frame::message(
                    payload.as_bytes()[split..].to_vec(),
                    OpCode::Data(Data::Continue),
                    true,
                )))
                .await
                .unwrap();
                ws.feed(Message::text(r#"{"type":"md_event_detail","data":{}}"#))
                    .await
                    .unwrap();
                ws.flush().await.unwrap();
                while let Some(Ok(message)) = ws.next().await {
                    if message.is_text() && message.to_text().unwrap().contains("ping") {
                        ws.send(Message::text(r#"{"type":"pong"}"#)).await.unwrap();
                    }
                }
            }
        });
        let mut feed = if inline {
            Feed::inline(config)
        } else {
            Feed::spawn(config)
        }
        .unwrap();
        for generation in 1..=2 {
            assert!(
                matches!(next(&mut feed).await, Input::Connected { generation:g, .. } if g==generation)
            );
            assert!(matches!(next(&mut feed).await, Input::Message { .. }));
            let Input::Message { bytes, receive, .. } = next(&mut feed).await else {
                panic!()
            };
            assert_eq!(bytes.as_ref(), expected.as_bytes());
            assert_eq!(receive.is_some(), diagnostics);
            if let Some(timing) = receive {
                assert_eq!(timing.generation, generation);
                assert_eq!(timing.lost, 0);
                let range = timing.plaintext.unwrap();
                let first = timing.first_observed_read.unwrap();
                assert_eq!(first.batch, timing.socket.unwrap().first);
                assert_eq!(Some(first.ended), timing.first_read);
                assert!(Some(first.ended) <= timing.last_read);
                assert!(
                    range.last > range.first,
                    "fragmented large message spans plaintext reads"
                );
                assert!(timing.socket.is_some());
                assert!(timing.work.polls > 0);
                #[cfg(target_os = "linux")]
                {
                    let d = timing.work.read_detail;
                    assert!(d.reads > 0 && d.bytes > 0);
                    assert_eq!(d.copied_bytes, 0);
                    let internals = [
                        d.readiness,
                        d.preparation,
                        d.syscall,
                        d.metadata,
                        d.delivery,
                    ]
                    .iter()
                    .map(|p| p.exclusive_ns.unwrap())
                    .sum::<u64>();
                    assert!(
                        internals <= timing.work.socket_ns.unwrap(),
                        "disjoint internals stay inside socket work"
                    );
                }
            }
            let Input::Message { bytes, .. } = next(&mut feed).await else {
                panic!()
            };
            assert_eq!(bytes.as_ref(), br#"{"type":"md_event_detail","data":{}}"#);
            if generation == 1 {
                feed.request_recovery();
                assert!(!disconnected(&mut feed).await.2);
            }
        }
        feed.shutdown().await;
        server.abort();
    }
}

#[tokio::test]
async fn tls_does_not_accept_an_untrusted_certificate() {
    for (inline, diagnostics) in [(false, false), (true, false), (false, true), (true, true)]
        .into_iter()
        .filter(|(_, diagnostics)| {
            !diagnostics
                || cfg!(all(
                    feature = "receive-timestamps",
                    feature = "receive-stages"
                ))
        })
    {
        let (listener, mut config) = fixture().await;
        config.receive = diagnostics.then(|| ReceiveConfig {
            software_timestamps: true,
            ..ReceiveConfig::default()
        });
        let server_peer = TlsPeer::new().unwrap();
        let unrelated_peer = TlsPeer::new().unwrap();
        config.url = config.url.replacen("ws:", "wss:", 1);
        config.tls_config = Some(unrelated_peer.client);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            assert!(
                TlsAcceptor::from(server_peer.server)
                    .accept(socket)
                    .await
                    .is_err()
            );
        });
        let mut feed = if inline {
            Feed::inline(config)
        } else {
            Feed::spawn(config)
        }
        .unwrap();
        let (generation, reason, _) = disconnected(&mut feed).await;
        assert_eq!(generation, 0);
        assert!(reason.contains("certificate"), "{reason}");
        feed.shutdown().await;
        server.await.unwrap();
    }
}

#[tokio::test]
async fn stalled_tls_setup_releases_the_socket_on_timeout_and_shutdown() {
    use tokio::io::AsyncReadExt;

    for (inline, diagnostics) in [(false, false), (true, false), (false, true), (true, true)] {
        for shutdown in [false, true] {
            let (listener, mut config) = fixture().await;
            config.receive = diagnostics.then(ReceiveConfig::default);
            config.url = config.url.replacen("ws:", "wss:", 1);
            config.tls_config = Some(TlsPeer::new().unwrap().client);
            config.connect_timeout = if shutdown {
                Duration::from_secs(30)
            } else {
                Duration::from_millis(200)
            };
            config.retry_min = Duration::from_secs(30);
            config.retry_max = Duration::from_secs(30);
            let (started, ready) = tokio::sync::oneshot::channel();
            let mut server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                // Observe the ClientHello, but never return a handshake response.
                socket.read_exact(&mut [0; 1]).await.unwrap();
                started.send(()).unwrap();
                tokio::io::copy(&mut socket, &mut tokio::io::sink())
                    .await
                    .unwrap();
            });
            let mut feed = if inline {
                Feed::inline(config)
            } else {
                Feed::spawn(config)
            }
            .unwrap();
            timeout(Duration::from_secs(2), async {
                tokio::select! {
                    result = ready => result.unwrap(),
                    input = feed.recv() => panic!("setup finished before the stalled peer: {input:?}"),
                }
            })
            .await
            .unwrap();
            if !shutdown {
                let (generation, reason, lossy) = disconnected(&mut feed).await;
                assert_eq!(generation, 0);
                assert!(reason.contains("connection timeout"), "{reason}");
                assert!(!lossy);
                // Timeout must release setup ownership before explicit shutdown.
                timeout(Duration::from_secs(1), &mut server)
                    .await
                    .unwrap()
                    .unwrap();
            }
            timeout(Duration::from_secs(1), feed.shutdown())
                .await
                .unwrap();
            if shutdown {
                // Completion means the peer observed EOF after a canceled recv.
                timeout(Duration::from_secs(1), server)
                    .await
                    .unwrap()
                    .unwrap();
            }
        }
    }
}

#[tokio::test]
async fn canceled_inline_receive_retains_tls_handshake_and_partial_frame() {
    for diagnostics in [false, true] {
        canceled_receive(diagnostics).await;
    }
}
async fn canceled_receive(diagnostics: bool) {
    let (listener, mut config) = fixture().await;
    config.receive = diagnostics.then(ReceiveConfig::default);
    let peer = TlsPeer::new().unwrap();
    config.url = config.url.replacen("ws:", "wss:", 1);
    config.tls_config = Some(peer.client);
    config.ping_interval = Duration::from_secs(10);
    let (handshake, proceed) = tokio::sync::oneshot::channel();
    let (frame, complete) = tokio::sync::oneshot::channel();
    let payload = br#"{"type":"subscribed","market_ids":[1]}"#;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        proceed.await.unwrap();
        let tls = TlsAcceptor::from(peer.server).accept(socket).await.unwrap();
        let mut ws = accept_async(tls).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.get_mut()
            .write_all(&[0x81, payload.len() as u8])
            .await
            .unwrap();
        ws.get_mut().write_all(&payload[..10]).await.unwrap();
        ws.get_mut().flush().await.unwrap();
        complete.await.unwrap();
        ws.get_mut().write_all(&payload[10..]).await.unwrap();
        ws.get_mut().flush().await.unwrap();
        while ws.next().await.is_some() {}
    });
    let mut feed = Feed::inline(config).unwrap();
    assert!(
        timeout(Duration::from_millis(15), feed.recv())
            .await
            .is_err()
    );
    handshake.send(()).unwrap();
    assert!(matches!(next(&mut feed).await, Input::Connected { .. }));
    assert!(
        timeout(Duration::from_millis(15), feed.recv())
            .await
            .is_err()
    );
    frame.send(()).unwrap();
    let Input::Message { bytes, .. } = next(&mut feed).await else {
        panic!()
    };
    assert_eq!(bytes.as_ref(), payload);
    feed.shutdown().await;
    server.abort();
}

#[tokio::test]
#[cfg(all(
    target_os = "linux",
    feature = "receive-timestamps",
    feature = "receive-stages"
))]
async fn independent_receive_policies_preserve_tls_bytes_and_qualify_partial_work() {
    use pax_client::{measurement::SampleRate, websocket::receive::policy::ReceivePolicy};
    for policy in [
        ReceivePolicy::OFF,
        ReceivePolicy::TIMESTAMPS,
        ReceivePolicy {
            socket: SampleRate::OFF,
            tls: SampleRate::ALL,
            websocket: SampleRate::every(2),
            association: SampleRate::every(2),
            ..ReceivePolicy::DETAILED
        },
    ] {
        for inline in [false, true] {
            let (listener, mut config) = fixture().await;
            config.receive = Some(ReceiveConfig {
                policy,
                software_timestamps: true,
                ..ReceiveConfig::default()
            });
            let peer = TlsPeer::new().unwrap();
            config.url = config.url.replacen("ws:", "wss:", 1);
            config.tls_config = Some(peer.client);
            config.read_buffer_bytes = 1024;
            let payload = format!(r#"{{"type":"snapshot","padding":"{}"}}"#, "x".repeat(32768));
            let expected = payload.clone();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let tls = TlsAcceptor::from(peer.server).accept(socket).await.unwrap();
                let mut ws = accept_async(tls).await.unwrap();
                ws.next().await.unwrap().unwrap();
                ws.send(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
                    .await
                    .unwrap();
                ws.send(Message::text(payload)).await.unwrap();
                while ws.next().await.is_some() {}
            });
            let mut feed = if inline {
                Feed::inline(config)
            } else {
                Feed::spawn(config)
            }
            .unwrap();
            assert!(matches!(next(&mut feed).await, Input::Connected { .. }));
            assert!(matches!(next(&mut feed).await, Input::Message { .. }));
            let Input::Message { bytes, receive, .. } = next(&mut feed).await else {
                panic!()
            };
            assert_eq!(bytes.as_ref(), expected.as_bytes());
            if policy == ReceivePolicy::OFF {
                assert!(receive.is_none());
                feed.shutdown().await;
                server.abort();
                continue;
            }
            let timing = receive.unwrap();
            assert_eq!(timing.policy, policy);
            assert_eq!(timing.work.socket.samples, 0);
            assert_eq!(timing.work.socket_ns, None);
            if policy.tls.enabled() {
                assert!(timing.work.tls.samples > 0);
                assert_eq!(
                    timing.work.tls_ns, None,
                    "missing nested socket work cannot be subtracted"
                );
                assert!(timing.plaintext.is_some());
            } else {
                assert_eq!(timing.work.tls.samples, 0);
                assert_eq!(timing.work.websocket.samples, 0);
                assert_eq!(timing.work.pending.samples, 0);
                assert!(timing.plaintext.is_none());
                assert!(
                    timing.software.at.is_none(),
                    "read stamps have no proven message association"
                );
            }
            feed.shutdown().await;
            server.abort();
        }
    }
}
