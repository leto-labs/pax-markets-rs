//! Fixed dialing preserves independently observed TLS and HTTP identities.
use super::*;
use pax_client::{test_support::TlsPeer, websocket::receive::ReceiveConfig};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::accept_hdr_async;

#[test]
fn malformed_urls_fail_before_runtime_or_retry() {
    for url in [
        "ws://",
        "wss:///path",
        "ws://:80/",
        "ws://localhost:0/",
        "ws://localhost:65536/",
        "ws://localhost:no/",
        "ws://localhost:/",
        "ws://[::1",
        "ws://[]/",
        "ws://[not-an-ip]/",
        "ws://localhost:+80/",
        "ws://user:secret@localhost/",
        "ws://localhost/#fragment",
    ] {
        for fixed in [false, true] {
            let mut config = FeedConfig::public(vec![MarketId(1)]);
            config.url = url.into();
            config.connect_address = fixed.then(|| "127.0.0.1:80".parse().unwrap());
            assert!(config.validate().is_err(), "accepted {url}");
            // No runtime is installed: valid configurations would reach task setup.
            assert!(Feed::spawn(config.clone()).is_err(), "spawn accepted {url}");
            assert!(Feed::inline(config).is_err(), "inline accepted {url}");
        }
    }
}

#[test]
fn valid_dns_ipv4_and_ipv6_urls_validate_without_resolution() {
    for url in [
        "wss://example.invalid/",
        "ws://localhost:8080/public?market=1",
        "ws://127.0.0.1:80/",
        "ws://[::1]/",
        "wss://[::1]:443/public",
        "ws://[2001:db8:0:0:0:0:0:1]:8080/",
        "ws://[::ffff:127.0.0.1]/",
    ] {
        let mut config = FeedConfig::public(vec![MarketId(1)]);
        config.url = url.into();
        config.validate().unwrap();
    }
}

#[tokio::test]
#[allow(clippy::result_large_err)] // Tungstenite fixes its handshake callback's error type.
async fn pinned_tls_preserves_hostname_host_and_reconnect_peer() {
    for inline in [false, true] {
        for measured in [false, true] {
            if measured
                && !cfg!(any(
                    feature = "receive-timestamps",
                    feature = "receive-stages"
                ))
            {
                continue;
            }
            let (listener, mut config) = fixture().await;
            let address = listener.local_addr().unwrap();
            let peer = TlsPeer::new().unwrap();
            config.url = format!("wss://localhost:{}/public", address.port());
            config.connect_address = Some(address);
            config.tls_config = Some(peer.client);
            config.receive = measured.then(ReceiveConfig::default);
            config.socket_readbacks = cfg!(all(target_os = "linux", feature = "socket-readbacks"));
            let server = tokio::spawn(async move {
                let acceptor = TlsAcceptor::from(peer.server);
                for _ in 0..2 {
                    let (tcp, _) = listener.accept().await.unwrap();
                    let tls = acceptor.accept(tcp).await.unwrap();
                    assert_eq!(tls.get_ref().1.server_name(), Some("localhost"));
                    let mut ws = accept_hdr_async(
                        tls,
                        |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                         response| {
                            assert_eq!(
                                request.headers()["host"],
                                format!("localhost:{}", address.port())
                            );
                            assert_eq!(request.uri().path(), "/public");
                            Ok(response)
                        },
                    )
                    .await
                    .unwrap();
                    ws.next().await.unwrap().unwrap();
                    ws.send(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
                        .await
                        .unwrap();
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
            let health = feed.health();
            for generation in 1..=2 {
                assert!(
                    matches!(next(&mut feed).await, Input::Connected {generation: g, ..} if g == generation)
                );
                assert_eq!(health.borrow().remote_peer, Some(address));
                assert_eq!(
                    health.borrow().socket_readback.is_some(),
                    cfg!(all(target_os = "linux", feature = "socket-readbacks"))
                );
                assert!(matches!(next(&mut feed).await, Input::Message { .. }));
                if generation == 1 {
                    feed.request_recovery();
                    disconnected(&mut feed).await;
                    assert!(!health.borrow().connected);
                    assert_eq!(health.borrow().remote_peer, None);
                    assert!(health.borrow().socket_readback.is_none());
                }
            }
            feed.shutdown().await;
            assert_eq!(health.borrow().remote_peer, None);
            assert!(health.borrow().socket_readback.is_none());
            timeout(Duration::from_secs(3), server)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

#[tokio::test]
async fn pinned_address_does_not_override_certificate_hostname() {
    for inline in [false, true] {
        for measured in [false, true] {
            if measured
                && !cfg!(any(
                    feature = "receive-timestamps",
                    feature = "receive-stages"
                ))
            {
                continue;
            }
            let (listener, mut config) = fixture().await;
            let address = listener.local_addr().unwrap();
            let peer = TlsPeer::new().unwrap();
            config.url = format!("wss://wrong.invalid:{}/", address.port());
            config.connect_address = Some(address);
            config.tls_config = Some(peer.client);
            config.receive = measured.then(ReceiveConfig::default);
            config.retry_min = Duration::from_secs(1);
            config.retry_max = Duration::from_secs(1);
            let server = tokio::spawn(async move {
                let (tcp, _) = listener.accept().await.unwrap();
                assert!(TlsAcceptor::from(peer.server).accept(tcp).await.is_err());
            });
            let mut feed = if inline {
                Feed::inline(config)
            } else {
                Feed::spawn(config)
            }
            .unwrap();
            let health = feed.health();
            let (_, reason, lossy) = disconnected(&mut feed).await;
            assert!(reason.contains("certificate"), "{reason}");
            assert!(!lossy);
            assert!(!health.borrow().connected);
            assert_eq!(health.borrow().remote_peer, None);
            feed.shutdown().await;
            timeout(Duration::from_secs(3), server)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

#[test]
fn fixed_destination_is_validated_before_spawning() {
    for address in [
        "127.0.0.1:0",
        "127.0.0.1:80",
        "0.0.0.0:443",
        "224.0.0.1:443",
        "[::]:443",
        "[ff02::1]:443",
    ] {
        let mut config = FeedConfig::public(vec![MarketId(1)]);
        config.connect_address = Some(address.parse().unwrap());
        assert!(config.validate().is_err(), "{address}");
    }
    let mut config = FeedConfig::public(vec![MarketId(1)]);
    assert_eq!(config.connect_address, None);
    config.connect_address = Some("127.0.0.1:443".parse().unwrap());
    assert!(config.validate().is_ok());
}
