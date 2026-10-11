//! Actual TLS sockets and generation boundaries independently exercise the readback.
use super::Readback;
use crate::{
    test_support::TlsPeer,
    websocket::{
        Feed, FeedConfig, Input,
        receive::{ReceiveConfig, policy::ReceivePolicy},
    },
};
use futures_util::{SinkExt, StreamExt};
use pax_types::units::MarketId;
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::Notify};
use tokio_tungstenite::tungstenite::Message;

fn assert_options(readback: Option<Readback>) {
    let Some(Readback::Available { options }) = readback else {
        panic!("connected Linux TLS socket did not provide options: {readback:?}");
    };
    assert!(options.tcp_nodelay);
    assert!(options.tcp_maxseg > 0);
    assert!(options.receive_buffer > 0);
    assert!(options.send_buffer > 0);
}

#[tokio::test]
async fn ordinary_and_measured_delivery_clear_and_refresh_actual_socket_readbacks() {
    for inline in [true, false] {
        for measured in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let peer = TlsPeer::new().unwrap();
            let mut config = FeedConfig::public(vec![MarketId(1)]);
            config.url = format!("wss://localhost:{}/", listener.local_addr().unwrap().port());
            config.tls_config = Some(peer.client);
            config.socket_readbacks = true;
            config.retry_min = Duration::from_millis(5);
            config.retry_max = Duration::from_millis(5);
            config.receive = measured.then(|| ReceiveConfig {
                policy: ReceivePolicy::OFF,
                ..ReceiveConfig::default()
            });
            let permit_reconnect = Arc::new(Notify::new());
            let permit = permit_reconnect.clone();
            let server = tokio::spawn(async move {
                for generation in 1..=2 {
                    if generation == 2 {
                        permit.notified().await;
                    }
                    let (tcp, _) = listener.accept().await.unwrap();
                    let tls = tokio_rustls::TlsAcceptor::from(peer.server.clone())
                        .accept(tcp)
                        .await
                        .unwrap();
                    let mut ws = tokio_tungstenite::accept_async(tls).await.unwrap();
                    assert!(ws.next().await.unwrap().unwrap().is_text());
                    ws.send(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
                        .await
                        .unwrap();
                    ws.send(Message::text(
                        r#"{"type":"snapshot","data":{"market_id":1}}"#,
                    ))
                    .await
                    .unwrap();
                    // Recovery/drop closes the socket; the next handshake is gated.
                    let _ = ws.next().await;
                }
            });
            let mut feed = if inline {
                Feed::inline(config).unwrap()
            } else {
                Feed::spawn(config).unwrap()
            };
            let health = feed.health();
            let workload = async {
                assert!(matches!(
                    feed.recv().await,
                    Some(Input::Connected { generation: 1, .. })
                ));
                assert_options(health.borrow().socket_readback);
                for _ in 0..2 {
                    assert!(matches!(
                        feed.recv().await,
                        Some(Input::Message { generation: 1, .. })
                    ));
                }
                feed.request_recovery();
                assert!(matches!(
                    feed.recv().await,
                    Some(Input::Disconnected { generation: 1, .. })
                ));
                assert!(!health.borrow().connected);
                assert!(health.borrow().socket_readback.is_none());
                permit_reconnect.notify_one();
                assert!(matches!(
                    feed.recv().await,
                    Some(Input::Connected { generation: 2, .. })
                ));
                assert_options(health.borrow().socket_readback);
                assert_eq!(health.borrow().generation, 2);
                for _ in 0..2 {
                    assert!(matches!(
                        feed.recv().await,
                        Some(Input::Message { generation: 2, .. })
                    ));
                }
            };
            tokio::time::timeout(Duration::from_secs(5), workload)
                .await
                .unwrap();
            drop(feed);
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap();
        }
    }
}
