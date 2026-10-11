//! Public transport behavior against bounded local fixture servers, never Pax.
use futures_util::{SinkExt, StreamExt};
use pax_client::websocket::{Feed, FeedConfig, Input};
use pax_types::units::MarketId;
use std::time::Duration;
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};

async fn fixture() -> (TcpListener, FeedConfig) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = FeedConfig::public(vec![MarketId(1)]);
    config.url = format!("ws://{}", listener.local_addr().unwrap());
    config.ping_interval = Duration::from_millis(60);
    config.pong_timeout = Duration::from_millis(30);
    config.connect_timeout = Duration::from_secs(2);
    config.retry_min = Duration::from_millis(20);
    config.retry_max = Duration::from_millis(100);
    (listener, config)
}
async fn next(feed: &mut Feed) -> Input {
    timeout(Duration::from_secs(3), feed.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn disconnected(feed: &mut Feed) -> (u64, String, bool) {
    loop {
        if let Input::Disconnected {
            generation,
            reason,
            lossy,
            ..
        } = next(feed).await
        {
            return (generation, reason, lossy);
        }
    }
}

#[tokio::test]
async fn ordered_application_bytes_and_heartbeat_are_preserved() {
    for inline in [false, true] {
        let (listener, mut config) = fixture().await;
        config.delivery_sample_every = if cfg!(feature = "delivery-traces") {
            2
        } else {
            0
        };
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let request = ws.next().await.unwrap().unwrap();
            assert_eq!(
                request.into_text().unwrap(),
                r#"{"action":"subscribe","market_ids":[1]}"#
            );
            ws.send(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
                .await
                .unwrap();
            ws.send(Message::text(r#"{"type":"snapshot","data":{"test":true}}"#))
                .await
                .unwrap();
            while let Some(Ok(message)) = ws.next().await {
                if message.is_text() && message.to_text().unwrap() == r#"{"action":"ping"}"# {
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
        let health = feed.health();
        assert!(matches!(
            next(&mut feed).await,
            Input::Connected { generation: 1, .. }
        ));
        assert!(matches!(
            next(&mut feed).await,
            Input::Message {
                generation: 1,
                timing: None,
                ..
            }
        ));
        let Input::Message {
            bytes,
            received,
            timing,
            ..
        } = next(&mut feed).await
        else {
            panic!()
        };
        assert_eq!(timing.is_some(), cfg!(feature = "delivery-traces"));
        if let Some(timing) = timing {
            assert!(received <= timing.prepared);
            assert!(timing.prepared <= timing.delivered.unwrap());
            assert!(timing.pending.is_some());
        }
        assert_eq!(&bytes[..], br#"{"type":"snapshot","data":{"test":true}}"#);
        timeout(Duration::from_secs(2), async {
            loop {
                if health.borrow().last_pong.is_some() {
                    break;
                }
                next(&mut feed).await;
            }
        })
        .await
        .unwrap();
        assert!(health.borrow().round_trip.is_some());
        feed.shutdown().await;
        server.abort();
    }
}

#[tokio::test]
async fn partial_ack_and_recovery_create_new_ordered_generations() {
    for inline in [false, true] {
        let (listener, config) = fixture().await;
        let server = tokio::spawn(async move {
            for index in 0..3 {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = accept_async(stream).await.unwrap();
                ws.next().await;
                let ids = if index == 0 { "[]" } else { "[1]" };
                ws.send(Message::text(format!(
                    r#"{{"type":"subscribed","market_ids":{ids}}}"#
                )))
                .await
                .unwrap();
                while let Some(Ok(message)) = ws.next().await {
                    if message.is_text() && message.to_text().unwrap() == r#"{"action":"ping"}"# {
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
        let (generation, reason, _) = disconnected(&mut feed).await;
        assert_eq!(generation, 1);
        assert!(reason.contains("omitted"));
        assert!(matches!(
            next(&mut feed).await,
            Input::Connected { generation: 2, .. }
        ));
        next(&mut feed).await; // ack ensures the new session is running
        for _ in 0..100 {
            feed.request_recovery();
        }
        assert!(
            disconnected(&mut feed)
                .await
                .1
                .contains("resynchronization")
        );
        assert!(matches!(
            next(&mut feed).await,
            Input::Connected { generation: 3, .. }
        ));
        feed.shutdown().await;
        server.abort();
    }
}

#[tokio::test]
async fn full_queue_reports_loss_and_disconnects_without_blocking_health() {
    let (listener, mut config) = fixture().await;
    config.queue_capacity = 1;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        ws.next().await;
        ws.send(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
            .await
            .unwrap();
        while ws.next().await.is_some() {}
    });
    let mut feed = Feed::spawn(config).unwrap();
    let mut health = feed.health();
    timeout(Duration::from_secs(2), async {
        loop {
            if health.borrow().reason.is_some() {
                break;
            }
            health.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(!health.borrow().connected);
    assert!(matches!(
        next(&mut feed).await,
        Input::Connected { generation: 1, .. }
    ));
    let (_, reason, lossy) = disconnected(&mut feed).await;
    assert!(lossy);
    assert!(reason.contains("queue"));
    feed.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn oversized_messages_invalidate_continuity() {
    for inline in [false, true] {
        let (listener, mut config) = fixture().await;
        config.max_message_bytes = 128;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            ws.next().await;
            ws.send(Message::text("x".repeat(1024))).await.unwrap();
            while ws.next().await.is_some() {}
        });
        let mut feed = if inline {
            Feed::inline(config)
        } else {
            Feed::spawn(config)
        }
        .unwrap();
        assert!(disconnected(&mut feed).await.2);
        feed.shutdown().await;
        server.abort();
    }
}

#[tokio::test]
async fn missing_application_pong_recovers_even_when_the_socket_is_open() {
    for inline in [false, true] {
        let (listener, config) = fixture().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            ws.next().await;
            ws.send(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
                .await
                .unwrap();
            while ws.next().await.is_some() {}
        });
        let mut feed = if inline {
            Feed::inline(config)
        } else {
            Feed::spawn(config)
        }
        .unwrap();
        assert!(disconnected(&mut feed).await.1.contains("pong timeout"));
        feed.shutdown().await;
        server.abort();
    }
}

#[tokio::test]
async fn shutdown_interrupts_a_long_retry_wait() {
    for inline in [false, true] {
        let (listener, mut config) = fixture().await;
        drop(listener);
        config.retry_min = Duration::from_secs(30);
        config.retry_max = Duration::from_secs(30);
        let mut feed = if inline {
            Feed::inline(config)
        } else {
            Feed::spawn(config)
        }
        .unwrap();
        disconnected(&mut feed).await;
        timeout(Duration::from_secs(1), feed.shutdown())
            .await
            .unwrap();
    }
}

#[test]
fn invalid_bounds_fail_before_spawning() {
    let mut config = FeedConfig::public(vec![MarketId(1)]);
    config.queue_capacity = 0;
    assert!(config.validate().is_err());
    config.queue_capacity = 1;
    config.markets.push(MarketId(1));
    assert!(config.validate().is_err());
}

#[path = "transport/delivery.rs"]
mod delivery;

#[path = "transport/tls.rs"]
mod tls;

#[path = "transport/policy.rs"]
mod policy;

#[path = "transport/endpoint.rs"]
mod endpoint;

#[path = "transport/lifecycle_progress.rs"]
mod lifecycle_progress;
