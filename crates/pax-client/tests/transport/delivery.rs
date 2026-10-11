//! Inline cancellation and sampled-delivery recovery regressions.
use super::*;

#[tokio::test]
async fn inline_receive_cancellation_preserves_handshake_and_partial_frames() {
    use tokio::{io::AsyncWriteExt, sync::oneshot};
    let (listener, config) = fixture().await;
    let (handshake, proceed) = oneshot::channel();
    let (frame, complete) = oneshot::channel();
    let payload = br#"{"type":"subscribed","market_ids":[1]}"#;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        proceed.await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        ws.next().await.unwrap().unwrap();
        // One unmasked server text frame, intentionally incomplete across a
        // canceled recv. The prefix contains no complete application message.
        let split = payload.len() / 2;
        ws.get_mut()
            .write_all(&[0x81, payload.len() as u8])
            .await
            .unwrap();
        ws.get_mut().write_all(&payload[..split]).await.unwrap();
        complete.await.unwrap();
        ws.get_mut().write_all(&payload[split..]).await.unwrap();
        while ws.next().await.is_some() {}
    });
    let mut feed = Feed::inline(config).unwrap();
    assert!(
        timeout(Duration::from_millis(15), feed.recv())
            .await
            .is_err()
    );
    handshake.send(()).unwrap();
    assert!(matches!(
        next(&mut feed).await,
        Input::Connected { generation: 1, .. }
    ));
    assert!(
        timeout(Duration::from_millis(15), feed.recv())
            .await
            .is_err()
    );
    frame.send(()).unwrap();
    let Input::Message {
        bytes,
        timing,
        generation,
        ..
    } = next(&mut feed).await
    else {
        panic!()
    };
    assert_eq!(generation, 1);
    assert_eq!(&bytes[..], payload);
    assert!(timing.is_none());
    feed.stop();
    assert!(feed.recv().await.is_none());
    assert!(feed.recv().await.is_none());
    feed.shutdown().await;
    server.abort();
}

#[tokio::test]
#[cfg(feature = "delivery-traces")]
async fn delivery_sampling_stays_aligned_after_a_dropped_input() {
    let (listener, mut config) = fixture().await;
    config.queue_capacity = 2;
    config.delivery_sample_every = 2;
    config.ping_interval = Duration::from_secs(10);
    let server = tokio::spawn(async move {
        for generation in 1..=2 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            ws.next().await.unwrap().unwrap();
            ws.send(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
                .await
                .unwrap();
            if generation == 1 {
                // Connected + ack occupy both slots. This application input
                // is dropped, but must not change future delivered ordinals.
                ws.send(Message::text(r#"{"type":"snapshot","data":{}}"#))
                    .await
                    .unwrap();
            }
            while ws.next().await.is_some() {}
        }
    });
    let mut feed = Feed::spawn(config).unwrap();
    let mut health = feed.health();
    timeout(Duration::from_secs(2), async {
        while health.borrow().reason.is_none() {
            health.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        next(&mut feed).await,
        Input::Connected { generation: 1, .. }
    ));
    assert!(matches!(
        next(&mut feed).await,
        Input::Message { timing: None, .. }
    ));
    assert!(disconnected(&mut feed).await.2);
    assert!(matches!(
        next(&mut feed).await,
        Input::Connected { generation: 2, .. }
    ));
    assert!(matches!(
        next(&mut feed).await,
        Input::Message {
            generation: 2,
            timing: Some(_),
            ..
        }
    ));
    feed.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn inline_ack_deadline_starts_after_subscription_is_sent() {
    let (listener, mut config) = fixture().await;
    config.connect_timeout = Duration::from_millis(200);
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.send(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
            .await
            .unwrap();
        while ws.next().await.is_some() {}
    });
    let mut feed = Feed::inline(config).unwrap();
    assert!(matches!(
        next(&mut feed).await,
        Input::Connected { generation: 1, .. }
    ));
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(matches!(
        next(&mut feed).await,
        Input::Message { generation: 1, .. }
    ));
    feed.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn unrelated_messages_do_not_satisfy_the_subscription_deadline() {
    for inline in [false, true] {
        let (listener, mut config) = fixture().await;
        config.connect_timeout = Duration::from_millis(500);
        config.ping_interval = Duration::from_secs(10);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            ws.next().await.unwrap().unwrap();
            // Keep restarting the receive loop with valid unrelated input.
            // An accidentally renewed deadline would never expire.
            loop {
                tokio::select! {
                    message = ws.next() => if !matches!(message, Some(Ok(_))) { break; },
                    _ = tokio::time::sleep(Duration::from_millis(25)) => {
                        if ws.send(Message::text(r#"{"type":"pong"}"#)).await.is_err() { break; }
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
        assert!(matches!(
            next(&mut feed).await,
            Input::Connected { generation: 1, .. }
        ));
        assert!(matches!(
            next(&mut feed).await,
            Input::Message { generation: 1, .. }
        ));
        let (generation, reason, lossy) = timeout(Duration::from_secs(2), disconnected(&mut feed))
            .await
            .unwrap();
        assert_eq!(generation, 1);
        assert_eq!(reason, "subscription acknowledgment timeout");
        assert!(!lossy);
        feed.shutdown().await;
        server.abort();
    }
}

#[tokio::test]
async fn small_read_chunks_preserve_larger_messages_in_both_delivery_modes() {
    for inline in [false, true] {
        let (listener, mut config) = fixture().await;
        config.read_buffer_bytes = 1024;
        config.max_message_bytes = 32768;
        let payload = format!(r#"{{"type":"snapshot","padding":"{}"}}"#, "x".repeat(16384));
        let expected = payload.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
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
        let Input::Message { bytes, .. } = next(&mut feed).await else {
            panic!()
        };
        assert_eq!(bytes.as_ref(), expected.as_bytes());
        feed.shutdown().await;
        server.abort();
    }
}
