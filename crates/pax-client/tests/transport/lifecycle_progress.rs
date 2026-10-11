//! Active TLS cancellation, connection lifetime and buffered-message fairness.
use super::*;
use pax_client::test_support::TlsPeer;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::oneshot,
};
use tokio_rustls::TlsAcceptor;

async fn peer_eof(socket: &mut (impl tokio::io::AsyncRead + Unpin)) {
    let mut bytes = [0; 1024];
    timeout(Duration::from_secs(2), async {
        loop {
            match socket.read(&mut bytes).await {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => break,
                Err(error) => panic!("peer read failed: {error}"),
            }
        }
    })
    .await
    .expect("owned connection did not close");
}

#[tokio::test]
async fn active_tls_setup_deadline_shutdown_and_drop_close_the_connection() {
    for inline in [false, true] {
        for operation in ["deadline", "shutdown", "drop"] {
            let (listener, mut config) = fixture().await;
            config.url = config.url.replacen("ws:", "wss:", 1);
            config.connect_timeout = Duration::from_millis(250);
            config.retry_min = Duration::from_secs(10);
            config.retry_max = Duration::from_secs(10);
            let (hello, mut started) = oneshot::channel();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut header = [0; 5];
                socket.read_exact(&mut header).await.unwrap();
                assert_eq!(header[0], 22, "TLS handshake record");
                let length = usize::from(u16::from_be_bytes([header[3], header[4]]));
                assert!(length > 0 && length <= 16384);
                let mut body = vec![0; length];
                socket.read_exact(&mut body).await.unwrap();
                assert_eq!(body[0], 1, "ClientHello");
                hello.send(()).unwrap();
                peer_eof(&mut socket).await;
            });
            let mut feed = if inline {
                Feed::inline(config)
            } else {
                Feed::spawn(config)
            }
            .unwrap();
            timeout(Duration::from_secs(2), async {
                tokio::select! {
                    result = &mut started => result.unwrap(),
                    input = feed.recv() => panic!("unexpected input before ClientHello: {input:?}"),
                }
            })
            .await
            .unwrap();
            match operation {
                "deadline" => {
                    let (_, reason, lossy) = disconnected(&mut feed).await;
                    assert_eq!(reason, "connection timeout");
                    assert!(!lossy);
                    feed.shutdown().await;
                }
                "shutdown" => feed.shutdown().await,
                "drop" => drop(feed),
                _ => unreachable!(),
            }
            timeout(Duration::from_secs(3), server)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

#[tokio::test]
async fn encrypted_partial_frame_survives_recv_cancellation_and_closes_on_drop() {
    let (listener, mut config) = fixture().await;
    let peer = TlsPeer::new().unwrap();
    config.url = config.url.replacen("ws:", "wss:", 1);
    config.tls_config = Some(peer.client);
    config.ping_interval = Duration::from_secs(10);
    let (prefix, mut started) = oneshot::channel();
    let (finish, complete) = oneshot::channel();
    let payload = br#"{"type":"subscribed","market_ids":[1]}"#;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let tls = TlsAcceptor::from(peer.server).accept(socket).await.unwrap();
        let mut ws = accept_async(tls).await.unwrap();
        ws.next().await.unwrap().unwrap();
        let split = payload.len() / 2;
        ws.get_mut()
            .write_all(&[0x81, payload.len() as u8])
            .await
            .unwrap();
        ws.get_mut().write_all(&payload[..split]).await.unwrap();
        ws.get_mut().flush().await.unwrap();
        prefix.send(()).unwrap();
        complete.await.unwrap();
        ws.get_mut().write_all(&payload[split..]).await.unwrap();
        ws.get_mut().flush().await.unwrap();
        // Read the underlying TCP stream: a missing TLS close_notify is allowed
        // on drop, but the owned TCP descriptors must actually close.
        peer_eof(ws.get_mut().get_mut().0).await;
    });
    let mut feed = Feed::inline(config).unwrap();
    assert!(matches!(
        next(&mut feed).await,
        Input::Connected { generation: 1, .. }
    ));
    tokio::select! {
        result = &mut started => result.unwrap(),
        input = feed.recv() => panic!("partial frame delivered: {input:?}"),
    }
    assert!(
        timeout(Duration::from_millis(15), feed.recv())
            .await
            .is_err()
    );
    finish.send(()).unwrap();
    let Input::Message {
        bytes,
        generation: 1,
        ..
    } = next(&mut feed).await
    else {
        panic!()
    };
    assert_eq!(&bytes[..], payload);
    drop(feed);
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn buffered_tls_websocket_messages_allow_other_tasks_to_progress() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    const COUNT: usize = 4096;
    let (listener, mut config) = fixture().await;
    let peer = TlsPeer::new().unwrap();
    config.url = config.url.replacen("ws:", "wss:", 1);
    config.tls_config = Some(peer.client);
    config.ping_interval = Duration::from_secs(10);
    let (sent, ready) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let tls = TlsAcceptor::from(peer.server).accept(socket).await.unwrap();
        let mut ws = accept_async(tls).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.feed(Message::text(r#"{"type":"subscribed","market_ids":[1]}"#))
            .await
            .unwrap();
        for _ in 0..COUNT {
            ws.feed(Message::text(r#"{"type":"snapshot"}"#))
                .await
                .unwrap();
        }
        ws.flush().await.unwrap();
        sent.send(()).unwrap();
        peer_eof(ws.get_mut().get_mut().0).await;
    });
    let mut feed = Feed::inline(config).unwrap();
    assert!(matches!(next(&mut feed).await, Input::Connected { .. }));
    assert!(matches!(next(&mut feed).await, Input::Message { .. }));
    timeout(Duration::from_secs(2), ready)
        .await
        .unwrap()
        .unwrap();
    let consumed = Arc::new(AtomicUsize::new(0));
    let (start, started) = oneshot::channel();
    let count = consumed.clone();
    let witness = tokio::spawn(async move {
        started.await.unwrap();
        count.load(Ordering::Relaxed)
    });
    let consumer = tokio::spawn(async move {
        let mut start = Some(start);
        for n in 1..=COUNT {
            let Input::Message {
                bytes,
                generation: 1,
                ..
            } = next(&mut feed).await
            else {
                panic!()
            };
            assert_eq!(&bytes[..], br#"{"type":"snapshot"}"#);
            consumed.store(n, Ordering::Relaxed);
            if let Some(start) = start.take() {
                start.send(()).unwrap();
            }
        }
        feed.shutdown().await;
    });
    let at = timeout(Duration::from_secs(2), witness)
        .await
        .unwrap()
        .unwrap();
    assert!((1..COUNT).contains(&at), "witness ran after {at} messages");
    timeout(Duration::from_secs(3), consumer)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}
