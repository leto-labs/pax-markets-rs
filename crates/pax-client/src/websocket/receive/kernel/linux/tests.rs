use super::*;

#[tokio::test]
async fn pending_read_registers_wakeup_and_leaves_no_borrowed_buffer() {
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut server, _) = listener.accept().await.unwrap();
    let mut state = SocketTimestamps::new(
        &client,
        &ReceiveConfig {
            policy: ReceivePolicy::OFF,
            ..Default::default()
        },
    );
    let mut storage = [0; 64];
    let mut buf = ReadBuf::new(&mut storage);
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(state.poll_read(
            Pin::new(&mut client),
            cx,
            &mut buf
        )))
        .await
        .is_pending()
    );
    assert!(buf.filled().is_empty());
    // Cancel the first caller; a new read buffer and waker must receive all bytes.
    let mut storage = [0; 64];
    let mut next = ReadBuf::new(&mut storage);
    server.write_all(b"after cancellation").await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        std::future::poll_fn(|cx| state.poll_read(Pin::new(&mut client), cx, &mut next)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(next.filled(), b"after cancellation");
}
#[cfg(test)]
mod ancillary {
    use super::*;
    use tokio::io::AsyncWriteExt;
    #[tokio::test]
    async fn software_tcp_metadata_and_truncation_preserve_all_bytes() {
        for truncated in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (mut sender, _) = listener.accept().await.unwrap();
            let mut timestamps = SocketTimestamps::new(
                &client,
                &ReceiveConfig {
                    software_timestamps: true,
                    ..ReceiveConfig::default()
                },
            );
            if !timestamps.enabled {
                eprintln!(
                    "software timestamp capability unavailable: {}",
                    timestamps.software.status
                );
                continue;
            }
            if truncated {
                timestamps.ancillary = Vec::new();
            }
            sender.write_all(b"fragment").await.unwrap();
            sender.write_all(b"coalesced").await.unwrap();
            sender.shutdown().await.unwrap();
            let mut stream = client;
            let mut collected = Vec::new();
            let mut available = false;
            loop {
                let mut bytes = [0; 64];
                let mut buf = ReadBuf::new(&mut bytes);
                std::future::poll_fn(|cx| {
                    timestamps.poll_read(Pin::new(&mut stream), cx, &mut buf)
                })
                .await
                .unwrap();
                if buf.filled().is_empty() {
                    break;
                }
                collected.extend_from_slice(buf.filled());
                if truncated {
                    assert_eq!(timestamps.software.status, "ancillary metadata truncated");
                    assert!(timestamps.software.at.is_none());
                } else {
                    available |= timestamps.software.at.is_some();
                    assert_eq!(timestamps.software.source, "kernel-software/realtime");
                }
            }
            assert_eq!(collected, b"fragmentcoalesced");
            if !truncated {
                assert!(available, "loopback delivered no alignable timestamp");
            }
        }
    }
    #[test]
    fn stamp_after_read_but_before_calibration_is_withheld() {
        let base = Instant::now() - Duration::from_millis(1);
        let calibration = Calibration::bracket(
            1_000_000,
            base + Duration::from_nanos(100),
            base + Duration::from_nanos(100),
        )
        .unwrap();
        let stamp = align_stamp(
            TimeSpec::new(0, 999_975),
            "test",
            &mut None,
            Ok(Some(calibration)),
            base + Duration::from_nanos(50),
        );
        assert!(stamp.at.is_none());
        assert_eq!(stamp.status, "timestamp maps to future; interval withheld");
    }
    #[test]
    fn missing_and_invalid_timestamps_have_no_cross_clock_interval() {
        let mut model = Calibration::system().ok();
        let absent = align_stamp(
            TimeSpec::new(0, 0),
            "test",
            &mut model,
            Calibration::system().map(Some),
            Instant::now(),
        );
        assert!(absent.at.is_none());
        assert!(absent.raw_ns.is_none());
        let invalid = align_stamp(
            TimeSpec::new(-1, 0),
            "test",
            &mut model,
            Calibration::system().map(Some),
            Instant::now(),
        );
        assert_eq!(invalid.status, "invalid ancillary timespec");
    }
}

#[cfg(all(test, feature = "receive-stages"))]
mod work_tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn detailed_read_counts_pending_stale_readiness_and_initialization() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut sender, _) = listener.accept().await.unwrap();
        let mut timestamps = SocketTimestamps::new(&client, &ReceiveConfig::default());
        let mut memory = [std::mem::MaybeUninit::<u8>::uninit(); 131072];
        let mut buf = ReadBuf::uninit(&mut memory);
        let pending = std::future::poll_fn(|cx| {
            Poll::Ready(timestamps.poll_read(Pin::new(&mut client), cx, &mut buf))
        })
        .await;
        assert!(pending.is_pending());
        let work = timestamps.take_work();
        assert_eq!(work.pending, 1);
        assert_eq!(work.syscall.eligible, 0);
        assert_eq!(work.readiness.samples, work.readiness.eligible);
        sender.write_all(b"stale readiness").await.unwrap();
        client.readable().await.unwrap();
        // Drain through a raw safe syscall, deliberately leaving Tokio's cached
        // readiness set. The adapter must clear it after a real WouldBlock.
        assert_eq!(
            nix::sys::socket::recv(client.as_raw_fd(), &mut [0u8; 64], MsgFlags::MSG_DONTWAIT)
                .unwrap(),
            15
        );
        let pending = std::future::poll_fn(|cx| {
            Poll::Ready(timestamps.poll_read(Pin::new(&mut client), cx, &mut buf))
        })
        .await;
        assert!(pending.is_pending());
        let work = timestamps.take_work();
        assert_eq!(work.would_block, 1);
        assert_eq!(work.syscall.eligible, 1);
        assert_eq!(work.initialized_bytes, 131072);
        assert_eq!(work.bytes, 0);
        sender.write_all(b"actual bytes").await.unwrap();
        std::future::poll_fn(|cx| timestamps.poll_read(Pin::new(&mut client), cx, &mut buf))
            .await
            .unwrap();
        assert_eq!(buf.filled(), b"actual bytes");
        let work = timestamps.take_work();
        assert_eq!((work.bytes, work.reads, work.initialized_bytes), (12, 1, 0));
        assert_eq!(work.syscall.samples, work.syscall.eligible);
        assert!(work.metadata.exclusive_ns.is_some());
        // Select timestamp reads while independently suppressing detail clocks.
        timestamps.configure(ReceivePolicy::TIMESTAMPS);
        timestamps.enabled = true;
        buf.clear();
        sender.write_all(b"untimed").await.unwrap();
        std::future::poll_fn(|cx| timestamps.poll_read(Pin::new(&mut client), cx, &mut buf))
            .await
            .unwrap();
        let work = timestamps.take_work();
        assert_eq!(work.bytes, 7);
        assert_eq!(work.syscall.samples, 0);
        assert_eq!(work.syscall.exclusive_ns, None);
        assert_eq!(work.metadata.sampled_ns, 0);
    }
}
