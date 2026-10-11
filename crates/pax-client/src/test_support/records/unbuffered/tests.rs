use super::*;

#[test]
fn fresh_verified_suites_preserve_fragmented_coalesced_and_large_input() {
    let long = vec![b'x'; INPUT_BYTES + 1];
    let input: &[&[u8]] = &[b"first", &long, b"last"];
    for cipher in [
        rustls::CipherSuite::TLS13_AES_128_GCM_SHA256,
        rustls::CipherSuite::TLS13_AES_256_GCM_SHA384,
        rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256,
    ] {
        for chunk in [1, 17, 1024, 16384] {
            let mut prepared = prepare_with_cipher(input, 1024, cipher).unwrap();
            assert_eq!(prepared.cipher, format!("{cipher:?}"));
            assert_eq!(prepared.version, "TLSv1_3");
            assert!(prepared.batches[1].records > 1);
            let ciphertext: Vec<_> = prepared
                .batches
                .iter()
                .flat_map(|b| b.ciphertext.iter().copied())
                .collect();
            let mut output = vec![0; input.iter().map(|s| s.len()).sum()];
            assert_eq!(
                prepared
                    .receiver
                    .receive(&ciphertext, &mut output, chunk)
                    .unwrap(),
                output.len()
            );
            assert_eq!(output, input.concat());
            prepared.receiver.finish().unwrap();
        }
    }
}
#[test]
fn partial_input_crosses_calls_and_incomplete_final_input_is_refused() {
    for split in [1, 4, 5, 11, 25] {
        let mut prepared = prepare(&[b"authenticated data"], 16384).unwrap();
        let record = &prepared.batches[0].ciphertext;
        let mut output = [0; 18];
        assert_eq!(
            prepared
                .receiver
                .receive(&record[..split], &mut output, 7)
                .unwrap(),
            0
        );
        assert!(matches!(
            prepared.receiver.finish(),
            Err(Error::State("incomplete final ciphertext"))
        ));
        assert_eq!(
            prepared
                .receiver
                .receive(&record[split..], &mut output, 3)
                .unwrap(),
            18
        );
        assert_eq!(&output, b"authenticated data");
        prepared.receiver.finish().unwrap();
    }
}
#[test]
fn corrupt_and_old_records_poison_the_rejected_stream() {
    let mut prepared = prepare(&[b"authenticated data"], 16384).unwrap();
    let record = &prepared.batches[0].ciphertext;
    let mut output = [0; 18];
    prepared
        .receiver
        .receive(record, &mut output, 1024)
        .unwrap();
    assert!(matches!(
        prepared.receiver.receive(record, &mut output, 1024),
        Err(Error::Tls(_))
    ));
    assert!(matches!(
        prepared.receiver.receive(&[], &mut output, 1),
        Err(Error::State("previously rejected stream"))
    ));
    assert!(prepared.receiver.finish().is_err());
    let mut prepared = prepare(&[b"authenticated data"], 16384).unwrap();
    *prepared.batches[0].ciphertext.last_mut().unwrap() ^= 1;
    assert!(matches!(
        prepared
            .receiver
            .receive(&prepared.batches[0].ciphertext, &mut output, 17),
        Err(Error::Tls(_))
    ));
}
#[test]
fn bounds_and_unsupported_suites_are_explicit_errors() {
    let mut prepared = prepare(&[b"data"], 16384).unwrap();
    assert!(matches!(
        prepared.receiver.receive(&[], &mut [], 0),
        Err(Error::State("read chunk must be positive"))
    ));
    let mut prepared = prepare(&[b"data"], 16384).unwrap();
    assert!(matches!(
        prepared
            .receiver
            .receive(&prepared.batches[0].ciphertext, &mut [0; 3], 1024),
        Err(Error::State("output capacity exceeded"))
    ));
    let long = vec![b'x'; INPUT_BYTES + 1];
    let mut prepared = prepare(&[&long], 1024).unwrap();
    assert!(matches!(
        prepared.receiver.receive(
            &prepared.batches[0].ciphertext,
            &mut vec![0; long.len()],
            usize::MAX
        ),
        Err(Error::State("input capacity exceeded"))
    ));
    assert!(
        prepare_with_cipher(
            &[b"data"],
            1024,
            rustls::CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
        )
        .is_err()
    );
    assert!(prepare(&[b"data"], 1).is_err());
}
#[test]
fn hostname_and_certificate_verification_are_not_bypassed() {
    assert!(prepare_peer(&[b"data"], 1024, TlsPeer::new().unwrap(), "foreign.invalid").is_err());
    let trusted = TlsPeer::new().unwrap();
    let foreign = TlsPeer::new().unwrap();
    let mut receiver = Receiver::new(&trusted, "localhost").unwrap();
    let mut server = ServerConnection::new(foreign.server).unwrap();
    assert!(handshake(&mut receiver, &mut server).is_err());
}
#[test]
fn key_update_preserves_authenticated_bytes_but_reply_delivery_is_not_claimed() {
    let peer = TlsPeer::new().unwrap();
    let mut config = (*peer.server).clone();
    config.send_tls13_tickets = 0;
    let mut server = ServerConnection::new(Arc::new(config)).unwrap();
    let mut receiver = Receiver::new(&peer, "localhost").unwrap();
    handshake(&mut receiver, &mut server).unwrap();
    server.refresh_traffic_keys().unwrap();
    server.writer().write_all(b"updated keys").unwrap();
    let mut encrypted = Vec::new();
    while server.wants_write() {
        server.write_tls(&mut encrypted).unwrap();
    }
    let mut output = [0; 12];
    assert_eq!(receiver.receive(&encrypted, &mut output, 17).unwrap(), 12);
    assert_eq!(&output, b"updated keys");
    receiver.finish().unwrap();
    // Pinned CommonState queues the response until a later write; this fixture
    // only establishes authenticated receive, not full-duplex response delivery.
}
#[test]
fn close_and_exposed_outbound_states_are_refused() {
    let peer = TlsPeer::new().unwrap();
    let mut config = (*peer.server).clone();
    config.send_tls13_tickets = 0;
    let mut server = ServerConnection::new(Arc::new(config)).unwrap();
    let mut receiver = Receiver::new(&peer, "localhost").unwrap();
    handshake(&mut receiver, &mut server).unwrap();
    // Explicit local refresh exposes EncodeTlsData, unlike a deferred peer reply.
    match receiver
        .connection
        .process_tls_records(&mut [])
        .state
        .unwrap()
    {
        ConnectionState::WriteTraffic(write) => write.refresh_traffic_keys().unwrap(),
        state => panic!("unexpected {state:?}"),
    }
    assert!(matches!(
        receiver.receive(&[0], &mut [], 1),
        Err(Error::State("post-handshake control output unsupported"))
    ));
    assert!(receiver.finish().is_err());
    let mut receiver = Receiver::new(&peer, "localhost").unwrap();
    let mut server = ServerConnection::new(peer.server).unwrap();
    handshake(&mut receiver, &mut server).unwrap();
    server.send_close_notify();
    let mut encrypted = Vec::new();
    while server.wants_write() {
        server.write_tls(&mut encrypted).unwrap();
    }
    assert!(matches!(
        receiver.receive(&encrypted, &mut [], 17),
        Err(Error::State("peer closed fixed-record fixture"))
    ));
    assert!(receiver.finish().is_err());
}
