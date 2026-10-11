//! Bounded local HTTP metadata tests.
use pax_client::metadata::{ClientError, MetadataClient};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn server(body: Vec<u8>, length_header: bool) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/metadata", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 4096];
        let mut used = 0;
        loop {
            let count = stream.read(&mut request[used..]).await.unwrap();
            assert!(count > 0, "complete fixture request header");
            used += count;
            if request[..used].windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                break;
            }
            assert!(used < request.len(), "bounded fixture request header");
        }
        let mut header = "HTTP/1.1 200 OK\r\nConnection: close\r\n".to_owned();
        if length_header {
            header.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        header.push_str("\r\n");
        stream.write_all(header.as_bytes()).await.unwrap();
        stream.write_all(&body).await.unwrap();
    });
    url
}
#[tokio::test]
async fn discovers_metadata_and_bounds_bodies_with_or_without_content_length() {
    let fixture = include_bytes!("../../pax-types/tests/fixtures/metadata.json");
    let client = MetadataClient::new(Duration::from_secs(2), 2048).unwrap();
    let info = client
        .fetch(&server(fixture.to_vec(), true).await)
        .await
        .unwrap();
    assert_eq!(info.markets.len(), 1);
    for header in [true, false] {
        let result = client.fetch(&server(vec![b' '; 4096], header).await).await;
        assert!(matches!(result, Err(ClientError::BodyLimit(2048))));
    }
}

#[tokio::test]
async fn validates_utf8_in_ignored_metadata_fields() {
    let fixture = include_bytes!("../../pax-types/tests/fixtures/metadata.json");
    let client = MetadataClient::new(Duration::from_secs(2), 2048).unwrap();
    for (extra, valid) in [("✓".as_bytes(), true), (&[0xff][..], false)] {
        let mut body = fixture.to_vec();
        body.truncate(body.iter().rposition(|byte| *byte == b'}').unwrap());
        body.extend_from_slice(b",\"extra\":\"");
        body.extend_from_slice(extra);
        body.extend_from_slice(b"\"}");
        let result = client.fetch(&server(body, true).await).await;
        assert_eq!(result.is_ok(), valid, "ignored fields must still be UTF-8");
        if !valid {
            assert!(matches!(result, Err(ClientError::Encoding(_))));
        }
    }
}
