//! Ordinary fast path plus opt-in layered receive diagnostics.
use super::{
    FeedConfig,
    receive::{Collector, PlainIo, ReceiveTiming, SocketIo, kernel},
};
use futures_util::{Sink, Stream};
use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, client_async_tls_with_config,
    client_async_with_config, connect_async_tls_with_config,
    tungstenite::{self, Message, client::IntoClientRequest, protocol::WebSocketConfig},
};
type Plain = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Instrumented = WebSocketStream<PlainIo>;
#[allow(clippy::large_enum_variant)] // Keep the ordinary stream inline in its existing boxed driver.
pub(crate) enum Socket {
    Ordinary(Plain),
    Measured {
        stream: Box<Instrumented>,
        last: Option<ReceiveTiming>,
    },
}
impl Socket {
    #[cfg(all(target_os = "linux", feature = "socket-readbacks"))]
    pub fn options(&self) -> super::socket_options::Readback {
        let tcp = match self {
            Self::Ordinary(stream) => match stream.get_ref() {
                MaybeTlsStream::Plain(socket) => Some(socket),
                MaybeTlsStream::Rustls(stream) => Some(stream.get_ref().0),
                _ => None,
            },
            Self::Measured { stream, .. } => stream.get_ref().tcp(),
        };
        tcp.map(super::socket_options::read).unwrap_or(
            super::socket_options::Readback::Unavailable {
                error: super::socket_options::Unavailable::UnsupportedTransport,
            },
        )
    }
    // Keep the large setup future and its construction off the established
    // supervisor's poll stack. One allocation ends after connect or cancellation;
    // ordinary message polling keeps static dispatch and adds no allocation.
    #[cold]
    #[inline(never)]
    pub fn connect(
        config: &FeedConfig,
        ws: WebSocketConfig,
        generation: u64,
        revision: u64,
    ) -> Pin<Box<impl Future<Output = Result<Self, tungstenite::Error>> + Send + '_>> {
        Box::pin(Self::connect_inner(config, ws, generation, revision))
    }
    async fn connect_inner(
        config: &FeedConfig,
        ws: WebSocketConfig,
        generation: u64,
        revision: u64,
    ) -> Result<Self, tungstenite::Error> {
        let Some(receive) = config.receive.as_ref().filter(|_| {
            cfg!(any(
                feature = "receive-timestamps",
                feature = "receive-association",
                feature = "receive-stages"
            ))
        }) else {
            if let Some(address) = config.connect_address {
                let tcp = TcpStream::connect(address).await?;
                tcp.set_nodelay(true)?;
                // Supplying the TCP stream changes only dialing: the URL remains
                // the request/TLS identity. Source: https://docs.rs/tokio-tungstenite/0.30.0/tokio_tungstenite/fn.client_async_tls_with_config.html
                return client_async_tls_with_config(
                    &config.url,
                    tcp,
                    Some(ws),
                    config.tls_config.clone().map(Connector::Rustls),
                )
                .await
                .map(|(stream, _)| Self::Ordinary(stream));
            }
            return connect_async_tls_with_config(
                &config.url,
                Some(ws),
                true,
                config.tls_config.clone().map(Connector::Rustls),
            )
            .await
            .map(|(stream, _)| Self::Ordinary(stream));
        };
        let request = config.url.as_str().into_client_request()?;
        let uri = request.uri();
        let host = uri
            .host()
            .ok_or(tungstenite::Error::Url(
                tungstenite::error::UrlError::NoHostName,
            ))?
            .trim_matches(['[', ']'])
            .to_owned();
        let tls = uri.scheme_str() == Some("wss");
        let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });
        let tcp = match config.connect_address {
            Some(address) => TcpStream::connect(address).await?,
            None => TcpStream::connect((host.as_str(), port)).await?,
        };
        tcp.set_nodelay(true)?;
        let state = Collector::with_revision(receive.clone(), generation, revision);
        let timestamps = kernel::SocketTimestamps::new(&tcp, receive);
        let measured = SocketIo::new(tcp, timestamps, receive.policy);
        let transport = if tls {
            let client = config.tls_config.clone().unwrap_or_else(|| {
                let roots = rustls::RootCertStore::from_iter(
                    webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
                );
                Arc::new(
                    rustls::ClientConfig::builder()
                        .with_root_certificates(roots)
                        .with_no_client_auth(),
                )
            });
            let name = rustls::pki_types::ServerName::try_from(host).map_err(|_| {
                tungstenite::Error::Url(tungstenite::error::UrlError::UnableToConnect(
                    "invalid TLS server name".into(),
                ))
            })?;
            MaybeTlsStream::Rustls(
                tokio_rustls::TlsConnector::from(client)
                    .connect(name, measured)
                    .await?,
            )
        } else {
            MaybeTlsStream::Plain(measured)
        };
        let plaintext = PlainIo::new(transport, state, tls);
        let (mut stream, _) = client_async_with_config(request, plaintext, Some(ws)).await?;
        stream.get_mut().collector.handshake_done();
        Ok(Self::Measured {
            stream: Box::new(stream),
            last: None,
        })
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Self::Ordinary(stream) => match stream.get_ref() {
                MaybeTlsStream::Plain(socket) => socket.peer_addr(),
                MaybeTlsStream::Rustls(stream) => stream.get_ref().0.peer_addr(),
                _ => Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "unsupported TLS connector",
                )),
            },
            Self::Measured { stream, .. } => stream.get_ref().peer_addr(),
        }
    }
    pub fn configure(
        &mut self,
        config: Option<super::receive::ReceiveConfig>,
        revision: u64,
    ) -> Result<bool, super::receive::control::UpdateError> {
        use super::receive::{control::UpdateError, policy::ReceivePolicy};
        match self {
            Self::Ordinary(_) => {
                if config.as_ref().is_some_and(|c| c.policy.enabled()) {
                    Err(UpdateError::ReconnectRequired(
                        "receive adapters not initialized",
                    ))
                } else {
                    Ok(false)
                }
            }
            Self::Measured { stream, last } => {
                let config = config.unwrap_or_else(|| super::receive::ReceiveConfig {
                    policy: ReceivePolicy::OFF,
                    ..stream.get_ref().collector.config.clone()
                });
                let partial = stream.get_mut().configure(config, revision)?;
                *last = None;
                Ok(partial)
            }
        }
    }
    pub fn take_timing(&mut self) -> Option<ReceiveTiming> {
        match self {
            Self::Ordinary(_) => None,
            Self::Measured { last, .. } => last.take(),
        }
    }
}
impl Stream for Socket {
    type Item = Result<Message, tungstenite::Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Self::Ordinary(stream) => Pin::new(stream).poll_next(cx),
            Self::Measured { stream, last } => {
                let probe = stream.get_mut().collector.poll_start();
                let result = Pin::new(stream.as_mut()).poll_next(cx);
                let collector = &mut stream.get_mut().collector;
                collector.poll_end(probe, result.is_pending());
                if let Poll::Ready(Some(Ok(message))) = &result {
                    *last = collector
                        .protocol_message(matches!(message, Message::Text(_) | Message::Binary(_)));
                }
                result
            }
        }
    }
}
impl Sink<Message> for Socket {
    type Error = tungstenite::Error;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.get_mut() {
            Self::Ordinary(s) => Pin::new(s).poll_ready(cx),
            Self::Measured { stream, .. } => Pin::new(stream.as_mut()).poll_ready(cx),
        }
    }
    fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        match self.get_mut() {
            Self::Ordinary(s) => Pin::new(s).start_send(message),
            Self::Measured { stream, .. } => Pin::new(stream.as_mut()).start_send(message),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.get_mut() {
            Self::Ordinary(s) => Pin::new(s).poll_flush(cx),
            Self::Measured { stream, .. } => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.get_mut() {
            Self::Ordinary(s) => Pin::new(s).poll_close(cx),
            Self::Measured { stream, .. } => Pin::new(stream.as_mut()).poll_close(cx),
        }
    }
}
