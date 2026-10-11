//! Layer-owned receive state. Nested I/O never acquires shared collector locks.
use super::{
    Collector, ProbeWork, ReadObservation, collector::ns, kernel::SocketTimestamps,
    policy::ReceivePolicy,
};
use crate::measurement::Sampler;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tokio_tungstenite::MaybeTlsStream;

pub(crate) struct SocketIo {
    inner: TcpStream,
    timestamps: Box<SocketTimestamps>,
    work: ProbeWork,
    sampler: Sampler,
    observe_reads: bool,
    reads: u64,
    first: Option<ReadObservation>,
    last: Option<(u64, Instant)>,
}
impl SocketIo {
    pub fn new(inner: TcpStream, timestamps: SocketTimestamps, policy: ReceivePolicy) -> Self {
        Self {
            inner,
            timestamps: Box::new(timestamps),
            work: ProbeWork::default(),
            sampler: Sampler::new(policy.socket).expect("validated socket policy"),
            observe_reads: policy.association.enabled() || policy.timestamps.enabled(),
            reads: 0,
            first: None,
            last: None,
        }
    }
}
impl AsyncRead for SocketIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let start = (cfg!(feature = "receive-stages") && this.sampler.observe()).then(Instant::now);
        let result = this
            .timestamps
            .poll_read(Pin::new(&mut this.inner), cx, buf);
        let nonempty = buf.filled().len() > before;
        let end = (start.is_some() || (nonempty && this.observe_reads)).then(Instant::now);
        let elapsed = start
            .zip(end)
            .map(|(a, b)| ns(b.saturating_duration_since(a)));
        this.work.record(elapsed, elapsed);
        if nonempty {
            this.reads = this.reads.saturating_add(1);
            if this.observe_reads
                && let Some(end) = end
            {
                this.first.get_or_insert(ReadObservation {
                    batch: this.reads,
                    ended: end,
                    software: this.timestamps.software,
                    hardware: this.timestamps.hardware,
                });
                this.last = Some((this.reads, end));
            }
        }
        result
    }
}

pub(crate) struct PlainIo {
    inner: MaybeTlsStream<SocketIo>,
    pub collector: Collector,
    tls: bool,
    sampler: Sampler,
}
impl PlainIo {
    #[cfg(all(target_os = "linux", feature = "socket-readbacks"))]
    pub(crate) fn tcp(&self) -> Option<&TcpStream> {
        match &self.inner {
            MaybeTlsStream::Plain(socket) => Some(&socket.inner),
            MaybeTlsStream::Rustls(stream) => Some(&stream.get_ref().0.inner),
            _ => None,
        }
    }
    pub fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        match &self.inner {
            MaybeTlsStream::Plain(socket) => socket.inner.peer_addr(),
            MaybeTlsStream::Rustls(stream) => stream.get_ref().0.inner.peer_addr(),
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "unsupported TLS connector",
            )),
        }
    }
    pub fn new(inner: MaybeTlsStream<SocketIo>, collector: Collector, tls: bool) -> Self {
        let mut io = Self {
            sampler: Sampler::new(collector.config.policy.tls).expect("validated stream policy"),
            inner,
            collector,
            tls,
        };
        let socket = io.socket();
        socket.work = ProbeWork::default();
        let _ = socket.timestamps.take_work();
        socket.first = None;
        socket.last = None;
        io
    }
    pub fn configure(
        &mut self,
        config: super::ReceiveConfig,
        revision: u64,
    ) -> Result<bool, super::control::UpdateError> {
        use super::control::UpdateError;
        let previous = &self.collector.config;
        if config.software_timestamps != previous.software_timestamps
            || config.hardware != previous.hardware
            || config.capacity != previous.capacity
            || config.phc_calibration != previous.phc_calibration
            || config.phc_worker_cpu != previous.phc_worker_cpu
            || config.phc_deferred_mapping != previous.phc_deferred_mapping
            || config.phc_refresh_interval != previous.phc_refresh_interval
        {
            return Err(UpdateError::ReconnectRequired(
                "socket/clock source or association capacity",
            ));
        }
        if config.policy.association.enabled() && !previous.policy.association.enabled() {
            return Err(UpdateError::ReconnectRequired(
                "frame observation must start at the upgrade boundary",
            ));
        }
        self.sampler
            .configure(config.policy.tls)
            .expect("validated policy");
        let socket = self.socket();
        socket
            .sampler
            .configure(config.policy.socket)
            .expect("validated policy");
        socket.timestamps.configure(config.policy);
        let _ = socket.timestamps.take_work();
        socket.observe_reads =
            config.policy.association.enabled() || config.policy.timestamps.enabled();
        socket.work = ProbeWork::default();
        socket.first = None;
        socket.last = None;
        let counts = socket.timestamps.counts();
        let socket_counts = socket.sampler.counts();
        // A buffered frame can be delivered before the next adapter read. Update
        // cached counts at the policy boundary too, so it cannot expose a reset
        // domain's old denominator under the new revision.
        self.collector.selection.timestamps = counts.0;
        self.collector.selection.read_detail = counts.1;
        self.collector.selection.socket = cfg!(feature = "receive-stages").then_some(socket_counts);
        self.collector.selection.tls =
            cfg!(feature = "receive-stages").then(|| self.sampler.counts());
        Ok(self.collector.configure(config, revision))
    }
    fn socket(&mut self) -> &mut SocketIo {
        // Pinned tokio-rustls exposes the owned IO without aliases to its mutable
        // connection state: https://docs.rs/tokio-rustls/0.26.6/tokio_rustls/client/struct.TlsStream.html#method.get_mut
        match &mut self.inner {
            MaybeTlsStream::Plain(socket) => socket,
            MaybeTlsStream::Rustls(stream) => stream.get_mut().0,
            _ => unreachable!("only explicitly constructed plain/rustls streams"),
        }
    }
}
impl AsyncRead for PlainIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let socket = this.socket();
        // TLS handshake work predates this adapter; discard it at construction.
        let reads = socket.reads;
        let start = (cfg!(feature = "receive-stages") && this.sampler.observe()).then(Instant::now);
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        let elapsed = start.map(|start| ns(start.elapsed()));
        let socket = this.socket();
        let socket_work = std::mem::take(&mut socket.work);
        let read_detail = socket.timestamps.take_work();
        let counts = socket.timestamps.counts();
        let socket_counts = socket.sampler.counts();
        let new_reads = socket.reads.saturating_sub(reads);
        let window = (
            socket.first.take(),
            socket.last,
            socket.timestamps.software,
            socket.timestamps.hardware,
        );
        let nested = socket_work.inclusive_since(ProbeWork::default());
        // Preserve reads that have not yet exposed plaintext across Pending polls.
        let state = &mut this.collector;
        state.selection.timestamps = counts.0;
        state.selection.read_detail = counts.1;
        state.selection.socket = cfg!(feature = "receive-stages").then_some(socket_counts);
        state.selection.tls = cfg!(feature = "receive-stages").then(|| this.sampler.counts());
        if let Some(first) = window.0 {
            state.first.get_or_insert(first);
        }
        state.last = window.1;
        state.software = window.2;
        state.hardware = window.3;
        if new_reads != 0
            && let Some((batch, ended)) = window.1
        {
            state.read_observation(ReadObservation {
                batch,
                ended,
                software: window.2,
                hardware: window.3,
            });
        }
        state.work.reads = state.work.reads.saturating_add(new_reads);
        state.work.socket.merge(socket_work);
        state.work.read_detail.merge(read_detail);
        let exclusive = elapsed
            .zip(nested)
            .and_then(|(outer, inner)| outer.checked_sub(inner));
        state
            .stream_total
            .record(if this.tls { elapsed } else { nested }, exclusive);
        // Plain transport uses the same adapter boundary. It has no TLS work;
        // its wrapper overhead is left in the WebSocket inclusive observation.
        if this.tls {
            state.work.tls.record(elapsed, exclusive);
        }
        if buf.filled().len() > before {
            state.plaintext(&buf.filled()[before..]);
        }
        result
    }
}
macro_rules! forward_write {
    ($wrapper:ty) => {
        impl AsyncWrite for $wrapper {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
            }
            fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Pin::new(&mut self.get_mut().inner).poll_flush(cx)
            }
            fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
            }
        }
    };
}
forward_write!(SocketIo);
forward_write!(PlainIo);

#[cfg(all(
    test,
    target_os = "linux",
    feature = "receive-timestamps",
    feature = "receive-stages"
))]
mod selection_tests {
    use super::*;
    use crate::{
        measurement::{SampleRate, SelectionCounts},
        websocket::receive::ReceiveConfig,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[tokio::test]
    async fn reset_counts_are_visible_before_another_socket_read() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut sender, _) = listener.accept().await.unwrap();
        let config = ReceiveConfig::default();
        let kernel = SocketTimestamps::new(&client, &config);
        let socket = SocketIo::new(client, kernel, config.policy);
        let mut io = PlainIo::new(
            MaybeTlsStream::Plain(socket),
            Collector::new(config.clone(), 1),
            false,
        );
        sender.write_all(b"data").await.unwrap();
        io.read_exact(&mut [0; 4]).await.unwrap();
        assert!(io.collector.selection.socket.unwrap().eligible > 0);
        let old_timestamps = io.collector.selection.timestamps;
        let mut next = config;
        next.policy.socket = SampleRate::every(2);
        next.policy.tls = SampleRate::every(3);
        next.policy.read_detail = SampleRate::every(5);
        io.configure(next, 1).unwrap();
        assert_eq!(
            io.collector.selection.socket,
            Some(SelectionCounts::default())
        );
        assert_eq!(io.collector.selection.tls, Some(SelectionCounts::default()));
        assert_eq!(
            io.collector.selection.read_detail,
            Some(SelectionCounts::default())
        );
        assert_eq!(io.collector.selection.timestamps, old_timestamps);
    }
}
