//! Bounded public feed with ordered generation boundaries and explicit recovery.

pub mod connection;
pub mod receive;
mod socket;
pub mod socket_options;
pub mod subscription;
mod supervisor;
mod timing;

pub use timing::DeliveryTiming;

use crate::metadata::ClientError;
use bytes::Bytes;
use futures_util::{StreamExt, stream::BoxStream};
use pax_types::units::MarketId;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

/// Public production feed; no account handshake is used.
pub const MARKET_DATA_URL: &str = "wss://api.pax.markets/v1/public/spot/market-data";

/// Finite transport bounds and recovery timing.
#[derive(Clone, Debug)]
pub struct FeedConfig {
    /// Public WebSocket endpoint or local fixture-server endpoint.
    pub url: String,
    /// Optional fixed TCP destination for controlled path comparisons. The URL
    /// still supplies the TLS server name and WebSocket Host header. Its port
    /// must match the URL; `None` retains normal DNS resolution on every attempt.
    pub connect_address: Option<SocketAddr>,
    /// Explicit market selection discovered through metadata.
    pub markets: Vec<MarketId>,
    /// Complete application-message byte bound.
    pub max_message_bytes: usize,
    /// WebSocket read chunk size, 1 KiB..=1 MiB, independent of message limit.
    pub read_buffer_bytes: usize,
    /// Maximum queued messages and lifecycle notifications.
    pub queue_capacity: usize,
    /// Sample every Nth application message for delivery timing; zero disables it.
    pub delivery_sample_every: u64,
    /// Optional TLS configuration for explicit custom trust roots. The default
    /// uses public roots and verifies the server certificate and name.
    pub tls_config: Option<Arc<rustls::ClientConfig>>,
    /// Opt-in receive instrumentation; absent preserves the ordinary transport.
    pub receive: Option<receive::ReceiveConfig>,
    /// Opt-in connection-only Linux TCP readbacks. Requires `socket-readbacks`;
    /// no settings change or per-update queries occur. Disabled by default.
    pub socket_readbacks: bool,
    /// Application JSON ping period, strictly less than 30 seconds.
    pub ping_interval: Duration,
    /// Maximum wait for a requested application pong.
    pub pong_timeout: Duration,
    /// Connect and subscription acknowledgment timeout.
    pub connect_timeout: Duration,
    /// Minimum reconnect delay.
    pub retry_min: Duration,
    /// Maximum reconnect delay.
    pub retry_max: Duration,
}
impl FeedConfig {
    /// Portable production defaults with no credentials or order APIs.
    pub fn public(markets: Vec<MarketId>) -> Self {
        Self {
            url: MARKET_DATA_URL.into(),
            connect_address: None,
            markets,
            max_message_bytes: 1024 * 1024,
            read_buffer_bytes: 128 * 1024,
            queue_capacity: 1024,
            delivery_sample_every: 0,
            tls_config: None,
            receive: None,
            socket_readbacks: false,
            ping_interval: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(8),
            connect_timeout: Duration::from_secs(10),
            retry_min: Duration::from_millis(250),
            retry_max: Duration::from_secs(10),
        }
    }
    /// Reject invalid settings before spawning a task or connecting.
    pub fn validate(&self) -> Result<(), ClientError> {
        subscription::request(&self.markets)?;
        if self.socket_readbacks && !cfg!(all(target_os = "linux", feature = "socket-readbacks")) {
            return Err(ClientError::Config(
                "socket-readbacks require a supported Linux build",
            ));
        }
        if self.delivery_sample_every != 0 && !cfg!(feature = "delivery-traces") {
            return Err(ClientError::Config("delivery-traces probes compiled out"));
        }
        if let Some(receive) = &self.receive {
            receive.validate().map_err(ClientError::Config)?;
        }
        if self.max_message_bytes == 0
            || self.max_message_bytes > 64 * 1024 * 1024
            || !(1024..=1024 * 1024).contains(&self.read_buffer_bytes)
            || self.queue_capacity == 0
            || self.queue_capacity > 65536
            || self.ping_interval.is_zero()
            || self.ping_interval >= Duration::from_secs(30)
            || self.pong_timeout.is_zero()
            || self.pong_timeout >= self.ping_interval
            || self.connect_timeout.is_zero()
            || self.connect_timeout > Duration::from_secs(300)
            || self.retry_min.is_zero()
            || self.retry_max < self.retry_min
            || self.retry_max > Duration::from_secs(300)
        {
            return Err(ClientError::Config(
                "invalid receive bounds, heartbeat or backoff",
            ));
        }
        if !self.url.starts_with("wss://") && !self.url.starts_with("ws://") {
            return Err(ClientError::Config("WebSocket URL must use ws or wss"));
        }
        // Use the same URI parser as Tungstenite, before either delivery mode
        // starts. An explicit invalid port must not become the scheme default.
        let uri = self
            .url
            .parse::<tokio_tungstenite::tungstenite::http::Uri>()
            .map_err(|_| ClientError::Config("invalid WebSocket URL"))?;
        let authority = uri
            .authority()
            .ok_or(ClientError::Config("WebSocket URL requires a host"))?;
        let host = authority.host();
        if host.is_empty() || authority.as_str().contains('@') || self.url.contains('#') {
            return Err(ClientError::Config(
                "invalid public WebSocket authority or fragment",
            ));
        }
        if host.starts_with('[')
            && host
                .strip_prefix('[')
                .and_then(|host| host.strip_suffix(']'))
                .is_none_or(|host| host.parse::<std::net::Ipv6Addr>().is_err())
        {
            return Err(ClientError::Config("invalid WebSocket IPv6 host"));
        }
        let port = if authority.as_str() == host {
            if self.url.starts_with("wss://") {
                443
            } else {
                80
            }
        } else {
            let text = authority.as_str().strip_prefix(host).unwrap_or_default();
            if !text.strip_prefix(':').is_some_and(|port| {
                !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit())
            }) {
                return Err(ClientError::Config("invalid WebSocket port"));
            }
            uri.port_u16()
                .filter(|port| *port != 0)
                .ok_or(ClientError::Config("invalid WebSocket port"))?
        };
        if let Some(address) = self.connect_address
            && (address.port() == 0
                || address.port() != port
                || address.ip().is_unspecified()
                || address.ip().is_multicast())
        {
            return Err(ClientError::Config("invalid fixed WebSocket destination"));
        }
        Ok(())
    }
}

/// Raw input with ordered connection-generation boundaries.
#[derive(Debug)]
pub enum Input {
    /// Connected; all selected books must await fresh snapshots.
    Connected {
        /// Local monotonic connection boundary.
        at: Instant,
        /// Strictly increasing connection identity.
        generation: u64,
    },
    /// Complete application bytes, retained without another payload copy.
    Message {
        /// Connection identity.
        generation: u64,
        /// Local application availability, after TLS and frame assembly.
        received: Instant,
        /// Optional transport and delivery markers, separate from canonical bytes.
        timing: Option<DeliveryTiming>,
        /// Optional qualified receive work and batch association, never payload data.
        receive: Option<Box<receive::ReceiveTiming>>,
        /// Exact original bytes, including controls needed for capture.
        bytes: Bytes,
    },
    /// Previous connection ended; no next-generation data precedes this boundary.
    Disconnected {
        /// Local monotonic termination boundary.
        at: Instant,
        /// Connection identity.
        generation: u64,
        /// Human-readable cause.
        reason: String,
        /// At least one received input could not be delivered.
        lossy: bool,
    },
}

/// Latest transport health, available even while the input queue is saturated.
#[derive(Clone, Debug, Default)]
pub struct Health {
    /// Current connection identity.
    pub generation: u64,
    /// Socket/session health, distinct from book synchronization.
    pub connected: bool,
    /// Actual remote TCP endpoint of the current connection. A fixed destination
    /// is an experiment setting, never evidence of venue placement. Absent while
    /// disconnected or if the socket endpoint cannot be queried.
    pub remote_peer: Option<SocketAddr>,
    /// Requested cold TCP observation for this generation, absent when disabled
    /// or disconnected. Values are read after the WebSocket handshake.
    pub socket_readback: Option<socket_options::Readback>,
    /// Last application heartbeat response.
    pub last_pong: Option<Instant>,
    /// Measured application ping/pong round trip, never a one-way estimate.
    pub round_trip: Option<Duration>,
    /// Last disconnect or connection failure.
    pub reason: Option<String>,
}

/// Running feed; dropping it cancels the supervisor and closes its socket.
pub struct Feed {
    receiver: Receiver,
    health: watch::Receiver<Health>,
    recovery: watch::Sender<u64>,
    stop: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
    measurement_control: receive::control::MeasurementControl,
}
enum Receiver {
    Queued(mpsc::Receiver<Input>),
    Inline(BoxStream<'static, Input>),
}
impl Feed {
    /// Start background transport inside a Tokio runtime.
    pub fn spawn(config: FeedConfig) -> Result<Self, ClientError> {
        Self::start(config, false, receive::dispatch::Dispatch::Embedded)
    }
    /// Drive transport in the receiving task with no per-message task handoff.
    /// Keep polling `recv` to service heartbeats. Canceling a pending receive is
    /// safe: its in-flight socket/connect operation remains owned by this feed.
    pub fn inline(config: FeedConfig) -> Result<Self, ClientError> {
        Self::start(config, true, receive::dispatch::Dispatch::Embedded)
    }
    /// Background feed with a separate preallocated diagnostic stream. `Input`
    /// keeps receive metadata empty; join observations by generation/message ID.
    /// Dropped samples affect diagnostic coverage only, never canonical bytes.
    #[cfg(feature = "diagnostic-handoff")]
    pub fn spawn_with_diagnostics(
        config: FeedConfig,
        capacity: usize,
    ) -> Result<(Self, receive::DiagnosticReceiver), ClientError> {
        Self::with_diagnostics(config, capacity, false)
    }
    /// Inline feed with the same bounded diagnostic publication as the queued feed.
    #[cfg(feature = "diagnostic-handoff")]
    pub fn inline_with_diagnostics(
        config: FeedConfig,
        capacity: usize,
    ) -> Result<(Self, receive::DiagnosticReceiver), ClientError> {
        Self::with_diagnostics(config, capacity, true)
    }
    #[cfg(feature = "diagnostic-handoff")]
    fn with_diagnostics(
        config: FeedConfig,
        capacity: usize,
        inline: bool,
    ) -> Result<(Self, receive::DiagnosticReceiver), ClientError> {
        config.validate()?;
        let (dispatch, receiver) =
            receive::dispatch::Dispatch::queued(capacity).map_err(ClientError::Config)?;
        Ok((Self::start(config, inline, dispatch)?, receiver))
    }
    fn start(
        config: FeedConfig,
        inline: bool,
        diagnostics: receive::dispatch::Dispatch,
    ) -> Result<Self, ClientError> {
        config.validate()?;
        let capacity = config.queue_capacity;
        let (health_tx, health) = watch::channel(Health::default());
        let (recovery, recovery_rx) = watch::channel(0);
        let (stop, stop_rx) = watch::channel(false);
        let (measurement_control, control_rx, policy_status) =
            receive::control::MeasurementControl::channel(receive::control::MeasurementUpdate {
                receive: config.receive.clone(),
                delivery_sample_every: config.delivery_sample_every,
            });
        let driver = supervisor::Driver::new(
            config,
            health_tx,
            recovery_rx,
            stop_rx,
            control_rx,
            policy_status,
            diagnostics,
        );
        let (receiver, task) = if inline {
            (Receiver::Inline(driver.into_stream()), None)
        } else {
            let (sender, receiver) = mpsc::channel(capacity);
            (
                Receiver::Queued(receiver),
                Some(tokio::spawn(supervisor::run(driver, sender))),
            )
        };
        Ok(Self {
            receiver,
            health,
            recovery,
            stop,
            task,
            measurement_control,
        })
    }
    /// Wait for the next ordered input; `None` means the supervisor stopped.
    pub async fn recv(&mut self) -> Option<Input> {
        let (mut input, pending) = match &mut self.receiver {
            Receiver::Queued(receiver) => (receiver.recv().await?, receiver.len()),
            Receiver::Inline(stream) => {
                // Buffered frames need not perform another I/O operation. Give
                // them a finite cooperative budget as channel receives do.
                // https://docs.rs/tokio/1.53.2/tokio/task/fn.consume_budget.html
                tokio::task::consume_budget().await;
                (stream.next().await?, 0)
            }
        };
        input.delivered(pending);
        Some(input)
    }
    /// Take an already queued input without waiting. Inline delivery returns
    /// `None`; it must be driven through `recv` so I/O polling stays explicit.
    pub fn try_recv(&mut self) -> Option<Input> {
        let Receiver::Queued(receiver) = &mut self.receiver else {
            return None;
        };
        let mut input = receiver.try_recv().ok()?;
        input.delivered(receiver.len());
        Some(input)
    }
    /// Independently bounded runtime measurement control. Inline feeds must be
    /// polled concurrently while the caller awaits an update acknowledgment.
    pub fn measurement_control(&self) -> receive::control::MeasurementControl {
        self.measurement_control.clone()
    }
    /// Observe liveness independently of a potentially full message queue.
    pub fn health(&self) -> watch::Receiver<Health> {
        self.health.clone()
    }
    /// Coalesce requests for a fresh connection and synchronization snapshot.
    pub fn request_recovery(&self) {
        self.recovery
            .send_modify(|version| *version = version.wrapping_add(1));
    }
    /// Request cancellation without discarding inputs already queued. A consumer
    /// can drain `recv()` to `None` before calling `shutdown()`.
    pub fn stop(&self) {
        let _ = self.stop.send(true);
    }
    /// Cancel network attempts, waits and the active session, then join the task.
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for Feed {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
impl Input {
    fn delivered(&mut self, pending: usize) {
        if cfg!(feature = "delivery-traces")
            && let Self::Message {
                timing: Some(timing),
                ..
            } = self
        {
            timing.delivered = Some(Instant::now());
            timing.pending = Some(pending);
        }
    }
}
