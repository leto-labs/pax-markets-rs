//! One reconnect state machine shared by inline and queued delivery.
use super::{
    FeedConfig, Health, Input,
    connection::{Connection, Failure},
    socket::Socket,
};
use futures_util::{
    StreamExt,
    stream::{self, BoxStream},
};
use std::time::{Duration, Instant};
use tokio::{
    sync::{mpsc, watch},
    time,
};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

pub(crate) struct Driver {
    config: FeedConfig,
    diagnostics: super::receive::dispatch::Dispatch,
    health: watch::Sender<Health>,
    recovery: watch::Receiver<u64>,
    stop: watch::Receiver<bool>,
    connection: Option<Connection>,
    control: mpsc::Receiver<super::receive::control::Request>,
    policy_status: watch::Sender<super::receive::control::Effective>,
    generation: u64,
    messages: u64,
    backoff: Duration,
    retry_at: time::Instant,
    connected_at: Option<Instant>,
    failure: Option<Failure>,
}
impl Driver {
    pub fn new(
        config: FeedConfig,
        health: watch::Sender<Health>,
        recovery: watch::Receiver<u64>,
        stop: watch::Receiver<bool>,
        control: mpsc::Receiver<super::receive::control::Request>,
        policy_status: watch::Sender<super::receive::control::Effective>,
        diagnostics: super::receive::dispatch::Dispatch,
    ) -> Self {
        let backoff = config.retry_min;
        Self {
            config,
            diagnostics,
            control,
            policy_status,
            health,
            recovery,
            stop,
            connection: None,
            generation: 0,
            messages: 0,
            backoff,
            retry_at: time::Instant::now(),
            connected_at: None,
            failure: None,
        }
    }
    pub fn into_stream(self) -> BoxStream<'static, Input> {
        // Unfold retains the in-flight future when StreamExt::next is canceled.
        // Box the seed once, avoiding moves of the socket state for every item.
        // https://docs.rs/futures-util/0.3.34/futures_util/stream/fn.unfold.html
        stream::unfold(Box::new(self), |mut state| async move {
            let input = state.next().await?;
            Some((input, state))
        })
        .fuse()
        .boxed()
    }
    fn fail(&mut self, failure: Failure) {
        if self.failure.is_none() {
            self.connection = None;
            if self
                .connected_at
                .take()
                .is_some_and(|at| at.elapsed() >= self.config.ping_interval)
            {
                self.backoff = self.config.retry_min;
            }
            self.retry_at = time::Instant::now() + self.backoff;
            self.backoff = self.backoff.saturating_mul(2).min(self.config.retry_max);
        }
        // A failing control can itself overflow the delivery queue. Override
        // the reason/loss flag without scheduling a second reconnect backoff.
        self.health.send_replace(Health {
            generation: self.generation,
            connected: false,
            reason: Some(failure.reason.clone()),
            ..Health::default()
        });
        self.failure = Some(failure);
    }
    async fn next(&mut self) -> Option<Input> {
        loop {
            if *self.stop.borrow() {
                return None;
            }
            if let Some(failure) = self.failure.take() {
                return Some(Input::Disconnected {
                    at: Instant::now(),
                    generation: self.generation,
                    reason: failure.reason,
                    lossy: failure.lossy,
                });
            }
            if let Some(connection) = &mut self.connection {
                let result = tokio::select! {
                    biased;
                    _ = self.stop.changed() => return None,
                    _ = self.recovery.changed() => Err(Failure::protocol("consumer requested resynchronization")),
                    result = connection.next(&mut self.config, self.generation, &self.health, &mut self.messages, &mut self.control, &self.policy_status, &mut self.diagnostics) => result,
                };
                match result {
                    Ok((input, failure)) => {
                        if let Some(failure) = failure {
                            self.fail(failure);
                        }
                        return Some(input);
                    }
                    Err(failure) => {
                        self.fail(failure);
                        continue;
                    }
                }
            }
            tokio::select! {
                biased;
                _ = self.stop.changed() => return None,
                Some(request) = self.control.recv() => {
                    let _=request.reply.send(Err(super::receive::control::UpdateError::Disconnected));
                    continue;
                },
                _ = time::sleep_until(self.retry_at) => {},
            }
            let ws_config = WebSocketConfig::default()
                .read_buffer_size(self.config.read_buffer_bytes)
                .max_message_size(Some(self.config.max_message_bytes))
                .max_frame_size(Some(self.config.max_message_bytes));
            let connect = tokio::select! {
                biased;
                _ = self.stop.changed() => return None,
                result = time::timeout(self.config.connect_timeout,
                    Socket::connect(&self.config, ws_config, self.generation.saturating_add(1), self.policy_status.borrow().revision)) => result,
            };
            match connect {
                Ok(Ok(socket)) => {
                    self.generation = self.generation.checked_add(1)?;
                    let at = Instant::now();
                    self.connected_at = Some(at);
                    let remote_peer = socket.peer_addr().ok();
                    #[cfg(all(target_os = "linux", feature = "socket-readbacks"))]
                    let socket_readback = self.config.socket_readbacks.then(|| socket.options());
                    #[cfg(not(all(target_os = "linux", feature = "socket-readbacks")))]
                    let socket_readback = None;
                    self.connection = Some(Connection::new(socket, &self.config));
                    self.health.send_replace(Health {
                        generation: self.generation,
                        connected: true,
                        remote_peer,
                        socket_readback,
                        ..Health::default()
                    });
                    self.recovery.borrow_and_update();
                    return Some(Input::Connected {
                        generation: self.generation,
                        at,
                    });
                }
                Ok(Err(error)) => self.fail(Failure::protocol(error.to_string())),
                Err(_) => self.fail(Failure::protocol("connection timeout")),
            }
        }
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        self.health.send_modify(|state| {
            state.connected = false;
            state.remote_peer = None;
            state.socket_readback = None;
            state.reason = Some("feed stopped".into());
        });
    }
}

pub(crate) async fn run(mut driver: Driver, sender: mpsc::Sender<Input>) {
    while !sender.is_closed() {
        let Some(input) = driver.next().await else {
            break;
        };
        if matches!(input, Input::Message { .. }) {
            if sender.try_send(input).is_err() {
                // Sampling follows delivered application inputs, like the book
                // consumer. An overflow must not offset their sample ordinals
                // permanently after recovery.
                driver.messages = driver.messages.wrapping_sub(1);
                driver.fail(Failure {
                    reason: "input queue overflow or receiver closed".into(),
                    lossy: true,
                });
            }
        } else {
            // Lifecycle boundaries wait for bounded space, but cancellation can
            // interrupt them. Health already exposes loss while the queue is full.
            let sent = tokio::select! {
                biased;
                _ = driver.stop.changed() => false,
                result = sender.send(input) => result.is_ok(),
            };
            if !sent {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pax_types::units::MarketId;

    #[tokio::test]
    async fn failed_control_overflow_keeps_one_reconnect_delay() {
        let config = FeedConfig::public(vec![MarketId(1)]);
        let (health, state) = watch::channel(Health::default());
        let (_recovery, recovery) = watch::channel(0);
        let (_stop, stop) = watch::channel(false);
        let (_control, control, policy_status) =
            super::super::receive::control::MeasurementControl::channel(
                super::super::receive::control::MeasurementUpdate {
                    receive: None,
                    delivery_sample_every: 0,
                },
            );
        let mut driver = Driver::new(
            config,
            health,
            recovery,
            stop,
            control,
            policy_status,
            super::super::receive::dispatch::Dispatch::Embedded,
        );
        driver.fail(Failure::protocol("subscription omitted selected markets"));
        let retry_at = driver.retry_at;
        let backoff = driver.backoff;
        driver.fail(Failure {
            reason: "input queue overflow".into(),
            lossy: true,
        });
        assert_eq!(driver.retry_at, retry_at);
        assert_eq!(driver.backoff, backoff);
        assert_eq!(
            state.borrow().reason.as_deref(),
            Some("input queue overflow")
        );
        assert!(matches!(
            driver.next().await,
            Some(Input::Disconnected { lossy: true, .. })
        ));
    }
}
