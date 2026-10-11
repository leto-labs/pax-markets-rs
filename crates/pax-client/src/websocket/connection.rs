//! One public WebSocket session: controls, heartbeat and bounded delivery.
use super::{DeliveryTiming, FeedConfig, Health, Input, socket::Socket, subscription};
use futures_util::{SinkExt, StreamExt};
use pax_types::units::MarketId;
use serde::Deserialize;
use std::{borrow::Cow, time::Instant};
use tokio::{
    sync::{mpsc, watch},
    time::{self, Instant as Timer},
};
use tokio_tungstenite::tungstenite::Message;

pub(crate) struct Failure {
    pub reason: String,
    pub lossy: bool,
}
impl Failure {
    pub fn protocol(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            lossy: false,
        }
    }
}

#[derive(Deserialize)]
struct Control<'a> {
    #[serde(rename = "type", borrow)]
    kind: Cow<'a, str>,
    market_ids: Option<Vec<MarketId>>,
    message: Option<String>,
    error: Option<String>,
}

pub(crate) struct Connection {
    socket: Socket,
    ping: time::Interval,
    subscription_deadline: Timer,
    subscribed: bool,
    acknowledged: bool,
    pong_due: Option<Timer>,
    ping_started: Option<Instant>,
}
impl Connection {
    pub fn new(socket: Socket, config: &FeedConfig) -> Self {
        let mut ping = time::interval_at(Timer::now() + config.ping_interval, config.ping_interval);
        ping.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        Self {
            socket,
            ping,
            subscription_deadline: Timer::now() + config.connect_timeout,
            subscribed: false,
            acknowledged: false,
            pong_due: None,
            ping_started: None,
        }
    }
    // The caller retains this future across ordinary receive cancellation. A
    // recovery or stop deliberately drops it together with the whole connection.
    #[allow(clippy::too_many_arguments)] // One session owns data, health and bounded operator control.
    pub async fn next(
        &mut self,
        config: &mut FeedConfig,
        generation: u64,
        health: &watch::Sender<Health>,
        messages: &mut u64,
        control: &mut mpsc::Receiver<super::receive::control::Request>,
        policy_status: &watch::Sender<super::receive::control::Effective>,
        diagnostics: &mut super::receive::dispatch::Dispatch,
    ) -> Result<(Input, Option<Failure>), Failure> {
        if !self.subscribed {
            let subscription =
                subscription::request(&config.markets).expect("validated subscription");
            send(&mut self.socket, Message::text(subscription), config).await?;
            // A caller may pause after Connected, or its queued boundary may
            // wait for capacity. Ack/heartbeat deadlines begin after sending
            // the subscription, not while waiting for the caller to receive it.
            self.subscription_deadline = Timer::now() + config.connect_timeout;
            self.ping.reset_at(Timer::now() + config.ping_interval);
            self.subscribed = true;
        }
        loop {
            let pong_deadline = self.pong_due.unwrap_or(self.subscription_deadline);
            tokio::select! {
                Some(request) = control.recv() => {
                    use super::receive::control::{Applied,Effective,UpdateError};
                    let result = (|| {
                        request.update.validate()?;
                        let revision=policy_status.borrow().revision.checked_add(1).ok_or(UpdateError::RevisionExhausted)?;
                        let partial=self.socket.configure(request.update.receive.clone(),revision)?;
                        config.receive=request.update.receive.clone();
                        config.delivery_sample_every=request.update.delivery_sample_every;
                        policy_status.send_replace(Effective { revision,settings:request.update });
                        Ok(Applied { revision,generation,after_message:*messages,partial_observation_discarded:partial })
                    })();
                    let _=request.reply.send(result);
                },
                // select! evaluates even disabled branch expressions. Defer
                // runtime-handle acquisition until an active deadline is polled.
                // https://docs.rs/tokio/1.53.2/tokio/macro.select.html
                _ = async { time::sleep_until(self.subscription_deadline).await }, if !self.acknowledged => return Err(Failure::protocol("subscription acknowledgment timeout")),
                _ = async { time::sleep_until(pong_deadline).await }, if self.pong_due.is_some() => return Err(Failure::protocol("application pong timeout")),
                _ = self.ping.tick() => {
                    send(&mut self.socket, Message::text(r#"{"action":"ping"}"#), config).await?;
                    self.ping_started = Some(Instant::now());
                    self.pong_due = Some(Timer::now()+config.pong_timeout);
                }
                message = self.socket.next() => {
                    let received = Instant::now();
                    let message = match message {
                        Some(Ok(message)) => message,
                        Some(Err(error)) => return Err(Failure { reason:error.to_string(), lossy:true }),
                        None => return Err(Failure::protocol("socket ended")),
                    };
                    match message {
                        Message::Close(_) => return Err(Failure::protocol("peer closed connection")),
                        Message::Ping(_) => {
                            // Tungstenite queues the protocol pong; flush it. Only
                            // application JSON pings satisfy the venue idle rule.
                            match time::timeout(config.pong_timeout, self.socket.flush()).await {
                                Ok(Ok(())) => {},
                                Ok(Err(error)) => return Err(Failure::protocol(error.to_string())),
                                Err(_) => return Err(Failure::protocol("protocol pong write timeout")),
                            }
                            continue;
                        }
                        Message::Pong(_) | Message::Frame(_) => continue,
                        Message::Text(_) | Message::Binary(_) => {}
                    }
                    let receive = self.socket.take_timing();
                    let bytes = message.into_data();
                    *messages = messages.wrapping_add(1);
                    let receive=diagnostics.observation(receive,*messages,received);
                    let sampled = cfg!(feature="delivery-traces") && config.delivery_sample_every != 0 && messages.is_multiple_of(config.delivery_sample_every);
                    // Exact type-first market tags skip only transport control
                    // parsing. Consumers still validate the complete message.
                    let control = if market_prefix(&bytes) { Ok(None) }
                        else { serde_json::from_slice::<Control<'_>>(&bytes).map(Some) };
                    let mut failure = None;
                    match control {
                        Ok(Some(control)) => match control.kind.as_ref() {
                            "subscribed" => {
                                if control.market_ids.as_deref().is_some_and(|ids| subscription::acknowledged(&config.markets,ids)) {
                                    self.acknowledged = true;
                                } else { failure = Some("subscription omitted selected markets".to_owned()); }
                            }
                            "pong" => {
                                let rtt = self.ping_started.take().map(|started| received.saturating_duration_since(started));
                                self.pong_due = None;
                                health.send_modify(|state| { state.last_pong = Some(received); if rtt.is_some() { state.round_trip = rtt; } });
                            }
                            "error" => failure = Some(control.message.or(control.error).unwrap_or_else(|| "service error".into())),
                            "unsubscribed" => failure = Some("unexpected unsubscription".into()),
                            _ => {}
                        },
                        Ok(None) => {},
                        Err(error) => failure = Some(format!("invalid public control envelope: {error}")),
                    }
                    let timing = sampled.then(|| DeliveryTiming { prepared: Instant::now(), delivered: None, pending: None });
                    return Ok((Input::Message { generation, received, timing, receive, bytes }, failure.map(Failure::protocol)));
                }
            }
        }
    }
}

async fn send(socket: &mut Socket, message: Message, config: &FeedConfig) -> Result<(), Failure> {
    match time::timeout(config.pong_timeout, socket.send(message)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(Failure::protocol(error.to_string())),
        Err(_) => Err(Failure::protocol("application control write timeout")),
    }
}

fn market_prefix(bytes: &[u8]) -> bool {
    bytes.starts_with(br#"{"type":"md_event_detail","#)
        || bytes.starts_with(br#"{"type":"snapshot","#)
}

#[cfg(test)]
mod tests {
    use super::market_prefix;
    #[test]
    fn only_exact_market_tags_skip_control_parsing() {
        for bytes in [
            br#"{"type":"snapshot","data":{}}"#.as_slice(),
            br#"{"type":"md_event_detail","data":{}}"#,
        ] {
            assert!(market_prefix(bytes));
        }
        for bytes in [
            br#"{"type":"pong"}"#.as_slice(),
            br#"{"type":"snapshot_error","message":"x"}"#,
            br#"{"data":{},"type":"snapshot"}"#,
            br#"{ "type": "snapshot", "data": {} }"#,
            br#"{"type":"snapshot"}"#,
        ] {
            assert!(!market_prefix(bytes));
        }
    }
}
