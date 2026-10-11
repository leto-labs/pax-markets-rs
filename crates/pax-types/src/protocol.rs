//! Typed public JSON decoding without an intermediate object tree.

mod diagnostics;
mod envelope;
pub mod event;
mod fast_event;
pub mod snapshot;

use crate::units::MarketId;
use envelope::{Data, Envelope};
use event::Event;
use snapshot::Snapshot;

/// Public application messages accepted by this client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Message {
    /// Fresh synchronization base.
    Snapshot(Snapshot),
    /// Incremental market transition.
    Event(Event),
    /// Markets acknowledged by a subscription.
    Subscribed(Vec<MarketId>),
    /// Markets removed by an unsubscribe.
    Unsubscribed(Vec<MarketId>),
    /// Application heartbeat response.
    Pong,
    /// Service error; public observation must recover explicitly.
    Error(String),
}

/// A malformed or unsupported application message.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// Message bytes are not UTF-8, including in otherwise ignored fields.
    #[error("invalid public message encoding: {0}")]
    Encoding(#[from] std::str::Utf8Error),
    /// Invalid JSON or required field representation.
    #[error("invalid public message: {0}")]
    Invalid(#[from] serde_json::Error),
    /// Malformed field, with its exact object/array path and original JSON error.
    #[error("invalid public message at {path}: {source}")]
    Field {
        /// Object keys and array indices locating the unusable field.
        path: String,
        /// Original parser error, including line and column within the decoded JSON.
        #[source]
        source: serde_json::Error,
    },
    /// Unknown state-changing semantics.
    #[error("unsupported public protocol: {0}")]
    Unsupported(String),
    /// Required message data was absent.
    #[error("public message missing {0}")]
    Missing(&'static str),
}

/// Decode one complete application message without a network or runtime.
///
/// Unknown message types and removal qualifiers fail explicitly. Additional
/// irrelevant fields are tolerated. Transport and replay must enforce their byte
/// bounds before calling this function. Numeric types support JSON slice/string
/// deserializers that can borrow RawValue, not generic intermediate Value trees.
pub fn decode(bytes: &[u8]) -> Result<Message, DecodeError> {
    // A complete compact match accounts for every byte: ASCII structural
    // literals, checked ASCII decimal prefixes, and JSON whitespace. It cannot
    // accept non-UTF-8 input, so no separate UTF-8 pass is needed on success.
    if let Some(event) = fast_event::parse(bytes) {
        return Ok(Message::Event(event));
    }
    // A demonstrated serializer layout otherwise requires scanning a borrowed
    // raw envelope and then its payload. Keep this candidate after the public
    // compact success so its recognition adds no work to that path.
    if let Some(event) = fast_event::parse_sorted(bytes) {
        return Ok(Message::Event(event));
    }
    // Other layouts still validate the whole message, including ignored fields.
    // Serde's StrRead then borrows keys and values without repeated UTF-8 checks.
    // https://docs.rs/serde_json/1.0.151/serde_json/fn.from_str.html
    let text = std::str::from_utf8(bytes)?;
    decode_general(text)
}

fn decode_general(text: &str) -> Result<Message, DecodeError> {
    let envelope: Envelope<'_> = serde_json::from_str(text)
        .map_err(|error| diagnostics::locate::<Envelope<'_>>(text.as_bytes(), "", error))?;
    match envelope.kind.as_ref() {
        "snapshot" => {
            let snapshot = match envelope.data.ok_or(DecodeError::Missing("data"))? {
                Data::Snapshot(value) => value,
                Data::Raw(raw) => serde_json::from_str(raw.get()).map_err(|error| {
                    diagnostics::locate::<Snapshot>(raw.get().as_bytes(), "data", error)
                })?,
                Data::Event(_) => unreachable!("type cannot change after payload decoding"),
            };
            Ok(Message::Snapshot(snapshot))
        }
        "md_event_detail" => {
            let event = match envelope.data.ok_or(DecodeError::Missing("data"))? {
                Data::Event(value) => value,
                Data::Raw(raw) => serde_json::from_str(raw.get()).map_err(|error| {
                    diagnostics::locate::<Event>(raw.get().as_bytes(), "data", error)
                })?,
                Data::Snapshot(_) => unreachable!("type cannot change after payload decoding"),
            };
            event
                .validate_protocol()
                .map_err(|e| DecodeError::Unsupported(e.into()))?;
            Ok(Message::Event(event))
        }
        "subscribed" => Ok(Message::Subscribed(
            envelope
                .market_ids
                .ok_or(DecodeError::Missing("market_ids"))?,
        )),
        "unsubscribed" => Ok(Message::Unsubscribed(
            envelope
                .market_ids
                .ok_or(DecodeError::Missing("market_ids"))?,
        )),
        "pong" => Ok(Message::Pong),
        "error" => Ok(Message::Error(
            envelope
                .message
                .or(envelope.error)
                .ok_or(DecodeError::Missing("error message"))?,
        )),
        other => Err(DecodeError::Unsupported(other.into())),
    }
}
