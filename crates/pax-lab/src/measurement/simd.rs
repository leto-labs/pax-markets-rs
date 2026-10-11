//! Optional exact event adapter for simd-json, independent of RawValue.
//!
//! The parser rewrites its input. Copying into reusable scratch is deliberately
//! part of decode, preserving immutable recording bytes. Sources: simd-json
//! 0.18.1 src/serde.rs (from_slice_with_buffers), src/serde/de.rs (128-bit visitors).
use pax_types::protocol::{self, Message};
use pax_types::{
    protocol::event::{Event, EventData, EventType, Order, Side, TopOfBook},
    units::{MarketId, PriceTicks, Quantity, Sequence, Signed},
};
use serde::{Deserialize, Deserializer, de::Visitor};
use std::fmt;

/// Reusable experimental parser, bounded to the same 1 MiB public message limit.
#[derive(Default)]
pub struct Decoder {
    scratch: Vec<u8>,
    buffers: simd_json::Buffers,
}

/// Explicit parse, framing, or protocol failure; no partial event is returned.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Input is outside this experiment's event contract.
    #[error("{0}")]
    Contract(&'static str),
    /// JSON syntax or exact typed conversion failed.
    #[error(transparent)]
    Json(#[from] simd_json::Error),
}

impl Decoder {
    /// Decode and validate one event, including mutable input preparation.
    /// Snapshots/controls use the standard decoder and are outside this experiment.
    pub fn decode(&mut self, bytes: &[u8]) -> Result<Event, Error> {
        if bytes.len() > 1024 * 1024 {
            return Err(Error::Contract("SIMD event exceeds 1 MiB bound"));
        }
        match self.decode_simd(bytes) {
            Ok(event) => Ok(event),
            // Unknown extensions can contain valid JSON numbers outside SIMD's
            // numeric representation. Preserve the canonical decoder's policy.
            Err(_) => match protocol::decode(bytes) {
                Ok(Message::Event(event)) => Ok(event),
                _ => Err(Error::Contract("invalid or unsupported event")),
            },
        }
    }
    fn decode_simd(&mut self, bytes: &[u8]) -> Result<Event, Error> {
        self.scratch.clear();
        self.scratch.extend_from_slice(bytes);
        let wire: Envelope<'_> =
            simd_json::serde::from_slice_with_buffers(&mut self.scratch, &mut self.buffers)?;
        if wire.kind != "md_event_detail" {
            return Err(Error::Contract("expected md_event_detail"));
        }
        let event = Event {
            tob: wire.data.tob.into(),
            data: wire.data.data.into(),
            order: wire.data.order.into(),
        };
        event.validate_protocol().map_err(Error::Contract)?;
        Ok(event)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<'a> {
    #[serde(rename = "type", borrow)]
    kind: &'a str,
    data: WireEvent,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireEvent {
    tob: WireTop,
    data: Data,
    order: WireOrder,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Data {
    #[serde(deserialize_with = "code")]
    event_type: EventType,
    #[serde(deserialize_with = "code")]
    event_qual: u8,
    #[serde(deserialize_with = "code")]
    event_side: Side,
    #[serde(deserialize_with = "signed")]
    event_delta_qty: Signed,
    event_abs_ticks: PriceTicks,
    #[serde(deserialize_with = "signed")]
    event_tick_depth: Signed,
    event_level_depth: Quantity,
    event_level_begin_qty: Quantity,
    #[serde(deserialize_with = "signed")]
    event_level_delta_qty: Signed,
    #[serde(deserialize_with = "signed")]
    event_total_px: Signed,
    seq_num: Sequence,
}
impl From<Data> for EventData {
    fn from(d: Data) -> Self {
        Self {
            event_type: d.event_type,
            event_qual: d.event_qual,
            event_side: d.event_side,
            event_delta_qty: d.event_delta_qty,
            event_abs_ticks: d.event_abs_ticks,
            event_tick_depth: d.event_tick_depth,
            event_level_depth: d.event_level_depth,
            event_level_begin_qty: d.event_level_begin_qty,
            event_level_delta_qty: d.event_level_delta_qty,
            event_total_px: d.event_total_px,
            seq_num: d.seq_num,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireOrder {
    #[serde(deserialize_with = "code")]
    side: Side,
    market_id: MarketId,
    #[serde(deserialize_with = "signed")]
    qty: Signed,
    ticks: PriceTicks,
    #[serde(deserialize_with = "code")]
    tif: u8,
    #[serde(deserialize_with = "code")]
    origin: u8,
}
impl From<WireOrder> for Order {
    fn from(o: WireOrder) -> Self {
        Self {
            side: o.side,
            market_id: o.market_id,
            qty: o.qty,
            ticks: o.ticks,
            tif: o.tif,
            origin: o.origin,
        }
    }
}
fn signed<'de, D: Deserializer<'de>>(d: D) -> Result<Signed, D::Error> {
    struct Exact;
    impl Visitor<'_> for Exact {
        type Value = Signed;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("an exact signed 128-bit integer or integer string")
        }
        fn visit_i64<E: serde::de::Error>(self, n: i64) -> Result<Signed, E> {
            Ok(Signed(n.into()))
        }
        fn visit_u64<E: serde::de::Error>(self, n: u64) -> Result<Signed, E> {
            Ok(Signed(n.into()))
        }
        fn visit_i128<E: serde::de::Error>(self, n: i128) -> Result<Signed, E> {
            Ok(Signed(n))
        }
        fn visit_u128<E: serde::de::Error>(self, n: u128) -> Result<Signed, E> {
            i128::try_from(n).map(Signed).map_err(E::custom)
        }
        fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Signed, E> {
            if s.starts_with('+') {
                return Err(E::custom("explicit positive sign is not allowed"));
            }
            s.parse::<i128>().map(Signed).map_err(E::custom)
        }
    }
    // Float visits intentionally retain the default rejection, including 1.0
    // and 1e0. Conversion through f64 would silently lose wide integer bits.
    d.deserialize_any(Exact)
}

// Positive integer codes stay distinct from signed or floating zero.
fn code<'de, D: Deserializer<'de>, T: TryFrom<u8>>(d: D) -> Result<T, D::Error>
where
    T::Error: fmt::Display,
{
    struct Code;
    impl Visitor<'_> for Code {
        type Value = u8;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("an unsigned numeric code")
        }
        fn visit_u64<E: serde::de::Error>(self, n: u64) -> Result<u8, E> {
            u8::try_from(n).map_err(E::custom)
        }
    }
    T::try_from(d.deserialize_any(Code)?).map_err(serde::de::Error::custom)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireTop {
    ask_qty: Quantity,
    bid_qty: Quantity,
    ask_ticks: PriceTicks,
    bid_ticks: PriceTicks,
}
impl From<WireTop> for TopOfBook {
    fn from(t: WireTop) -> Self {
        Self {
            ask_qty: t.ask_qty,
            bid_qty: t.bid_qty,
            ask_ticks: t.ask_ticks,
            bid_ticks: t.bid_ticks,
        }
    }
}
