//! Checked compact, lexically sorted JSON layout. Used only after the observed
//! public layout fails; no extra work is added to successful public fast decoding.
//! Sorting is a standard serializer form, not an observed Pax gateway guarantee.
use super::{Event, EventData, Order, Reader, TopOfBook, trimmed};
use crate::units::{MarketId, PriceTicks, Quantity, Sequence, Signed};

pub(super) fn parse(bytes: &[u8]) -> Option<Event> {
    if !bytes.starts_with(br#"{"data":{"data":{"event_abs_ticks":"#) {
        return None;
    }
    let mut reader = Reader(trimmed(bytes).strip_suffix(br#"}},"type":"md_event_detail"}"#)?);
    let data = EventData {
        event_abs_ticks: PriceTicks(reader.number(br#"{"data":{"data":{"event_abs_ticks":"#)?),
        event_delta_qty: Signed(reader.number(br#","event_delta_qty":"#)?),
        event_level_begin_qty: Quantity(reader.number(br#","event_level_begin_qty":"#)?),
        event_level_delta_qty: Signed(reader.number(br#","event_level_delta_qty":"#)?),
        event_level_depth: Quantity(reader.number(br#","event_level_depth":"#)?),
        event_qual: reader.code(br#","event_qual":"#)?,
        event_side: reader.code(br#","event_side":"#)?.try_into().ok()?,
        event_tick_depth: Signed(reader.number(br#","event_tick_depth":"#)?),
        event_total_px: Signed(reader.number(br#","event_total_px":"#)?),
        event_type: reader.code(br#","event_type":"#)?.try_into().ok()?,
        seq_num: Sequence(reader.number(br#","seq_num":"#)?),
    };
    let order = Order {
        market_id: MarketId(reader.number(br#"},"order":{"market_id":"#)?),
        origin: reader.code(br#","origin":"#)?,
        qty: Signed(reader.number(br#","qty":"#)?),
        side: reader.code(br#","side":"#)?.try_into().ok()?,
        ticks: PriceTicks(reader.number(br#","ticks":"#)?),
        tif: reader.code(br#","tif":"#)?,
    };
    let tob = TopOfBook {
        ask_qty: Quantity(reader.number(br#"},"tob":{"ask_qty":"#)?),
        ask_ticks: PriceTicks(reader.number(br#","ask_ticks":"#)?),
        bid_qty: Quantity(reader.number(br#","bid_qty":"#)?),
        bid_ticks: PriceTicks(reader.number(br#","bid_ticks":"#)?),
    };
    if !reader.0.is_empty() {
        return None;
    }
    let event = Event { tob, data, order };
    event.validate_protocol().ok()?;
    Some(event)
}
