//! Exact recognition of the observed compact event layout. Any mismatch returns
//! to the general decoder, including reordered/additional fields and escapes.
//! No successful fast decode can leave unconsumed bytes or skip a field.
//! Every accepted byte is an ASCII literal, decimal digit, sign, quote, or JSON
//! whitespace. UTF-8 validation of unrecognized layouts belongs to the caller.
//!
//! Layout evidence: the retained public streams under pax-book/tests/fixtures.
//! A separately checked sorted-serializer layout runs only after this layout
//! fails; it is not evidence of current gateway byte ordering.
//! Contract: <https://docs.pax.markets/api-reference/websocket-messages/>.
use super::event::{Event, EventData, Order, TopOfBook};
use crate::units::{MarketId, PriceTicks, Quantity, Sequence, Signed};
mod sorted;

pub(super) fn parse_sorted(bytes: &[u8]) -> Option<Event> {
    sorted::parse(bytes)
}

pub(super) fn parse(bytes: &[u8]) -> Option<Event> {
    // Check the closing layout before parsing fields. Extra envelope members
    // often change this suffix, so they need not pay for an almost-full attempt.
    // JSON permits only these four trailing whitespace characters.
    let text = trimmed(bytes);
    let mut reader = Reader(text.strip_suffix(b"}}}")?);
    let tob = TopOfBook {
        ask_qty: Quantity(
            reader.number(br#"{"type":"md_event_detail","data":{"tob":{"ask_qty":"#)?,
        ),
        bid_qty: Quantity(reader.number(br#","bid_qty":"#)?),
        ask_ticks: PriceTicks(reader.number(br#","ask_ticks":"#)?),
        bid_ticks: PriceTicks(reader.number(br#","bid_ticks":"#)?),
    };
    let data = EventData {
        event_type: reader
            .code(br#"},"data":{"event_type":"#)?
            .try_into()
            .ok()?,
        event_qual: reader.code(br#","event_qual":"#)?,
        event_side: reader.code(br#","event_side":"#)?.try_into().ok()?,
        event_delta_qty: Signed(reader.number(br#","event_delta_qty":"#)?),
        event_abs_ticks: PriceTicks(reader.number(br#","event_abs_ticks":"#)?),
        event_tick_depth: Signed(reader.number(br#","event_tick_depth":"#)?),
        event_level_depth: Quantity(reader.number(br#","event_level_depth":"#)?),
        event_level_begin_qty: Quantity(reader.number(br#","event_level_begin_qty":"#)?),
        event_level_delta_qty: Signed(reader.number(br#","event_level_delta_qty":"#)?),
        event_total_px: Signed(reader.number(br#","event_total_px":"#)?),
        seq_num: Sequence(reader.number(br#","seq_num":"#)?),
    };
    let order = Order {
        side: reader.code(br#"},"order":{"side":"#)?.try_into().ok()?,
        market_id: MarketId(reader.number(br#","market_id":"#)?),
        qty: Signed(reader.number(br#","qty":"#)?),
        ticks: PriceTicks(reader.number(br#","ticks":"#)?),
        tif: reader.code(br#","tif":"#)?,
        origin: reader.code(br#","origin":"#)?,
    };
    if !reader.0.is_empty() {
        return None;
    }
    let event = Event { tob, data, order };
    event.validate_protocol().ok()?;
    Some(event)
}

fn trimmed(bytes: &[u8]) -> &[u8] {
    let mut end = bytes.len();
    while end > 0 && matches!(bytes[end - 1], b' ' | b'\t' | b'\r' | b'\n') {
        end -= 1;
    }
    &bytes[..end]
}

struct Reader<'a>(&'a [u8]);
impl Reader<'_> {
    /// Recognize the observed single-digit numeric codes. The next structural
    /// prefix (or end check) rejects extra digits; other encodings use Serde.
    #[inline(always)]
    fn code(&mut self, prefix: &[u8]) -> Option<u8> {
        let text = self.0.strip_prefix(prefix)?;
        let digit = *text.first()?;
        if !digit.is_ascii_digit() {
            return None;
        }
        // The successful first-byte check makes this one-byte advance valid.
        self.0 = &text[1..];
        Some(digit - b'0')
    }
    /// Convert the integer and locate its end in one operation, then enforce
    /// the enclosing JSON integer grammar.
    // Specialize literal prefixes; measured tradeoffs are in docs/performance.md.
    #[inline(always)]
    fn number<T: atoi_simd::Parse>(&mut self, prefix: &[u8]) -> Option<T> {
        let text = self.0.strip_prefix(prefix)?;
        let (quoted, text) = text
            .strip_prefix(b"\"")
            .map_or((false, text), |s| (true, s));
        let digits = text.strip_prefix(b"-").unwrap_or(text);
        // Permit leading zeroes inside integer strings, but never a '+' sign.
        // https://docs.rs/atoi_simd/0.18.1/atoi_simd/fn.parse_prefix.html
        let (integer, end) = atoi_simd::parse_prefix::<T, true, false>(text).ok()?;
        let count = end.checked_sub(text.len() - digits.len())?;
        if count == 0 || (!quoted && count > 1 && digits.starts_with(b"0")) {
            return None;
        }
        // Check the dependency's returned length before advancing the cursor.
        let remaining = text.get(end..)?;
        self.0 = if quoted {
            remaining.strip_prefix(b"\"")?
        } else {
            remaining
        };
        Some(integer)
    }
}

#[cfg(test)]
mod tests;
