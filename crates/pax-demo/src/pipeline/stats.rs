//! Exact time-weighted coverage; updates use receive offsets, not UI polling time.
use pax_book::state::{Book, Status};
use pax_types::{market::Ratio, protocol::event::Side};
use serde::Serialize;

#[derive(Clone, Debug, Default)]
pub(super) struct Weighted {
    valid_ns: u128,
    spread_ns: u128,
    spread_ticks_ns: Option<u128>,
    ask_qty_ns: Option<u128>,
    bid_qty_ns: Option<u128>,
    initialized: bool,
}
/// Exact coverage and weighted means, rendered as decimal strings.
#[derive(Clone, Debug, Serialize)]
pub struct Coverage {
    /// Total observed interval, including disconnected periods.
    pub observed_ns: String,
    /// Time with a synchronized book, including one-sided books.
    pub valid_ns: String,
    /// Time when both best prices existed.
    pub two_sided_ns: String,
    /// Time-weighted spread in ticks, excluding invalid and one-sided periods.
    pub mean_spread_ticks: Option<String>,
    /// Time-weighted total represented ask quantity in native base units.
    pub mean_ask_quantity: Option<String>,
    /// Time-weighted total represented bid quantity in native base units.
    pub mean_bid_quantity: Option<String>,
}
impl Weighted {
    pub(super) fn advance(&mut self, book: &Book, duration: u64) {
        if book.status() != Status::Live || duration == 0 {
            return;
        }
        if !self.initialized {
            self.spread_ticks_ns = Some(0);
            self.ask_qty_ns = Some(0);
            self.bid_qty_ns = Some(0);
            self.initialized = true;
        }
        let duration = u128::from(duration);
        self.valid_ns += duration;
        if let Ok(Some(spread)) = book.spread() {
            self.spread_ns += duration;
            add_weight(&mut self.spread_ticks_ns, u128::from(spread.0), duration);
        }
        for (side, sum) in [
            (Side::Ask, &mut self.ask_qty_ns),
            (Side::Bid, &mut self.bid_qty_ns),
        ] {
            let quantity = book.total_quantity(side).unwrap();
            add_weight(sum, quantity, duration);
        }
    }
    pub(super) fn report(&self, observed_ns: u64) -> Coverage {
        let mean = |value: Option<u128>, time| {
            value.and_then(|v| Ratio::new(v, time).ok()?.decimal(9).ok())
        };
        Coverage {
            observed_ns: observed_ns.to_string(),
            valid_ns: self.valid_ns.to_string(),
            two_sided_ns: self.spread_ns.to_string(),
            mean_spread_ticks: mean(self.spread_ticks_ns, self.spread_ns),
            mean_ask_quantity: mean(self.ask_qty_ns, self.valid_ns),
            mean_bid_quantity: mean(self.bid_qty_ns, self.valid_ns),
        }
    }
}
fn add_weight(sum: &mut Option<u128>, value: u128, duration: u128) {
    *sum = sum.and_then(|sum| value.checked_mul(duration)?.checked_add(sum));
}
