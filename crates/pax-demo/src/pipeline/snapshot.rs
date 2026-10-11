//! Bounded numeric report extraction, independent of formatting and percentiles.
use super::{Pipeline, stats::Weighted};
use crate::measurement::trace::{TraceId, TraceWindow};
use pax_book::{liquidity::FillEstimate, state::Status};
use pax_types::{
    market::MarketUnits,
    protocol::{event::Side, snapshot::Level},
    units::{MarketId, PriceTicks, Quantity, Sequence},
};
use std::sync::Arc;

/// Immutable report input, bounded by display depth and the trace capacity.
/// It holds no reference or lock into the live reconstruction owner.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub(super) observed_ns: u64,
    pub(super) connected: bool,
    pub(super) messages: u64,
    pub(super) applied: u64,
    pub(super) rejected: u64,
    pub(super) last_error: Option<String>,
    pub(super) last_rejection: Option<super::view::RejectedInput>,
    pub(super) markets: Vec<MarketSnapshot>,
    pub(super) latency: TraceWindow,
}
#[derive(Clone, Debug)]
pub(super) struct MarketSnapshot {
    pub id: MarketId,
    pub units: Arc<MarketUnits>,
    pub status: &'static str,
    pub generation: u64,
    pub sequence: Option<Sequence>,
    pub update_age_ns: Option<u64>,
    pub trace_id: Option<TraceId>,
    pub spread: Option<PriceTicks>,
    pub asks: Vec<Level>,
    pub bids: Vec<Level>,
    pub ask_levels: usize,
    pub bid_levels: usize,
    pub ask_quantity: Option<u128>,
    pub bid_quantity: Option<u128>,
    pub weighted: Weighted,
    pub estimates: [Option<FillEstimate>; 2],
}
impl Pipeline {
    /// Copy numeric report inputs. Depth is capped at 1,000 rows per side and
    /// traces retain their existing capacity. Execution estimates and a numeric
    /// weighted-interval preview are computed here; formatting and sorting are not.
    pub fn snapshot(&self, now_ns: u64, depth: usize, requested: Quantity) -> Snapshot {
        let now_ns = now_ns.max(self.offset_ns);
        let depth = depth.min(1000);
        let markets = self
            .markets
            .iter()
            .map(|(&id, state)| {
                let book = self.session.book(id).unwrap();
                let asks = book.levels(Side::Ask).unwrap_or_default();
                let bids = book.levels(Side::Bid).unwrap_or_default();
                let mut weighted = state.weighted.clone();
                weighted.advance(book, now_ns - state.weighted_at_ns);
                MarketSnapshot {
                    id,
                    units: state.units.clone(),
                    status: match book.status() {
                        Status::Live => "live",
                        Status::AwaitingSnapshot => "awaiting snapshot",
                        Status::Invalid(_) => "invalid",
                    },
                    generation: book.generation(),
                    sequence: book.sequence(),
                    update_age_ns: state.last_update_ns.map(|at| now_ns.saturating_sub(at)),
                    trace_id: state.last_trace_id,
                    spread: book.spread().ok().flatten(),
                    asks: asks[..asks.len().min(depth)].to_vec(),
                    bids: bids[..bids.len().min(depth)].to_vec(),
                    ask_levels: asks.len(),
                    bid_levels: bids.len(),
                    ask_quantity: book.total_quantity(Side::Ask).ok(),
                    bid_quantity: book.total_quantity(Side::Bid).ok(),
                    weighted,
                    estimates: [
                        book.estimate(Side::Bid, requested).ok(),
                        book.estimate(Side::Ask, requested).ok(),
                    ],
                }
            })
            .collect();
        let mut latency = self.traces.snapshot(now_ns);
        latency.processing = Some(self.probes.status());
        latency.receive_capacity_drops = self.receive_capacity_drops;
        latency.receive_age_expirations = self.receive_age_expirations.saturating_add(
            self.receive_traces
                .iter()
                .filter(|t| now_ns.saturating_sub(t.received_ns) > latency.window_ns)
                .count() as u64,
        );
        latency.receive = self
            .receive_traces
            .iter()
            .filter(|t| now_ns.saturating_sub(t.received_ns) <= latency.window_ns)
            .copied()
            .collect();
        Snapshot {
            observed_ns: now_ns,
            connected: self.connected,
            messages: self.messages,
            applied: self.applied,
            rejected: self.rejected,
            last_error: self.last_error.clone(),
            last_rejection: self.last_rejection.clone(),
            markets,
            latency,
        }
    }
}

impl Snapshot {
    pub(crate) fn attach_receive(&mut self, worker: &crate::measurement::worker::ReceiveWorker) {
        worker.attach(&mut self.latency, self.observed_ns, self.messages);
    }
}
