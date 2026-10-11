//! Coalesced reporting work, never included in packet turnover measurements.
use super::{Pipeline, snapshot::Snapshot, stats::Coverage};
use crate::measurement::trace::{TraceId, TraceWindow};
use pax_types::{
    market::{MarketUnits, Ratio},
    units::Quantity,
};
use serde::Serialize;

/// JSON presentation snapshot. Exact venue integers are strings for browsers.
#[derive(Clone, Debug, Serialize)]
pub struct View {
    /// Input source mode, such as live or replay.
    pub mode: String,
    /// Monotonic input observation boundary.
    pub observed_ns: u64,
    /// Connection state; independent of quiet-market update age.
    pub connected: bool,
    /// Accepted complete application messages, including control traffic.
    pub messages: String,
    /// Committed snapshots and events.
    pub applied: String,
    /// Rejected messages.
    pub rejected: String,
    /// Most recent continuity error, bounded to 512 characters.
    pub last_error: Option<String>,
    /// Original cause and input identity of the most recent rejected message.
    /// Retained across reconnects, independently of the transport recovery reason.
    pub last_rejection: Option<RejectedInput>,
    /// Selected markets in identifier order.
    pub markets: Vec<MarketView>,
    /// Bounded sampled processing diagnostics.
    pub latency: TraceWindow,
}
/// One bounded rejection diagnostic, never a current-book health assertion.
#[derive(Clone, Debug, Serialize)]
pub struct RejectedInput {
    /// Connection generation and complete-message index.
    pub id: TraceId,
    /// Original monotonic observation offset in the server session.
    pub observed_ns: u64,
    /// Decoded venue market identifier, absent for malformed input or controls.
    pub market: Option<String>,
    /// Decoded sequence, absent when the input does not provide one.
    pub sequence: Option<String>,
    /// Original rejection cause, limited to 512 Unicode characters.
    pub reason: String,
}
/// One synchronized ladder level, formatted using metadata.
#[derive(Clone, Debug, Serialize)]
pub struct LevelView {
    /// Exact ticks.
    pub ticks: String,
    /// Quote tokens per base token.
    pub price: String,
    /// Base tokens.
    pub quantity: String,
    /// Exact native base units.
    pub native_quantity: String,
}
/// Size-based represented-depth execution estimate.
#[derive(Clone, Debug, Serialize)]
pub struct EstimateView {
    /// Buy or sell direction.
    pub side: &'static str,
    /// Requested native quantity.
    pub requested: String,
    /// Filled native quantity.
    pub filled: String,
    /// Explicit native-quantity shortfall.
    pub shortfall: String,
    /// Weighted quote price, unavailable for an empty side.
    pub average_price: Option<String>,
}
/// Health, prices, and liquidity for one selected market.
#[derive(Clone, Debug, Serialize)]
pub struct MarketView {
    /// Exact venue identifier.
    pub id: String,
    /// Metadata market label.
    pub name: String,
    /// Live, awaiting snapshot, or invalid.
    pub status: &'static str,
    /// Connection generation.
    pub generation: String,
    /// Last committed sequence, retained even when invalid.
    pub sequence: Option<String>,
    /// Last accepted update age, separate from heartbeat health.
    pub update_age_ns: Option<u64>,
    /// Latest committed input identity; its trace may be unsampled or expired.
    pub trace_id: Option<TraceId>,
    /// Spread in quote tokens, absent if invalid or one-sided.
    pub spread: Option<String>,
    /// Sorted represented ask levels, truncated only for display.
    pub asks: Vec<LevelView>,
    /// Sorted represented bid levels, truncated only for display.
    pub bids: Vec<LevelView>,
    /// Full represented ask level count.
    pub ask_levels: usize,
    /// Full represented bid level count.
    pub bid_levels: usize,
    /// Exact total represented ask quantity in native units, or `unavailable`.
    pub ask_quantity: String,
    /// Exact total represented bid quantity in native units, or `unavailable`.
    pub bid_quantity: String,
    /// Time-weighted coverage and liquidity.
    pub coverage: Coverage,
    /// Two directions for the requested size.
    pub estimates: Vec<EstimateView>,
}
impl Pipeline {
    /// Render an immutable report, preserving the recorded input clock.
    /// Live callers extract a snapshot and render it on the report worker.
    pub fn view(&self, mode: &str, now_ns: u64, depth: usize, requested: Quantity) -> View {
        self.snapshot(now_ns, depth, requested).render(mode)
    }
}
impl Snapshot {
    /// Format an immutable numeric snapshot and calculate its bounded percentiles.
    pub fn render(self, mode: &str) -> View {
        let markets = self
            .markets
            .into_iter()
            .map(|market| {
                let units = &market.units;
                let convert = |levels: Vec<pax_types::protocol::snapshot::Level>| {
                    levels
                        .into_iter()
                        .map(|level| LevelView {
                            ticks: level.ticks.to_string(),
                            price: units
                                .price(level.ticks)
                                .and_then(|v| v.decimal(12))
                                .unwrap_or_else(|_| "overflow".into()),
                            quantity: units
                                .quantity(level.qty)
                                .and_then(|v| v.decimal(12))
                                .unwrap_or_else(|_| "overflow".into()),
                            native_quantity: level.qty.to_string(),
                        })
                        .collect()
                };
                let estimates = market
                    .estimates
                    .into_iter()
                    .enumerate()
                    .filter_map(|(index, fill)| {
                        let fill = fill?;
                        Some(EstimateView {
                            side: if index == 0 { "buy" } else { "sell" },
                            requested: fill.requested.to_string(),
                            filled: fill.filled.to_string(),
                            shortfall: fill.shortfall.to_string(),
                            average_price: average_price(fill.quantity_ticks, fill.filled.0, units),
                        })
                    })
                    .collect();
                MarketView {
                    id: market.id.to_string(),
                    name: units.market().market_name.clone(),
                    status: market.status,
                    generation: market.generation.to_string(),
                    sequence: market.sequence.map(|v| v.to_string()),
                    update_age_ns: market.update_age_ns,
                    trace_id: market.trace_id,
                    spread: market
                        .spread
                        .and_then(|v| units.price(v).ok()?.decimal(12).ok()),
                    asks: convert(market.asks),
                    bids: convert(market.bids),
                    ask_levels: market.ask_levels,
                    bid_levels: market.bid_levels,
                    ask_quantity: market
                        .ask_quantity
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "unavailable".into()),
                    bid_quantity: market
                        .bid_quantity
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "unavailable".into()),
                    coverage: market.weighted.report(self.observed_ns),
                    estimates,
                }
            })
            .collect();
        View {
            mode: mode.into(),
            observed_ns: self.observed_ns,
            connected: self.connected,
            messages: self.messages.to_string(),
            applied: self.applied.to_string(),
            rejected: self.rejected.to_string(),
            last_error: self.last_error,
            last_rejection: self.last_rejection,
            markets,
            latency: self.latency.summarize(),
        }
    }
}

fn average_price(quantity_ticks: u128, filled: u64, units: &MarketUnits) -> Option<String> {
    Ratio::new(
        quantity_ticks.checked_mul(u128::from(units.market().tick_size.0))?,
        u128::from(filled) * u128::from(units.quote_units()),
    )
    .ok()?
    .decimal(12)
    .ok()
}

impl View {
    /// Suppress trading quotes when independent transport health has already
    /// detected a failure or newer generation that ordered input has not reached.
    /// Canonical state and recorded accounting remain owned by the input pipeline.
    pub fn qualify_transport(&mut self, connected: bool, generation: u64) {
        let generation = generation.to_string();
        self.connected &= connected;
        for market in &mut self.markets {
            if !connected || market.generation != generation {
                market.status = "transport unavailable";
                market.spread = None;
                market.asks.clear();
                market.bids.clear();
                market.estimates.clear();
                market.ask_levels = 0;
                market.bid_levels = 0;
                market.ask_quantity = "unavailable".into();
                market.bid_quantity = "unavailable".into();
            }
        }
    }
}
