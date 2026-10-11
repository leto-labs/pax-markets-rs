//! Aggregated snapshot levels from the public channel.

use crate::units::{MarketId, PriceTicks, Quantity, Sequence};
use serde::{Deserialize, Serialize};

/// One aggregated level, not an individual order.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Level {
    /// Price in market ticks.
    pub ticks: PriceTicks,
    /// Aggregated base quantity.
    pub qty: Quantity,
}

/// A snapshot and the event sequence it includes.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Snapshot {
    /// Owning market.
    pub market_id: MarketId,
    /// Asks, cheapest first.
    pub ask_agg_data: Vec<Level>,
    /// Bids, highest first.
    pub bid_agg_data: Vec<Level>,
    /// Last included market event.
    pub latest_seq_num: Sequence,
}
