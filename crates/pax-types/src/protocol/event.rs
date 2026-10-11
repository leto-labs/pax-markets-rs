//! Public event contracts; all state-changing fields are required.
//!
//! Source: <https://docs.pax.markets/api-reference/websocket-messages/>.

use crate::units::{MarketId, PriceTicks, Quantity, Sequence, Signed};
use serde::{Deserialize, Serialize};

/// Side of an aggregated ladder.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(try_from = "u8", into = "u8")]
pub enum Side {
    /// Sell liquidity.
    Ask = 1,
    /// Buy liquidity.
    Bid = 2,
}

impl Side {
    /// Return the opposite ladder.
    pub fn opposite(self) -> Self {
        match self {
            Self::Ask => Self::Bid,
            Self::Bid => Self::Ask,
        }
    }
}
impl From<Side> for u8 {
    fn from(side: Side) -> Self {
        side as u8
    }
}
impl TryFrom<u8> for Side {
    type Error = &'static str;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Ask),
            2 => Ok(Self::Bid),
            _ => Err("unsupported side"),
        }
    }
}

/// State-changing event classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(try_from = "u8", into = "u8")]
pub enum EventType {
    /// Resting liquidity added.
    Add = 1,
    /// Liquidity canceled or executed.
    Remove = 2,
    /// Immediate order found no liquidity.
    Miss = 3,
}
impl From<EventType> for u8 {
    fn from(kind: EventType) -> Self {
        kind as u8
    }
}
impl TryFrom<u8> for EventType {
    type Error = &'static str;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Add),
            2 => Ok(Self::Remove),
            3 => Ok(Self::Miss),
            _ => Err("unsupported event type"),
        }
    }
}

/// Post-event best prices and quantities. Empty sides require both fields zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct TopOfBook {
    /// Best ask quantity; zero for an empty side.
    pub ask_qty: Quantity,
    /// Best bid quantity; zero for an empty side.
    pub bid_qty: Quantity,
    /// Best ask ticks.
    pub ask_ticks: PriceTicks,
    /// Best bid ticks.
    pub bid_ticks: PriceTicks,
}

/// Facts describing the deepest touched level and aggregate event effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct EventData {
    /// Addition, removal or immediate miss.
    pub event_type: EventType,
    /// 2 = execution, 3 = cancel, 4 = execute-and-rest; inapplicable otherwise.
    pub event_qual: u8,
    /// Maker side for executions; modified side otherwise.
    pub event_side: Side,
    /// Signed total quantity change.
    pub event_delta_qty: Signed,
    /// Deepest touched price.
    pub event_abs_ticks: PriceTicks,
    /// Tick distance from previous best, clamped to zero for price improvement.
    pub event_tick_depth: Signed,
    /// Zero-based level index.
    pub event_level_depth: Quantity,
    /// Pre-event quantity on the deepest level.
    pub event_level_begin_qty: Quantity,
    /// Signed change at that level; negative final quantity changes sides.
    pub event_level_delta_qty: Signed,
    /// Quantity-times-ticks, with special XA semantics.
    pub event_total_px: Signed,
    /// Contiguous per-market event sequence.
    pub seq_num: Sequence,
}

/// Public description of the triggering order, without account attribution.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Order {
    /// Triggering order side.
    pub side: Side,
    /// Owning market.
    pub market_id: MarketId,
    /// Trigger quantity; cancellations are negative in the public feed.
    pub qty: Signed,
    /// Limit ticks.
    pub ticks: PriceTicks,
    /// 1 = immediate-or-cancel, 2 = good-until-canceled.
    pub tif: u8,
    /// 1 = external, 2 = lambda, 3 = admin.
    pub origin: u8,
}

/// One complete public market event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Event {
    /// Post-event best prices and quantities.
    pub tob: TopOfBook,
    /// Event transition facts.
    pub data: EventData,
    /// Triggering order.
    pub order: Order,
}

impl Event {
    /// Check enum combinations before exposing an applicable event.
    pub fn validate_protocol(&self) -> Result<(), &'static str> {
        if self.data.event_type == EventType::Remove && !matches!(self.data.event_qual, 2..=4) {
            return Err("unsupported removal qualifier");
        }
        if !matches!(self.order.tif, 1 | 2) || !matches!(self.order.origin, 1..=3) {
            return Err("unsupported order time-in-force or origin");
        }
        Ok(())
    }
}
