//! Exact displayed-depth estimates, qualified by synchronized book state.

use crate::state::{Book, BookError};
use pax_types::{
    market::{MetadataError, Ratio},
    protocol::event::Side,
    units::{PriceTicks, Quantity, Sequence},
};

/// An estimate from represented depth; not a guarantee of fills or queue position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FillEstimate {
    /// Session generation used.
    pub generation: u64,
    /// Sequence used.
    pub sequence: Sequence,
    /// Requested base quantity.
    pub requested: Quantity,
    /// Quantity available in represented depth.
    pub filled: Quantity,
    /// Unfilled quantity; unknown deeper liquidity is not invented.
    pub shortfall: Quantity,
    /// Exact sum of filled quantity times price ticks.
    pub quantity_ticks: u128,
    /// Last represented price used, absent for an empty side.
    pub worst_ticks: Option<PriceTicks>,
    /// Number of levels consumed.
    pub levels: usize,
}

impl FillEstimate {
    /// Quantity-weighted average in tick units, unavailable for zero fills.
    pub fn average_ticks(&self) -> Result<Option<Ratio>, MetadataError> {
        if self.filled.0 == 0 {
            return Ok(None);
        }
        Ratio::new(self.quantity_ticks, u128::from(self.filled.0)).map(Some)
    }
}

/// One cumulative depth point.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Depth {
    /// Level price.
    pub ticks: PriceTicks,
    /// Quantity at this level.
    pub quantity: Quantity,
    /// Cumulative quantity, including this level.
    pub cumulative_quantity: u128,
    /// Cumulative quantity-times-ticks, including this level.
    pub cumulative_quantity_ticks: u128,
}

impl Book {
    /// Best ask minus best bid, unavailable when either side is empty.
    pub fn spread(&self) -> Result<Option<PriceTicks>, BookError> {
        self.require_live()?;
        match (self.asks.first(), self.bids.first()) {
            (Some(ask), Some(bid)) => Ok(Some(PriceTicks(ask.ticks.0 - bid.ticks.0))),
            _ => Ok(None),
        }
    }
    /// Estimate a buy (`Bid`) or sell (`Ask`) against represented opposite depth.
    pub fn estimate(
        &self,
        order_side: Side,
        requested: Quantity,
    ) -> Result<FillEstimate, BookError> {
        self.require_live()?;
        if requested.0 == 0 {
            return Err(BookError::Inconsistent("zero requested quantity"));
        }
        let mut remaining = requested.0;
        let mut value = 0u128;
        let mut levels = 0;
        let mut worst_ticks = None;
        for level in self.ladder(order_side.opposite()) {
            let fill = remaining.min(level.qty.0);
            value = value
                .checked_add(u128::from(fill) * u128::from(level.ticks.0))
                .ok_or(BookError::Overflow)?;
            remaining -= fill;
            levels += 1;
            worst_ticks = Some(level.ticks);
            if remaining == 0 {
                break;
            }
        }
        Ok(FillEstimate {
            generation: self.generation,
            sequence: self.sequence.unwrap(),
            requested,
            filled: Quantity(requested.0 - remaining),
            shortfall: Quantity(remaining),
            quantity_ticks: value,
            worst_ticks,
            levels,
        })
    }
    /// Build cumulative represented depth for reporting, outside event application.
    pub fn depth(&self, side: Side) -> Result<Vec<Depth>, BookError> {
        self.require_live()?;
        let mut qty = 0u128;
        let mut value = 0u128;
        self.ladder(side)
            .iter()
            .map(|level| {
                qty = qty
                    .checked_add(u128::from(level.qty.0))
                    .ok_or(BookError::Overflow)?;
                value = value
                    .checked_add(u128::from(level.qty.0) * u128::from(level.ticks.0))
                    .ok_or(BookError::Overflow)?;
                Ok(Depth {
                    ticks: level.ticks,
                    quantity: level.qty,
                    cumulative_quantity: qty,
                    cumulative_quantity_ticks: value,
                })
            })
            .collect()
    }
}
