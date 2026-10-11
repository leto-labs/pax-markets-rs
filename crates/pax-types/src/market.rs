//! Metadata-defined exact conversions. No floating point is used.
//!
//! Source: <https://docs.pax.markets/data-formats/>.

use crate::units::{AssetId, MarketId, PriceTicks, Quantity};
use serde::{Deserialize, Serialize};

/// Exact nonnegative ratio. Decimal output explicitly truncates toward zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Ratio {
    numerator: u128,
    denominator: u128,
}

/// Invalid metadata or an exact arithmetic result outside the supported range.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MetadataError {
    /// Missing or inconsistent reference/units.
    #[error("invalid metadata: {0}")]
    Invalid(&'static str),
    /// Checked arithmetic failed.
    #[error("exact arithmetic overflow")]
    Overflow,
}

impl Ratio {
    /// Construct a ratio; a zero denominator is rejected.
    pub fn new(numerator: u128, denominator: u128) -> Result<Self, MetadataError> {
        if denominator == 0 {
            return Err(MetadataError::Invalid("zero denominator"));
        }
        Ok(Self {
            numerator,
            denominator,
        })
    }
    /// Unrounded numerator.
    pub fn numerator(self) -> u128 {
        self.numerator
    }
    /// Nonzero denominator.
    pub fn denominator(self) -> u128 {
        self.denominator
    }
    /// Format with at most 18 fractional digits, truncating toward zero.
    /// Trailing zeroes are removed. Overflow is explicit, never wrapped.
    pub fn decimal(self, digits: usize) -> Result<String, MetadataError> {
        if digits > 18 {
            return Err(MetadataError::Invalid("at most 18 decimal digits"));
        }
        let mut output = (self.numerator / self.denominator).to_string();
        let mut remainder = self.numerator % self.denominator;
        if digits > 0 && remainder != 0 {
            output.push('.');
            for _ in 0..digits {
                remainder = remainder.checked_mul(10).ok_or(MetadataError::Overflow)?;
                output.push(char::from(b'0' + (remainder / self.denominator) as u8));
                remainder %= self.denominator;
                if remainder == 0 {
                    break;
                }
            }
            while output.ends_with('0') {
                output.pop();
            }
            if output.ends_with('.') {
                output.pop();
            }
        }
        Ok(output)
    }
}

/// Asset units returned by the public exchange-info endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Asset {
    /// Venue identifier.
    pub asset_id: AssetId,
    /// Display name.
    pub asset_name: String,
    /// Label for the native unit.
    pub native_unit_name: String,
    /// Native units per whole token.
    pub num_native_units: Quantity,
}

/// Market parameters returned by public exchange-info.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Market {
    /// Venue identifier.
    pub market_id: MarketId,
    /// Display name; never interpreted as a hard-coded symbol mapping.
    pub market_name: String,
    /// Base asset reference.
    pub base_asset_id: AssetId,
    /// Quote asset reference.
    pub quote_asset_id: AssetId,
    /// Quote native units per price tick.
    pub tick_size: Quantity,
    /// Base native units per allowed quantity step.
    pub qty_step_size: Quantity,
    /// Maximum accepted order quantity.
    pub max_qty: Quantity,
    /// Maximum accepted limit ticks.
    pub max_ticks: PriceTicks,
    /// Minimum order notional in quote native units.
    pub min_notional: Quantity,
}

/// Public metadata for the queried environment.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ExchangeInfo {
    /// Assets and exact native units.
    pub assets: Vec<Asset>,
    /// Markets enabled in this environment.
    pub markets: Vec<Market>,
}

impl ExchangeInfo {
    /// Validate all references, identifiers, bounds, and unit factors.
    pub fn validate(&self) -> Result<(), MetadataError> {
        let mut assets = std::collections::HashSet::new();
        for asset in &self.assets {
            if asset.asset_id.0 == 0
                || asset.num_native_units.0 == 0
                || !assets.insert(asset.asset_id)
            {
                return Err(MetadataError::Invalid(
                    "duplicate/zero asset identifier or units",
                ));
            }
        }
        let mut markets = std::collections::HashSet::new();
        for market in &self.markets {
            if market.market_id.0 == 0 || !markets.insert(market.market_id) {
                return Err(MetadataError::Invalid("duplicate/zero market identifier"));
            }
            if !assets.contains(&market.base_asset_id) || !assets.contains(&market.quote_asset_id) {
                return Err(MetadataError::Invalid("missing asset reference"));
            }
            if market.base_asset_id == market.quote_asset_id
                || market.tick_size.0 == 0
                || market.qty_step_size.0 == 0
                || market.max_qty.0 < market.qty_step_size.0
                || market.max_ticks.0 == 0
            {
                return Err(MetadataError::Invalid(
                    "invalid market unit factors or bounds",
                ));
            }
        }
        Ok(())
    }

    /// Resolve one market after validating the entire metadata response.
    pub fn resolve(&self, id: MarketId) -> Result<MarketUnits, MetadataError> {
        self.validate()?;
        let market = self
            .markets
            .iter()
            .find(|m| m.market_id == id)
            .ok_or(MetadataError::Invalid("unknown market"))?;
        let base = self
            .assets
            .iter()
            .find(|a| a.asset_id == market.base_asset_id)
            .unwrap();
        let quote = self
            .assets
            .iter()
            .find(|a| a.asset_id == market.quote_asset_id)
            .unwrap();
        Ok(MarketUnits {
            market: market.clone(),
            base_units: base.num_native_units.0,
            quote_units: quote.num_native_units.0,
        })
    }
}

/// Validated market with resolved native units.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketUnits {
    market: Market,
    base_units: u64,
    quote_units: u64,
}

impl MarketUnits {
    /// Market parameters.
    pub fn market(&self) -> &Market {
        &self.market
    }
    /// Number of base native units per token.
    pub fn base_units(&self) -> u64 {
        self.base_units
    }
    /// Number of quote native units per token.
    pub fn quote_units(&self) -> u64 {
        self.quote_units
    }
    /// Convert ticks into quote tokens per base token.
    pub fn price(&self, ticks: PriceTicks) -> Result<Ratio, MetadataError> {
        Ratio::new(
            u128::from(ticks.0) * u128::from(self.market.tick_size.0),
            u128::from(self.quote_units),
        )
    }
    /// Convert quantity into base tokens.
    pub fn quantity(&self, qty: Quantity) -> Result<Ratio, MetadataError> {
        Ratio::new(u128::from(qty.0), u128::from(self.base_units))
    }
    /// Convert quantity-times-ticks into quote native units without rounding.
    pub fn notional(&self, qty_ticks: u128) -> Result<Ratio, MetadataError> {
        Ratio::new(
            qty_ticks
                .checked_mul(u128::from(self.market.tick_size.0))
                .ok_or(MetadataError::Overflow)?,
            u128::from(self.base_units),
        )
    }
}
