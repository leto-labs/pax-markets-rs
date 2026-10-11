//! Public subscription validation and serialization.
use crate::metadata::ClientError;
use pax_types::units::MarketId;
use serde::Serialize;

#[derive(Serialize)]
struct Subscribe<'a> {
    action: &'static str,
    market_ids: &'a [MarketId],
}

pub(crate) fn request(markets: &[MarketId]) -> Result<String, ClientError> {
    if markets.is_empty() || markets.iter().any(|id| id.0 == 0) {
        return Err(ClientError::Config("select at least one nonzero market"));
    }
    let mut unique = markets.to_vec();
    unique.sort_unstable();
    unique.dedup();
    if unique.len() != markets.len() {
        return Err(ClientError::Config("duplicate selected market"));
    }
    let text = serde_json::to_string(&Subscribe {
        action: "subscribe",
        market_ids: markets,
    })?;
    if text.len() > 16 * 1024 {
        return Err(ClientError::Config(
            "subscription exceeds venue message bound",
        ));
    }
    Ok(text)
}

pub(crate) fn acknowledged(expected: &[MarketId], actual: &[MarketId]) -> bool {
    expected.len() == actual.len() && expected.iter().all(|market| actual.contains(market))
}
