//! Exact JSON integer units. Numeric tokens and integer strings are accepted.
//!
//! Prices and quantities cannot be substituted accidentally:
//! ```compile_fail
//! use pax_types::units::{PriceTicks, Quantity};
//! let price: PriceTicks = Quantity(100);
//! ```

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use std::{borrow::Cow, fmt};

// RawValue preserves large JSON integer tokens without floating point or an
// object tree. Common unescaped integer strings borrow their original bytes.
// https://docs.rs/serde_json/latest/serde_json/value/struct.RawValue.html
fn integer_text(raw: &str) -> Result<Cow<'_, str>, &'static str> {
    let text = match raw.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        Some(s) if s.contains('\\') => {
            Cow::Owned(serde_json::from_str::<String>(raw).map_err(|_| "invalid integer string")?)
        }
        Some(s) => Cow::Borrowed(s),
        None => Cow::Borrowed(raw),
    };
    // FromStr checks decimal syntax and range in the conversion pass. It also
    // accepts a leading '+', which the wire contract deliberately excludes.
    // https://doc.rust-lang.org/std/primitive.i128.html#impl-FromStr-for-i128
    if text.starts_with('+') {
        return Err("explicit positive sign is not allowed");
    }
    Ok(text)
}

macro_rules! integer_unit {
    ($name:ident, $repr:ty, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(pub $repr);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

integer_unit!(AssetId, u64, "An asset identifier assigned by Pax.");
integer_unit!(MarketId, u64, "A market identifier assigned by Pax.");
integer_unit!(
    Sequence,
    u64,
    "A sequence scoped to one market and connection generation."
);
integer_unit!(PriceTicks, u32, "A price in integer market ticks.");
integer_unit!(Quantity, u64, "A quantity in base asset native units.");
integer_unit!(
    Signed,
    i128,
    "An exact signed event quantity or quantity-times-ticks value."
);

#[cfg(test)]
mod tests;

// Unsigned JSON integers fit serde_json's exact u64 path. A visitor avoids
// scanning each token as RawValue and then parsing that same token a second time.
macro_rules! unsigned_deserialize {
    ($name:ident, $repr:ty) => {
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct IntegerVisitor;
                impl serde::de::Visitor<'_> for IntegerVisitor {
                    type Value = $name;
                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str("an unsigned integer number or integer string")
                    }
                    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
                        <$repr>::try_from(value).map($name).map_err(E::custom)
                    }
                    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                        // Parsing already validates all digits and overflow;
                        // reject only the extra sign accepted by FromStr.
                        if value.starts_with('+') {
                            return Err(E::custom("explicit positive sign is not allowed"));
                        }
                        value.parse::<$repr>().map($name).map_err(E::custom)
                    }
                }
                deserializer.deserialize_any(IntegerVisitor)
            }
        }
    };
}
unsigned_deserialize!(AssetId, u64);
unsigned_deserialize!(MarketId, u64);
unsigned_deserialize!(Sequence, u64);
unsigned_deserialize!(PriceTicks, u32);
unsigned_deserialize!(Quantity, u64);

impl<'de> Deserialize<'de> for Signed {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = <&RawValue>::deserialize(deserializer)?;
        let text = integer_text(raw.get()).map_err(serde::de::Error::custom)?;
        text.parse::<i128>()
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}
