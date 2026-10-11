//! Decode known type-first payloads directly. Preserve an order-independent raw
//! fallback when `data` precedes `type`; never assume JSON member ordering.
use super::{event::Event, snapshot::Snapshot};
use crate::units::MarketId;
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use serde_json::value::RawValue;
use std::{borrow::Cow, fmt};

pub(super) enum Data<'a> {
    Snapshot(Snapshot),
    Event(Event),
    Raw(&'a RawValue),
}
pub(super) struct Envelope<'a> {
    pub kind: Cow<'a, str>,
    pub data: Option<Data<'a>>,
    pub market_ids: Option<Vec<MarketId>>,
    pub message: Option<String>,
    pub error: Option<String>,
}
#[derive(Deserialize)]
struct Kind<'a>(#[serde(borrow)] Cow<'a, str>);
#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum Field {
    #[serde(rename = "type")]
    Kind,
    Data,
    MarketIds,
    Message,
    Error,
    #[serde(other)]
    Other,
}
impl<'de> Deserialize<'de> for Envelope<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct EnvelopeVisitor;
        impl<'de> Visitor<'de> for EnvelopeVisitor {
            type Value = Envelope<'de>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a public message envelope")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut kind: Option<Cow<'de, str>> = None;
                let mut data = None;
                let mut market_ids = None;
                let mut message = None;
                let mut error = None;
                let mut seen = 0u8;
                while let Some(field) = map.next_key::<Field>()? {
                    let (bit, name) = match field {
                        Field::Kind => (1, "type"),
                        Field::Data => (2, "data"),
                        Field::MarketIds => (4, "market_ids"),
                        Field::Message => (8, "message"),
                        Field::Error => (16, "error"),
                        Field::Other => (0, ""),
                    };
                    if seen & bit != 0 {
                        return Err(de::Error::duplicate_field(name));
                    }
                    seen |= bit;
                    match field {
                        Field::Kind => kind = Some(map.next_value::<Kind<'de>>()?.0),
                        Field::Data => {
                            data = match kind.as_deref() {
                                Some("snapshot") => {
                                    map.next_value::<Option<Snapshot>>()?.map(Data::Snapshot)
                                }
                                Some("md_event_detail") => {
                                    map.next_value::<Option<Event>>()?.map(Data::Event)
                                }
                                _ => map.next_value::<Option<&'de RawValue>>()?.map(Data::Raw),
                            }
                        }
                        Field::MarketIds => market_ids = map.next_value()?,
                        Field::Message => message = map.next_value()?,
                        Field::Error => error = map.next_value()?,
                        Field::Other => {
                            map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Envelope {
                    kind: kind.ok_or_else(|| de::Error::missing_field("type"))?,
                    data,
                    market_ids,
                    message,
                    error,
                })
            }
        }
        deserializer.deserialize_map(EnvelopeVisitor)
    }
}
