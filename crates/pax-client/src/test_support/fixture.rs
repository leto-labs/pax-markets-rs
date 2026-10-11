//! Long deterministic transport inputs with monotonic event sequences.
use bytes::Bytes;
use pax_types::{
    protocol::{
        self, Message,
        event::{EventType, Side, TopOfBook},
        snapshot::{Level, Snapshot},
    },
    units::{MarketId, PriceTicks, Quantity, Sequence, Signed},
};

/// Prepared wire bytes and an independently specified final ladder.
pub struct Fixture {
    /// Human-readable workload identity; synthetic inputs are labeled.
    pub name: String,
    /// Ordered subscription acknowledgement, snapshot and event messages.
    pub messages: Vec<Bytes>,
    /// Final state, specified without applying the event algorithm under test.
    pub expected: Snapshot,
    /// Number of state-changing event messages.
    pub events: usize,
}
impl Fixture {
    /// Exact length-delimited wire-input digest. Preparation is outside timing.
    pub fn digest(&self) -> String {
        let mut hash = blake3::Hasher::new();
        for bytes in &self.messages {
            hash.update(&(bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
        }
        hash.finalize().to_hex().to_string()
    }
    /// Alternate a deep-level addition and its exact cancellation, using unique
    /// increasing sequences. Supports odd lengths with a known final +1 quantity.
    pub fn synthetic(events: usize, depth: u32) -> Result<Self, &'static str> {
        if !(1..=1_000_000).contains(&events) || !(2..=100_000).contains(&depth) {
            return Err("synthetic fixture bounds");
        }
        let snapshot = Snapshot {
            market_id: MarketId(1),
            latest_seq_num: Sequence(0),
            ask_agg_data: (0..depth)
                .map(|i| Level {
                    ticks: PriceTicks(1_100_010 + i * 2),
                    qty: Quantity(1000),
                })
                .collect(),
            bid_agg_data: (0..depth)
                .map(|i| Level {
                    ticks: PriceTicks(1_000_000 - i * 2),
                    qty: Quantity(1000),
                })
                .collect(),
        };
        let price = snapshot.ask_agg_data.last().unwrap().ticks;
        let Message::Event(mut template) =
            protocol::decode(include_bytes!("../../../pax-types/tests/fixtures/xa.json"))
                .map_err(|_| "event template")?
        else {
            return Err("event template kind");
        };
        template.tob = TopOfBook {
            ask_ticks: snapshot.ask_agg_data[0].ticks,
            ask_qty: Quantity(1000),
            bid_ticks: snapshot.bid_agg_data[0].ticks,
            bid_qty: Quantity(1000),
        };
        template.data.event_side = Side::Ask;
        template.data.event_abs_ticks = price;
        template.data.event_tick_depth = Signed(i128::from((depth - 1) * 2));
        template.data.event_level_depth = Quantity(u64::from(depth - 1));
        template.order.side = Side::Ask;
        template.order.ticks = price;
        let mut messages = Vec::with_capacity(events + 2);
        messages.push(Bytes::from_static(
            br#"{"type":"subscribed","market_ids":[1]}"#,
        ));
        messages.push(Bytes::from(format!(
            r#"{{"type":"snapshot","data":{}}}"#,
            serde_json::to_string(&snapshot).map_err(|_| "snapshot serialization")?
        )));
        for index in 0..events {
            let add = index % 2 == 0;
            let delta = if add { 1 } else { -1 };
            let mut event = template;
            event.data.seq_num = Sequence(index as u64 + 1);
            event.data.event_type = if add {
                EventType::Add
            } else {
                EventType::Remove
            };
            event.data.event_qual = if add { 0 } else { 3 };
            event.data.event_delta_qty = Signed(delta);
            event.data.event_level_begin_qty = Quantity(if add { 1000 } else { 1001 });
            event.data.event_level_delta_qty = Signed(delta);
            event.data.event_total_px = Signed(delta * i128::from(price.0));
            event.order.qty = Signed(delta);
            messages.push(Bytes::from(format!(
                r#"{{"type":"md_event_detail","data":{}}}"#,
                serde_json::to_string(&event).map_err(|_| "event serialization")?
            )));
        }
        let mut expected = snapshot;
        expected.latest_seq_num = Sequence(events as u64);
        expected.ask_agg_data.last_mut().unwrap().qty.0 += (events % 2) as u64;
        Ok(Self {
            name: format!("synthetic-monotonic-add-cancel-depth{depth}"),
            messages,
            expected,
            events,
        })
    }
}
