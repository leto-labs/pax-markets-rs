//! Retained public data, documented execute-and-rest, and labeled synthetic depths.
use pax_book::state::Book;
use pax_types::{
    protocol::{
        self, Message,
        event::{Event, EventType, Side},
        snapshot::{Level, Snapshot},
    },
    units::{MarketId, PriceTicks, Quantity, Sequence, Signed},
};
pub struct Workload {
    pub name: String,
    pub snapshot: Snapshot,
    pub raw_snapshot: Vec<u8>,
    pub raw: Vec<Vec<u8>>,
    pub events: Vec<Event>,
    reference: Snapshot,
}
impl Workload {
    // Even a decode-only filtered run must verify canonical reconstruction once
    // against the independent final snapshot, outside all timing intervals.
    pub fn check_decoded(&self, expected: &Book) {
        let mut book = Book::new(MarketId(1), 1024, 100_000).unwrap();
        book.snapshot(0, self.snapshot.clone()).unwrap();
        for raw in &self.raw {
            let Message::Event(event) = protocol::decode(raw).unwrap() else {
                panic!("event workload");
            };
            book.event(0, &event).unwrap();
        }
        assert_eq!(digest(&book), digest(expected));
    }
    pub fn expected(&self) -> Book {
        let mut book = Book::new(MarketId(1), 1024, 100_000).unwrap();
        book.snapshot(0, self.reference.clone()).unwrap();
        book
    }
}
pub fn digest(book: &Book) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(&book.sequence().unwrap().0.to_le_bytes());
    for side in [Side::Ask, Side::Bid] {
        let levels = book.levels(side).unwrap();
        hash.update(&(levels.len() as u64).to_le_bytes());
        for level in levels {
            hash.update(&level.ticks.0.to_le_bytes());
            hash.update(&level.qty.0.to_le_bytes());
        }
    }
    hash.finalize().to_hex().to_string()
}
fn retained(name: &str, input: &[u8], reference: &[u8]) -> Workload {
    let mut snapshot = None;
    let mut raw_snapshot = None;
    let mut raw = Vec::new();
    let mut events = Vec::new();
    for line in input.split(|b| *b == b'\n').filter(|line| !line.is_empty()) {
        match protocol::decode(line).unwrap() {
            Message::Snapshot(value) => {
                snapshot = Some(value);
                raw_snapshot = Some(line.to_vec());
            }
            Message::Event(value) => {
                raw.push(line.to_vec());
                events.push(value);
            }
            _ => {}
        }
    }
    Workload {
        name: name.into(),
        snapshot: snapshot.unwrap(),
        raw_snapshot: raw_snapshot.unwrap(),
        raw,
        events,
        reference: reference_snapshot(reference),
    }
}
fn reference_snapshot(bytes: &[u8]) -> Snapshot {
    let Message::Snapshot(snapshot) = protocol::decode(bytes).unwrap() else {
        panic!("expected reference snapshot")
    };
    snapshot
}
pub fn all() -> Vec<Workload> {
    let Message::Snapshot(documented) = protocol::decode(include_bytes!(
        "../../../pax-types/tests/fixtures/snapshot.json"
    ))
    .unwrap() else {
        panic!()
    };
    let xa = include_bytes!("../../../pax-types/tests/fixtures/xa.json");
    let Message::Event(event) = protocol::decode(xa).unwrap() else {
        panic!()
    };
    let mut result = vec![
        retained(
            "public-2026-10-07-depth20",
            include_bytes!("../../../pax-book/tests/fixtures/public-sample.jsonl"),
            include_bytes!("../../../pax-book/tests/fixtures/public-sample-final.json"),
        ),
        retained(
            "public-active-2026-10-07-depth20",
            include_bytes!("../../../pax-book/tests/fixtures/public-active.jsonl"),
            include_bytes!("../../../pax-book/tests/fixtures/public-active-final.json"),
        ),
        Workload {
            name: "documented-XA".into(),
            snapshot: documented,
            raw_snapshot: include_bytes!("../../../pax-types/tests/fixtures/snapshot.json")
                .to_vec(),
            raw: vec![xa.to_vec()],
            events: vec![event],
            reference: reference_snapshot(include_bytes!(
                "../../../pax-types/tests/fixtures/xa-final.json"
            )),
        },
    ];
    // Deliberately alter the public envelope at its final field. Recognition
    // must fall back, while the independent final book reference stays identical.
    // This is derived data, not a venue sample.
    let mut additive = retained(
        "derived-public-late-field-depth20",
        include_bytes!("../../../pax-book/tests/fixtures/public-sample.jsonl"),
        include_bytes!("../../../pax-book/tests/fixtures/public-sample-final.json"),
    );
    for raw in &mut additive.raw {
        assert_eq!(raw.pop(), Some(b'}'));
        raw.extend_from_slice(br#", "future_field":true}"#);
    }
    result.push(additive);
    for depth in [20, 200, 2000] {
        let snapshot = Snapshot {
            market_id: MarketId(1),
            latest_seq_num: Sequence(0),
            ask_agg_data: (0..depth)
                .map(|i| Level {
                    ticks: PriceTicks(100010 + i * 2),
                    qty: Quantity(1000),
                })
                .collect(),
            bid_agg_data: (0..depth)
                .map(|i| Level {
                    ticks: PriceTicks(100000 - i * 2),
                    qty: Quantity(1000),
                })
                .collect(),
        };
        let price = snapshot.ask_agg_data.last().unwrap().ticks;
        let mut add = event;
        add.tob = pax_types::protocol::event::TopOfBook {
            ask_ticks: PriceTicks(100010),
            ask_qty: Quantity(1000),
            bid_ticks: PriceTicks(100000),
            bid_qty: Quantity(1000),
        };
        add.data.event_type = EventType::Add;
        add.data.event_qual = 0;
        add.data.event_side = Side::Ask;
        add.data.event_delta_qty = Signed(1);
        add.data.event_abs_ticks = price;
        add.data.event_tick_depth = Signed(i128::from((depth - 1) * 2));
        add.data.event_level_depth = Quantity(u64::from(depth - 1));
        add.data.event_level_begin_qty = Quantity(1000);
        add.data.event_level_delta_qty = Signed(1);
        add.data.event_total_px = Signed(i128::from(price.0));
        add.data.seq_num = Sequence(1);
        add.order.side = Side::Ask;
        add.order.qty = Signed(1);
        add.order.ticks = price;
        let mut cancel = add;
        cancel.data.event_type = EventType::Remove;
        cancel.data.event_qual = 3;
        cancel.data.event_delta_qty = Signed(-1);
        cancel.data.event_level_begin_qty = Quantity(1001);
        cancel.data.event_level_delta_qty = Signed(-1);
        cancel.data.event_total_px = Signed(-i128::from(price.0));
        cancel.data.seq_num = Sequence(2);
        cancel.order.qty = Signed(-1);
        let events = vec![add, cancel];
        let raw = events
            .iter()
            .map(|e| {
                serde_json::to_vec(&serde_json::json!({"type":"md_event_detail", "data":e}))
                    .unwrap()
            })
            .collect();
        let mut reference = snapshot.clone();
        reference.latest_seq_num = Sequence(2); // Addition followed by exact cancellation.
        result.push(Workload {
            name: format!("synthetic-deep-add-cancel-depth{depth}"),
            raw_snapshot: snapshot_bytes(&snapshot),
            snapshot,
            events,
            raw,
            reference,
        });
        let sweep = wide_sweep(result.last().unwrap(), depth);
        if depth == 20 {
            result.push(wide_integers(result.last().unwrap()));
        }
        result.push(sweep);
    }
    result
}

fn snapshot_bytes(snapshot: &Snapshot) -> Vec<u8> {
    format!(
        "{{\"type\":\"snapshot\",\"data\":{}}}",
        serde_json::to_string(snapshot).unwrap()
    )
    .into_bytes()
}
fn wide_integers(base: &Workload) -> Workload {
    // Use a non-round scale so lossy floating-point decoding cannot accidentally
    // reproduce these quantities or notionals. The exact add/cancel pair still
    // restores the independently specified starting levels.
    let factor = 1_000_000_000_000_003_u64;
    let mut snapshot = base.snapshot.clone();
    let mut reference = base.reference.clone();
    for snapshot in [&mut snapshot, &mut reference] {
        for level in snapshot
            .ask_agg_data
            .iter_mut()
            .chain(&mut snapshot.bid_agg_data)
        {
            level.qty.0 = level.qty.0.checked_mul(factor).unwrap();
        }
    }
    let mut events = base.events.clone();
    for event in &mut events {
        for quantity in [
            &mut event.tob.ask_qty,
            &mut event.tob.bid_qty,
            &mut event.data.event_level_begin_qty,
        ] {
            quantity.0 = quantity.0.checked_mul(factor).unwrap();
        }
        for quantity in [
            &mut event.data.event_delta_qty,
            &mut event.data.event_level_delta_qty,
            &mut event.data.event_total_px,
            &mut event.order.qty,
        ] {
            quantity.0 = quantity.0.checked_mul(i128::from(factor)).unwrap();
        }
        assert!(event.data.event_total_px.0.unsigned_abs() > u128::from(u64::MAX));
    }
    let raw = events
        .iter()
        .map(|event| {
            // Serialize directly: a generic JSON Value cannot preserve every i128.
            let raw = format!(
                r#"{{"type":"md_event_detail","data":{}}}"#,
                serde_json::to_string(event).unwrap()
            )
            .into_bytes();
            assert_eq!(protocol::decode(&raw).unwrap(), Message::Event(*event));
            raw
        })
        .collect();
    Workload {
        name: "synthetic-wide-integers-depth20".into(),
        raw_snapshot: snapshot_bytes(&snapshot),
        snapshot,
        raw,
        events,
        reference,
    }
}

// A complete execution across one side. Every level uses u64-max quantity;
// closed-form sums and an explicitly emptied ask side form the reference.
fn wide_sweep(base: &Workload, depth: u32) -> Workload {
    let mut snapshot = base.snapshot.clone();
    for level in snapshot
        .ask_agg_data
        .iter_mut()
        .chain(&mut snapshot.bid_agg_data)
    {
        level.qty = Quantity(u64::MAX);
    }
    let first = snapshot.ask_agg_data.first().unwrap().ticks.0;
    let last = snapshot.ask_agg_data.last().unwrap().ticks.0;
    let price_sum = i128::from(depth) * (i128::from(first) + i128::from(last)) / 2;
    let total_quantity = i128::from(depth) * i128::from(u64::MAX);
    let mut event = base.events[0];
    event.tob.ask_ticks = PriceTicks(0);
    event.tob.ask_qty = Quantity(0);
    event.tob.bid_qty = Quantity(u64::MAX);
    event.data.event_type = EventType::Remove;
    event.data.event_qual = 2;
    event.data.event_delta_qty = Signed(-total_quantity);
    event.data.event_level_begin_qty = Quantity(u64::MAX);
    event.data.event_level_delta_qty = Signed(-i128::from(u64::MAX));
    event.data.event_total_px = Signed(-price_sum * i128::from(u64::MAX));
    event.order.side = Side::Bid;
    event.order.qty = Signed(total_quantity);
    event.order.tif = 1;
    let mut reference = snapshot.clone();
    reference.ask_agg_data.clear();
    reference.latest_seq_num = Sequence(1);
    Workload {
        name: format!("synthetic-wide-sweep-depth{depth}"),
        raw_snapshot: snapshot_bytes(&snapshot),
        snapshot,
        raw: vec![
            format!(
                "{{\"type\":\"md_event_detail\",\"data\":{}}}",
                serde_json::to_string(&event).unwrap()
            )
            .into_bytes(),
        ],
        events: vec![event],
        reference,
    }
}
