//! Independent offline synchronization, transition, and liquidity contracts.
use pax_book::state::{Book, BookError, Outcome, Status};
use pax_types::{
    protocol::{
        Message, decode,
        event::{Event, Side},
    },
    units::{MarketId, Quantity, Sequence},
};

fn fixture_snapshot() -> pax_types::protocol::snapshot::Snapshot {
    let Message::Snapshot(snapshot) = decode(include_bytes!(
        "../../pax-types/tests/fixtures/snapshot.json"
    ))
    .unwrap() else {
        panic!()
    };
    snapshot
}
fn xa() -> Event {
    let Message::Event(event) =
        decode(include_bytes!("../../pax-types/tests/fixtures/xa.json")).unwrap()
    else {
        panic!()
    };
    event
}
fn live() -> Book {
    let mut book = Book::new(MarketId(1), 16, 128).unwrap();
    book.snapshot(0, fixture_snapshot()).unwrap();
    book
}

#[test]
fn documented_execute_and_rest_reconciles_multiple_levels() {
    let mut book = live();
    assert_eq!(book.event(0, &xa()).unwrap(), Outcome::Applied);
    assert_eq!(book.levels(Side::Ask).unwrap().len(), 1);
    assert_eq!(book.levels(Side::Bid).unwrap()[0].qty, Quantity(1000));
    assert_eq!(book.top().unwrap(), xa().tob);
    assert_eq!(book.sequence(), Some(Sequence(3)));
}

#[test]
fn buffered_overlap_and_duplicates_have_exactly_one_effect() {
    let mut book = Book::new(MarketId(1), 16, 128).unwrap();
    let mut old = xa();
    old.data.seq_num = Sequence(2);
    book.event(0, &xa()).unwrap();
    book.event(0, &old).unwrap();
    book.event(0, &xa()).unwrap();
    assert_eq!(
        book.snapshot(0, fixture_snapshot()).unwrap(),
        Outcome::Synchronized
    );
    assert_eq!(book.event(0, &xa()).unwrap(), Outcome::Duplicate);
    assert_eq!(book.top().unwrap(), xa().tob);
}

#[test]
fn sequence_gap_invalidates_and_requires_fresh_generation() {
    let mut book = live();
    let mut event = xa();
    event.data.seq_num = Sequence(4);
    assert_eq!(
        book.event(0, &event),
        Err(BookError::Gap {
            expected: 3,
            received: 4
        })
    );
    assert!(book.levels(Side::Ask).is_err());
    assert!(book.snapshot(0, fixture_snapshot()).is_err());
    book.reset(1).unwrap();
    assert_eq!(book.event(0, &xa()).unwrap(), Outcome::OldGeneration);
    book.snapshot(1, fixture_snapshot()).unwrap();
    book.event(1, &xa()).unwrap();
    assert_eq!(book.status(), Status::Live);
}

#[test]
fn corrupted_prestate_notional_or_tob_never_exposes_valid_liquidity() {
    let edits: [fn(&mut Event); 8] = [
        |e| e.data.event_level_begin_qty.0 += 1,
        |e| e.data.event_total_px.0 += 1,
        |e| e.data.event_delta_qty.0 += 1,
        |e| e.data.event_tick_depth.0 += 1,
        |e| e.data.event_level_depth.0 += 1,
        |e| e.tob.bid_qty.0 += 1,
        |e| e.data.event_level_delta_qty.0 = i128::MIN,
        |e| e.order.side = Side::Ask,
    ];
    for edit in edits {
        let mut book = live();
        let mut event = xa();
        edit(&mut event);
        assert!(book.event(0, &event).is_err());
        assert!(matches!(book.status(), Status::Invalid(_)));
        assert!(book.top().is_err());
        assert!(book.estimate(Side::Bid, Quantity(1)).is_err());
        assert_eq!(book.sequence(), Some(Sequence(2)));
    }
}

#[test]
fn buffer_and_depth_limits_are_enforced() {
    let mut book = Book::new(MarketId(1), 1, 2).unwrap();
    book.event(0, &xa()).unwrap();
    assert!(matches!(book.event(0, &xa()), Err(BookError::Capacity(_))));
    book.reset(1).unwrap();
    assert!(matches!(
        book.snapshot(1, fixture_snapshot()),
        Err(BookError::Capacity(_))
    ));
}

#[test]
fn snapshot_rejects_duplicate_crossed_unsorted_and_zero_levels() {
    let edits: [fn(&mut pax_types::protocol::snapshot::Snapshot); 4] = [
        |s| s.ask_agg_data[1].ticks = s.ask_agg_data[0].ticks,
        |s| s.bid_agg_data[0].ticks = s.ask_agg_data[0].ticks,
        |s| s.ask_agg_data.swap(0, 2),
        |s| s.ask_agg_data[0].qty = Quantity(0),
    ];
    for edit in edits {
        let mut snapshot = fixture_snapshot();
        edit(&mut snapshot);
        let mut book = Book::new(MarketId(1), 16, 128).unwrap();
        assert!(book.snapshot(0, snapshot).is_err());
        assert!(book.top().is_err());
    }
}

#[test]
fn liquidity_estimates_report_exact_value_and_shortfall() {
    let book = live();
    let fill = book.estimate(Side::Bid, Quantity(6000)).unwrap();
    assert_eq!(
        fill.quantity_ticks,
        2000 * 100010 + 3000 * 100012 + 1000 * 100015
    );
    assert_eq!(fill.filled, Quantity(6000));
    assert_eq!(fill.shortfall, Quantity(0));
    assert_eq!(fill.levels, 3);
    let fill = book.estimate(Side::Bid, Quantity(10000)).unwrap();
    assert_eq!(fill.shortfall, Quantity(500));
    assert_eq!(
        book.depth(Side::Ask)
            .unwrap()
            .last()
            .unwrap()
            .cumulative_quantity,
        9500
    );
    assert!(book.estimate(Side::Bid, Quantity(0)).is_err());
}

#[test]
fn empty_side_has_no_spread_or_fabricated_fill() {
    let mut snapshot = fixture_snapshot();
    snapshot.bid_agg_data.clear();
    let mut book = Book::new(MarketId(1), 16, 128).unwrap();
    book.snapshot(0, snapshot).unwrap();
    assert_eq!(book.spread().unwrap(), None);
    let fill = book.estimate(Side::Ask, Quantity(100)).unwrap();
    assert_eq!(fill.filled, Quantity(0));
    assert_eq!(fill.shortfall, Quantity(100));
    assert_eq!(fill.average_ticks().unwrap(), None);
}

#[test]
fn mirrored_xa_and_immediate_miss_preserve_side_semantics() {
    use pax_types::{
        protocol::event::EventType,
        units::{PriceTicks, Signed},
    };
    let mut snapshot = fixture_snapshot();
    let center = 200000;
    let reflect = |levels: Vec<pax_types::protocol::snapshot::Level>| {
        levels
            .into_iter()
            .map(|mut l| {
                l.ticks = PriceTicks(center - l.ticks.0);
                l
            })
            .collect()
    };
    let old_asks = std::mem::take(&mut snapshot.ask_agg_data);
    snapshot.ask_agg_data = reflect(std::mem::take(&mut snapshot.bid_agg_data));
    snapshot.bid_agg_data = reflect(old_asks);
    let mut event = xa();
    event.data.event_side = Side::Bid;
    event.data.event_abs_ticks = PriceTicks(center - event.data.event_abs_ticks.0);
    event.data.event_total_px = Signed(-399956000);
    event.order.side = Side::Ask;
    event.order.ticks = event.data.event_abs_ticks;
    event.tob.ask_ticks = PriceTicks(99988);
    event.tob.ask_qty = Quantity(1000);
    event.tob.bid_ticks = PriceTicks(99985);
    event.tob.bid_qty = Quantity(4500);
    let mut book = Book::new(MarketId(1), 16, 128).unwrap();
    book.snapshot(0, snapshot).unwrap();
    book.event(0, &event).unwrap();
    let before = book.top().unwrap();
    event.data.event_type = EventType::Miss;
    event.data.event_qual = 0;
    event.data.event_delta_qty = Signed(0);
    event.data.event_level_delta_qty = Signed(0);
    event.data.event_total_px = Signed(0);
    event.data.seq_num = Sequence(4);
    event.order.tif = 1;
    book.event(0, &event).unwrap();
    assert_eq!(book.top().unwrap(), before);
    assert_eq!(book.sequence(), Some(Sequence(4)));
}

#[test]
fn public_streams_match_independently_derived_final_snapshots() {
    for (input, reference, count) in [
        (
            include_str!("fixtures/public-sample.jsonl"),
            include_bytes!("fixtures/public-sample-final.json").as_slice(),
            12,
        ),
        (
            include_str!("fixtures/public-active.jsonl"),
            include_bytes!("fixtures/public-active-final.json").as_slice(),
            272,
        ),
    ] {
        let mut book = Book::new(MarketId(1), 1024, 100_000).unwrap();
        let mut events = 0;
        for line in input.lines() {
            match decode(line.as_bytes()).unwrap() {
                Message::Snapshot(snapshot) => {
                    book.snapshot(0, snapshot).unwrap();
                }
                Message::Event(event) => {
                    book.event(0, &event)
                        .unwrap_or_else(|error| panic!("sequence {}: {error}", event.data.seq_num));
                    events += 1;
                }
                _ => {}
            }
        }
        let Message::Snapshot(expected) = decode(reference).unwrap() else {
            panic!()
        };
        assert_eq!(events, count);
        assert_eq!(book.sequence(), Some(expected.latest_seq_num));
        assert_eq!(book.levels(Side::Ask).unwrap(), expected.ask_agg_data);
        assert_eq!(book.levels(Side::Bid).unwrap(), expected.bid_agg_data);
    }
}

#[test]
fn cached_quantities_follow_execute_and_rest_and_hide_invalid_state() {
    let mut book = Book::new(MarketId(1), 16, 100).unwrap();
    let Message::Snapshot(snapshot) = decode(include_bytes!(
        "../../pax-types/tests/fixtures/snapshot.json"
    ))
    .unwrap() else {
        panic!()
    };
    let Message::Event(event) =
        decode(include_bytes!("../../pax-types/tests/fixtures/xa.json")).unwrap()
    else {
        panic!()
    };
    book.snapshot(0, snapshot).unwrap();
    assert_eq!(book.total_quantity(Side::Ask).unwrap(), 9500);
    assert_eq!(book.total_quantity(Side::Bid).unwrap(), 15500);
    book.event(0, &event).unwrap();
    assert_eq!(book.total_quantity(Side::Ask).unwrap(), 4500);
    assert_eq!(book.total_quantity(Side::Bid).unwrap(), 16500);
    book.invalidate(pax_book::state::BookError::Unavailable);
    assert!(book.total_quantity(Side::Ask).is_err());
}
