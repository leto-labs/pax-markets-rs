//! Closed-form wide-quantity sweeps at both ends of the u32 price range.
use pax_book::state::{Book, Outcome};
use pax_types::{
    protocol::{
        self, Message,
        event::Side,
        snapshot::{Level, Snapshot},
    },
    units::{MarketId, PriceTicks, Quantity, Sequence, Signed},
};

#[test]
fn wide_sweeps_match_arithmetic_series_on_both_sides() {
    let Message::Event(template) =
        protocol::decode(include_bytes!("../../pax-types/tests/fixtures/xa.json")).unwrap()
    else {
        panic!()
    };
    for count in [1u32, 2, 20, 200, 2000] {
        for high_prices in [false, true] {
            let ask_start = if high_prices {
                u32::MAX - count + 1
            } else {
                count + 2
            };
            let bid_start = ask_start - 2;
            let snapshot = Snapshot {
                market_id: MarketId(1),
                latest_seq_num: Sequence(0),
                ask_agg_data: (0..count)
                    .map(|i| Level {
                        ticks: PriceTicks(ask_start + i),
                        qty: Quantity(u64::MAX),
                    })
                    .collect(),
                bid_agg_data: (0..count)
                    .map(|i| Level {
                        ticks: PriceTicks(bid_start - i),
                        qty: Quantity(u64::MAX),
                    })
                    .collect(),
            };
            for side in [Side::Ask, Side::Bid] {
                let levels = if side == Side::Ask {
                    &snapshot.ask_agg_data
                } else {
                    &snapshot.bid_agg_data
                };
                let first = levels[0].ticks;
                let last = levels.last().unwrap().ticks;
                let quantity = i128::from(count) * i128::from(u64::MAX);
                let value = (i128::from(first.0) + i128::from(last.0)) * i128::from(count) / 2
                    * i128::from(u64::MAX);
                let mut event = template;
                event.data.event_type = pax_types::protocol::event::EventType::Remove;
                event.data.event_qual = 2;
                event.data.event_side = side;
                event.data.event_delta_qty = Signed(-quantity);
                event.data.event_abs_ticks = last;
                event.data.event_tick_depth = Signed(i128::from(count - 1));
                event.data.event_level_depth = Quantity(u64::from(count - 1));
                event.data.event_level_begin_qty = Quantity(u64::MAX);
                event.data.event_level_delta_qty = Signed(-i128::from(u64::MAX));
                event.data.event_total_px = Signed(-value);
                event.data.seq_num = Sequence(1);
                event.order.side = side.opposite();
                event.order.qty = Signed(quantity);
                event.order.ticks = last;
                event.order.tif = 1;
                event.tob.ask_ticks = if side == Side::Ask {
                    PriceTicks(0)
                } else {
                    snapshot.ask_agg_data[0].ticks
                };
                event.tob.ask_qty = Quantity(if side == Side::Ask { 0 } else { u64::MAX });
                event.tob.bid_ticks = if side == Side::Bid {
                    PriceTicks(0)
                } else {
                    snapshot.bid_agg_data[0].ticks
                };
                event.tob.bid_qty = Quantity(if side == Side::Bid { 0 } else { u64::MAX });
                let mut book = Book::new(MarketId(1), 4, count as usize).unwrap();
                book.snapshot(0, snapshot.clone()).unwrap();
                assert_eq!(book.event(0, &event).unwrap(), Outcome::Applied);
                assert!(book.levels(side).unwrap().is_empty());
                assert_eq!(book.total_quantity(side).unwrap(), 0);
                assert_eq!(
                    book.total_quantity(side.opposite()).unwrap(),
                    quantity as u128
                );
                assert_eq!(book.top().unwrap(), event.tob);
            }
        }
    }
}
