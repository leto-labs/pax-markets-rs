//! A sorted-vector implementation checked against independently mutated maps.
use pax_book::state::Book;
use pax_types::{
    protocol::{
        event::{Event, EventData, EventType, Order, Side, TopOfBook},
        snapshot::{Level, Snapshot},
    },
    units::{MarketId, PriceTicks, Quantity, Sequence, Signed},
};
use std::collections::BTreeMap;

type Ladder = BTreeMap<u32, u64>;
fn levels(map: &Ladder, side: Side) -> Vec<Level> {
    let mut v: Vec<_> = map
        .iter()
        .map(|(&ticks, &qty)| Level {
            ticks: PriceTicks(ticks),
            qty: Quantity(qty),
        })
        .collect();
    if side == Side::Bid {
        v.reverse();
    }
    v
}
fn top(asks: &Ladder, bids: &Ladder) -> TopOfBook {
    let (&ask, &aq) = asks.first_key_value().unwrap_or((&0, &0));
    let (&bid, &bq) = bids.last_key_value().unwrap_or((&0, &0));
    TopOfBook {
        ask_ticks: PriceTicks(ask),
        ask_qty: Quantity(aq),
        bid_ticks: PriceTicks(bid),
        bid_qty: Quantity(bq),
    }
}

#[test]
fn generated_add_cancel_and_sweep_traces_match_independent_maps() {
    for seed in [1u64, 7, 0x12345678, 0xdeadbeef] {
        let mut rng = seed;
        let mut asks: Ladder = (110..142).map(|p| (p, 1000)).collect();
        let mut bids: Ladder = (60..92).map(|p| (p, 1000)).collect();
        let mut book = Book::new(MarketId(1), 16, 256).unwrap();
        book.snapshot(
            0,
            Snapshot {
                market_id: MarketId(1),
                ask_agg_data: levels(&asks, Side::Ask),
                bid_agg_data: levels(&bids, Side::Bid),
                latest_seq_num: Sequence(0),
            },
        )
        .unwrap();
        for sequence in 1..=4000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let side = if rng & 1 == 0 { Side::Ask } else { Side::Bid };
            let map = if side == Side::Ask {
                &mut asks
            } else {
                &mut bids
            };
            let action = (rng >> 2) % 5;
            let price = if side == Side::Ask {
                110 + ((rng >> 8) % 32) as u32
            } else {
                60 + ((rng >> 8) % 32) as u32
            };
            let before = levels(map, side);
            let mut index = before.partition_point(|l| {
                if side == Side::Ask {
                    l.ticks.0 < price
                } else {
                    l.ticks.0 > price
                }
            });
            let mut touched = price;
            let mut begin = *map.get(&price).unwrap_or(&0);
            let mut delta = 1 + ((rng >> 16) % 1000) as i128;
            let mut level_delta = delta;
            let mut value = delta * i128::from(price);
            let mut kind = EventType::Add;
            let mut qual = 0;
            let mut order_side = side;
            if action == 0 && !before.is_empty() {
                index = ((rng >> 12) as usize) % before.len().min(5);
                touched = before[index].ticks.0;
                begin = before[index].qty.0;
                let deepest_fill = begin / 2 + 1;
                let mut total = 0i128;
                value = 0;
                // The oracle applies individual fills to map entries. It does
                // not use the production prefix mutation or signed-delta plan.
                for level in before.iter().take(index + 1) {
                    let fill = if level.ticks.0 == touched {
                        deepest_fill
                    } else {
                        level.qty.0
                    };
                    total += i128::from(fill);
                    value -= i128::from(fill) * i128::from(level.ticks.0);
                    if fill == level.qty.0 {
                        map.remove(&level.ticks.0);
                    } else {
                        map.insert(level.ticks.0, level.qty.0 - fill);
                    }
                }
                delta = -total;
                level_delta = -i128::from(deepest_fill);
                kind = EventType::Remove;
                qual = 2;
                order_side = side.opposite();
            } else if action <= 2 && begin > 0 {
                let removed = begin.min(delta as u64);
                delta = -i128::from(removed);
                level_delta = delta;
                value = delta * i128::from(price);
                kind = EventType::Remove;
                qual = 3;
                if removed == begin {
                    map.remove(&price);
                } else {
                    map.insert(price, begin - removed);
                }
            } else {
                map.insert(price, begin + delta as u64);
            }
            let depth = before.first().map_or(0, |l| {
                if side == Side::Ask {
                    i128::from(touched) - i128::from(l.ticks.0)
                } else {
                    i128::from(l.ticks.0) - i128::from(touched)
                }
            });
            let event = Event {
                tob: top(&asks, &bids),
                data: EventData {
                    event_type: kind,
                    event_qual: qual,
                    event_side: side,
                    event_delta_qty: Signed(delta),
                    event_abs_ticks: PriceTicks(touched),
                    event_tick_depth: Signed(depth.max(0)),
                    event_level_depth: Quantity(index as u64),
                    event_level_begin_qty: Quantity(begin),
                    event_level_delta_qty: Signed(level_delta),
                    event_total_px: Signed(value),
                    seq_num: Sequence(sequence),
                },
                order: Order {
                    side: order_side,
                    market_id: MarketId(1),
                    qty: Signed(if qual == 3 { delta } else { delta.abs() }),
                    ticks: PriceTicks(touched),
                    tif: 2,
                    origin: 1,
                },
            };
            book.event(0, &event)
                .unwrap_or_else(|e| panic!("seed={seed} sequence={sequence}: {e}"));
            assert_eq!(
                book.total_quantity(Side::Ask).unwrap(),
                asks.values().copied().map(u128::from).sum::<u128>()
            );
            assert_eq!(
                book.total_quantity(Side::Bid).unwrap(),
                bids.values().copied().map(u128::from).sum::<u128>()
            );
            assert_eq!(book.levels(Side::Ask).unwrap(), levels(&asks, Side::Ask));
            assert_eq!(book.levels(Side::Bid).unwrap(), levels(&bids, Side::Bid));
        }
    }
}
