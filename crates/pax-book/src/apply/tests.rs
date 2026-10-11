use super::*;

#[test]
fn every_in_range_hint_matches_an_independent_linear_price_count() {
    for depth in [0, 1, 2, 3, 20, 63, 200] {
        for side in [Side::Ask, Side::Bid] {
            let mut ladder: Vec<_> = (0..depth)
                .map(|i| Level {
                    ticks: PriceTicks(3 * i + 2),
                    qty: Quantity(10),
                })
                .collect();
            if side == Side::Bid {
                ladder.reverse();
            }
            for price in 0..=depth * 3 + 4 {
                // Count all prices independently, without checking adjacent
                // levels or using the production insertion-position algorithm.
                let expected = ladder
                    .iter()
                    .filter(|level| match side {
                        Side::Ask => level.ticks.0 < price,
                        Side::Bid => level.ticks.0 > price,
                    })
                    .count();
                for hint in (0..=u64::from(depth) + 1).chain([u64::MAX]) {
                    assert_eq!(
                        checked_position(&ladder, side, PriceTicks(price), hint),
                        (hint == expected as u64).then_some(expected),
                        "side={side:?}, depth={depth}, price={price}, hint={hint}"
                    );
                }
            }
        }
    }
}
