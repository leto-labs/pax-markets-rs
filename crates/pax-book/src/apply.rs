//! Preflight exact event effects before mutating capacity-reusing ladders.

use crate::state::{Book, BookError, Phase};
use pax_types::{
    protocol::{
        event::{Event, EventType, Side},
        snapshot::Level,
    },
    units::{PriceTicks, Quantity},
};

struct Transition {
    index: usize,
    exists: bool,
    sweep: bool,
    final_qty: u64,
    rested: u64,
    side_quantity: u128,
    opposite_quantity: u128,
}

impl Book {
    pub(crate) fn apply(
        &mut self,
        event: &Event,
        observe: &mut impl FnMut(Phase),
    ) -> Result<(), BookError> {
        event.validate_protocol().map_err(BookError::Inconsistent)?;
        let data = &event.data;
        if data.event_type == EventType::Miss {
            if data.event_delta_qty.0 != 0
                || data.event_level_delta_qty.0 != 0
                || data.event_total_px.0 != 0
                || event.order.tif != 1
            {
                return Err(BookError::Inconsistent("immediate miss changed liquidity"));
            }
            observe(Phase::Preflight);
        } else {
            let transition = self.preflight(event)?;
            observe(Phase::Preflight);
            self.commit(data.event_side, data.event_abs_ticks, transition);
        }
        observe(Phase::Mutation);
        self.check_uncrossed()?;
        if self.raw_top() != event.tob {
            return Err(BookError::Inconsistent("post-event top of book mismatch"));
        }
        observe(Phase::Validation);
        Ok(())
    }

    fn preflight(&self, event: &Event) -> Result<Transition, BookError> {
        let d = &event.data;
        let price = d.event_abs_ticks;
        if price.0 == 0 {
            return Err(BookError::Inconsistent("zero event price"));
        }
        let ladder = self.ladder(d.event_side);
        let index = checked_position(ladder, d.event_side, price, d.event_level_depth.0)
            .ok_or(BookError::Inconsistent("deepest-level pre-state mismatch"))?;
        let existing = ladder.get(index).filter(|level| level.ticks == price);
        let begin = existing.map_or(0, |level| level.qty.0);
        if d.event_level_begin_qty.0 != begin {
            return Err(BookError::Inconsistent("deepest-level pre-state mismatch"));
        }
        let tick_depth = ladder.first().map_or(0, |best| match d.event_side {
            Side::Ask => i128::from(price.0) - i128::from(best.ticks.0),
            Side::Bid => i128::from(best.ticks.0) - i128::from(price.0),
        });
        if d.event_tick_depth.0 != tick_depth.max(0) {
            return Err(BookError::Inconsistent("tick depth mismatch"));
        }
        let level_delta = d.event_level_delta_qty.0;
        let final_signed = i128::from(begin)
            .checked_add(level_delta)
            .ok_or(BookError::Overflow)?;
        let sweep = d.event_type == EventType::Remove && matches!(d.event_qual, 2 | 4);
        let mut prefix_qty = 0i128;
        let mut prefix_value = 0i128;
        if sweep {
            // Validated ladders have distinct positive u32 prices and u64
            // quantities. With N = u32::MAX and Q = u64::MAX, quantity is at
            // most N*Q < 2^96 and notional is at most Q*N*(N+1)/2 < 2^127.
            // Every nonnegative prefix therefore fits i128. Wrapping additions
            // are exact here and remove redundant per-level overflow branches;
            // arithmetic involving incoming signed deltas remains checked below.
            for level in &ladder[..index] {
                // Keep the type bounds explicit so widening a unit requires
                // revisiting the proof, rather than silently weakening it.
                let quantity: u64 = level.qty.0;
                let ticks: u32 = level.ticks.0;
                prefix_qty = prefix_qty.wrapping_add(i128::from(quantity));
                prefix_value = prefix_value.wrapping_add(i128::from(quantity) * i128::from(ticks));
            }
        }
        if d.event_delta_qty.0
            != level_delta
                .checked_sub(prefix_qty)
                .ok_or(BookError::Overflow)?
        {
            return Err(BookError::Inconsistent("aggregate quantity mismatch"));
        }
        let mut rested = 0u64;
        match d.event_type {
            EventType::Add => {
                if level_delta <= 0
                    || event.order.side != d.event_side
                    || event.order.tif != 2
                    || event.order.ticks != price
                    || event.order.qty.0 != level_delta
                {
                    return Err(BookError::Inconsistent("invalid liquidity addition"));
                }
                if existing.is_none() && ladder.len() == self.max_levels {
                    return Err(BookError::Capacity("ladder levels"));
                }
            }
            EventType::Remove => {
                if level_delta >= 0 {
                    return Err(BookError::Inconsistent("nonnegative removal"));
                }
                if sweep {
                    if d.event_delta_qty
                        .0
                        .checked_neg()
                        .is_none_or(|removed| removed > event.order.qty.0)
                    {
                        return Err(BookError::Inconsistent("execution exceeds order quantity"));
                    }
                    let limit_crosses = match d.event_side {
                        Side::Ask => event.order.ticks >= price,
                        Side::Bid => event.order.ticks <= price,
                    };
                    if event.order.side != d.event_side.opposite() || !limit_crosses {
                        return Err(BookError::Inconsistent("execution order side or limit"));
                    }
                } else if event.order.side != d.event_side {
                    return Err(BookError::Inconsistent("cancel side mismatch"));
                }
                if d.event_qual == 4 {
                    if final_signed >= 0 || event.order.tif != 2 || event.order.ticks != price {
                        return Err(BookError::Inconsistent("XA must execute and rest"));
                    }
                    rested = u64::try_from(final_signed.checked_neg().ok_or(BookError::Overflow)?)
                        .map_err(|_| BookError::Overflow)?;
                    if prefix_qty
                        .checked_add(i128::from(begin))
                        .ok_or(BookError::Overflow)?
                        == 0
                    {
                        return Err(BookError::Inconsistent("XA without execution"));
                    }
                    if d.event_delta_qty.0.checked_neg() != Some(event.order.qty.0) {
                        return Err(BookError::Inconsistent("XA order quantity mismatch"));
                    }
                    if self.ladder(d.event_side.opposite()).len() == self.max_levels {
                        return Err(BookError::Capacity("opposite ladder levels"));
                    }
                } else if existing.is_none() || final_signed < 0 {
                    return Err(BookError::Inconsistent("removal exceeds existing level"));
                }
            }
            EventType::Miss => unreachable!("miss has no ladder transition"),
        }
        let expected_value = if rested > 0 {
            (i128::from(rested) - i128::from(begin))
                .checked_mul(i128::from(price.0))
                .and_then(|value| value.checked_sub(prefix_value))
        } else {
            level_delta
                .checked_mul(i128::from(price.0))
                .and_then(|value| value.checked_sub(prefix_value))
        }
        .ok_or(BookError::Overflow)?;
        if d.event_total_px.0 != expected_value {
            return Err(BookError::Inconsistent("aggregate notional mismatch"));
        }
        let final_qty = if rested > 0 {
            0
        } else {
            u64::try_from(final_signed).map_err(|_| BookError::Overflow)?
        };
        let maker_delta = d
            .event_delta_qty
            .0
            .checked_add(i128::from(rested))
            .ok_or(BookError::Overflow)?;
        let side_quantity = self
            .raw_quantity(d.event_side)
            .checked_add_signed(maker_delta)
            .ok_or(BookError::Overflow)?;
        let opposite_quantity = self
            .raw_quantity(d.event_side.opposite())
            .checked_add(u128::from(rested))
            .ok_or(BookError::Overflow)?;
        Ok(Transition {
            index,
            exists: existing.is_some(),
            sweep,
            final_qty,
            rested,
            side_quantity,
            opposite_quantity,
        })
    }

    fn commit(&mut self, side: Side, ticks: PriceTicks, transition: Transition) {
        let Transition {
            index,
            exists,
            sweep,
            final_qty,
            rested,
            side_quantity,
            opposite_quantity,
        } = transition;
        (self.ask_quantity, self.bid_quantity) = match side {
            Side::Ask => (side_quantity, opposite_quantity),
            Side::Bid => (opposite_quantity, side_quantity),
        };
        let ladder = self.ladder_mut(side);
        if exists {
            if final_qty == 0 {
                if sweep {
                    ladder.drain(..=index);
                } else {
                    ladder.remove(index);
                }
            } else {
                ladder[index].qty = Quantity(final_qty);
                if sweep {
                    ladder.drain(..index);
                }
            }
        } else if sweep {
            ladder.drain(..index);
        } else {
            ladder.insert(
                index,
                Level {
                    ticks,
                    qty: Quantity(final_qty),
                },
            );
        }
        if rested > 0 {
            // In an uncrossed pre-state, a crossed order's residual is strictly
            // better than the existing opposite best. The TOB check follows.
            self.ladder_mut(side.opposite()).insert(
                0,
                Level {
                    ticks,
                    qty: Quantity(rested),
                },
            );
        }
    }
}

fn checked_position(
    ladder: &[Level],
    side: Side,
    ticks: PriceTicks,
    claimed: u64,
) -> Option<usize> {
    let index = usize::try_from(claimed).ok()?;
    if index > ladder.len() {
        return None;
    }
    // Snapshot validation and every mutation preserve strict price ordering.
    // The claimed lower bound is exact iff its predecessor sorts before the
    // price and its successor does not. Validate both boundaries in O(1).
    let before = |level: &Level| match side {
        Side::Ask => level.ticks < ticks,
        Side::Bid => level.ticks > ticks,
    };
    if index > 0 && !before(&ladder[index - 1]) {
        return None;
    }
    if ladder.get(index).is_some_and(before) {
        return None;
    }
    Some(index)
}

#[cfg(test)]
mod tests;
