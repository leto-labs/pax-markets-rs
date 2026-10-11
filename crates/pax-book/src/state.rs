//! Per-market book ownership and explicit synchronization state.

use pax_types::{
    protocol::{
        event::{Event, Side, TopOfBook},
        snapshot::{Level, Snapshot},
    },
    units::{MarketId, PriceTicks, Quantity, Sequence},
};

/// Failure that makes reconstructed liquidity unavailable until resynchronization.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BookError {
    /// A contract or state consistency check failed.
    #[error("book consistency failure: {0}")]
    Inconsistent(&'static str),
    /// An exact calculation overflowed.
    #[error("book arithmetic overflow")]
    Overflow,
    /// Input buffer or depth bound was exceeded.
    #[error("book resource limit exceeded: {0}")]
    Capacity(&'static str),
    /// Input belongs to another market.
    #[error("input market does not match book")]
    Market,
    /// No valid snapshot and contiguous stream is available.
    #[error("book is not synchronized")]
    Unavailable,
    /// An input sequence skipped one or more events.
    #[error("sequence gap: expected {expected}, received {received}")]
    Gap {
        /// Next sequence expected.
        expected: u64,
        /// Sequence received instead.
        received: u64,
    },
}

/// Synchronization status; transport liveness is a separate application concern.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    /// Buffering events before a snapshot.
    AwaitingSnapshot,
    /// Snapshot reconciled and every accepted event validated.
    Live,
    /// A fresh generation and snapshot are required.
    Invalid(BookError),
}

/// Result of a successful synchronization operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    /// Snapshot and buffered overlap reconciled.
    Synchronized,
    /// Next event applied.
    Applied,
    /// Event stored until a snapshot arrives.
    Buffered,
    /// An already-applied sequence had no effect.
    Duplicate,
    /// Input from a previous session was ignored.
    OldGeneration,
}

/// One bounded deterministic aggregated orderbook.
#[derive(Debug)]
pub struct Book {
    pub(crate) market: MarketId,
    pub(crate) generation: u64,
    pub(crate) status: Status,
    pub(crate) sequence: Option<Sequence>,
    pub(crate) asks: Vec<Level>,
    pub(crate) bids: Vec<Level>,
    pub(crate) ask_quantity: u128,
    pub(crate) bid_quantity: u128,
    pub(crate) pending: Vec<Event>,
    pub(crate) max_buffered: usize,
    pub(crate) max_levels: usize,
}

/// Completed event stage. Observers run synchronously and must not block.
/// No clock, allocation, or runtime is imposed on ordinary `Book::event` callers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    /// Exact event and pre-state checks completed.
    Preflight,
    /// Ladder mutation completed.
    Mutation,
    /// Reconstructed post-state checks completed.
    Validation,
}

impl Book {
    /// Create a book awaiting a snapshot in generation zero.
    pub fn new(
        market: MarketId,
        max_buffered: usize,
        max_levels: usize,
    ) -> Result<Self, BookError> {
        if market.0 == 0 || max_buffered == 0 || max_levels == 0 {
            return Err(BookError::Inconsistent("zero market or resource bound"));
        }
        Ok(Self {
            market,
            generation: 0,
            status: Status::AwaitingSnapshot,
            sequence: None,
            asks: Vec::new(),
            bids: Vec::new(),
            ask_quantity: 0,
            bid_quantity: 0,
            pending: Vec::new(),
            max_buffered,
            max_levels,
        })
    }
    /// Market owned by this instance.
    pub fn market(&self) -> MarketId {
        self.market
    }
    /// Current transport generation.
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// Synchronization state and invalidation reason.
    pub fn status(&self) -> Status {
        self.status
    }
    /// Last successfully committed sequence (not a usable quote if invalid).
    pub fn sequence(&self) -> Option<Sequence> {
        self.sequence
    }
    /// Start a strictly newer connection generation, reusing ladder capacities.
    pub fn reset(&mut self, generation: u64) -> Result<(), BookError> {
        if generation <= self.generation {
            return Err(BookError::Inconsistent("generation must increase"));
        }
        self.generation = generation;
        self.status = Status::AwaitingSnapshot;
        self.sequence = None;
        self.asks.clear();
        self.bids.clear();
        self.ask_quantity = 0;
        self.bid_quantity = 0;
        self.pending.clear();
        Ok(())
    }
    /// Invalidate continuity after an external transport or decoder failure.
    pub fn invalidate(&mut self, reason: BookError) {
        self.status = Status::Invalid(reason);
        self.pending.clear();
    }
    /// Sorted levels from a synchronized book. Invalid intermediate state is private.
    pub fn levels(&self, side: Side) -> Result<&[Level], BookError> {
        self.require_live()?;
        Ok(self.ladder(side))
    }
    /// Exact total represented native quantity, maintained during transitions.
    /// Constant-time and unavailable whenever the book is not synchronized.
    pub fn total_quantity(&self, side: Side) -> Result<u128, BookError> {
        self.require_live()?;
        Ok(self.raw_quantity(side))
    }
    pub(crate) fn raw_quantity(&self, side: Side) -> u128 {
        match side {
            Side::Ask => self.ask_quantity,
            Side::Bid => self.bid_quantity,
        }
    }
    /// Reconstructed best quotes. An empty side uses zero ticks and quantity.
    pub fn top(&self) -> Result<TopOfBook, BookError> {
        self.require_live()?;
        Ok(self.raw_top())
    }
    pub(crate) fn require_live(&self) -> Result<(), BookError> {
        if self.status == Status::Live {
            Ok(())
        } else {
            Err(BookError::Unavailable)
        }
    }
    pub(crate) fn ladder(&self, side: Side) -> &[Level] {
        match side {
            Side::Ask => &self.asks,
            Side::Bid => &self.bids,
        }
    }
    pub(crate) fn ladder_mut(&mut self, side: Side) -> &mut Vec<Level> {
        match side {
            Side::Ask => &mut self.asks,
            Side::Bid => &mut self.bids,
        }
    }
    pub(crate) fn raw_top(&self) -> TopOfBook {
        let zero = Level {
            ticks: PriceTicks(0),
            qty: Quantity(0),
        };
        let ask = self.asks.first().unwrap_or(&zero);
        let bid = self.bids.first().unwrap_or(&zero);
        TopOfBook {
            ask_qty: ask.qty,
            ask_ticks: ask.ticks,
            bid_qty: bid.qty,
            bid_ticks: bid.ticks,
        }
    }
    pub(crate) fn check_uncrossed(&self) -> Result<(), BookError> {
        if let (Some(ask), Some(bid)) = (self.asks.first(), self.bids.first())
            && bid.ticks >= ask.ticks
        {
            return Err(BookError::Inconsistent("crossed or locked book"));
        }
        Ok(())
    }
    fn check_generation(&mut self, generation: u64) -> Result<bool, BookError> {
        if generation < self.generation {
            return Ok(false);
        }
        if generation > self.generation {
            let reason = BookError::Inconsistent("unannounced generation");
            self.invalidate(reason);
            return Err(reason);
        }
        Ok(true)
    }
    /// Load a validated snapshot and reconcile buffered events in sequence order.
    pub fn snapshot(&mut self, generation: u64, snapshot: Snapshot) -> Result<Outcome, BookError> {
        if !self.check_generation(generation)? {
            return Ok(Outcome::OldGeneration);
        }
        if snapshot.market_id != self.market {
            return Err(BookError::Market);
        }
        if self.status != Status::AwaitingSnapshot {
            return Err(BookError::Unavailable);
        }
        let result = self.load_snapshot(snapshot);
        if let Err(reason) = result {
            self.invalidate(reason);
        }
        result
    }
    fn load_snapshot(&mut self, mut snapshot: Snapshot) -> Result<Outcome, BookError> {
        validate_levels(&snapshot.ask_agg_data, Side::Ask, self.max_levels)?;
        validate_levels(&snapshot.bid_agg_data, Side::Bid, self.max_levels)?;
        // Absorb initial insertions without reallocating an event's hot path.
        // Snapshot setup pays for bounded geometric headroom; never reserve the
        // entire maximum depth for every quiet market.
        for levels in [&mut snapshot.ask_agg_data, &mut snapshot.bid_agg_data] {
            let desired = levels
                .len()
                .saturating_add(1)
                .checked_next_power_of_two()
                .unwrap_or(self.max_levels)
                .max(8)
                .min(self.max_levels);
            if levels.capacity() < desired {
                levels.reserve_exact(desired - levels.len());
            }
        }
        self.ask_quantity = snapshot
            .ask_agg_data
            .iter()
            .try_fold(0u128, |sum, level| sum.checked_add(u128::from(level.qty.0)))
            .ok_or(BookError::Overflow)?;
        self.bid_quantity = snapshot
            .bid_agg_data
            .iter()
            .try_fold(0u128, |sum, level| sum.checked_add(u128::from(level.qty.0)))
            .ok_or(BookError::Overflow)?;
        self.asks = snapshot.ask_agg_data;
        self.bids = snapshot.bid_agg_data;
        self.check_uncrossed()?;
        self.sequence = Some(snapshot.latest_seq_num);
        self.status = Status::Live;
        // Reuse the bounded pre-snapshot buffer instead of reallocating it.
        let mut pending = std::mem::take(&mut self.pending);
        pending.sort_unstable_by_key(|event| event.data.seq_num);
        let result = pending
            .iter()
            .try_for_each(|event| self.event(self.generation, event).map(|_| ()));
        pending.clear();
        self.pending = pending;
        result?;
        Ok(Outcome::Synchronized)
    }
    /// Buffer or apply one event; duplicates and old generations never mutate levels.
    pub fn event(&mut self, generation: u64, event: &Event) -> Result<Outcome, BookError> {
        self.event_with_observer(generation, event, |_| {})
    }
    /// Apply with stage completion markers for sampled diagnostics. Failed stages
    /// do not emit completion; sequence checks occur before the first marker.
    pub fn event_with_observer(
        &mut self,
        generation: u64,
        event: &Event,
        mut observe: impl FnMut(Phase),
    ) -> Result<Outcome, BookError> {
        if !self.check_generation(generation)? {
            return Ok(Outcome::OldGeneration);
        }
        if event.order.market_id != self.market {
            return Err(BookError::Market);
        }
        match self.status {
            Status::Invalid(_) => return Err(BookError::Unavailable),
            Status::AwaitingSnapshot => {
                if self.pending.len() == self.max_buffered {
                    let reason = BookError::Capacity("pre-snapshot events");
                    self.invalidate(reason);
                    return Err(reason);
                }
                self.pending.push(*event);
                return Ok(Outcome::Buffered);
            }
            Status::Live => {}
        }
        let last = self.sequence.expect("live book has a sequence").0;
        if event.data.seq_num.0 <= last {
            return Ok(Outcome::Duplicate);
        }
        let result = (|| {
            let expected = last.checked_add(1).ok_or(BookError::Overflow)?;
            if event.data.seq_num.0 != expected {
                return Err(BookError::Gap {
                    expected,
                    received: event.data.seq_num.0,
                });
            }
            self.apply(event, &mut observe)?;
            self.sequence = Some(event.data.seq_num);
            Ok(Outcome::Applied)
        })();
        if let Err(reason) = result {
            self.invalidate(reason);
        }
        result
    }
}

fn validate_levels(levels: &[Level], side: Side, limit: usize) -> Result<(), BookError> {
    if levels.len() > limit {
        return Err(BookError::Capacity("snapshot depth"));
    }
    if levels
        .iter()
        .any(|level| level.ticks.0 == 0 || level.qty.0 == 0)
    {
        return Err(BookError::Inconsistent("zero snapshot price or quantity"));
    }
    if levels.windows(2).any(|pair| match side {
        Side::Ask => pair[0].ticks >= pair[1].ticks,
        Side::Bid => pair[0].ticks <= pair[1].ticks,
    }) {
        return Err(BookError::Inconsistent(
            "unsorted or duplicate snapshot price",
        ));
    }
    Ok(())
}
