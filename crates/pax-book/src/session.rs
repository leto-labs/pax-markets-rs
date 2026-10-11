//! Deterministic connection generations and selected-market transitions.
//!
//! Callers decode messages and supply explicit generations. No transport, clock,
//! capture or presentation work is required by the session. Observers run before
//! mutation so callers can account for the previous valid liquidity interval.

use crate::state::{Book, BookError, Outcome, Phase};
use pax_types::{
    protocol::Message,
    units::{MarketId, Sequence},
};
use std::collections::BTreeMap;

/// A rejected session input. A message rejection invalidates every selected book.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SessionError {
    /// Invalid selected markets or resource bounds at construction.
    #[error("invalid session configuration: {0}")]
    Configuration(&'static str),
    /// A connection boundary did not start a strictly newer disconnected session.
    #[error("connection generation order")]
    Generation,
    /// Input has no matching active connection.
    #[error("message outside active connection")]
    Disconnected,
    /// Input references a market that this session does not own.
    #[error("input for unselected market {0}")]
    Market(MarketId),
    /// The venue reported a failure requiring transport recovery.
    #[error("venue error: {0}")]
    Venue(String),
    /// The stream unexpectedly stopped a subscription.
    #[error("unexpected unsubscribe")]
    Unsubscribed,
    /// A selected book rejected its next transition.
    #[error("market {market}: {source}")]
    Book {
        /// Selected market that rejected the input.
        market: MarketId,
        /// Exact synchronization or validation failure.
        #[source]
        source: BookError,
    },
}
impl SessionError {
    fn continuity_reason(&self) -> BookError {
        match self {
            Self::Book { source, .. } => *source,
            _ => BookError::Inconsistent("session continuity failure"),
        }
    }
}

/// Successful input disposition; controls never masquerade as book commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionOutcome {
    /// A selected book accepted the snapshot/event with this disposition.
    Book {
        /// Book that consumed the input.
        market: MarketId,
        /// Last committed sequence, including snapshot/buffer reconciliation.
        sequence: Option<Sequence>,
        /// Commit, buffered input or duplicate.
        outcome: Outcome,
    },
    /// A subscription acknowledgement or application pong.
    Control,
    /// Delayed input from an earlier generation; no book or observer was touched.
    OldGeneration,
}
impl SessionOutcome {
    /// True only after a snapshot or event successfully committed.
    pub const fn committed(self) -> bool {
        matches!(
            self,
            Self::Book {
                outcome: Outcome::Synchronized | Outcome::Applied,
                ..
            }
        )
    }
}

/// Single owner of bounded books and their connection generation.
///
/// Mutable books are not exposed: generation and invalidation remain atomic
/// across selected markets. A failed message closes continuity until a newer
/// generation is announced. Individual quotes still require reconciled snapshots.
#[derive(Debug)]
pub struct Session {
    books: BTreeMap<MarketId, Book>,
    generation: u64,
    connected: bool,
}
impl Session {
    /// Select 1..=256 distinct nonzero markets with per-book buffer/depth bounds.
    /// Returns a configuration error before accepting any connection or input.
    pub fn new(
        markets: &[MarketId],
        max_buffered: usize,
        max_levels: usize,
    ) -> Result<Self, SessionError> {
        if markets.is_empty() || markets.len() > 256 {
            return Err(SessionError::Configuration("selected market bounds"));
        }
        let mut books = BTreeMap::new();
        for &market in markets {
            let book = Book::new(market, max_buffered, max_levels)
                .map_err(|_| SessionError::Configuration("market or book bounds"))?;
            if books.insert(market, book).is_some() {
                return Err(SessionError::Configuration("duplicate market"));
            }
        }
        Ok(Self {
            books,
            generation: 0,
            connected: false,
        })
    }

    /// Current connection generation; zero before the first connection.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Whether this session accepts input from its current connection.
    /// This does not mean each selected book has received a snapshot yet.
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Read a selected book, whose own status determines quote availability.
    pub fn book(&self, market: MarketId) -> Option<&Book> {
        self.books.get(&market)
    }

    /// Selected books in deterministic ascending market order.
    pub fn books(&self) -> impl ExactSizeIterator<Item = &Book> {
        self.books.values()
    }

    /// Begin a newer generation while disconnected. Invalid boundaries leave
    /// the existing generation and every book unchanged.
    pub fn connect(&mut self, generation: u64) -> Result<(), SessionError> {
        self.connect_observed(generation, |_| {})
    }

    /// Begin a generation after observing each previous book before reset.
    /// The synchronous hook must not block or panic.
    pub fn connect_observed(
        &mut self,
        generation: u64,
        mut before: impl FnMut(&Book),
    ) -> Result<(), SessionError> {
        if self.connected || generation <= self.generation {
            return Err(SessionError::Generation);
        }
        for book in self.books.values_mut() {
            before(book);
            // No mutable book escapes this owner; all book generations equal
            // the session generation checked above.
            book.reset(generation)
                .expect("session generation invariant");
        }
        self.generation = generation;
        self.connected = true;
        Ok(())
    }

    /// Invalidate transport/decoder continuity and require a newer generation.
    pub fn invalidate(&mut self, reason: BookError) {
        self.invalidate_observed(reason, |_| {});
    }

    /// Observe previous liquidity before invalidating every selected book.
    /// The synchronous hook must not block or panic.
    /// Hooks can repeat at the same caller offset after a rejected transition;
    /// a caller's elapsed interval must then be zero.
    pub fn invalidate_observed(&mut self, reason: BookError, mut before: impl FnMut(&Book)) {
        self.connected = false;
        for book in self.books.values_mut() {
            before(book);
            book.invalidate(reason);
        }
    }

    /// Apply decoded input without required instrumentation or dynamic dispatch.
    /// Old input is ignored. Any other rejection invalidates all current quotes.
    pub fn apply(
        &mut self,
        generation: u64,
        message: Message,
    ) -> Result<SessionOutcome, SessionError> {
        self.apply_observed(generation, message, |_| {}, |_| {})
    }

    /// Apply with a previous-book hook and optional event completion phases.
    /// The synchronous hooks must not block or panic.
    /// Snapshot reconciliation retains the single-book algorithm's semantics.
    /// Controls and old generations emit no book phase markers.
    pub fn apply_observed(
        &mut self,
        generation: u64,
        message: Message,
        mut before: impl FnMut(&Book),
        phase: impl FnMut(Phase),
    ) -> Result<SessionOutcome, SessionError> {
        if generation < self.generation {
            return Ok(SessionOutcome::OldGeneration);
        }
        let result = if generation != self.generation || !self.connected {
            Err(SessionError::Disconnected)
        } else {
            self.transition(generation, message, &mut before, phase)
        };
        if let Err(error) = &result {
            self.invalidate_observed(error.continuity_reason(), before);
        }
        result
    }

    fn transition(
        &mut self,
        generation: u64,
        message: Message,
        mut before: impl FnMut(&Book),
        phase: impl FnMut(Phase),
    ) -> Result<SessionOutcome, SessionError> {
        let (market, message) = match message {
            Message::Snapshot(snapshot) => (snapshot.market_id, Message::Snapshot(snapshot)),
            Message::Event(event) => (event.order.market_id, Message::Event(event)),
            Message::Error(reason) => return Err(SessionError::Venue(reason)),
            Message::Unsubscribed(_) => return Err(SessionError::Unsubscribed),
            Message::Subscribed(_) | Message::Pong => return Ok(SessionOutcome::Control),
        };
        let book = self
            .books
            .get_mut(&market)
            .ok_or(SessionError::Market(market))?;
        before(book);
        let outcome = match message {
            Message::Snapshot(snapshot) => book.snapshot(generation, snapshot),
            Message::Event(event) => book.event_with_observer(generation, &event, phase),
            _ => unreachable!("control messages return before book lookup"),
        }
        .map_err(|source| SessionError::Book { market, source })?;
        Ok(SessionOutcome::Book {
            market,
            sequence: book.sequence(),
            outcome,
        })
    }
}
