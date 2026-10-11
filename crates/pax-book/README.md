# pax-book

Deterministic bounded reconstruction of aggregated Pax liquidity. Snapshot overlap,
per-market sequence/generation checks, adds, cancels, sweeps, execute-and-rest and
immediate misses share one synchronous engine. Invalid state cannot supply quotes.

Sorted vectors reuse capacity; event application does not clone full ladders.
Supplied level positions are checked against both adjacent prices in constant
time. A mismatched index invalidates the book before mutation.
Liquidity estimates retain exact integer notionals and report depth shortfalls.
The engine has no network, async runtime, storage, or UI dependency.

Modules: `session`, `state`, `apply`, `liquidity`.

`Session` owns selected markets and connection generations. It accepts decoded
messages and returns typed commit, buffered, duplicate, control and obsolete
generation outcomes. A message error invalidates every selected quote; recovery
requires a newer generation and fresh snapshots. Pre-mutation hooks support
elapsed-liquidity accounting without imposing clocks or allocation on callers.

```rust
use pax_book::session::{Session, SessionOutcome};
use pax_types::{protocol::Message, units::MarketId};

let mut session = Session::new(&[MarketId(1)], 1024, 100_000)?;
session.connect(1)?;
assert_eq!(session.apply(1, Message::Pong)?, SessionOutcome::Control);
// A connected transport still needs a reconciled snapshot before quotes exist.
assert!(session.book(MarketId(1)).unwrap().top().is_err());
# Ok::<(), pax_book::session::SessionError>(())
```

[Architecture](../../docs/architecture.md)

Run `scripts/cargo.sh test -p pax-book --locked`. The suite includes 16,000
seeded add/cancel/sweep transitions against independent ordered maps, documented
and mirrored XA events, corruption, synchronization, and exact liquidity tests.

Empty sides use zero price and quantity in expected top-of-book messages. Insertion
into an empty side expects zero tick depth. These are explicitly supported encodings,
not claims that every venue reset/empty-side variant has been observed live.
