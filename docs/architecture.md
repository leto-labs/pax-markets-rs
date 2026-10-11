# Architecture

```text
Pax metadata + WebSocket
        │
    pax-client  ── raw messages + connection generations
        │
    pax-types   ── exact protocol values
        │
    pax-book    ── reconciled, validated book state
        │
    pax-demo    ── capture/replay, sampled timing, coalesced views
        │
  HTML + small JavaScript modules
```

`pax-client` owns connection, subscriptions, heartbeat, backoff, and bounded
transport delivery. `Feed::inline` lets the caller drive the connection without
an extra per-message task handoff; `Feed::spawn` provides background delivery.
Both preserve connection generations and signal lost continuity. TLS certificate
and hostname verification stay enabled.

`pax-types` validates metadata and decodes JSON using exact integer units. The
observed compact event format has an allocation-free decoder; other valid layouts
use the general typed parser. Both validate the full input, including UTF-8 and
integer grammar. Prices and quantities are formatted using metadata only at the
presentation boundary.

`pax-book::Session` reconciles snapshots and ordered per-market events. It buffers
bounded pre-snapshot events, handles overlap and duplicates, and invalidates books
on gaps or malformed transitions. The engine checks the event's reported top of
book after adds, cancellations, sweeps, and execute-and-rest transitions. Invalid
or disconnected books cannot provide a current quote. Sorted vectors reuse
capacity, and event application does not clone the entire book.

The demo's `pipeline` applies the same engine to live inputs and replay. Capture
uses a bounded writer; a lossy or incomplete journal is not accepted as a valid
replay. A separate report worker prepares coalesced snapshots. Slow HTTP clients
and browsers do not block ingestion. Browser code separates request ownership,
book rendering, primary latency charts, and optional diagnostic views.

`pax-lab` has two offline benchmarks; `pax-ops` is an optional executable using the
AWS CLI and CloudFormation. Neither is a dependency of the SDK libraries.

## Protocol evidence

The reference is the [official API documentation](https://docs.pax.markets/).
Small public and synthetic inputs in `crates/pax-types/tests/fixtures/` and
`crates/pax-book/tests/fixtures/` retain provenance. Synthetic edge cases are
explicitly examples, not claims about every live venue behavior. Discover active
market IDs through metadata rather than treating documentation examples as a
current market list.

Independent ordered-map tests exercise reconstruction against seeded transitions.
Malformed input, sequence gaps, snapshot overlap, reconnects, exact integer
boundaries, capture corruption, and replay determinism have dedicated tests.
