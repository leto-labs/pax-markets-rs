# pax-client

Public HTTP metadata and TLS/WebSocket transport. No credentials or account API
are needed. TLS certificate and hostname verification remain enabled.

`Feed::inline` drives the connection in the caller's task without a per-message
handoff. `Feed::spawn` provides bounded background delivery. Both expose ordered
connection generations, subscription acknowledgments, heartbeat, reconnects, and
recovery controls. Keep polling inline `recv` to service the connection; canceling
a pending receive preserves the in-flight operation.

Messages retain their original bytes and local availability time. Queue overflow
ends continuity and reports loss. A separate health handle remains readable when
the consumer stalls. Metadata responses are bounded and validated before use.
The [SDK example](../../examples/README.md) composes this transport with exact
protocol decoding and book reconciliation.

```rust
use pax_client::websocket::FeedConfig;
use pax_types::units::MarketId;
// Use an ID returned by metadata discovery when connecting live.
let config = FeedConfig::public(vec![MarketId(1)]);
assert!(config.receive.is_none());
assert_eq!(config.delivery_sample_every, 0);
config.validate().unwrap();
```

`FeedConfig::read_buffer_bytes` controls WebSocket read chunks (default 128 KiB),
independently of the complete-message limit. Larger messages span multiple reads.
`connect_address` can select an explicit TCP peer while retaining the URL's TLS
hostname verification and SNI. Leave it unset for normal DNS resolution.

## Optional measurements

The default build includes `receive-timestamps`, `receive-stages` (with
`receive-association`), `delivery-traces`, and `diagnostic-handoff`. Collection is
selected separately through `FeedConfig`. Use `default-features = false` in a path
or Git dependency for the minimal client; the packages are not on crates.io.
Unsupported measurement requests fail configuration before connecting.

`FeedConfig::receive` enables bounded socket/TLS/WebSocket diagnostics. Linux
software timestamps are socket-local requests; hardware timestamps require an
explicit interface and readable PHC device. The SDK does not configure NIC filters,
clocks, IRQs or host policies. Timing describes associated read windows, not proven
first contributing packets. Clock uncertainty and incomplete observations remain
explicit. See [measurement boundaries](../../docs/performance.md).

Rates can differ by operation type. `Feed::measurement_control()` accepts bounded
runtime changes and exposes the acknowledged policy. The `*_with_diagnostics`
constructors separate observations from market inputs into a preallocated queue;
drain it outside ingestion and check its independent loss counters. Diagnostic
loss does not invalidate an otherwise complete market-data stream.

Additional features are `clock-model-details` for clock-model annotations,
`phc-background` for a read-only calibration worker, and `socket-readbacks` for
connection-time Linux TCP option queries. Configuration and clock assumptions are
documented on their types in the generated API docs. `measurement::build_info`
records executable provenance outside ingestion; `measurement::schema` describes
observation identities and coverage.

## Checks

```sh
scripts/cargo.sh test -p pax-client --locked
scripts/cargo.sh doc -p pax-client --no-deps --locked
```

Tests use local HTTP/WebSocket/TLS fixtures. The explicit `test-support` feature
exposes authenticated component fixtures for tests and benchmarks; it does not
change live transport. `scripts/cargo.sh run -p pax-client --example public_sample`
performs a separate ten-second public observation.
