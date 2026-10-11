# pax-demo

Public market-data CLI and small browser demo. `markets` discovers metadata;
`observe` reconstructs selected books; `capture` writes a finite raw journal;
`replay` verifies it and reproduces state without network access.

The shared `pipeline` owns books, exact time-weighted liquidity, recovery decisions,
and bounded sampled traces. `runner` composes it with async transport. Capture
uses a dedicated bounded writer; HTTP and terminal readers consume coalesced
snapshots. A bounded report thread prepares live numeric snapshots; callback and
report costs are separately visible. Inline/queued delivery, read chunk size,
bounded draining and runtime workers are explicit experiment controls. The responsive orderbook and latency pages use native HTML/CSS/JavaScript.

```sh
scripts/cargo.sh demo serve --fixture
scripts/cargo.sh demo markets
scripts/cargo.sh demo observe --markets 1 --seconds 30
scripts/cargo.sh demo capture --markets 1 --seconds 60 --output captures/sample
scripts/cargo.sh demo replay captures/sample
scripts/cargo.sh turnover --smoke
```

Market IDs are explicit and must exist in the fetched metadata. Quantity options
use native base units. Reports distinguish represented depth from unknown deeper
liquidity, shortfalls, disconnected intervals, and quiet synchronized books.
Capture directories must be new. Replay rejects incomplete, lossy, corrupted,
unsupported, and oversized input. Matching state digests exclude timing noise.

See [architecture](../../docs/architecture.md) and [measurements](../../docs/performance.md).

The latency page also keeps a bounded tab-local 1/5-minute trend of the primary
SDK window summaries. It observes at most once per second, preserves stale and
polling gaps, and resets on incompatible source, policy, generation or session.
These overlapping 10-second p50/p99 windows are not independent time buckets.
Browser observation time labels the chart's axis only; it does not enter SDK
latency, and no historical collection runs in ingestion.

The `processing-traces` build group controls optional application clocks;
`telemetry-export` controls diagnostic sidecar persistence. Client measurement
groups are forwarded individually. `--no-default-features` retains public data,
reconstruction, capture/replay and the responsive demo without optional probes.
Use the `capabilities` command to inspect the resolved groups and build identity.

`Pipeline::configure_measurements` accepts an independent
`measurement::policy::ProcessingPolicy` between inputs. Changed domains restart
their own sample phase; unchanged domains keep theirs. The returned revision and
message boundary qualify subsequent samples. Null durations mean unobserved work.

```rust
use pax_demo::measurement::{policy::ProcessingPolicy,capabilities::CAPABILITIES};
assert!(ProcessingPolicy::OFF.validate().is_ok());
assert_eq!(ProcessingPolicy::every(8).validate().is_ok(),CAPABILITIES.processing_traces);
```

Independent collection/output settings are available through
[`measurement::settings::Settings`]. Runtime processing changes use
[`pipeline::Pipeline::configure_measurements`]; live transport changes use the
bounded controller obtained from [`measurement::status::Diagnostics::transport_control`].
The status handle remains readable when a report or file worker is stalled.

```rust
use pax_demo::measurement::settings::Settings;
let mut settings = Settings::off();
settings.publication_ms = 500; // does not select extra ingestion probes
settings.browser_poll_ms = 1000;
settings.validate()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```
