# Pax Markets Rust

A small Rust project for exploring [Pax Markets](https://docs.pax.markets/) public
orderbooks: reconstruct the book, watch liquidity, and see where update latency goes.

The SDK uses exact integers, bounded queues, and explicit recovery when a stream
loses continuity. The demo is plain HTML, CSS, and JavaScript, with responsive book
ladders, latency flame charts, and a rolling 1/5-minute view. Public data needs no
account or API key. This is an independent project, not an official Pax client.

## Try it

Start with the [build prerequisites](docs/build.md): Rust via rustup and a native
C/C++ toolchain. From the repository root:

```sh
# Offline demo using a retained public fixture.
scripts/cargo.sh demo serve --fixture
# Open http://127.0.0.1:8080 (orderbook) or /latency.

# Discover current markets before choosing subscriptions.
scripts/cargo.sh demo markets
# Replace MARKET_ID with an ID returned above.
scripts/cargo.sh run --locked --profile release-perf -p pax-demo -- \
  serve --markets MARKET_ID --delivery inline --dedicated-ingestion
```

The HTML and JavaScript are embedded in the Rust binary. Running the demo needs
no Node.js, Docker, AWS account, or configuration file. `scripts/cargo.sh` is a
small optional sccache wrapper; ordinary `cargo` accepts the same arguments.
For the Linux receive-to-book latency view, use the
[instrumented demo command](docs/performance.md#live-latency-demo).

`Last book update` means time since the last accepted market event. It can be tens
of seconds old on a quiet market. `Display refreshed` separately tells you whether
the browser is receiving current publications. A disconnected feed or stalled
publisher is marked stale.

Rejected inputs invalidate the book and trigger resynchronization. Once fresh
books are committed, the last rejection remains in collapsed recovery history;
an unresolved rejection stays visible as a pending recovery warning.

## Use the SDK

[`examples/public-book.rs`](examples/public-book.rs) composes the three library
crates without the web server. See [the consumer example](examples/README.md) for
path dependencies and an offline integration check.

| Crate | Responsibility |
| --- | --- |
| `pax-types` | Exact units, metadata validation, JSON protocol decoding |
| `pax-client` | Public metadata, authenticated TLS/WebSocket transport, reconnects |
| `pax-book` | Deterministic snapshot/event reconciliation and liquidity queries |
| `pax-demo` | CLI, capture/replay, sampled measurements, tiny web UI |
| `pax-lab` | Offline decode/book and TLS/WebSocket benchmarks |
| `pax-ops` | Optional namespaced AWS deployment |

The book engine has no async, network, or web dependencies. No order submission
or account API is implemented. The current transport uses the normal OS network
stack; measurements and allocator experiments are optional.

## Develop

```sh
scripts/check.sh
node --test crates/pax-demo/tests/browser/*.test.mjs
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
scripts/cargo.sh turnover --smoke
```

- [Build and test](docs/build.md)
- [Architecture and protocol handling](docs/architecture.md)
- [Latency measurements and benchmarks](docs/performance.md)
- [Optional AWS deployment](docs/deploy.md)

MIT licensed. Version 0.0.1 is available as source; Cargo publishing is disabled.
