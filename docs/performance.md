# Latency and benchmarks

The primary SDK metric is **first measurable associated ingress → validated book
commit**. The UI prefers a qualified first-observed NIC or kernel receive window.
A completion-read batch is a separately labeled fallback. These are read-window
proxies: buffering, TLS records, and TCP timestamp placement prevent a claim that
we observed the exact first contributing packet.

| Phase | What it contains |
| --- | --- |
| RX timestamp → observed read end | Kernel delivery, wakeup/scheduling, socket read, and timestamp alignment |
| Between associated reads | Input availability, transport progress, and scheduling |
| Last read → complete message | Remaining TLS authentication/decryption, WebSocket framing, and buffered residence |
| Complete message → decode | Consumer handoff/dispatch |
| Decode → validated commit | JSON decode, book checks, mutation, validation |

The table and flame chart select the actual events at p50 and p99 **total**
latency. Their phase percentages add to that event's total. Independently measured
phase percentiles generally do not add up. The 1/5-minute graph records overlapping
10-second sample windows in the current tab, not independent per-second buckets.

Clock alignment carries an explicit uncertainty estimate. Missing clocks or
ambiguous associations stay unavailable. Socket/TLS active-work measurements are
separate diagnostics, not additional spans to add to the primary timeline. Their
wall time can include preemption and does not isolate cryptographic instructions.

Heartbeat RTT and optional route probes describe a round trip. RTT/2 assumes a
symmetric route; it is not an exchange event timestamp. True exchange-origin
latency needs a documented source timestamp and an adequate clock model. Browser
frame callbacks estimate a render opportunity, not physical pixel presentation.

## Live latency demo

Discover a current market with `scripts/cargo.sh demo markets`, then replace
`MARKET_ID` below. This Linux command uses the hosted demo's optimized build,
inline delivery, dedicated ingestion and one-in-eight message sampling:

```sh
scripts/cargo.sh run --locked --profile release-perf -p pax-demo -- \
  --runtime-workers 2 serve --markets MARKET_ID \
  --delivery inline --dedicated-ingestion \
  --sample-every 8 --receive-diagnostics \
  --receive-software --receive-sample-every 8
```

Open `http://127.0.0.1:8080/latency`. Sampling emitted messages does not remove
all per-read instrumentation cost. The hosted application also uses explicit
CPU affinity, a configured NIC hardware clock, and optional route probes; these
depend on the host. This command uses kernel software timestamps. Matching
application settings does not reproduce another host's network or clock accuracy.

Plain `scripts/cargo.sh demo ...` uses a debug build and is intended for functional
checks. The fixture demo uses historical inputs at an artificial cadence; it
does not measure live network latency.

## Offline benchmarks

```sh
scripts/cargo.sh turnover --smoke
scripts/cargo.sh bench -p pax-lab --no-default-features --bench packet_turnover
scripts/cargo.sh bench -p pax-lab --bench protocol_components -- --smoke --fragmented --read-bytes 17
```

The first workload reports decode, apply, combined, and instrumented durations,
timer overhead, input/state hashes, CPU, compiler, and build flags. Loading,
resetting, and final-state verification are untimed. Public and deterministic
synthetic workloads cover different depths and message forms. Treat smoke runs as
correctness checks, not latency evidence.

The component workload isolates TLS and WebSocket work on authenticated offline
fixtures. Its timings exclude the network and handshake. Use `--help` on either
benchmark for workload selection. Preserve the emitted identity and sample count
when comparing results; do not compare unrelated hosts or instrumented and
uninstrumented runs as if they were controlled pairs.

## Optional measurements

Client build groups include `receive-timestamps`, `receive-association`,
`receive-stages`, `delivery-traces`, and `diagnostic-handoff`. Optional
`clock-model-details`, `phc-background`, and `socket-readbacks` add specialized
clock/socket diagnostics. The demo adds `processing-traces` and `telemetry-export`.
`--no-default-features` keeps reconstruction, capture/replay, and the demo without
optional probes. `pax-demo capabilities` reports the compiled groups.

Runtime policies independently sample receive operations, delivery, processing,
and browser timing. Publication cadence is separate from sampling. Diagnostic
queues are bounded and expose drops; overload cannot silently become a claim of
complete measurement coverage. The SDK never changes NIC filters, IRQ placement,
or host clock configuration on its own.

Allocator (`mimalloc`), SIMD JSON (`simd-experiment`), cycle-clock
(`cputicks-experiment`), allocation accounting (`allocation-profile`), and profiler
(`hotpath`) comparisons remain explicit benchmark features. The normal SDK uses
safe Rust and portable OS networking. No kernel-bypass runtime is required.
