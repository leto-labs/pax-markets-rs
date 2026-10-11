# pax-lab

Offline benchmarks for the SDK. `packet_turnover` measures decode, book mutation,
and combined processing on retained public and deterministic synthetic messages.
It verifies outputs against independent expectations and reports build/workload
identity alongside latency. `protocol_components` isolates TLS and WebSocket work
using authenticated local fixtures, excluding handshake and network time.

```sh
scripts/cargo.sh turnover --smoke
scripts/cargo.sh bench -p pax-lab --bench protocol_components -- --smoke
```

Optional executable features compare allocator choices, SIMD JSON, cycle timing,
and profiling. `demo-integration` adds application/report comparisons; the default
benchmark dependency graph does not include the web server.

See [measurement limits](../../docs/performance.md). Smoke runs check correctness;
retain input identity, build flags, CPU, sample counts, and timer overhead for
performance comparisons.
