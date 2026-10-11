# Retained public feed sample

Source: `wss://api.pax.markets/v1/public/spot/market-data`, production public
market 1, collected 2026-10-07 UTC for ten seconds using the client public-sample
example. Exact application messages are retained, including the subscription
acknowledgment. This is a diagnostic fixture, not the versioned capture format.

The snapshot contained 20 levels on each side. That observation is not a claim
of complete depth or a protocol depth limit. Empty-side and reset encodings were
not observed. This sample contains no account data or credentials.

Observed protocol details: cancellation `order.qty` is negative, while ladder
quantities stay unsigned. A new best ask reports `event_tick_depth: 0` even when
its price improves by several ticks. The engine validates depth as a nonnegative
(saturating) distance from the previous best. The regression covers these cases.

## Active one-minute sample

`public-active.jsonl` retains exact messages from a public CLI recording starting
2026-10-07 21:50:17 UTC, lasting sixty seconds. It contains one 20-level-per-side
snapshot, 272 contiguous events (sequences 45085–45356), one subscription
acknowledgment, and two application pongs. The events comprise 136 additions and
136 cancellations; there are no executions, resets, or observed empty sides.
Reconstruction rejected no message.

The source journal was 133,224 bytes with BLAKE3
`7c9a9746225f4ee867331c6bcdf41fd2feb7150ba83daa014fc3a130498c2a14`.
Live and offline replay produced final state digest
`985b00394c3daa7579fe439daa876c6e106d82c08d859f628426bb2446863a1d`
and identical final liquidity reports. The full versioned recording remains
outside tracked source; this fixture extracts only complete application bytes,
one per line. Its timestamps are provenance, not exchange origination times.

## Independent final-state references

`public-sample-final.json` and `public-active-final.json` are reference snapshots,
not received venue snapshots. `derive_add_cancel_snapshot.py` derives them using
Python integer arithmetic and dictionaries, independently of the Rust engine. It
checks sequence continuity, pre-state, quantities, notional and post-event best
quotes. It deliberately rejects executions and resets; those use separate
documented fixtures and the generated Rust map oracle. Reproduce with:

```sh
scripts/check-fixtures.sh
```

Benchmarks load these final snapshots without calling the event engine to derive
expectations. A consistently wrong optimized transition can no longer define its
own benchmark's expected result.
