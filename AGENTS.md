# Contributor notes

This is an independent public Pax Markets orderbook client and demo.

- Check the directory, branch, and Git status before editing; preserve unrelated work.
- Start with `README.md` and `docs/build.md`. Keep documentation short and describe
  implemented behavior. Do not add a separate planning framework.
- Keep content suitable for public release: no credentials, private project names,
  account identifiers, internal deployment receipts, or paths to other repositories.
- Verify protocol changes against <https://docs.pax.markets/> and small public
  fixtures with source and capture time. Discover markets from metadata.
- Use workspace dependencies and lints. Keep the book engine independent of
  transport, runtime, storage, and UI. Use `name.rs`, not handwritten `mod.rs`.
- Default to safe Rust. Isolate justified unsafe code, state its invariants, and
  verify equivalent behavior. Use typed library errors and documented public APIs.
- Preserve exact prices, quantities, sequences, and signed deltas. A usable book
  needs a reconciled snapshot and contiguous events. Gaps invalidate quotes and
  require recovery; duplicates and pre-snapshot events need explicit handling.
- Bound queues. Capture loss must be visible and invalidate replay. Live ingestion,
  replay, tests, and benchmarks must use the same deterministic transitions.
- Keep rendering and slow outputs outside ingestion. Maintain responsive pages at
  360 CSS pixels, accessible controls, and independent feed/display freshness.
- SDK latency ends at validated book commit. Label read-window proxies and clock
  uncertainty; do not claim exchange-origin latency from RTT or traceroute.
  Phase percentages must use matched-event totals, not sums of phase percentiles.
- Benchmark offline fixtures before optimizing. Record workload, build, CPU,
  allocator, sample counts, and percentiles. Keep measurements optional and bounded.
- Use `scripts/cargo.sh`; run focused checks, then relevant workspace checks.
  Review correctness, simplicity, security, and performance before committing.
- Do not publish packages or send messages to third parties without authorization.
