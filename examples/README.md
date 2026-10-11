# Public SDK consumer

[`public-book.rs`](public-book.rs) composes only `pax-client`, `pax-types` and
`pax-book`. It uses caller-driven `Feed::inline`, exact decoding and `Session`.
It invalidates quotes on disconnect or malformed data, requests transport recovery
after a rejected input, and waits for the next generation and snapshot.

```sh
# Verify minimal/default client graphs outside workspace feature unification.
python3 scripts/check-sdk-consumer.py
# Also discover a public metadata entry and observe it for fifteen seconds.
python3 scripts/check-sdk-consumer.py --live
```

The runner copies the source and the existing documented synthetic `snapshot.json`
and `xa.json` fixtures into a temporary Cargo project. It preserves the repository
dependency lock, supplies explicit core path dependencies, and uses a separate
target directory. Offline checks compare independently specified integer top
levels after an execute-and-rest event and verify invalidation/recovery. They need
no venue access, credentials, demo, native backend or test-support feature.

Select the three core packages from the same Git revision or checkout. A local
consumer can use:

```toml
[dependencies]
pax-client = { path = "../pax-markets-rs/crates/pax-client", default-features = false }
pax-types = { path = "../pax-markets-rs/crates/pax-types" }
pax-book = { path = "../pax-markets-rs/crates/pax-book" }
tokio = { version = "1.53.2", features = ["macros", "rt", "time"] }
```

Continue polling inline receive to service heartbeats. Keep blocking output and
slow consumers outside ingestion. The example prints only its final observation;
that observation is historical after the feed stops. It demonstrates composition,
not a trading process or performance harness. Convert native units using metadata;
canonical book state stays integer.
