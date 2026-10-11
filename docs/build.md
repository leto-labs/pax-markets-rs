# Build and test

The local demo needs Rust via rustup, Git, and a C/C++ build toolchain for the
TLS dependency. Rust, Clippy, and rustfmt are pinned by `rust-toolchain.toml`;
rustup selects that toolchain when you run Cargo in this checkout.

On Debian/Ubuntu, install the native tools with:

```sh
sudo apt-get update
sudo apt-get install -y build-essential git
```

On macOS, install Xcode Command Line Tools (`xcode-select --install`). See the
TLS provider's [Linux](https://aws.github.io/aws-lc-rs/requirements/linux.html)
and [macOS](https://aws.github.io/aws-lc-rs/requirements/apple.html) prerequisites.
Validation and the hosted demo currently run on Linux x86_64; Linux receive
timestamps and hardware clock options are platform-specific.

The first Cargo build downloads dependencies. After
`scripts/cargo.sh fetch --locked`, fixture runs and tests can use `--offline`.
Python 3.11+ is needed for repository tooling, Node.js 22+ for browser tests,
and ShellCheck for shell linting. These are not demo runtime dependencies.
Docker and AWS CLI v2 are only needed for the optional deployment workflow.

Use `scripts/cargo.sh` for Cargo commands. It enables sccache when available and
disables incremental compilation while caching. `PAX_SCCACHE=0` opts out;
`PAX_SCCACHE=1` requires it. CI installs the version in `.sccache-version`.
`SCCACHE_DIR` and `SCCACHE_CACHE_SIZE` can override local cache settings.

```sh
scripts/check.sh                       # format, check, lint, tests, rustdoc, bench smoke
scripts/check-measurements.sh          # minimal/additive instrumentation builds
python3 scripts/check-sdk-consumer.py  # isolated minimal/default SDK integration
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
node --test crates/pax-demo/tests/browser/*.test.mjs
scripts/check-fixtures.sh
```

`test-all`, `check-all`, `lint`, `demo`, and `turnover` are Cargo aliases in
`.cargo/config.toml`. `scripts/cargo.sh doc --workspace --no-deps` produces API docs
in `target/doc`. Each library includes its README as crate-level documentation.

The five top-level scripts are the Cargo wrapper, the main check command, and
checks for fixture integrity, SDK consumers, and measurement feature combinations.
Their tests live in `scripts/tests/`. Optional AWS build/release helpers and tests
are grouped under `infra/aws/`; none runs as part of starting the local demo.

`build-support/build_info.rs` embeds commit, compiler and feature metadata at build
time. `pax-demo capabilities` displays it, and captures/benchmarks retain it for
reproducibility. It does not read credentials or run Git during ingestion.

Browser integration scripts in `crates/pax-demo/tests/browser/*.playwright.js`
run against `serve --fixture` on port 8080 using `playwright-cli run-code
--filename=…`. They check mobile/desktop overflow, charts, pause/resume, stale
states, escaping, and focus/scroll preservation. These are development tools;
there is no frontend bundler or browser dependency in the shipped demo.

Portable binaries use the default CPU target. `release-perf` keeps debug symbols
with release optimization. `RUSTFLAGS='-C target-cpu=native'` is a local benchmark
choice: do not distribute that binary to an incompatible CPU.

Optional allocator, SIMD decoder comparison, cycle-clock, and profiler benchmarks
live in `pax-lab`. The default allocator remains the system allocator. Check all
optional benchmark code with:

```sh
scripts/cargo.sh clippy -p pax-lab --all-features --all-targets -- -D warnings
```

For AL2023-compatible deployment binaries, use `infra/aws/scripts/build-al2023.sh` as
explained in [deployment](deploy.md). The builder snapshots non-ignored source;
keep secrets and bulk captures in ignored directories. Source changes after the
snapshot cannot alter an in-progress build.
