#!/usr/bin/env bash
# The same Rust correctness checks used locally and in CI. No Pax connection.
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

scripts/cargo.sh fmt --all -- --check
scripts/cargo.sh check-all
scripts/cargo.sh lint
scripts/cargo.sh test-all
RUSTDOCFLAGS="${RUSTDOCFLAGS:+$RUSTDOCFLAGS }-D warnings" scripts/cargo.sh doc --workspace --no-deps --locked
scripts/cargo.sh turnover --smoke
scripts/cargo.sh turnover --snapshot-mode all --smoke

scripts/cargo.sh bench -p pax-lab --bench protocol_components --locked -- --smoke --fragmented --read-bytes 17
