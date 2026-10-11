#!/usr/bin/env bash
# Recompute public add/cancel reference snapshots without the Rust engine.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
fixtures="$repo_root/crates/pax-book/tests/fixtures"
for corpus in public-sample public-active; do
    python3 "$fixtures/derive_add_cancel_snapshot.py" "$fixtures/$corpus.jsonl" \
        | cmp - "$fixtures/$corpus-final.json"
done
