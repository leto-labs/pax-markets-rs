#!/usr/bin/env bash
set -euo pipefail

# Explicitly select compiler caching without making sccache a prerequisite.
# https://github.com/mozilla/sccache/blob/main/docs/Rust.md
cache_mode="${PAX_SCCACHE:-auto}"
case "${cache_mode}" in
  auto|0|1) ;;
  *) echo 'PAX_SCCACHE must be auto, 0, or 1' >&2; exit 2 ;;
esac

if [[ "${cache_mode}" != 0 ]] && cache_binary="$(command -v sccache)"; then
  export RUSTC_WRAPPER="${cache_binary}"
  export CARGO_INCREMENTAL=0
  export SCCACHE_DIR="${SCCACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/sccache}"
  export SCCACHE_CACHE_SIZE="${SCCACHE_CACHE_SIZE:-20G}"
elif [[ "${cache_mode}" == 1 ]]; then
  echo 'PAX_SCCACHE=1 requires sccache on PATH' >&2
  exit 1
else
  # Empty environment values override wrappers in user-level Cargo config too.
  export RUSTC_WRAPPER=''
  export RUSTC_WORKSPACE_WRAPPER=''
fi

exec cargo "$@"
