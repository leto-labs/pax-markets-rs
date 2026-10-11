#!/usr/bin/env bash
# Compile and exercise additive groups without dependency defaults masking omissions.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
for groups in '' socket-readbacks receive-timestamps clock-model-details receive-association receive-stages delivery-traces processing-traces telemetry-export 'receive-timestamps,processing-traces' 'receive-timestamps,receive-association,delivery-traces,processing-traces' 'receive-stages,delivery-traces,telemetry-export'; do
    feature_args=()
    if [[ -n "$groups" ]]; then
        feature_args=(--features "$groups")
    fi
    scripts/cargo.sh clippy -p pax-demo --all-targets --no-default-features --locked "${feature_args[@]}" -- -D warnings
    scripts/cargo.sh test -p pax-demo --no-default-features --lib --test capabilities --locked "${feature_args[@]}"
done
scripts/cargo.sh test -p pax-client --no-default-features --test capabilities --locked
scripts/cargo.sh test -p pax-client --no-default-features --lib --test capabilities --features socket-readbacks --locked
scripts/cargo.sh test -p pax-client --no-default-features --test capabilities --features receive-stages --locked
scripts/cargo.sh test -p pax-client --no-default-features --test capabilities --features receive-association --locked
scripts/cargo.sh test -p pax-client --no-default-features --test capabilities --features receive-timestamps,receive-association --locked
scripts/cargo.sh test -p pax-client --no-default-features --lib --features receive-timestamps,receive-association --locked
scripts/cargo.sh test -p pax-client --no-default-features --lib --test capabilities --features clock-model-details,receive-association --locked
scripts/cargo.sh test -p pax-client --no-default-features --lib measurement::queue --features diagnostic-handoff --locked
scripts/cargo.sh test -p pax-client --no-default-features --test capabilities --features receive-timestamps,delivery-traces --locked
# A minimal graph must not gain the ancillary platform dependency transitively.
measurement_tree="$(scripts/cargo.sh tree -p pax-client --no-default-features --edges normal --prefix none --locked)"
if [[ "$measurement_tree" =~ (^|$'\n')nix[[:space:]]v ]]; then
    echo 'unexpected timestamp dependency in minimal client' >&2
    exit 1
fi
for groups in '' socket-readbacks receive-timestamps clock-model-details 'receive-timestamps,receive-association' 'receive-stages,processing-traces,telemetry-export'; do
    feature_args=()
    if [[ -n "$groups" ]]; then feature_args=(--features "$groups"); fi
    RUSTDOCFLAGS="${RUSTDOCFLAGS:+$RUSTDOCFLAGS }-D warnings" scripts/cargo.sh doc -p pax-demo -p pax-client --no-default-features --no-deps --locked "${feature_args[@]}"
done
