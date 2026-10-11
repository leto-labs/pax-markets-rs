#!/usr/bin/env bash
# Internal workload, executed only from the builder's frozen /src snapshot.
set -euo pipefail
compile_started_ns=$(python3 -c "import time; print(time.monotonic_ns())")
binary=pax-demo
# An explicit target keeps build scripts portable on the development CPU.
# Target-only znver4 flags must never be executed by host build scripts.
feature_args=()
if [[ "$PAX_ARTIFACT_DEFAULT_FEATURES" == 0 ]]; then feature_args+=(--no-default-features); fi
if [[ -n "$PAX_ARTIFACT_FEATURES" ]]; then IFS=, read -ra requested_features <<< "$PAX_ARTIFACT_FEATURES"; for feature in "${requested_features[@]}"; do feature_args+=(--features "pax-demo/$feature"); done; fi
if [[ "$PAX_ARTIFACT_WORKLOAD" == ops ]]; then
  binary=pax-ops
  scripts/cargo.sh test --target x86_64-unknown-linux-gnu --locked --offline -p pax-ops > /artifact/operator-tests.log 2>&1
  python3 -m unittest discover -s infra/aws/tests -p "test_release_*.py" > /artifact/release-tests.log 2>&1
  scripts/cargo.sh build --target x86_64-unknown-linux-gnu --locked --offline --profile release-perf -p pax-ops
  built=/target/x86_64-unknown-linux-gnu/release-perf
  printf "unprofiled\n" > /artifact/optimization.txt
else
  scripts/cargo.sh build --target x86_64-unknown-linux-gnu --locked --offline --profile release-perf -p pax-demo --bin pax-demo "${feature_args[@]}"
  built=/target/x86_64-unknown-linux-gnu/release-perf
  printf "unprofiled\n" > /artifact/optimization.txt
fi
compile_done_ns=$(python3 -c "import time; print(time.monotonic_ns())")
cp "$built/$binary" "/artifact/$binary"
if [[ "$PAX_ARTIFACT_WORKLOAD" == sdk ]]; then
if [[ "$PAX_ARTIFACT_CPU" == x86-64 ]]; then
  /artifact/pax-demo capabilities > /artifact/capabilities.json
  /artifact/pax-demo serve --fixture --bind 127.0.0.1:8080 > /artifact/smoke.log 2>&1 &
  demo_pid=$!
  trap 'kill "$demo_pid" 2>/dev/null || true' EXIT
  python3 -c 'import json,time,urllib.request
for attempt in range(50):
    try:
        with urllib.request.urlopen("http://127.0.0.1:8080/api/state",timeout=1) as response:
            state=json.load(response)
        assert state
        print("AL2023 fixture HTTP smoke passed")
        break
    except OSError:
        time.sleep(.1)
else:
    raise SystemExit("AL2023 fixture HTTP smoke failed")'
fi
else
  /artifact/pax-ops --help > /artifact/operator-help.txt
fi
rustc --print cfg -C "target-cpu=$PAX_ARTIFACT_CPU" > /artifact/target-cfg.txt
rustc -vV > /artifact/compiler.txt
ldd --version > /artifact/libc.txt
readelf --version-info "/artifact/$binary" > /artifact/elf-versions.txt
python3 -c "import json,sys,time; a,b=map(int,sys.argv[1:]); print(json.dumps(dict(build_and_checks=b-a,packaging_smoke_and_metadata=time.monotonic_ns()-b)))" "$compile_started_ns" "$compile_done_ns" > /artifact/container-timing.json
