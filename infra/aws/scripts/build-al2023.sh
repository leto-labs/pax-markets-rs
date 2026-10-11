#!/usr/bin/env bash
# Local compatible artifact build; no credentials enter the container/image.
set -euo pipefail

repo_root="${PAX_ARTIFACT_REPO_ROOT:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)}"
if [[ "${PAX_ARTIFACT_FROZEN_SCRIPT:-0}" != 1 ]]; then
  # Bash reads subsequent commands from its script file after a long build.
  # Freeze the launcher so edits cannot change the file under that read cursor.
  frozen_script="$(mktemp)"
  trap 'rm -f -- "$frozen_script"' EXIT
  cp -- "${BASH_SOURCE[0]}" "$frozen_script"
  PAX_ARTIFACT_FROZEN_SCRIPT=1 PAX_ARTIFACT_REPO_ROOT="$repo_root" bash "$frozen_script" "$@"
  exit
fi
cd "$repo_root"
if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo 'Usage: infra/aws/scripts/build-al2023.sh NEW_OUTPUT_DIRECTORY [x86-64|znver4]' >&2
  exit 2
fi
output="$(realpath -m -- "$1")"
cpu="${2:-x86-64}"
case "$cpu" in x86-64|znver4) ;; *) echo 'CPU must be x86-64 or znver4' >&2; exit 2 ;; esac
features="${PAX_ARTIFACT_FEATURES:-}"
default_features="${PAX_ARTIFACT_DEFAULT_FEATURES:-1}"
pgo_scope="${PAX_ARTIFACT_PGO_SCOPE:-none}"
workload="${PAX_ARTIFACT_WORKLOAD:-sdk}"
image="${PAX_ARTIFACT_IMAGE_ID:-}"
if [[ -n "$image" && ! "$image" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo 'Existing build image must be an immutable sha256 ID' >&2
  exit 2
fi
case "$workload" in sdk|ops) ;; *) echo 'Artifact workload must be sdk or ops' >&2; exit 2 ;; esac
if [[ "$workload" == ops && ( -n "$features" || "$default_features" != 1 || "$pgo_scope" != none || "$cpu" != x86-64 ) ]]; then
  echo 'Operator artifacts require portable CPU flags and no SDK feature/PGO selection' >&2
  exit 2
fi
if [[ "$pgo_scope" != none ]]; then
  echo 'Artifact PGO selection is not supported' >&2
  exit 2
fi
case "$default_features" in 0|1) ;; *) echo 'Default-feature selection must be 0 or 1' >&2; exit 2 ;; esac
python3 - "$features" "$workload" <<'PY'
import sys,tomllib
allowed=tomllib.load(open('crates/pax-demo/Cargo.toml','rb'))['features']
requested=sys.argv[1].split(',') if sys.argv[1] else []
if len(set(requested))!=len(requested) or any(name not in allowed for name in requested):
    raise SystemExit('Artifact features must be unique known package feature names')
PY
if [[ -e "$output" ]]; then echo 'Output directory must be new' >&2; exit 2; fi
tick() { python3 -c 'import time; print(time.monotonic_ns())'; }
build_started_ns="$(tick)"
toolchain="$(rustc --print sysroot)"
docker=(docker)
if [[ "${PAX_DOCKER_SUDO:-0}" == 1 ]]; then docker=(sudo -n docker); fi
# An explicit local image keeps paired builds on exactly the same filesystem
# and manifest. Inspect refuses absent images; there is no pull/tag fallback.
if [[ -n "$image" ]]; then
  observed_image="$("${docker[@]}" image inspect --format '{{.Id}} {{.Os}} {{.Architecture}}' "$image")"
  if [[ "$observed_image" != "$image linux amd64" ]]; then
    echo 'Existing build image must match its exact ID and Linux amd64 platform' >&2
    exit 2
  fi
fi
mkdir -p "$output/source" "$output/target" "$output/artifact"
if [[ -n "$image" ]]; then
  printf '%s\n' "$image" > "$output/artifact/build-image.txt"
  printf 'existing\n' > "$output/artifact/build-image-source.txt"
else
  # Bind this run to its build result, independently of concurrent mutable tags.
  # https://docs.docker.com/reference/cli/docker/buildx/build/ (--iidfile)
  "${docker[@]}" build -f infra/aws/build.Dockerfile --iidfile "$output/artifact/build-image.txt" infra/aws
  image="$(< "$output/artifact/build-image.txt")"
  if [[ ! "$image" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo 'Container build did not return an immutable image ID' >&2
    exit 2
  fi
  printf 'built\n' > "$output/artifact/build-image-source.txt"
fi
image_ready_ns="$(tick)"
# Git's ignored-file selection excludes credentials, captures and compiler outputs.
# Include uncommitted public source so incremental development does not need a commit.
git ls-files -c -o --exclude-standard -z | tar --null -T - -cf - | tar -xf - -C "$output/source"
git rev-parse HEAD > "$output/artifact/source-revision.txt"
git status --porcelain > "$output/artifact/source-status.txt"
python3 - "$output/source" "$output/artifact/source-digest.txt" <<'PY'
import hashlib,pathlib,sys
root=pathlib.Path(sys.argv[1])
digest=hashlib.sha256()
for path in sorted(p for p in root.rglob('*') if p.is_file()):
    name=str(path.relative_to(root)).encode()
    data=path.read_bytes()
    digest.update(len(name).to_bytes(8,'little'))
    digest.update(name)
    digest.update(len(data).to_bytes(8,'little'))
    digest.update(data)
pathlib.Path(sys.argv[2]).write_text(digest.hexdigest()+'\n')
PY
registry="${CARGO_HOME:-$HOME/.cargo}/registry"
source_dirty=false
if [[ -s "$output/artifact/source-status.txt" ]]; then source_dirty=true; fi
snapshot_ready_ns="$(tick)"
"${docker[@]}" run --rm --pull=never --user "$(id -u):$(id -g)" \
  -v "$toolchain:/opt/rust:ro" -v "$output/source:/src:ro" \
  -v "$output/target:/target" -v "$output/artifact:/artifact" \
  -v "$registry:/cargo/registry" \
  -e PATH=/opt/rust/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  -e CARGO_HOME=/cargo -e CARGO_TARGET_DIR=/target -e PAX_SCCACHE=0 \
  -e RUSTFLAGS="-C target-cpu=$cpu" -e PAX_ARTIFACT_CPU="$cpu" \
  -e PAX_ARTIFACT_FEATURES="$features" -e PAX_ARTIFACT_DEFAULT_FEATURES="$default_features" \
  -e PAX_ARTIFACT_PGO_SCOPE="$pgo_scope" \
  -e PAX_ARTIFACT_WORKLOAD="$workload" \
  -e PAX_SOURCE_REVISION="$(< "$output/artifact/source-revision.txt")" \
  -e PAX_SOURCE_DIRTY="$source_dirty" -e PAX_SOURCE_DIGEST="$(< "$output/artifact/source-digest.txt")" \
  -e PAX_BUILD_PROFILE=release-perf \
  -w /src "$image" bash /src/infra/aws/scripts/build-al2023-container.sh
python3 - "$output/artifact" "$cpu" "$features" "$default_features" "$workload" "$build_started_ns" "$image_ready_ns" "$snapshot_ready_ns" <<'PY'
import hashlib,json,pathlib,sys,time
directory=pathlib.Path(sys.argv[1])
binary=directory/{'sdk':'pax-demo','ops':'pax-ops'}[sys.argv[5]]
started,image_ready,snapshot_ready=map(int,sys.argv[6:])
binary_digest=hashlib.sha256(binary.read_bytes()).hexdigest()
finished=time.monotonic_ns()
receipt={"schema":1,"platform":"x86_64-unknown-linux-gnu/al2023","cpu":sys.argv[2],
         "workload":sys.argv[5],
         "flags":f"-C target-cpu={sys.argv[2]}","profile":"release-perf",
         "requested_features":sys.argv[3].split(',') if sys.argv[3] else [],
         "default_features":sys.argv[4]=='1',
         "optimization":(directory/"optimization.txt").read_text().strip(),
         "source_revision":(directory/"source-revision.txt").read_text().strip(),
         "source_dirty":bool((directory/"source-status.txt").read_text()),
         "source_digest_sha256":(directory/"source-digest.txt").read_text().strip(),
         "binary_sha256":binary_digest,
         "container_smoke":sys.argv[2]=="x86-64",
         "smoke_kind":({'sdk':'fixture_http','ops':'operator_help_and_offline_checks'}[sys.argv[5]] if sys.argv[2]=='x86-64' else 'unavailable_on_build_cpu'),
         "compiler":(directory/"compiler.txt").read_text(),
         "target_features":[line.split('"')[1] for line in (directory/"target-cfg.txt").read_text().splitlines() if line.startswith('target_feature=')],
         "image_id":(directory/"build-image.txt").read_text().strip(),
         "image_source":(directory/"build-image-source.txt").read_text().strip(),
         "timing_ns":{"image":image_ready-started,"source_snapshot":snapshot_ready-image_ready,
                      "container_and_artifact":finished-snapshot_ready,"total":finished-started},
         "container_timing_ns":json.loads((directory/"container-timing.json").read_text()),
         "timing_scope":"cold monotonic build/check wall time including command overhead; container subphases are nested, not additive to parent phases; not SDK latency"}
(directory/"build.json").write_text(json.dumps(receipt,indent=2)+"\n")
print(f"Built {binary}; sha256={receipt['binary_sha256']}")
PY
