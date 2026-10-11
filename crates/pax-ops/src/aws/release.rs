//! Verified versioned artifact transfer and restricted systemd activation through SSM.

mod timing;

use super::Api;
use crate::{config::Scope, error::Error};
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use std::{
    path::{Path, PathBuf},
    process::Command,
};

/// Artifact prepared locally, with a verified manifest and immutable transfer identity.
#[derive(Debug, Clone)]
pub struct Artifact {
    /// Compressed release bundle.
    pub bundle: PathBuf,
    /// SHA-256 of the entire transferred bundle.
    pub digest: String,
    /// Verified source/build receipt.
    pub build: Value,
}
/// Hash a bounded local artifact with GNU coreutils; this work never runs in SDK ingestion.
pub fn sha256(path: &Path) -> Result<String, Error> {
    let metadata = path.symlink_metadata()?;
    if !metadata.is_file() || metadata.len() > 1024 * 1024 * 1024 {
        return Err(Error::Config(
            "artifact must be a regular file no larger than 1 GiB".into(),
        ));
    }
    let output = Command::new("sha256sum").arg("--").arg(path).output()?;
    let hash = std::str::from_utf8(&output.stdout)
        .ok()
        .and_then(|s| s.split_whitespace().next())
        .unwrap_or("");
    if !output.status.success() || hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(Error::Config("artifact SHA-256 calculation failed".into()));
    }
    Ok(hash.into())
}
/// Prepare or verify/reuse an identical bundle. Existing bundles are never overwritten.
pub fn prepare(directory: &Path, bundle: &Path) -> Result<Artifact, Error> {
    let build: Value = serde_json::from_reader(std::fs::File::open(directory.join("build.json"))?)?;
    if build["platform"] != "x86_64-unknown-linux-gnu/al2023"
        || !matches!(build["cpu"].as_str(), Some("x86-64" | "znver4"))
    {
        return Err(Error::Config("release must be an AL2023-compatible x86_64 build with an explicit supported CPU target".into()));
    }
    if build["binary_sha256"] != sha256(&directory.join("pax-demo"))? {
        return Err(Error::Config(
            "release binary does not match its build receipt".into(),
        ));
    }
    let features = build["target_features"].as_array().ok_or_else(|| {
        Error::Config("release requires the compiler's complete target-feature receipt".into())
    })?;
    if features.is_empty() || features.iter().any(|f| f.as_str().is_none()) {
        return Err(Error::Config("invalid target-feature receipt".into()));
    }
    let caps_path = directory.join("capabilities.json");
    if caps_path.exists() {
        let caps: Value = serde_json::from_reader(std::fs::File::open(caps_path)?)?;
        verify_capabilities(&build, &caps)?;
    } else if build["cpu"] == "x86-64" {
        return Err(Error::Config(
            "portable release requires a compatible smoke receipt".into(),
        ));
    }
    let mut files = std::fs::read_dir(directory)?
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<Vec<_>, _>>()?;
    files.sort();
    if files
        .iter()
        .filter(|p| p.file_name().is_some_and(|n| n != "SHA256SUMS"))
        .count()
        >= 128
    {
        return Err(Error::Config("release file count exceeds bounds".into()));
    }
    let mut checksums = String::new();
    let mut bytes = 0u64;
    for path in files {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| Error::Config("release filename must be UTF-8".into()))?;
        if name.len() > 96
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_ .".contains(&b))
            || name.contains(' ')
        {
            return Err(Error::Config(
                "release filenames must be simple ASCII names".into(),
            ));
        }
        if name == "SHA256SUMS" {
            continue;
        }
        bytes = bytes
            .checked_add(path.symlink_metadata()?.len())
            .ok_or_else(|| Error::Config("release size overflow".into()))?;
        if bytes > 1024 * 1024 * 1024 - 1024 * 1024 {
            return Err(Error::Config(
                "release payload exceeds archive bound".into(),
            ));
        }
        checksums.push_str(&format!("{}  {name}\n", sha256(&path)?));
    }
    if bundle.symlink_metadata().is_ok() {
        // Keep repeated preparation read-only. The helper checks every member
        // against current staged bytes and rejects aliases without extraction.
        sha256(bundle)?;
        let verified = Command::new("python3")
            .arg("-c")
            .arg(include_str!(
                "../../../../infra/aws/scripts/release_bundle.py"
            ))
            .arg(directory)
            .arg(bundle)
            .output()?;
        if !verified.status.success() {
            return Err(Error::Config("existing release bundle differs from staged files or archive bounds; use a new destination".into()));
        }
        return Ok(Artifact {
            digest: sha256(bundle)?,
            bundle: bundle.into(),
            build,
        });
    }
    if directory.join("SHA256SUMS").symlink_metadata().is_ok() {
        sha256(&directory.join("SHA256SUMS"))?;
    }
    std::fs::write(directory.join("SHA256SUMS"), checksums)?;
    let output = Command::new("tar")
        .args(["-czf"])
        .arg(bundle)
        .arg("-C")
        .arg(directory)
        .arg(".")
        .output()?;
    if !output.status.success() {
        let _ = std::fs::remove_file(bundle);
        return Err(Error::Config("release bundle creation failed".into()));
    }
    Ok(Artifact {
        digest: sha256(bundle)?,
        bundle: bundle.into(),
        build,
    })
}
/// Runtime inputs for a demo release, independent of SDK build defaults.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Options {
    /// Explicit stable-target authorization; not part of runtime configuration.
    #[serde(skip)]
    pub activate_stable: bool,
    /// Refuse mutation if the existing process is not already a healthy match.
    #[serde(skip)]
    pub require_noop: bool,
    /// Explicit active markets discovered from public metadata.
    pub markets: Vec<u64>,
    /// Optional target-local ingestion CPU.
    pub ingest_cpu: Option<usize>,
    /// Optional distinct background CPU, requiring ingestion CPU.
    pub background_cpu: Option<usize>,
    /// Explicit qualified NIC; timestamp generation/filter administration is separate.
    pub hardware_interface: Option<String>,
    /// Qualified read-only PHC device or stable ENA alias.
    pub hardware_clock: Option<PathBuf>,
    /// Health deadline; synchronized books and a connected feed are required.
    pub health_seconds: u32,
}
/// Publish an already prepared bundle without changing any guest or service.
pub fn publish(
    api: &mut impl Api,
    scope: &Scope,
    bucket: &str,
    artifact: &Artifact,
) -> Result<Value, Error> {
    super::verify_account(api, scope)?;
    let key = release_key(scope, bucket, artifact)?;
    api.upload(bucket, &key, &artifact.bundle)?;
    Ok(
        json!({"bucket":bucket,"key":key,"artifact_sha256":artifact.digest,"build":artifact.build,"activation":"none; artifact uploaded without service activation"}),
    )
}

fn release_key(scope: &Scope, bucket: &str, artifact: &Artifact) -> Result<String, Error> {
    if !bucket.starts_with(&format!("pax-{}-{}-", scope.account, scope.region))
        || bucket.len() > 63
        || !bucket
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-.".contains(&b))
        || artifact.digest.len() != 64
        || !artifact.digest.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(Error::Config(
            "release bucket is outside the expected project/account/region".into(),
        ));
    }
    Ok(format!("releases/{}/bundle.tar.gz", artifact.digest))
}

/// Transfer, checksum and activate an immutable release; rollback on failed live health.
pub fn deploy(
    api: &mut impl Api,
    scope: &Scope,
    instance: &str,
    bucket: &str,
    artifact: &Artifact,
    options: &Options,
) -> Result<Value, Error> {
    let validation_started = Instant::now();
    verify_instance(api, scope, instance)?;
    if !options.activate_stable {
        return Err(Error::Config(
            "stable deployment requires explicit --activate-stable".into(),
        ));
    }
    if options.markets.is_empty()
        || options.markets.len() > 16
        || options.markets.contains(&0)
        || !(10..=180).contains(&options.health_seconds)
        || (options.background_cpu.is_some() && options.ingest_cpu.is_none())
        || (options.hardware_interface.is_some() != options.hardware_clock.is_some())
    {
        return Err(Error::Config(
            "release requires bounded markets/health and valid CPU options".into(),
        ));
    }
    let key = release_key(scope, bucket, artifact)?;
    let config = blake3::hash(&serde_json::to_vec(options)?)
        .to_hex()
        .to_string();
    let id = format!("{}-{}", artifact.digest, &config[..16]);
    let markets = options
        .markets
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let mut args = format!(
        "--runtime-workers 2 serve --markets {markets} --bind 0.0.0.0:80 --delivery inline --dedicated-ingestion --sample-every 8 --receive-diagnostics --receive-software --receive-sample-every 8 --route-diagnostics"
    );
    if let Some(cpu) = options.ingest_cpu {
        args.push_str(&format!(" --ingest-cpu {cpu}"));
    }
    if let Some(cpu) = options.background_cpu {
        args.push_str(&format!(" --background-cpu {cpu}"));
    }
    if let (Some(interface), Some(clock)) = (&options.hardware_interface, &options.hardware_clock) {
        let clock = clock
            .to_str()
            .ok_or_else(|| Error::Config("PHC path must be UTF-8".into()))?;
        if interface.is_empty()
            || interface.len() > 15
            || !interface
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
            || !(clock == "/dev/ptp_ena"
                || clock.strip_prefix("/dev/ptp").is_some_and(|suffix| {
                    !suffix.is_empty() && suffix.bytes().all(|c| c.is_ascii_digit())
                }))
        {
            return Err(Error::Config(
                "hardware source requires an explicit interface and /dev/ptpN or /dev/ptp_ena"
                    .into(),
            ));
        }
        args.push_str(&format!(
            " --receive-interface {interface} --receive-phc {clock}"
        ));
    }
    let unit = format!(
        "[Unit]\nDescription=Pax read-only public market-data demo\nAfter=network-online.target\nWants=network-online.target\n[Service]\nUser=pax\nGroup=pax\nWorkingDirectory=/var/lib/pax\nExecStart=/opt/pax/current/pax-demo {args}\nRestart=on-failure\nRestartSec=5\nKillSignal=SIGINT\nTimeoutStopSec=20\nAmbientCapabilities=CAP_NET_BIND_SERVICE\nCapabilityBoundingSet=CAP_NET_BIND_SERVICE\nNoNewPrivileges=true\nProtectSystem=strict\nProtectHome=true\nPrivateTmp=true\nReadWritePaths=/var/lib/pax\nLimitNOFILE=8192\n[Install]\nWantedBy=multi-user.target\n"
    );
    let validation_ns = validation_started.elapsed().as_nanos();
    let request = serde_json::to_string(
        &json!({"release":id,"build":artifact.build,"unit":unit,"markets":options.markets.iter().map(u64::to_string).collect::<Vec<_>>()}),
    )?;
    // A JSON string literal is also a Python string literal here. Double
    // encoding preserves backslashes/newlines without heredoc interpolation.
    let request_literal = serde_json::to_string(&request)?;
    let noop = format!(
        "if python3 - <<'PAX_NOOP'\n{}\nimport sys\nresult=check(json.loads({request_literal}))\nprint(json.dumps(result))\nsys.exit(0 if result['eligible'] else 1)\nPAX_NOOP\nthen\n  echo 'pax_release_noop=1'\n  exit 0\nfi\n",
        include_str!("../../../../infra/aws/scripts/release_noop.py")
    );
    let preflight = format!(
        "#!/bin/bash\nset -euo pipefail\ninstall -d -o root -g root -m 0700 /var/lib/pax-ops\nexec 9>/var/lib/pax-ops/operator.lock\nflock -w 30 9\n{noop}"
    );
    let preflight_started = Instant::now();
    let checked = command(api, scope, instance, &preflight, 60)?;
    let preflight_ns = preflight_started.elapsed().as_nanos();
    if no_op(&checked) {
        return Ok(
            json!({"release":id,"artifact_sha256":artifact.digest,"instance":instance,"health":"connected and synchronized","noop":true,"guest_timing":{"preflight":timing::read(&checked),"activation":null},"ssm":checked,"timing_ns":{"validation":validation_ns,"preflight":preflight_ns,"upload":0,"activation":0},"timing_scope":"cold operator phases; guest timings are nested in preflight/activation, never additive or SDK latency"}),
        );
    }
    if options.require_noop {
        // Only expose the predicate's fixed vocabulary, never arbitrary guest
        // stdout (which can contain service arguments or infrastructure data).
        let reason = checked["StandardOutputContent"]
            .as_str()
            .and_then(|text| {
                text.lines().find_map(|line| {
                    let value: Value = serde_json::from_str(line).ok()?;
                    let reason = value["reason"].as_str()?;
                    (value["kind"] == "pax_release_preflight"
                        && matches!(
                            reason,
                            "request"
                                | "release"
                                | "unit"
                                | "build"
                                | "binary"
                                | "service"
                                | "process"
                                | "health"
                                | "changed_process"
                        ))
                    .then(|| reason.to_owned())
                })
            })
            .unwrap_or_else(|| "unavailable".into());
        return Err(Error::Config(format!(
            "existing deployment does not qualify for --require-noop ({reason}); no upload or activation performed"
        )));
    }
    let upload_started = Instant::now();
    api.upload(bucket, &key, &artifact.bundle)?;
    let upload_ns = upload_started.elapsed().as_nanos();
    let script = format!(
        r#"#!/bin/bash
set -euo pipefail
install -d -o root -g root -m 0700 /var/lib/pax-ops
exec 9>/var/lib/pax-ops/operator.lock
flock -w 30 9
release=/opt/pax/releases/{id}
{noop}
tick() {{ python3 -c 'import time; print(time.monotonic_ns())'; }}
stage_started=$(tick)
install -d -o root -g root -m 0755 /opt/pax /opt/pax/releases
if ! id pax >/dev/null 2>&1; then useradd --system --home-dir /var/lib/pax --shell /sbin/nologin pax; fi
install -d -o pax -g pax -m 0750 /var/lib/pax /var/lib/pax/captures /var/lib/pax/diagnostics
if [[ ! -d "$release" ]]; then
  stage=$(mktemp -d /opt/pax/releases/.stage-XXXXXX)
  trap 'rm -rf "$stage"' EXIT
  aws s3api get-object --bucket {bucket} --key {key} "$stage/bundle.tar.gz" >/dev/null
  echo '{digest}  '"$stage/bundle.tar.gz" | sha256sum -c -
  mkdir "$stage/extracted"
  tar -xzf "$stage/bundle.tar.gz" --no-same-owner --same-permissions -C "$stage/extracted"
  (cd "$stage/extracted" && sha256sum -c SHA256SUMS)
  mv "$stage/extracted" "$release"
else
  (cd "$release" && sha256sum -c SHA256SUMS)
fi
# A private source directory can contribute a 0700 archive root. The service
# needs traversal; keep the installed executable tree root-owned and immutable
# to its user, independently of the staging directory's mode and caller umask.
chmod 0755 "$release"
[[ $(uname -m) == x86_64 ]] || {{ echo 'release architecture mismatch' >&2; exit 1; }}
python3 - "$release/build.json" <<'PY'
import json,sys
build=json.load(open(sys.argv[1]))
rows=[set(line.split(':',1)[1].split()) for line in open('/proc/cpuinfo') if line.startswith('flags')]
if not rows: raise SystemExit('target CPU flags unavailable')
flags=set.intersection(*rows)
aliases={{'cmpxchg16b':'cx16','sse3':'pni','sse4.1':'sse4_1','sse4.2':'sse4_2','sha':'sha_ni','lzcnt':'abm','avx512bf16':'avx512_bf16','avx512bitalg':'avx512_bitalg','avx512vbmi2':'avx512_vbmi2','avx512vnni':'avx512_vnni','avx512vpopcntdq':'avx512_vpopcntdq'}}
required={{aliases.get(f,f) for f in build['target_features']}}
missing=required-flags
if missing: raise SystemExit('release CPU feature mismatch: '+','.join(sorted(missing)))
PY
"$release/pax-demo" capabilities > "$release/effective-capabilities.json"
python3 - "$release/build.json" "$release/effective-capabilities.json" <<'PY'
import json,sys
build,caps=(json.load(open(p)) for p in sys.argv[1:])
for expected,actual in [('source_revision','revision'),('source_digest_sha256','source_digest'),('profile','profile')]:
    if build[expected]!=caps[actual]: raise SystemExit('target executable provenance mismatch: '+actual)
if 'optimization' in build and build['optimization']!=caps.get('optimization'):
    raise SystemExit('target executable optimization provenance mismatch')
PY
stage_done=$(tick)
cat > "$release/pax-demo.service" <<'UNIT'
{unit}UNIT
old=''
if [[ -L /opt/pax/current && -f /opt/pax/current/pax-demo.service ]]; then
  old=$(readlink -f /opt/pax/current)
  [[ "$old" == /opt/pax/releases/* ]] || {{ echo 'current release is outside managed storage' >&2; exit 1; }}
fi
verify_previous_release() {{
  python3 - "$old/pax-demo.service" <<'PY'
{health_helper}
import sys
with open(sys.argv[1]) as unit:
    markets=unit_markets(unit.read(65537))
result=wait(markets, {health})
result['kind']='pax_release_restored_health'
print(json.dumps(result))
if not result['healthy']: raise SystemExit('previous release did not synchronize')
PY
}}
{activation_helper}
arm_release_rollback
ln -sfn "$release" /opt/pax/current.next
mv -Tf /opt/pax/current.next /opt/pax/current
ln -sfn /opt/pax/current/pax-demo.service /etc/systemd/system/pax-demo.service
systemctl daemon-reload
systemctl enable --force pax-demo.service >/dev/null
systemctl restart pax-demo.service
health_started=$(tick)
if ! python3 - "$stage_started" "$stage_done" "$health_started" <<'PY'
{health_helper}
import sys
result=wait('{markets}'.split(','), {health})
print(json.dumps(result))
timing=phases(*map(int,sys.argv[1:]),time.monotonic_ns())
print(json.dumps(timing))
if not result['healthy']:
    print(json.dumps(timing),file=sys.stderr)
    raise SystemExit('release failed live synchronization health')
PY
then
  exit 1
fi
if [[ -n "$old" && "$old" != "$release" ]]; then ln -sfn "$old" /opt/pax/previous; fi
rollback_armed=0
printf '%s\n' 'release={id}'
"#,
        digest = artifact.digest,
        health = options.health_seconds,
        health_helper = include_str!("../../../../infra/aws/scripts/release_health.py"),
        activation_helper = include_str!("../../../../infra/aws/scripts/release-activation.sh")
    );
    let activation_started = Instant::now();
    // Candidate and restored-release health each consume their own finite budget.
    let receipt = command(
        api,
        scope,
        instance,
        &script,
        options.health_seconds * 2 + 120,
    )?;
    let activation_ns = activation_started.elapsed().as_nanos();
    Ok(
        json!({"release":id,"artifact_sha256":artifact.digest,"instance":instance,"health":"connected and synchronized","noop":no_op(&receipt),"guest_timing":{"preflight":timing::read(&checked),"activation":timing::read(&receipt)},"ssm":receipt,"timing_ns":{"validation":validation_ns,"preflight":preflight_ns,"upload":upload_ns,"activation":activation_ns},"timing_scope":"cold operator phases; guest timings are nested in preflight/activation, never additive or SDK latency"}),
    )
}

fn no_op(receipt: &Value) -> bool {
    receipt["StandardOutputContent"]
        .as_str()
        .is_some_and(|text| text.lines().any(|line| line == "pax_release_noop=1"))
}

fn verify_capabilities(build: &Value, caps: &Value) -> Result<(), Error> {
    if caps["revision"] != build["source_revision"]
        || caps["source_digest"] != build["source_digest_sha256"]
        || caps["profile"] != build["profile"]
        || (build.get("optimization").is_some() && caps["optimization"] != build["optimization"])
    {
        return Err(Error::Config(
            "executable capabilities disagree with staged build provenance".into(),
        ));
    }
    Ok(())
}

/// Run a bounded SSM shell command on an explicitly owned instance.
/// Output is returned for non-secret inspection, never for credential provisioning.
pub fn command(
    api: &mut impl Api,
    scope: &Scope,
    instance: &str,
    script: &str,
    seconds: u32,
) -> Result<Value, Error> {
    execute(api, scope, instance, script, seconds)
}

fn execute(
    api: &mut impl Api,
    scope: &Scope,
    instance: &str,
    script: &str,
    seconds: u32,
) -> Result<Value, Error> {
    if seconds == 0 || seconds > 3600 {
        return Err(Error::Config("SSM deadline must be 1..3600 seconds".into()));
    }
    verify_instance(api, scope, instance)?;
    let sent=api.call("ssm","send-command",json!({"InstanceIds":[instance],"DocumentName":"AWS-RunShellScript","TimeoutSeconds":seconds.max(30),"Parameters":{"commands":[script],"executionTimeout":[seconds.to_string()]}}))?;
    let id = sent["Command"]["CommandId"]
        .as_str()
        .ok_or(Error::Missing("SSM CommandId"))?;
    let start = Instant::now();
    loop {
        match api.call(
            "ssm",
            "get-command-invocation",
            json!({"CommandId":id,"InstanceId":instance}),
        ) {
            Ok(result) => match result["Status"].as_str() {
                Some("Success") => return Ok(result),
                Some("Failed" | "Cancelled" | "TimedOut" | "Cancelling") => {
                    return Err(Error::Config(format!(
                        "SSM command {id} failed: {}",
                        result["StandardErrorContent"]
                            .as_str()
                            .unwrap_or("no error output")
                    )));
                }
                _ => {}
            },
            Err(e) if e.aws_code("InvocationDoesNotExist") => {}
            Err(e) => return Err(e),
        }
        if start.elapsed() > Duration::from_secs(u64::from(seconds) + 30) {
            let cancelled = api.call(
                "ssm",
                "cancel-command",
                json!({"CommandId":id,"InstanceIds":[instance]}),
            );
            if cancelled.is_err() {
                return Err(Error::Cleanup {
                    action: "SSM observation timed out".into(),
                    cleanup: format!("cancel command {id} could not be confirmed"),
                });
            }
            return Err(Error::Config(
                "SSM command observation timed out and cancellation was requested".into(),
            ));
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}
/// Confirm account, namespace, stack inventory and unexpired guest ownership.
pub fn verify_instance(api: &mut impl Api, scope: &Scope, instance: &str) -> Result<Value, Error> {
    super::host::verify(api, scope, instance)
}

#[cfg(test)]
mod tests;
