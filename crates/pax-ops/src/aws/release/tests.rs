use super::*;
use crate::aws::fake::Fake;

fn scope() -> Scope {
    Scope::new(
        "123456789012".into(),
        "ap-northeast-1".into(),
        "pax-test".into(),
    )
    .unwrap()
}
fn owned() -> Value {
    json!({"Reservations":[{"Instances":[{"InstanceId":"i-test","State":{"Name":"running"},"Tags":[{"Key":"pax:owner","Value":"pax-markets-rs"},{"Key":"aws:cloudformation:stack-name","Value":"pax-test"}]}]}]})
}
fn owned_stack() -> Value {
    json!({"Stacks":[{"StackName":"pax-test","StackStatus":"CREATE_COMPLETE","Tags":[{"Key":"pax:owner","Value":"pax-markets-rs"}]}]})
}
fn owned_resource() -> Value {
    json!({"StackResourceSummaries":[{"LogicalResourceId":"Host","ResourceType":"AWS::EC2::Instance","PhysicalResourceId":"i-test","ResourceStatus":"CREATE_COMPLETE"}]})
}
struct UploadFake {
    api: Fake,
    uploads: usize,
}
#[test]
fn publication_is_scoped_and_never_calls_guest_control() {
    let scope = scope();
    let artifact = Artifact {
        bundle: "fixture.tar.gz".into(),
        digest: "a".repeat(64),
        build: json!({}),
    };
    let mut api = UploadFake {
        api: Fake::new([Ok(json!({"Account":scope.account}))]),
        uploads: 0,
    };
    let result = publish(
        &mut api,
        &scope,
        "pax-123456789012-ap-northeast-1-test",
        &artifact,
    )
    .unwrap();
    assert_eq!(api.uploads, 1);
    assert_eq!(api.api.calls.len(), 1);
    assert_eq!(
        result["key"],
        format!("releases/{}/bundle.tar.gz", artifact.digest)
    );
    for bucket in [
        "unrelated",
        "pax-999999999999-ap-northeast-1-test",
        "pax-123456789012-ap-northeast-1-test/path",
    ] {
        let mut api = UploadFake {
            api: Fake::new([Ok(json!({"Account":scope.account}))]),
            uploads: 0,
        };
        assert!(publish(&mut api, &scope, bucket, &artifact).is_err());
        assert_eq!(api.uploads, 0);
    }
    let mut api = UploadFake {
        api: Fake::new([Ok(json!({"Account":"999999999999"}))]),
        uploads: 0,
    };
    assert!(
        publish(
            &mut api,
            &scope,
            "pax-123456789012-ap-northeast-1-test",
            &artifact
        )
        .is_err()
    );
    assert_eq!(api.uploads, 0);
}
impl Api for UploadFake {
    fn call(&mut self, service: &str, operation: &str, input: Value) -> Result<Value, Error> {
        self.api.call(service, operation, input)
    }
    fn upload(&mut self, _: &str, _: &str, _: &Path) -> Result<Value, Error> {
        self.uploads += 1;
        Ok(json!({}))
    }
}
#[test]
fn foreign_host_is_rejected_before_control_or_upload() {
    let scope = scope();
    let mut api = Fake::new([
        Ok(json!({"Account":scope.account})),
        Ok(json!({"Reservations":[]})),
    ]);
    assert!(command(&mut api, &scope, "i-test", "true", 10).is_err());
    assert_eq!(api.calls.len(), 2);
}
#[test]
fn failed_activation_surfaces_failure_and_includes_rollback() {
    let scope = scope();
    let mut api = UploadFake {
        api: Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(owned()),
            Ok(owned_stack()),
            Ok(owned_resource()),
            Ok(json!({"Account":scope.account})),
            Ok(owned()),
            Ok(owned_stack()),
            Ok(owned_resource()),
            Ok(json!({"Command":{"CommandId":"test-command"}})),
            Ok(
                json!({"Status":"Success","StandardOutputContent":"{\"kind\":\"pax_release_preflight\",\"eligible\":false,\"reason\":\"release\"}\n"}),
            ),
            Ok(json!({"Account":scope.account})),
            Ok(owned()),
            Ok(owned_stack()),
            Ok(owned_resource()),
            Ok(json!({"Command":{"CommandId":"activation-command"}})),
            Ok(
                json!({"Status":"Failed","StandardErrorContent":"release failed; previous release restored"}),
            ),
        ]),
        uploads: 0,
    };
    let artifact = Artifact {
        bundle: "unused.tar.gz".into(),
        digest: "a".repeat(64),
        build: json!({}),
    };
    let options = Options {
        activate_stable: true,
        require_noop: false,
        markets: vec![1],
        ingest_cpu: Some(0),
        background_cpu: Some(1),
        hardware_interface: None,
        hardware_clock: None,
        health_seconds: 10,
    };
    let failure = deploy(
        &mut api,
        &scope,
        "i-test",
        "pax-123456789012-ap-northeast-1-pax-test",
        &artifact,
        &options,
    )
    .unwrap_err();
    assert!(failure.to_string().contains("previous release restored"));
    assert_eq!(api.uploads, 1);
    let script = api
        .api
        .calls
        .iter()
        .rfind(|(_, op, _)| op == "send-command")
        .unwrap()
        .2["Parameters"]["commands"][0]
        .as_str()
        .unwrap();
    assert!(script.contains("mv -Tf /opt/pax/current.next /opt/pax/current"));
    assert!(script.contains("missing=required-flags"));
    assert!(script.contains("target executable provenance mismatch"));
    assert!(script.contains("systemctl stop pax-demo.service"));
    assert!(script.contains("book['status'] == 'live'"));
    assert!(script.contains("timeout=2) as response:"));
    assert!(script.contains("pax_release_guest_timing"));
    assert!(script.contains("-L /opt/pax/current && -f /opt/pax/current/pax-demo.service"));
    assert!(!script.contains("rm -rf /var/lib/pax"));
    assert!(
        std::process::Command::new("bash")
            .args(["-n", "-c", script])
            .status()
            .unwrap()
            .success()
    );
    verify_private_archive_installation(script);
    let verifier = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../infra/aws/tests/test_release_activation.py");
    let mut child = Command::new("python3")
        .arg(verifier)
        .stdin(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    std::io::Write::write_all(child.stdin.as_mut().unwrap(), script.as_bytes()).unwrap();
    drop(child.stdin.take());
    assert!(child.wait().unwrap().success());
    // Execute only the generated Python handoff, with controlled health/clock
    // inputs. The independent Python tests exercise real polling decisions.
    let block = script
        .split("if ! python3 - \"$stage_started\" \"$stage_done\" \"$health_started\" <<'PY'\n")
        .nth(1)
        .unwrap()
        .split("\nPY\n")
        .next()
        .unwrap();
    let call = block
        .lines()
        .find(|line| line.starts_with("result=wait("))
        .unwrap();
    for healthy in [true, false] {
        let replacement = format!(
            "sys.argv=['health','10','20','30']\ntime.monotonic_ns=lambda:90\nresult={{'healthy':{}}}",
            if healthy { "True" } else { "False" }
        );
        let output = std::process::Command::new("python3")
            .args(["-c", &block.replacen(call, &replacement, 1)])
            .output()
            .unwrap();
        assert_eq!(output.status.success(), healthy);
        let lines = String::from_utf8(output.stdout).unwrap();
        let timing: Value = serde_json::from_str(lines.lines().nth(1).unwrap()).unwrap();
        assert_eq!(timing["health_ns"], 60);
        assert_eq!(timing["total_ns"], 80);
    }
}

/// Execute the actual staging block with a private archive root and restrictive
/// umask. Numeric fixture ownership permits the same check without root locally.
fn verify_private_archive_installation(script: &str) {
    let output = Command::new("python3")
        .args([
            "-c",
            r#"
import hashlib, os, pathlib, shlex, subprocess, sys, tarfile, tempfile
with tempfile.TemporaryDirectory() as temporary:
    root = pathlib.Path(temporary)
    source = root / 'source'
    source.mkdir(mode=0o700)
    executable = source / 'pax-demo'
    executable.write_bytes(b'#!/bin/sh\nexit 0\n')
    executable.chmod(0o755)
    checksum = hashlib.sha256(executable.read_bytes()).hexdigest()
    (source / 'SHA256SUMS').write_text(checksum + '  pax-demo\n')
    bundle = root / 'bundle.tar.gz'
    with tarfile.open(bundle, 'w:gz') as archive:
        archive.add(source, arcname='.')
    stub = root / 'bin'
    stub.mkdir()
    aws = stub / 'aws'
    aws.write_text('#!/bin/bash\ncp -- "$TEST_BUNDLE" "${@: -1}"\n')
    aws.chmod(0o755)
    installed, data = root / 'installed', root / 'data'
    release = installed / 'releases' / 'fixture'
    block = sys.argv[1].split('stage_started=$(tick)\n', 1)[1].split('[[ $(uname -m)', 1)[0]
    block = block.replace('/opt/pax', str(installed)).replace('/var/lib/pax', str(data))
    ownership = '-o ' + str(os.getuid()) + ' -g ' + str(os.getgid())
    block = block.replace('-o root -g root', ownership).replace('-o pax -g pax', ownership)
    block = block.replace('id pax ', 'id -u ')  # Build containers may lack a passwd entry.
    block = block.replace('a' * 64, hashlib.sha256(bundle.read_bytes()).hexdigest())
    command = 'set -euo pipefail\numask 077\nrelease=' + shlex.quote(str(release)) + '\n' + block
    environment = dict(os.environ, PATH=str(stub) + os.pathsep + os.environ['PATH'], TEST_BUNDLE=str(bundle))
    for attempt in range(2):
        result = subprocess.run(['bash', '-c', command], env=environment, capture_output=True)
        assert result.returncode == 0, result.stderr.decode()
        for directory in [installed, installed / 'releases', release]:
            assert directory.stat().st_mode & 0o7777 == 0o755
        for directory in [data, data / 'captures', data / 'diagnostics']:
            assert directory.stat().st_mode & 0o7777 == 0o750
        assert (release / 'pax-demo').read_bytes() == executable.read_bytes()
        assert (release / 'pax-demo').stat().st_mode & 0o7777 == 0o755
        if attempt == 0:
            (data / 'captures' / 'retained').write_bytes(b'capture')
            release.chmod(0o700)  # Repair an existing release too; never erase data.
        else:
            assert (data / 'captures' / 'retained').read_bytes() == b'capture'
"#,
            script,
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
#[test]
fn stale_build_provenance_is_rejected() {
    let build = json!({"source_revision":"a","source_digest_sha256":"b","profile":"release-perf"});
    let good = json!({"revision":"a","source_digest":"b","profile":"release-perf"});
    assert!(verify_capabilities(&build, &good).is_ok());
    for key in ["revision", "source_digest", "profile"] {
        let mut wrong = good.clone();
        wrong[key] = json!("stale");
        assert!(verify_capabilities(&build, &wrong).is_err());
    }
    let mut profiled = build;
    profiled["optimization"] = json!("pgo-use:core:profile-digest");
    assert!(verify_capabilities(&profiled, &good).is_err());
    let mut matching = good;
    matching["optimization"] = profiled["optimization"].clone();
    assert!(verify_capabilities(&profiled, &matching).is_ok());
}

#[test]
fn repeated_preparation_verifies_without_rewriting_and_preserves_changed_bundles() {
    struct Fixture(std::path::PathBuf);
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root =
        Fixture(std::env::temp_dir().join(format!("pax-release-{}-{nonce}", std::process::id())));
    std::fs::create_dir(&root.0).unwrap();
    let directory = root.0.join("artifact");
    std::fs::create_dir(&directory).unwrap();
    std::fs::write(directory.join("pax-demo"), b"offline stand-in executable").unwrap();
    let build = json!({"platform":"x86_64-unknown-linux-gnu/al2023","cpu":"x86-64",
        "binary_sha256":sha256(&directory.join("pax-demo")).unwrap(),
        "target_features":["sse2"],"source_revision":"test-revision",
        "source_digest_sha256":"test-digest","profile":"release-perf"});
    let capabilities =
        json!({"revision":"test-revision","source_digest":"test-digest","profile":"release-perf"});
    std::fs::write(
        directory.join("build.json"),
        serde_json::to_vec(&build).unwrap(),
    )
    .unwrap();
    std::fs::write(
        directory.join("capabilities.json"),
        serde_json::to_vec(&capabilities).unwrap(),
    )
    .unwrap();
    let bundle = root.0.join("bundle.tar.gz");
    let first = prepare(&directory, &bundle).unwrap();
    let bytes = std::fs::read(&bundle).unwrap();
    let modified = bundle.metadata().unwrap().modified().unwrap();
    let manifest_modified = directory
        .join("SHA256SUMS")
        .metadata()
        .unwrap()
        .modified()
        .unwrap();
    let second = prepare(&directory, &bundle).unwrap();
    assert_eq!(first.digest, second.digest);
    assert_eq!(first.build, second.build);
    assert_eq!(bundle.metadata().unwrap().modified().unwrap(), modified);
    assert_eq!(
        directory
            .join("SHA256SUMS")
            .metadata()
            .unwrap()
            .modified()
            .unwrap(),
        manifest_modified
    );
    std::fs::write(directory.join("additional.txt"), "changed membership").unwrap();
    assert!(prepare(&directory, &bundle).is_err());
    assert_eq!(std::fs::read(&bundle).unwrap(), bytes);
}

fn guest_api(command: bool) -> UploadFake {
    let host = owned();
    let stack = owned_stack();
    let mut replies = vec![
        Ok(json!({"Account":"123456789012"})),
        Ok(host),
        Ok(stack),
        Ok(owned_resource()),
    ];
    if command {
        replies.extend([
            Ok(json!({"Command":{"CommandId":"fixture-command"}})),
            Ok(json!({"Status":"Success"})),
        ]);
    }
    UploadFake {
        api: Fake::new(replies),
        uploads: 0,
    }
}

#[test]
fn stable_activation_is_refused_before_upload_and_its_flag_is_not_configuration() {
    let mut api = guest_api(false);
    let artifact = Artifact {
        bundle: "unused.tar.gz".into(),
        digest: "a".repeat(64),
        build: json!({}),
    };
    let mut options = Options {
        activate_stable: false,
        require_noop: false,
        markets: vec![1],
        ingest_cpu: None,
        background_cpu: None,
        hardware_interface: None,
        hardware_clock: None,
        health_seconds: 30,
    };
    let before = serde_json::to_vec(&options).unwrap();
    assert!(
        deploy(
            &mut api,
            &scope(),
            "i-test",
            "pax-123456789012-ap-northeast-1-test",
            &artifact,
            &options
        )
        .unwrap_err()
        .to_string()
        .contains("--activate-stable")
    );
    assert_eq!(api.api.calls.len(), 4);
    assert_eq!(api.uploads, 0);
    options.activate_stable = true;
    options.require_noop = true;
    assert_eq!(before, serde_json::to_vec(&options).unwrap());
}

#[test]
fn healthy_preflight_skips_upload_and_activation_and_require_noop_refuses_mismatch() {
    for eligible in [false, true] {
        let mut api = guest_api(false);
        let second = guest_api(true);
        api.api.replies.extend(second.api.replies);
        let last = api.api.replies.back_mut().unwrap().as_mut().unwrap();
        last["StandardOutputContent"] = json!(if eligible {
            "pax_release_noop=1\n"
        } else {
            "{\"eligible\":false}\n"
        });
        let artifact = Artifact {
            bundle: "unused.tar.gz".into(),
            digest: "a".repeat(64),
            build: json!({}),
        };
        let options = Options {
            activate_stable: true,
            require_noop: true,
            markets: vec![1],
            ingest_cpu: None,
            background_cpu: None,
            hardware_interface: None,
            hardware_clock: None,
            health_seconds: 30,
        };
        let result = deploy(
            &mut api,
            &scope(),
            "i-test",
            "pax-123456789012-ap-northeast-1-test",
            &artifact,
            &options,
        );
        if eligible {
            let receipt = result.unwrap();
            assert_eq!(receipt["noop"], true);
            assert_eq!(receipt["timing_ns"]["upload"], 0);
            assert_eq!(receipt["timing_ns"]["activation"], 0);
        } else {
            assert!(result.unwrap_err().to_string().contains("--require-noop"));
        }
        assert_eq!(api.uploads, 0);
        assert_eq!(
            api.api
                .calls
                .iter()
                .filter(|(_, op, _)| op == "send-command")
                .count(),
            1
        );
        let script = api
            .api
            .calls
            .iter()
            .find(|(_, op, _)| op == "send-command")
            .unwrap()
            .2["Parameters"]["commands"][0]
            .as_str()
            .unwrap();
        assert!(!script.contains("systemctl restart"));
        assert!(!script.contains("ln -sfn"));
        // Execute the generated shell handoff with a controlled predicate. The
        // independent Python fixtures exercise the real release/process checks.
        // This catches quoting/newline bugs that a fake SSM result cannot.
        let start = script.find("if python3 - <<'PAX_NOOP'\n").unwrap();
        let block = &script[start..];
        let expression = block
            .lines()
            .find(|line| line.starts_with("result=check("))
            .unwrap();
        let stub = format!(
            "result={{'eligible': {}}}",
            if eligible { "True" } else { "False" }
        );
        let block = block.replacen(expression, &stub, 1);
        let output = std::process::Command::new("bash")
            .arg("-c")
            .arg(block)
            .output()
            .unwrap();
        assert!(output.status.success());
        let actual = json!({"StandardOutputContent":String::from_utf8(output.stdout).unwrap()});
        assert_eq!(no_op(&actual), eligible);
    }
}
