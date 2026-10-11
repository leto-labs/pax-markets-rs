//! Embed compiler and source identity outside runtime processing.
use std::{path::Path, process::Command};

fn output(command: &str, arguments: &[&str]) -> String {
    Command::new(command)
        .args(arguments)
        .output()
        .ok()
        .filter(|result| result.status.success())
        .and_then(|result| String::from_utf8(result.stdout).ok())
        .unwrap_or_else(|| "unknown".into())
        .trim()
        .to_owned()
}
// Cargo treats a missing watched path as stale on every invocation. Watching an
// existing parent also detects a future loose ref or an initially absent index.
fn watch(path: &Path) {
    let existing = path
        .ancestors()
        .find(|candidate| candidate.exists())
        .unwrap_or_else(|| Path::new("."));
    println!("cargo:rerun-if-changed={}", existing.display());
}

fn watch_git() {
    for name in ["HEAD", "index"] {
        let path = output("git", &["rev-parse", "--git-path", name]);
        if path != "unknown" {
            watch(Path::new(&path));
        }
    }
    let reference = output("git", &["symbolic-ref", "-q", "HEAD"]);
    if reference != "unknown" {
        let path = output("git", &["rev-parse", "--git-path", &reference]);
        if path != "unknown" {
            watch(Path::new(&path));
        }
    }
    let packed = output("git", &["rev-parse", "--git-path", "packed-refs"]);
    if packed != "unknown" && Path::new(&packed).exists() {
        watch(Path::new(&packed));
    }
    // Creating packed-refs removes the watched loose ref and invalidates it.
}

pub fn embed() {
    let mut features: Vec<_> = std::env::vars()
        .filter_map(|(key, _)| {
            key.strip_prefix("CARGO_FEATURE_")
                .map(|name| name.to_ascii_lowercase().replace('_', "-"))
        })
        .collect();
    features.sort();
    println!("cargo:rustc-env=PAX_FEATURES={}", features.join(","));
    watch_git();
    // Includes optional examples/benches/tests as they are added or removed.
    println!("cargo:rerun-if-changed=.");
    println!("cargo:rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS");
    println!("cargo:rerun-if-env-changed=PAX_OPTIMIZATION");
    for key in [
        "PAX_SOURCE_REVISION",
        "PAX_SOURCE_DIRTY",
        "PAX_SOURCE_DIGEST",
        "PAX_BUILD_PROFILE",
    ] {
        println!("cargo:rerun-if-env-changed={key}");
    }
    println!(
        "cargo:rustc-env=PAX_OPTIMIZATION={}",
        std::env::var("PAX_OPTIMIZATION").unwrap_or_else(|_| "unprofiled".into())
    );
    println!(
        "cargo:rustc-env=PAX_RUSTFLAGS={}",
        std::env::var("CARGO_ENCODED_RUSTFLAGS")
            .unwrap_or_default()
            .replace('\x1f', " ")
    );
    println!(
        "cargo:rustc-env=PAX_TARGET={}",
        std::env::var("TARGET").unwrap_or_else(|_| "unknown".into())
    );
    for directory in ["../pax-types/src", "../pax-book/src", "../pax-client/src"] {
        println!("cargo:rerun-if-changed={directory}");
    }
    if std::env::var_os("CARGO_FEATURE_DEMO_INTEGRATION").is_some() {
        println!("cargo:rerun-if-changed=../pax-demo/src");
    }
    println!("cargo:rerun-if-changed=../../Cargo.toml");
    println!("cargo:rerun-if-changed=../../Cargo.lock");
    println!(
        "cargo:rustc-env=PAX_REVISION={}",
        std::env::var("PAX_SOURCE_REVISION")
            .inspect(|revision| {
                assert!(
                    (revision.len() == 40 || revision.len() == 64)
                        && revision.bytes().all(|b| b.is_ascii_hexdigit()),
                    "PAX_SOURCE_REVISION must be a commit SHA"
                );
            })
            .unwrap_or_else(|_| output("git", &["rev-parse", "HEAD"]))
    );
    println!(
        "cargo:rustc-env=PAX_DIRTY={}",
        std::env::var("PAX_SOURCE_DIRTY")
            .inspect(|dirty| {
                assert!(
                    dirty == "true" || dirty == "false",
                    "PAX_SOURCE_DIRTY must be true or false"
                );
            })
            .unwrap_or_else(|_| (!output(
                "git",
                &["--no-optional-locks", "status", "--porcelain"]
            )
            .is_empty())
            .to_string())
    );
    let digest = std::env::var("PAX_SOURCE_DIGEST").unwrap_or_default();
    assert!(
        digest.is_empty() || (digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit())),
        "PAX_SOURCE_DIGEST must be a SHA-256 or absent"
    );
    println!("cargo:rustc-env=PAX_SOURCE_DIGEST={digest}");
    println!(
        "cargo:rustc-env=PAX_COMPILER={}",
        output("rustc", &["--version"])
    );
    println!(
        "cargo:rustc-env=PAX_PROFILE={}",
        std::env::var("PAX_BUILD_PROFILE")
            .inspect(|profile| {
                assert!(
                    profile
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
                    "PAX_BUILD_PROFILE must be a Cargo profile name"
                );
            })
            .unwrap_or_else(|_| std::env::var("PROFILE").unwrap_or_else(|_| "unknown".into()))
    );
}
