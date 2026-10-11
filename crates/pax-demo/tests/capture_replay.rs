//! Journal integrity, finite stopping, and exact raw-message replay.
use bytes::Bytes;
use pax_demo::{
    capture::{CaptureConfig, Manifest, Record, RecordKind, RecordStatus, Recorder},
    replay::Replay,
};
use pax_types::{market::ExchangeInfo, units::MarketId};
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
                "pax-capture-test-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )))
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn manifest() -> Manifest {
    let metadata: ExchangeInfo = serde_json::from_slice(include_bytes!(
        "../../pax-types/tests/fixtures/metadata.json"
    ))
    .unwrap();
    Manifest::new(metadata, vec![MarketId(1)], "fixture://offline".into()).unwrap()
}
fn config(path: &Path) -> CaptureConfig {
    CaptureConfig {
        directory: path.to_owned(),
        queue_capacity: 256,
        max_bytes: 1024 * 1024,
        max_record_bytes: 64 * 1024,
    }
}
fn records() -> Vec<Record> {
    vec![
        Record {
            offset_ns: 0,
            generation: 1,
            kind: RecordKind::Connected,
        },
        Record {
            offset_ns: 10,
            generation: 1,
            kind: RecordKind::Message(Bytes::from_static(include_bytes!(
                "../../pax-types/tests/fixtures/snapshot.json"
            ))),
        },
        Record {
            offset_ns: 20,
            generation: 1,
            kind: RecordKind::Message(Bytes::from_static(include_bytes!(
                "../../pax-types/tests/fixtures/xa.json"
            ))),
        },
        Record {
            offset_ns: 30,
            generation: 1,
            kind: RecordKind::Disconnected {
                reason: "fixture boundary".into(),
                lossy: false,
            },
        },
    ]
}
fn capture(path: &Path) -> Manifest {
    let mut recorder = Recorder::start(config(path), manifest()).unwrap();
    for record in records() {
        assert_eq!(recorder.record(record).unwrap(), RecordStatus::Recorded);
    }
    recorder.finish(40, "fixture complete").unwrap()
}

#[test]
fn complete_capture_preserves_bytes_boundaries_and_counts() {
    let path = Scratch::new();
    let manifest = capture(&path.0);
    assert_eq!(manifest.records, 4);
    assert_eq!(manifest.messages, 2);
    assert!(manifest.complete);
    let mut replay = Replay::open(&path.0, 1024 * 1024).unwrap();
    for expected in records() {
        let actual = replay.next_record().unwrap().unwrap();
        assert_eq!(actual.offset_ns, expected.offset_ns);
        assert_eq!(actual.generation, expected.generation);
        if let RecordKind::Message(expected) = expected.kind {
            let RecordKind::Message(actual) = actual.kind else {
                panic!()
            };
            assert_eq!(actual, expected);
        }
    }
    assert!(replay.next_record().unwrap().is_none());
}

#[test]
fn byte_budget_stops_cleanly_before_a_partial_record() {
    let path = Scratch::new();
    let input = records();
    let mut limits = config(&path.0);
    limits.max_bytes = 8 + input[0].encoded_len() as u64;
    let mut recorder = Recorder::start(limits, manifest()).unwrap();
    assert_eq!(
        recorder.record(input[0].clone()).unwrap(),
        RecordStatus::Recorded
    );
    assert_eq!(
        recorder.record(input[1].clone()).unwrap(),
        RecordStatus::LimitReached
    );
    let final_manifest = recorder.finish(100, "finished").unwrap();
    assert!(final_manifest.complete);
    assert_eq!(final_manifest.records, 1);
    assert_eq!(final_manifest.end_offset_ns, 0);
    assert!(Replay::open(&path.0, 1024).is_ok());
}

#[test]
fn tampered_and_truncated_journals_fail_before_replay() {
    for truncate in [true, false] {
        let path = Scratch::new();
        capture(&path.0);
        let file = path.0.join("events.pax");
        let mut data = std::fs::read(&file).unwrap();
        if truncate {
            data.pop();
        } else {
            *data.last_mut().unwrap() ^= 1;
        }
        std::fs::write(file, data).unwrap();
        assert!(Replay::open(&path.0, 1024 * 1024).is_err());
    }
}

#[test]
fn unsupported_incomplete_incorrect_counts_and_metadata_are_rejected() {
    for field in 0..4 {
        let path = Scratch::new();
        let mut value = capture(&path.0);
        match field {
            0 => value.version = 99,
            1 => value.complete = false,
            2 => value.messages += 1,
            _ => value.metadata.assets[0].num_native_units.0 += 1,
        };
        std::fs::write(
            path.0.join("manifest.json"),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
        assert!(Replay::open(&path.0, 1024 * 1024).is_err());
    }
}

#[test]
fn framing_bounds_are_checked_even_with_a_matching_digest() {
    let path = Scratch::new();
    let mut value = capture(&path.0);
    let file = path.0.join("events.pax");
    let mut data = std::fs::read(&file).unwrap();
    data[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    value.digest = blake3::hash(&data).to_hex().to_string();
    std::fs::write(file, data).unwrap();
    std::fs::write(
        path.0.join("manifest.json"),
        serde_json::to_vec(&value).unwrap(),
    )
    .unwrap();
    assert!(Replay::open(&path.0, 1024 * 1024).is_err());
}

#[test]
fn explicit_loss_and_backwards_offsets_cannot_complete() {
    for backwards in [true, false] {
        let path = Scratch::new();
        let mut recorder = Recorder::start(config(&path.0), manifest()).unwrap();
        recorder
            .record(Record {
                offset_ns: 10,
                generation: 1,
                kind: RecordKind::Connected,
            })
            .unwrap();
        if backwards {
            assert!(
                recorder
                    .record(Record {
                        offset_ns: 9,
                        generation: 1,
                        kind: RecordKind::Message(Bytes::from_static(b"{}"))
                    })
                    .is_err()
            );
        } else {
            recorder.invalidate();
        }
        assert!(recorder.finish(20, "stop").is_err());
        assert!(Replay::open(&path.0, 1024 * 1024).is_err());
    }
}

#[test]
fn existing_capture_is_never_overwritten() {
    let path = Scratch::new();
    capture(&path.0);
    let original = std::fs::read(path.0.join("events.pax")).unwrap();
    assert!(Recorder::start(config(&path.0), manifest()).is_err());
    assert_eq!(std::fs::read(path.0.join("events.pax")).unwrap(), original);
}

#[test]
fn manifest_validates_utf8_in_ignored_fields() {
    for valid in [true, false] {
        let path = Scratch::new();
        capture(&path.0);
        let manifest = path.0.join("manifest.json");
        let mut bytes = std::fs::read(&manifest).unwrap();
        let mut extra = b"\"future\":\"".to_vec();
        extra.extend_from_slice(if valid { "λ".as_bytes() } else { &[0xff] });
        extra.extend_from_slice(b"\",");
        bytes.splice(1..1, extra);
        std::fs::write(manifest, bytes).unwrap();
        assert_eq!(Replay::open(&path.0, 1024 * 1024).is_ok(), valid);
    }
}

#[test]
fn older_build_identity_remains_readable_without_invented_optimization_provenance() {
    let old = serde_json::json!({"version":"0.1.0","revision":"old","dirty":false,"compiler":"rustc","profile":"release"});
    let identity: pax_demo::capture::BuildInfo = serde_json::from_value(old).unwrap();
    assert!(identity.optimization.is_empty());
    let current = pax_demo::capture::current_build_info();
    assert!(!current.optimization.is_empty());
    let restored: pax_demo::capture::BuildInfo =
        serde_json::from_value(serde_json::to_value(&current).unwrap()).unwrap();
    assert_eq!(current.optimization, restored.optimization);
}
