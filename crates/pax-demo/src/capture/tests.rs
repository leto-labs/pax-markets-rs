//! Deterministic queue saturation and writer failure injection.
use super::*;
use std::sync::Barrier;

#[test]
fn saturated_writer_queue_is_marked_incomplete() {
    let directory = std::env::temp_dir().join(format!(
        "pax-writer-unit-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let metadata = serde_json::from_slice(include_bytes!(
        "../../../pax-types/tests/fixtures/metadata.json"
    ))
    .unwrap();
    let manifest = Manifest::new(metadata, vec![MarketId(1)], "fixture://queue".into()).unwrap();
    writer::manifest(&directory, &manifest, false).unwrap();
    let (sender, receiver) = mpsc::sync_channel(1);
    let failed = Arc::new(AtomicBool::new(false));
    let writer_failed = failed.clone();
    let barrier = Arc::new(Barrier::new(2));
    let writer_barrier = barrier.clone();
    let writer_path = directory.clone();
    let task = std::thread::spawn(move || {
        writer_barrier.wait();
        writer::run(writer_path, manifest, receiver, writer_failed)
    });
    let config = CaptureConfig {
        directory: directory.clone(),
        queue_capacity: 1,
        max_bytes: 10000,
        max_record_bytes: 1000,
    };
    let mut recorder = Recorder {
        sender: Some(sender),
        task: Some(task),
        failed,
        config,
        offered_bytes: 8,
        last_offset: 0,
        limited: false,
    };
    recorder
        .record(Record {
            offset_ns: 0,
            generation: 1,
            kind: RecordKind::Connected,
        })
        .unwrap();
    let second = recorder.record(Record {
        offset_ns: 1,
        generation: 1,
        kind: RecordKind::Message(Bytes::from_static(b"{}")),
    });
    barrier.wait();
    assert!(second.is_err());
    assert!(recorder.finish(2, "test").is_err());
    let manifest: Manifest =
        serde_json::from_slice(&std::fs::read(directory.join("manifest.json")).unwrap()).unwrap();
    assert!(!manifest.complete);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn writer_open_failure_preserves_incomplete_manifest() {
    let directory = std::env::temp_dir().join(format!(
        "pax-disk-failure-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let metadata = serde_json::from_slice(include_bytes!(
        "../../../pax-types/tests/fixtures/metadata.json"
    ))
    .unwrap();
    let state =
        Manifest::new(metadata, vec![MarketId(1)], "fixture://disk-failure".into()).unwrap();
    writer::manifest(&directory, &state, false).unwrap();
    // A directory at the journal path deterministically fails even when tests run as root.
    std::fs::create_dir(directory.join("events.pax")).unwrap();
    let (sender, receiver) = mpsc::sync_channel(1);
    drop(sender);
    let failed = Arc::new(AtomicBool::new(false));
    assert!(matches!(
        writer::run(directory.clone(), state, receiver, failed.clone()),
        Err(CaptureError::Io(_))
    ));
    assert!(
        failed.load(Ordering::Acquire),
        "failure visible without another input"
    );
    let saved: Manifest =
        serde_json::from_slice(&std::fs::read(directory.join("manifest.json")).unwrap()).unwrap();
    assert!(!saved.complete);
    std::fs::remove_dir_all(directory).unwrap();
}
#[test]
fn older_build_identity_has_no_invented_staged_digest() {
    let identity: super::BuildInfo = serde_json::from_value(serde_json::json!({
        "version":"0.1.0","revision":"old","dirty":false,"compiler":"test-rustc","profile":"release"
    }))
    .unwrap();
    assert!(identity.source_digest.is_none());
}
