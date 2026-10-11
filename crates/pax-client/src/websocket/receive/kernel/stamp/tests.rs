//! Deferred mapping retains original read markers and conservative rejection.
use super::*;
use crate::websocket::receive::clock::{Calibration, InvalidClock};
use std::time::Duration;
fn deferred(model: Calibration, read: Instant, raw: i128) -> Stamp {
    Stamp {
        raw_ns: Some(raw),
        source: "nic-hardware/phc",
        qualification: Some(ClockQualification {
            generation: 7,
            epoch: 3,
            method: ClockMethod::DriverExtended,
            age_ns: 0,
            bracket_uncertainty_ns: 1,
            assumed_drift_ppm: 100,
            mapping_deferred: true,
            rate_validated: false,
            model: Some(model),
            read: Some(read),
        }),
        ..Stamp::unavailable("deferred")
    }
}
#[test]
fn delayed_export_uses_original_read_and_never_export_time() {
    let base = Instant::now() - Duration::from_secs(3);
    let model = Calibration::bracket(1_000_000_000, base, base).unwrap();
    let read = base + Duration::from_millis(100);
    let pending = deferred(model, read, 1_050_000_000);
    assert!(pending.at.is_none());
    let resolved = pending.resolve();
    assert_eq!(resolved.at, Some(base + Duration::from_millis(50)));
    assert_eq!(resolved.raw_ns, Some(1_050_000_000));
    assert_eq!(resolved.uncertainty_ns, Some(10_000));
    assert_eq!(resolved.qualification.unwrap().generation, 7);
    assert_eq!(resolved.resolve().at, resolved.at);
}
#[test]
fn exported_qualification_omits_local_markers_and_accepts_older_fields() {
    let now = Instant::now();
    let model = Calibration::bracket(1000, now, now).unwrap();
    let qualification = deferred(model, now, 1000).qualification.unwrap();
    let mut json = serde_json::to_value(qualification).unwrap();
    assert!(json.get("model").is_none() && json.get("read").is_none());
    json.as_object_mut().unwrap().remove("rate_validated");
    let older: ClockQualification = serde_json::from_value(json).unwrap();
    assert!(!older.rate_validated);
    assert!(older.model.is_none() && older.read.is_none());
    assert_eq!(older.generation, qualification.generation);
}
#[test]
fn deferred_stale_future_and_steps_withhold_intervals_but_keep_raw_data() {
    let base = Instant::now() - Duration::from_secs(3);
    let model = Calibration::bracket(1_000_000_000, base, base).unwrap();
    let stale = deferred(model, base + Duration::from_secs(3), 1_000_000_000).resolve();
    assert!(stale.at.is_none() && stale.uncertainty_ns.is_none());
    assert!(stale.status.contains("stale"));
    let future = deferred(model, base + Duration::from_millis(1), 1_002_000_000).resolve();
    assert!(future.at.is_none() && future.status.contains("future"));
    let mut stepped = model;
    let next = Calibration::bracket(
        1_100_000_000,
        base + Duration::from_millis(1),
        base + Duration::from_millis(1),
    )
    .unwrap();
    assert_eq!(
        stepped.update(next, Duration::from_micros(10)),
        Err(InvalidClock::Step)
    );
    let result = deferred(stepped, base + Duration::from_millis(2), 1_100_000_000).resolve();
    assert!(result.at.is_none() && result.status.contains("discontinuity"));
    for stamp in [stale, future, result] {
        assert!(stamp.raw_ns.is_some());
    }
}
