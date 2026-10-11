//! Compact storage preserves exact public values without narrowing clock fields.
use super::*;
#[test]
fn local_storage_preserves_wide_fields_and_the_fixed_model_contract() {
    assert_eq!(std::mem::size_of::<Option<StoredQualification>>(), 32);
    let mut stamp = Stamp::unavailable("test");
    stamp.qualification = local_qualification(u64::MAX - 1, u64::MAX - 1, u64::MAX, u64::MAX);
    let model = stamp.qualification().unwrap();
    assert_eq!(model.generation, u64::MAX - 1);
    assert_eq!(model.epoch, u64::MAX - 1);
    assert_eq!(model.age_ns, u64::MAX);
    assert_eq!(model.bracket_uncertainty_ns, u64::MAX);
    assert_eq!(model.method, ClockMethod::UserBracket);
    assert_eq!(model.assumed_drift_ppm, 100);
    assert!(!model.rate_validated && !model.mapping_deferred);
    let encoded = serde_json::to_vec(&model).unwrap();
    let decoded: ClockQualification = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        serde_json::to_value(model).unwrap()
    );
    for (generation, epoch) in [(0, 1), (1, 0), (1, u64::MAX)] {
        assert!(local_qualification(generation, epoch, 0, 0).is_none());
    }
}
