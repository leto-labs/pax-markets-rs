use super::{TurnoverSummary, measure_message};
use std::time::Duration;

#[test]
fn timing_preserves_the_message_and_callback_result() {
    let message = b"recorded message";
    let (result, _elapsed) = measure_message(message, |input| {
        assert_eq!(input.as_ptr(), message.as_ptr());
        assert_eq!(input, message);
        input.len()
    });
    assert_eq!(result, message.len());
}

#[test]
fn timing_does_not_turn_a_processing_error_into_success() {
    let (result, _elapsed) = measure_message(b"invalid", |_| Err::<(), _>("invalid event"));
    assert_eq!(result, Err("invalid event"));
}

#[test]
fn empty_samples_do_not_produce_a_latency_claim() {
    assert_eq!(TurnoverSummary::from_samples(&mut []), None);
}

#[test]
fn percentiles_use_nearest_rank_for_unsorted_samples() {
    let mut samples: Vec<_> = (1..=100).rev().map(Duration::from_nanos).collect();
    let summary = TurnoverSummary::from_samples(&mut samples).unwrap();
    assert_eq!(summary.samples, 100);
    assert_eq!(summary.min, Duration::from_nanos(1));
    assert_eq!(summary.p50, Duration::from_nanos(50));
    assert_eq!(summary.p95, Duration::from_nanos(95));
    assert_eq!(summary.p99, Duration::from_nanos(99));
    assert_eq!(summary.max, Duration::from_nanos(100));
}

#[test]
fn small_sample_percentiles_round_up() {
    let mut samples = [
        Duration::from_nanos(30),
        Duration::from_nanos(10),
        Duration::from_nanos(20),
    ];
    let summary = TurnoverSummary::from_samples(&mut samples).unwrap();
    assert_eq!(summary.p50, Duration::from_nanos(20));
    assert_eq!(summary.p95, Duration::from_nanos(30));
    assert_eq!(summary.p99, Duration::from_nanos(30));
}

#[test]
fn single_and_zero_duration_samples_are_valid() {
    let mut samples = [Duration::ZERO];
    let summary = TurnoverSummary::from_samples(&mut samples).unwrap();
    assert_eq!(summary.samples, 1);
    assert_eq!(summary.min, Duration::ZERO);
    assert_eq!(summary.p99, Duration::ZERO);
    assert_eq!(summary.max, Duration::ZERO);
}
