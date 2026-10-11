//! Independent time-weighted accounting, recovery, and trace partition checks.
use bytes::Bytes;
use pax_demo::{
    capture::{Record, RecordKind},
    pipeline::Pipeline,
};
use pax_types::units::{MarketId, Quantity};

fn pipeline() -> Pipeline {
    pipeline_with_sampling(1)
}
fn pipeline_with_sampling(sample_every: u64) -> Pipeline {
    let metadata = serde_json::from_slice(include_bytes!(
        "../../pax-types/tests/fixtures/metadata.json"
    ))
    .unwrap();
    Pipeline::new(&metadata, &[MarketId(1)], sample_every).unwrap()
}
fn boundary(p: &mut Pipeline, time: u64, generation: u64, connected: bool) {
    p.process(
        &Record {
            offset_ns: time,
            generation,
            kind: if connected {
                RecordKind::Connected
            } else {
                RecordKind::Disconnected {
                    reason: "test".into(),
                    lossy: false,
                }
            },
        },
        0,
    )
    .unwrap();
}
fn message(
    p: &mut Pipeline,
    time: u64,
    generation: u64,
    bytes: &[u8],
) -> pax_demo::pipeline::ProcessResult {
    p.process(
        &Record {
            offset_ns: time,
            generation,
            kind: RecordKind::Message(Bytes::copy_from_slice(bytes)),
        },
        123,
    )
    .unwrap()
}
const SNAPSHOT: &[u8] = include_bytes!("../../pax-types/tests/fixtures/snapshot.json");
const XA: &[u8] = include_bytes!("../../pax-types/tests/fixtures/xa.json");

#[test]
fn rejection_cause_survives_resynchronization_and_a_valid_book() {
    let mut p = pipeline_with_sampling(0);
    boundary(&mut p, 0, 1, true);
    assert!(message(&mut p, 1, 1, SNAPSHOT).changed);
    assert!(message(&mut p, 2, 1, b"{").recover);
    let rejected = p
        .view("fixture", 2, 20, Quantity(1))
        .last_rejection
        .unwrap();
    assert_eq!((rejected.id.generation, rejected.id.message), (1, 2));
    assert_eq!(rejected.observed_ns, 2);
    assert!(rejected.market.is_none() && rejected.sequence.is_none());
    assert!(!rejected.reason.is_empty());
    boundary(&mut p, 3, 1, false);
    boundary(&mut p, 4, 2, true);
    assert!(message(&mut p, 5, 2, SNAPSHOT).changed);
    assert!(message(&mut p, 6, 2, XA).changed);
    let view = p.view("fixture", 6, 20, Quantity(1));
    assert_eq!(view.markets[0].status, "live");
    assert_eq!(view.last_error.as_deref(), Some("test"));
    let retained = view.last_rejection.unwrap();
    assert_eq!(retained.id, rejected.id);
    assert_eq!(retained.reason, rejected.reason);
    assert_eq!(view.rejected, "1");
}

#[test]
fn rejection_retains_decoded_identity_and_only_the_latest_bounded_cause() {
    let mut p = pipeline_with_sampling(0);
    boundary(&mut p, 0, 1, true);
    assert!(message(&mut p, 1, 1, SNAPSHOT).changed);
    // A real decoded event with a missing prior sequence, rather than malformed JSON.
    let mut event: serde_json::Value = serde_json::from_slice(XA).unwrap();
    event["data"]["data"]["seq_num"] = serde_json::json!("999");
    let bytes = serde_json::to_vec(&event).unwrap();
    assert!(message(&mut p, 2, 1, &bytes).recover);
    let view = p.view("fixture", 2, 20, Quantity(1));
    let rejection = view.last_rejection.unwrap();
    assert_eq!(rejection.market.as_deref(), Some("1"));
    assert_eq!(rejection.sequence.as_deref(), Some("999"));
    assert_eq!(view.markets[0].status, "invalid");
    boundary(&mut p, 3, 1, false);
    boundary(&mut p, 4, 2, true);
    let cause = "<script>💥</script>".repeat(100);
    let bytes = serde_json::to_vec(&serde_json::json!({"type":"error", "message":cause})).unwrap();
    assert!(message(&mut p, 5, 2, &bytes).recover);
    let view = p.view("fixture", 5, 20, Quantity(1));
    let rejection = view.last_rejection.unwrap();
    assert_eq!((rejection.id.generation, rejection.id.message), (2, 3));
    assert_eq!(rejection.reason.chars().count(), 512);
    assert!(rejection.market.is_none() && rejection.sequence.is_none());
    assert_eq!(view.rejected, "2");
}

#[test]
fn benchmark_observer_is_independent_of_sampling_and_preserves_canonical_state() {
    use pax_client::test_support::fixture::Fixture;
    let fixture = Fixture::synthetic(7, 20).unwrap();
    let mut reference = pipeline_with_sampling(1);
    let mut observed = pipeline_with_sampling(1_000_000);
    boundary(&mut reference, 0, 1, true);
    boundary(&mut observed, 0, 1, true);
    let origin = std::time::Instant::now();
    let mut points = Vec::new();
    for (index, bytes) in fixture.messages.iter().enumerate() {
        let record = Record {
            offset_ns: index as u64 + 1,
            generation: 1,
            kind: RecordKind::Message(bytes.clone()),
        };
        let expected = reference.process_received(&record, origin).unwrap();
        let actual = observed
            .process_observed(&record, origin, None, None, |point| points.push(point))
            .unwrap();
        assert_eq!(
            (actual.recover, actual.changed),
            (expected.recover, expected.changed)
        );
    }
    assert_eq!(reference.digest(), observed.digest());
    assert_eq!(points.len(), fixture.messages.len());
    assert_eq!(
        points
            .iter()
            .filter(|p| p.population == "event")
            .map(|p| p.sequence.unwrap().0)
            .collect::<Vec<_>>(),
        (1..=7).collect::<Vec<_>>()
    );
    assert!(
        points
            .iter()
            .enumerate()
            .all(|(i, p)| p.id.message == i as u64 + 1 && p.received == origin)
    );
    assert!(
        observed
            .view("test", 20, 20, Quantity(1))
            .latency
            .traces
            .is_empty()
    );
}

#[test]
fn numeric_snapshot_is_independent_of_later_book_changes_and_can_render_elsewhere() {
    let mut p = pipeline();
    boundary(&mut p, 0, 1, true);
    message(&mut p, 1, 1, SNAPSHOT);
    let digest = p.digest();
    let snapshot = p.snapshot(100, 1, Quantity(100_000));
    let expected = serde_json::to_value(snapshot.clone().render("frozen")).unwrap();
    assert_eq!(p.digest(), digest);
    assert!(message(&mut p, 2, 1, XA).changed);
    let rendered = std::thread::spawn(move || snapshot.render("frozen"))
        .join()
        .unwrap();
    assert_eq!(serde_json::to_value(&rendered).unwrap(), expected);
    assert_eq!(rendered.markets[0].asks.len(), 1);
    assert_eq!(rendered.markets[0].bids.len(), 1);
    assert_ne!(
        rendered.markets[0].sequence,
        p.view("current", 100, 1, Quantity(100_000)).markets[0].sequence
    );
}

#[test]
fn weighted_coverage_includes_quiet_time_excludes_disconnect_and_closes_final_interval() {
    let mut p = pipeline();
    boundary(&mut p, 0, 1, true);
    message(&mut p, 10, 1, SNAPSHOT);
    message(&mut p, 20, 1, XA);
    boundary(&mut p, 40, 1, false);
    boundary(&mut p, 70, 2, true);
    message(&mut p, 80, 2, SNAPSHOT);
    p.finish(100).unwrap();
    let view = p.view("fixture", 100, 20, Quantity(100_000));
    let market = &view.markets[0];
    assert_eq!(market.coverage.observed_ns, "100");
    assert_eq!(market.coverage.valid_ns, "50");
    assert_eq!(market.coverage.two_sided_ns, "50");
    assert_eq!(market.coverage.mean_spread_ticks.as_deref(), Some("7.2"));
    assert_eq!(market.coverage.mean_ask_quantity.as_deref(), Some("7500"));
    assert_eq!(market.coverage.mean_bid_quantity.as_deref(), Some("15900"));
    assert_eq!(market.estimates[0].shortfall, "90500");
    assert_eq!(market.update_age_ns, Some(20));
}

#[test]
fn preview_does_not_advance_input_clock_and_replay_digest_ignores_local_timings() {
    let mut a = pipeline();
    let mut b = pipeline();
    for p in [&mut a, &mut b] {
        boundary(p, 0, 1, true);
        message(p, 10, 1, SNAPSHOT);
    }
    let digest = a.digest();
    a.view("live", 1_000_000, 20, Quantity(100));
    assert_eq!(a.digest(), digest);
    for p in [&mut a, &mut b] {
        message(p, 20, 1, XA);
        p.finish(100).unwrap();
    }
    assert_eq!(a.digest(), b.digest());
    assert_eq!(
        a.view("replay", 100, 20, Quantity(100)).markets[0]
            .coverage
            .valid_ns,
        "90"
    );
}

#[test]
fn gap_hides_quotes_until_new_generation_and_snapshot() {
    let mut p = pipeline();
    boundary(&mut p, 0, 1, true);
    message(&mut p, 1, 1, SNAPSHOT);
    let gap = std::str::from_utf8(XA)
        .unwrap()
        .replace("\"seq_num\":\"3\"", "\"seq_num\":\"4\"");
    assert!(message(&mut p, 2, 1, gap.as_bytes()).recover);
    let view = p.view("live", 2, 20, Quantity(100));
    assert!(view.markets[0].asks.is_empty());
    assert_eq!(view.markets[0].status, "invalid");
    assert_eq!(view.markets[0].ask_quantity, "unavailable");
    assert_eq!(view.markets[0].bid_quantity, "unavailable");
    assert!(message(&mut p, 3, 1, XA).recover);
    boundary(&mut p, 4, 1, false);
    boundary(&mut p, 5, 2, true);
    assert!(!message(&mut p, 6, 1, XA).changed);
    let view = p.view("live", 6, 20, Quantity(100));
    assert_eq!(view.markets[0].status, "awaiting snapshot");
    assert_eq!(view.markets[0].ask_quantity, "unavailable");
    assert_eq!(view.markets[0].bid_quantity, "unavailable");
    assert!(message(&mut p, 7, 2, SNAPSHOT).changed);
    assert!(message(&mut p, 8, 2, XA).changed);
    let view = p.view("live", 8, 20, Quantity(100));
    assert_eq!(view.markets[0].ask_quantity, "4500");
    assert_eq!(view.markets[0].bid_quantity, "16500");
    // A valid empty snapshot is known zero liquidity, unlike an invalid book.
    boundary(&mut p, 9, 2, false);
    boundary(&mut p, 10, 3, true);
    let mut empty: serde_json::Value = serde_json::from_slice(SNAPSHOT).unwrap();
    empty["data"]["ask_agg_data"] = serde_json::json!([]);
    empty["data"]["bid_agg_data"] = serde_json::json!([]);
    assert!(message(&mut p, 11, 3, &serde_json::to_vec(&empty).unwrap()).changed);
    let view = p.view("live", 11, 20, Quantity(100));
    assert_eq!(view.markets[0].status, "live");
    assert_eq!(view.markets[0].ask_quantity, "0");
    assert_eq!(view.markets[0].bid_quantity, "0");
}

#[test]
fn trace_stages_are_exclusive_and_populations_stay_separate() {
    let mut p = pipeline();
    boundary(&mut p, 0, 1, true);
    message(&mut p, 1, 1, SNAPSHOT);
    message(&mut p, 2, 1, XA);
    message(&mut p, 3, 1, b"{\"type\":\"pong\"}");
    message(&mut p, 4, 1, b"{bad json}");
    let view = p.view("fixture", 4, 20, Quantity(100));
    assert_eq!(view.latency.populations.len(), 4);
    for trace in view.latency.traces {
        assert_eq!(
            trace.processing_ns.unwrap(),
            trace.decode_ns.unwrap()
                + trace.preflight_ns.unwrap_or(0)
                + trace.mutation_ns.unwrap_or(0)
                + trace.validation_ns.unwrap_or(0)
                + trace.other_ns.unwrap()
        );
        if trace.population == "event" {
            assert!(
                trace.preflight_ns.is_some()
                    && trace.mutation_ns.is_some()
                    && trace.validation_ns.is_some()
            );
        }
        assert!(trace.queue_ns.unwrap() >= 123);
    }
    assert!(
        p.view("fixture", 20_000_000_000, 20, Quantity(100))
            .latency
            .stale
    );
}

#[test]
fn buffered_and_duplicate_input_never_enters_the_commit_population() {
    use pax_client::measurement::schema::Boundary;
    let mut p = pipeline();
    boundary(&mut p, 0, 1, true);
    let mut observed = Vec::new();
    for (index, bytes) in [XA, SNAPSHOT, XA].into_iter().enumerate() {
        let result = p
            .process_observed(
                &Record {
                    offset_ns: index as u64 + 1,
                    generation: 1,
                    kind: RecordKind::Message(Bytes::copy_from_slice(bytes)),
                },
                std::time::Instant::now(),
                None,
                None,
                |point| observed.push(point.population),
            )
            .unwrap();
        assert_eq!(result.changed, index == 1);
        assert!(!result.recover);
    }
    assert_eq!(observed, ["buffered", "snapshot", "duplicate"]);
    let view = p.view("fixture", 3, 20, Quantity(100));
    assert_eq!(view.applied, "1");
    assert_eq!(view.markets[0].sequence.as_deref(), Some("3"));
    assert_eq!(view.markets[0].trace_id.unwrap().message, 2);
    assert_eq!(
        view.latency
            .traces
            .iter()
            .map(|t| t.population)
            .collect::<Vec<_>>(),
        observed
    );
    assert_eq!(view.latency.populations.len(), 3);
    for population in view.latency.populations {
        population.observation.validate().unwrap();
        assert_eq!(
            population.observation.boundary,
            if population.name == "snapshot" {
                Boundary::DecodeToBook
            } else {
                Boundary::DecodeToDisposition
            }
        );
    }
}

#[test]
fn snapshot_trace_identifies_the_reconciled_book_sequence() {
    let mut p = pipeline();
    boundary(&mut p, 0, 1, true);
    let buffered = message(&mut p, 1, 1, XA);
    assert!(!buffered.changed && !buffered.recover);
    assert!(message(&mut p, 2, 1, SNAPSHOT).changed);
    let view = p.view("fixture", 2, 20, Quantity(100));
    let market = &view.markets[0];
    assert_eq!(market.status, "live");
    assert_eq!(market.sequence.as_deref(), Some("3"));
    assert_eq!(market.bid_quantity, "16500");
    let trace = view.latency.traces.last().unwrap();
    assert_eq!(trace.population, "snapshot");
    assert_eq!(Some(trace.id), market.trace_id);
    assert_eq!(trace.sequence.unwrap().0, 3);
    assert_eq!(trace.received_ns, 2);
}

#[test]
fn live_receive_clock_covers_dispatch_wait_without_changing_canonical_state() {
    let mut live = pipeline();
    let mut replay = pipeline();
    for pipeline in [&mut live, &mut replay] {
        boundary(pipeline, 0, 1, true);
        message(pipeline, 1, 1, SNAPSHOT);
    }
    let record = Record {
        offset_ns: 2,
        generation: 1,
        kind: RecordKind::Message(Bytes::from_static(XA)),
    };
    // A message that was already received before this processing callback.
    let received = std::time::Instant::now() - std::time::Duration::from_millis(20);
    live.process_received(&record, received).unwrap();
    replay.process(&record, 0).unwrap();
    assert_eq!(live.digest(), replay.digest());
    let view = live.view("live", 2, 20, Quantity(100));
    let trace = view.latency.traces.last().unwrap();
    assert_eq!(trace.received_ns, 2);
    assert!(trace.queue_ns.unwrap() >= 20_000_000);
    assert!(
        u128::from(trace.queue_ns.unwrap()) + u128::from(trace.processing_ns.unwrap())
            <= received.elapsed().as_nanos()
    );
}

#[test]
fn sampling_keeps_unsampled_failures_and_preserves_recovery_state() {
    let mut sparse = pipeline_with_sampling(8);
    let mut full = pipeline();
    for p in [&mut sparse, &mut full] {
        boundary(p, 0, 1, true);
        assert!(message(p, 1, 1, SNAPSHOT).changed);
        assert!(message(p, 2, 1, XA).changed);
    }
    assert_eq!(sparse.digest(), full.digest());
    assert_eq!(
        sparse.view("fixture", 2, 20, Quantity(100)).markets[0]
            .trace_id
            .unwrap()
            .message,
        2,
        "even an unsampled commit must retain its own identity"
    );
    assert!(
        sparse
            .view("fixture", 2, 20, Quantity(100))
            .latency
            .traces
            .is_empty()
    );
    for p in [&mut sparse, &mut full] {
        let result = message(p, 3, 1, b"{bad json}");
        assert!(result.recover && !result.changed);
    }
    assert_eq!(sparse.digest(), full.digest());
    let view = sparse.view("fixture", 3, 20, Quantity(100));
    assert_eq!(view.markets[0].status, "invalid");
    assert_eq!(view.latency.traces.len(), 1);
    let trace = &view.latency.traces[0];
    assert_eq!(trace.id.message, 3);
    assert_eq!(trace.population, "rejected");
    assert!(trace.queue_ns.is_none());
    assert!(trace.processing_ns.is_none());
    assert!(trace.decode_ns.is_none());
    assert!(
        trace.preflight_ns.is_none()
            && trace.mutation_ns.is_none()
            && trace.validation_ns.is_none()
    );
    for p in [&mut sparse, &mut full] {
        boundary(p, 4, 1, false);
        boundary(p, 5, 2, true);
        assert!(message(p, 6, 2, SNAPSHOT).changed);
        assert!(message(p, 7, 2, XA).changed);
        for time in 8..=10 {
            assert!(!message(p, time, 2, b"{\"type\":\"pong\"}").recover);
        }
    }
    assert_eq!(sparse.digest(), full.digest());
    let view = sparse.view("fixture", 10, 20, Quantity(100));
    assert_eq!(view.markets[0].status, "live");
    assert_eq!(
        view.latency.traces.len(),
        1,
        "new connection has only five messages"
    );
    for p in [&mut sparse, &mut full] {
        for time in 11..=13 {
            assert!(!message(p, time, 2, b"{\"type\":\"pong\"}").recover);
        }
    }
    assert_eq!(sparse.digest(), full.digest());
    let view = sparse.view("fixture", 13, 20, Quantity(100));
    assert_eq!(view.latency.traces.len(), 2);
    assert_eq!(view.latency.traces[1].id.message, 11);
    assert_eq!(view.latency.traces[1].population, "control");
}

#[test]
fn delivery_boundaries_partition_wait_and_do_not_change_replay_state() {
    use pax_client::websocket::DeliveryTiming;
    use std::time::{Duration, Instant};
    let mut live = pipeline();
    let mut replay = pipeline();
    for p in [&mut live, &mut replay] {
        boundary(p, 0, 1, true);
        message(p, 1, 1, SNAPSHOT);
    }
    let record = Record {
        offset_ns: 2,
        generation: 1,
        kind: RecordKind::Message(Bytes::from_static(XA)),
    };
    let received = Instant::now() - Duration::from_millis(10);
    let timing = DeliveryTiming {
        prepared: received + Duration::from_millis(2),
        delivered: Some(received + Duration::from_millis(7)),
        pending: Some(3),
    };
    live.process_delivered(&record, received, Some(timing))
        .unwrap();
    replay.process(&record, 0).unwrap();
    assert_eq!(live.digest(), replay.digest());
    let view = live.view("live", 2, 20, Quantity(1));
    let trace = view.latency.traces.last().unwrap();
    let stages = trace.delivery.unwrap();
    assert_eq!(stages.transport_ns, 2_000_000);
    assert_eq!(stages.handoff_ns, 5_000_000);
    assert_eq!(stages.pending, 3);
    assert_eq!(
        stages.transport_ns + stages.handoff_ns + stages.preparation_ns,
        trace.queue_ns.unwrap()
    );
    assert!(trace.pending_gap_ns.is_some());
    let replay_view = replay.view("replay", 2, 20, Quantity(1));
    assert!(
        replay_view
            .latency
            .traces
            .last()
            .unwrap()
            .delivery
            .is_none()
    );
    assert!(
        replay_view
            .latency
            .traces
            .last()
            .unwrap()
            .pending_gap_ns
            .is_none()
    );
}

#[test]
fn inconsistent_or_incomplete_delivery_markers_are_unavailable() {
    use pax_client::websocket::DeliveryTiming;
    use std::time::{Duration, Instant};
    for invalid in 0..3 {
        let mut p = pipeline();
        boundary(&mut p, 0, 1, true);
        let received = Instant::now();
        let mut timing = DeliveryTiming {
            prepared: received,
            delivered: Some(received),
            pending: Some(0),
        };
        match invalid {
            0 => timing.delivered = None,
            1 => timing.pending = None,
            _ => timing.prepared = received - Duration::from_nanos(1),
        }
        p.process_delivered(
            &Record {
                offset_ns: 1,
                generation: 1,
                kind: RecordKind::Message(Bytes::from_static(SNAPSHOT)),
            },
            received,
            Some(timing),
        )
        .unwrap();
        assert!(
            p.view("test", 1, 1, Quantity(1)).latency.traces[0]
                .delivery
                .is_none()
        );
    }
}

#[test]
fn independent_health_suppresses_quotes_before_queued_boundary_arrives() {
    let mut pipeline = pipeline();
    boundary(&mut pipeline, 0, 1, true);
    message(&mut pipeline, 1, 1, SNAPSHOT);
    let digest = pipeline.digest();
    for (connected, generation) in [(false, 1), (true, 2)] {
        let mut view = pipeline.view("live", 2, 20, Quantity(1000));
        view.qualify_transport(connected, generation);
        assert_eq!(view.markets[0].status, "transport unavailable");
        assert!(view.markets[0].asks.is_empty());
        assert!(view.markets[0].estimates.is_empty());
        assert!(view.markets[0].spread.is_none());
        assert_eq!(pipeline.digest(), digest);
    }
}

#[test]
fn independent_market_intervals_survive_preview_global_failure_and_reconnect() {
    let mut metadata = pax_demo::runner::offline::fixture_metadata().unwrap();
    let mut second = metadata.markets[0].clone();
    second.market_id = MarketId(2);
    metadata.markets.push(second);
    let snapshot2 = std::str::from_utf8(SNAPSHOT)
        .unwrap()
        .replace("\"market_id\":\"1\"", "\"market_id\":\"2\"");
    let xa2 = std::str::from_utf8(XA)
        .unwrap()
        .replace("\"market_id\":\"1\"", "\"market_id\":\"2\"");
    let gap = std::str::from_utf8(XA)
        .unwrap()
        .replace("\"seq_num\":\"3\"", "\"seq_num\":\"5\"");
    // Both a selected-book failure and a failure before market dispatch must
    // close the old interval for every market at the same observation boundary.
    for failure in [gap.as_bytes(), b"{invalid"] {
        let mut p = Pipeline::new(&metadata, &[MarketId(1), MarketId(2)], 8).unwrap();
        boundary(&mut p, 0, 1, true);
        assert!(message(&mut p, 10, 1, SNAPSHOT).changed);
        assert!(message(&mut p, 15, 1, snapshot2.as_bytes()).changed);
        assert!(!message(&mut p, 18, 1, b"{\"type\":\"pong\"}").recover);
        assert!(message(&mut p, 20, 1, XA).changed);
        let digest = p.digest();
        let preview = p.view("preview", 1000, 0, Quantity(0));
        assert_eq!(preview.markets[0].coverage.valid_ns, "990");
        assert_eq!(preview.markets[1].coverage.valid_ns, "985");
        assert_eq!(p.digest(), digest);
        assert!(!message(&mut p, 25, 1, XA).changed); // Duplicate, same state.
        assert!(message(&mut p, 30, 1, xa2.as_bytes()).changed);
        assert!(message(&mut p, 40, 1, failure).recover);
        boundary(&mut p, 50, 1, false);
        boundary(&mut p, 60, 2, true);
        assert!(message(&mut p, 70, 2, SNAPSHOT).changed);
        assert!(message(&mut p, 80, 2, snapshot2.as_bytes()).changed);
        assert!(!message(&mut p, 85, 1, XA).changed); // Old generation.
        p.finish(100).unwrap();
        p.finish(100).unwrap(); // Repeated close must not count an interval twice.
        let view = p.view("fixture", 100, 0, Quantity(0));
        // Market 1: snapshot state for 10 + 30 ns, XA state for 20 ns.
        // Market 2: snapshot state for 15 + 20 ns, XA state for 10 ns.
        for (market, valid, spread, asks, bids) in [
            (
                &view.markets[0],
                "60",
                "7.666666666",
                "7833.333333333",
                "15833.333333333",
            ),
            (
                &view.markets[1],
                "45",
                "8.444444444",
                "8388.888888888",
                "15722.222222222",
            ),
        ] {
            assert_eq!(market.coverage.observed_ns, "100");
            assert_eq!(market.coverage.valid_ns, valid);
            assert_eq!(market.coverage.two_sided_ns, valid);
            assert_eq!(market.coverage.mean_spread_ticks.as_deref(), Some(spread));
            assert_eq!(market.coverage.mean_ask_quantity.as_deref(), Some(asks));
            assert_eq!(market.coverage.mean_bid_quantity.as_deref(), Some(bids));
        }
    }
}

#[test]
fn grouped_intervals_preserve_overflow_and_future_previews_do_not_poison_sums() {
    let mut snapshot: serde_json::Value = serde_json::from_slice(SNAPSHOT).unwrap();
    for level in snapshot["data"]["ask_agg_data"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .take(2)
    {
        level["qty"] = serde_json::json!(u64::MAX.to_string());
    }
    let snapshot = serde_json::to_vec(&snapshot).unwrap();
    let mut p = pipeline();
    boundary(&mut p, 0, 1, true);
    assert!(message(&mut p, 0, 1, &snapshot).changed);
    let preview = p.view("preview", u64::MAX, 0, Quantity(0));
    assert!(preview.markets[0].coverage.mean_ask_quantity.is_none());
    p.finish(100).unwrap();
    let actual = p.view("fixture", 100, 0, Quantity(0));
    assert_eq!(
        actual.markets[0].coverage.mean_ask_quantity.as_deref(),
        Some("36893488147419107730")
    );
    for part in 1..=3 {
        assert!(!message(&mut p, (u64::MAX / 4) * part, 1, b"{\"type\":\"pong\"}").recover);
    }
    p.finish(u64::MAX).unwrap();
    let actual = p.view("fixture", u64::MAX, 0, Quantity(0));
    let coverage = &actual.markets[0].coverage;
    assert_eq!(coverage.valid_ns, u64::MAX.to_string());
    assert_eq!(coverage.two_sided_ns, u64::MAX.to_string());
    assert_eq!(coverage.mean_spread_ticks.as_deref(), Some("10"));
    assert_eq!(coverage.mean_bid_quantity.as_deref(), Some("15500"));
    assert!(coverage.mean_ask_quantity.is_none());
}

#[test]
fn receive_metadata_preserves_digest_and_rejects_cross_generation() {
    use pax_client::websocket::receive::{
        ReadObservation, ReceiveTiming, ReceiveWork, kernel::Stamp,
    };
    use std::time::Instant;
    let mut plain = pipeline();
    let mut measured = pipeline();
    boundary(&mut plain, 0, 1, true);
    boundary(&mut measured, 0, 1, true);
    let received = Instant::now();
    let mut timing = ReceiveTiming {
        selection: Default::default(),
        revision: 0,
        complete_policy_window: true,
        policy: pax_client::websocket::receive::policy::ReceivePolicy::DETAILED,
        latest_read: None,
        first_observed_read: Some(ReadObservation {
            batch: 2,
            ended: received,
            software: Stamp::unavailable("initial read not selected"),
            hardware: Stamp::from_parts(
                Some(received - std::time::Duration::from_nanos(100)),
                Some(7),
                Some(20),
                "nic-hardware/phc",
                "test aligned",
            ),
        }),
        generation: 1,
        plaintext: None,
        socket: Some(pax_client::websocket::receive::BatchRange { first: 2, last: 3 }),
        first_read: Some(received),
        last_read: None,
        shared: true,
        buffered: true,
        coverage: "unavailable test",
        work: ReceiveWork::default(),
        lost: 2,
        sample_every: 1,
        software: Stamp::unavailable("missing"),
        hardware: Stamp::unavailable("permission"),
    };
    for (offset, bytes) in [(1, SNAPSHOT), (2, XA)] {
        let record = Record {
            offset_ns: offset,
            generation: 1,
            kind: RecordKind::Message(Bytes::copy_from_slice(bytes)),
        };
        plain.process(&record, 0).unwrap();
        measured
            .process_instrumented(&record, received, None, Some(timing))
            .unwrap();
        assert_eq!(plain.digest(), measured.digest());
        timing.generation = 2;
    }
    let view = measured.view("test", 2, 20, Quantity(1));
    assert_eq!(view.latency.receive.len(), 1);
    assert_eq!(view.latency.receive[0].lost, 2);
    assert_eq!(view.latency.receive[0].schema_version, 3);
    let initial = view.latency.receive[0].first_observed_read.unwrap();
    assert_eq!(initial.batch, 2);
    assert_eq!(initial.software.status, "initial read not selected");
    assert_eq!(initial.hardware.batch_to_message_ns, Some(100));
    assert_eq!(initial.hardware.uncertainty_ns, Some(20));
    assert!(
        view.latency.receive[0]
            .hardware
            .batch_to_message_ns
            .is_none()
    );
    assert!(
        view.latency.receive[0]
            .software
            .batch_to_message_ns
            .is_none()
    );
    assert!(
        plain
            .view("old capture", 2, 20, Quantity(1))
            .latency
            .receive
            .is_empty()
    );
    assert!(
        measured
            .view("expired", 20_000_000_000, 20, Quantity(1))
            .latency
            .receive
            .is_empty()
    );
}

#[test]
fn receive_sampling_is_independent_and_its_window_loss_is_visible() {
    use pax_client::websocket::receive::{ReceiveTiming, ReceiveWork, kernel::Stamp};
    use std::time::Instant;
    let mut p = pipeline_with_sampling(8);
    boundary(&mut p, 0, 1, true);
    let timing = ReceiveTiming {
        selection: Default::default(),
        revision: 0,
        complete_policy_window: true,
        policy: pax_client::websocket::receive::policy::ReceivePolicy::DETAILED,
        latest_read: None,
        first_observed_read: None,
        generation: 1,
        plaintext: None,
        socket: None,
        first_read: None,
        last_read: None,
        shared: false,
        buffered: true,
        coverage: "test",
        work: ReceiveWork::default(),
        lost: 0,
        sample_every: 1,
        software: Stamp::unavailable("test"),
        hardware: Stamp::unavailable("test"),
    };
    for offset in 1..=2050 {
        let record = Record {
            offset_ns: offset,
            generation: 1,
            kind: RecordKind::Message(Bytes::from_static(br#"{"type":"pong"}"#)),
        };
        p.process_instrumented(&record, Instant::now(), None, Some(timing))
            .unwrap();
        assert_eq!(p.latest_receive().unwrap().id.message, offset);
    }
    let view = p.view("test", 2050, 1, Quantity(1));
    assert_eq!(view.latency.receive.len(), 2048);
    assert_eq!(view.latency.receive_capacity_drops, 2);
    assert_eq!(view.latency.traces.len(), 256);
}

#[test]
fn reconnect_preserves_common_receive_processing_sampling_and_trace_identity() {
    use pax_client::websocket::receive::{ReceiveTiming, ReceiveWork, kernel::Stamp};
    use std::time::Instant;
    let mut p = pipeline_with_sampling(8);
    let mut offset = 0;
    let mut total = 0;
    for (generation, count) in [(1, 5), (2, 11), (3, 9)] {
        offset += 1;
        boundary(&mut p, offset, generation, true);
        for ordinal in 1..=count {
            offset += 1;
            total += 1;
            let receive = (ordinal % 8 == 0).then_some(ReceiveTiming {
                selection: Default::default(),
                revision: 0,
                complete_policy_window: true,
                policy: pax_client::websocket::receive::policy::ReceivePolicy::DETAILED,
                latest_read: None,
                first_observed_read: None,
                generation,
                plaintext: None,
                socket: None,
                first_read: None,
                last_read: None,
                shared: false,
                buffered: true,
                coverage: "synthetic sampling only",
                work: ReceiveWork::default(),
                lost: 0,
                sample_every: 8,
                software: Stamp::unavailable("sampling test"),
                hardware: Stamp::unavailable("sampling test"),
            });
            let record = Record {
                offset_ns: offset,
                generation,
                kind: RecordKind::Message(Bytes::from_static(if ordinal == 1 {
                    SNAPSHOT
                } else {
                    br#"{"type":"pong"}"#
                })),
            };
            p.process_instrumented(&record, Instant::now(), None, receive)
                .unwrap();
            if ordinal % 8 == 0 {
                let view = p.view("fixture", offset, 20, Quantity(1));
                let received = view.latency.receive.last().unwrap();
                let trace = view
                    .latency
                    .traces
                    .iter()
                    .find(|t| t.id == received.id)
                    .unwrap();
                assert_eq!((trace.id.generation, trace.id.message), (generation, total));
                assert!(trace.processing_ns.is_some() && trace.queue_ns.is_some());
                assert_eq!(trace.policy_revision, 0);
            }
        }
        let status = p
            .view("fixture", offset, 20, Quantity(1))
            .latency
            .processing
            .unwrap();
        assert_eq!(status.callback.eligible, count);
        assert_eq!(status.callback.selected, count / 8);
        offset += 1;
        boundary(&mut p, offset, generation, false);
    }
}

#[test]
fn processing_domains_change_independently_without_changing_reconstruction() {
    use pax_client::measurement::SampleRate;
    use pax_demo::measurement::policy::{BookPolicy, ProcessingPolicy};
    let fixture = pax_client::test_support::fixture::Fixture::synthetic(30, 20).unwrap();
    let mut plain = pipeline_with_sampling(0);
    let mut measured = pipeline();
    let policy = ProcessingPolicy {
        callback: SampleRate::every(3),
        decode: SampleRate::every(2),
        book: BookPolicy {
            preflight: SampleRate::every(5),
            mutation: SampleRate::every(7),
            validation: SampleRate::every(11),
        },
        bookkeeping: SampleRate::OFF,
    };
    let applied = measured.configure_measurements(policy).unwrap();
    assert_eq!(applied.revision, 1);
    assert_eq!(applied.after_message, 0);
    for p in [&mut plain, &mut measured] {
        boundary(p, 0, 1, true);
    }
    for (index, bytes) in fixture.messages.iter().enumerate() {
        let ordinal = index as u64 + 1;
        let record = Record {
            generation: 1,
            offset_ns: ordinal,
            kind: RecordKind::Message(bytes.clone()),
        };
        plain.process(&record, 0).unwrap();
        measured.process(&record, 0).unwrap();
        assert_eq!(plain.digest(), measured.digest());
        if ordinal == 15 {
            let applied = measured
                .configure_measurements(ProcessingPolicy {
                    book: BookPolicy {
                        preflight: SampleRate::every(7),
                        ..policy.book
                    },
                    ..policy
                })
                .unwrap();
            assert_eq!(applied.revision, 2);
            assert_eq!(applied.after_message, 15);
            let invalid = ProcessingPolicy {
                decode: SampleRate {
                    every: 2,
                    width: 3,
                    phase: 0,
                },
                ..policy
            };
            assert!(measured.configure_measurements(invalid).is_err());
            assert_eq!(
                measured.measurement_policy().book.preflight,
                SampleRate::every(7)
            );
        }
    }
    let view = measured.view("fixture", 32, 20, Quantity(1));
    for trace in &view.latency.traces {
        assert_eq!(
            trace.decode_ns.is_some(),
            trace.id.message.is_multiple_of(2)
        );
        assert_eq!(
            trace.processing_ns.is_some(),
            trace.id.message.is_multiple_of(3)
        );
        assert_eq!(
            trace.policy_revision,
            if trace.id.message <= 15 { 1 } else { 2 }
        );
        assert!(trace.bookkeeping_ns.is_none());
        if trace.population == "event" {
            let book_selected = if trace.id.message <= 15 {
                trace.id.message.is_multiple_of(5)
            } else {
                (trace.id.message - 15).is_multiple_of(7)
            };
            assert_eq!(trace.preflight_ns.is_some(), book_selected);
            assert_eq!(
                trace.mutation_ns.is_some(),
                trace.id.message.is_multiple_of(7)
            );
            assert_eq!(
                trace.validation_ns.is_some(),
                trace.id.message.is_multiple_of(11)
            );
        }
    }
    let status = view.latency.processing.unwrap();
    assert_eq!(status.callback.eligible, 32);
    assert_eq!(status.callback.selected, 10);
    assert_eq!(status.decode.eligible, 32);
    assert_eq!(status.decode.selected, 16);
    assert_eq!(status.preflight.eligible, 17);
    assert_eq!(status.preflight.selected, 2);
    assert_eq!(status.mutation.selected, 4);
    assert_eq!(status.validation.selected, 2);
    assert!(
        plain
            .view("fixture", 32, 20, Quantity(1))
            .latency
            .traces
            .is_empty()
    );
}

#[test]
fn live_replay_and_shared_headless_session_agree_through_recovery() {
    use pax_book::{
        session::Session,
        state::{BookError, Status},
    };
    use pax_types::protocol::{decode, event::Side};
    use std::time::Instant;
    for failure in [
        b"{malformed".as_slice(),
        br#"{"type":"unsubscribed","market_ids":[1]}"#,
        br#"{"type":"error","message":"fixture"}"#,
    ] {
        let mut live = pipeline_with_sampling(u64::from(cfg!(feature = "processing-traces")));
        let mut replay = pipeline_with_sampling(0);
        let mut headless = Session::new(&[MarketId(1)], 1024, 100_000).unwrap();
        let mut time = 0;
        for generation in 1..=2 {
            for pipeline in [&mut live, &mut replay] {
                boundary(pipeline, time, generation, true);
            }
            headless.connect(generation).unwrap();
            let old = generation - 1;
            for (input_generation, bytes) in [
                (old, XA),
                (generation, XA),
                (generation, SNAPSHOT),
                (generation, XA),
                (generation, br#"{"type":"pong"}"#.as_slice()),
                (generation, failure),
                (generation, SNAPSHOT),
            ] {
                time += 10;
                let record = Record {
                    offset_ns: time,
                    generation: input_generation,
                    kind: RecordKind::Message(Bytes::copy_from_slice(bytes)),
                };
                let expected = match decode(bytes) {
                    Ok(message) => headless
                        .apply(input_generation, message)
                        .map(|outcome| outcome.committed()),
                    Err(_) => {
                        headless.invalidate(BookError::Inconsistent("decoder continuity"));
                        Err(pax_book::session::SessionError::Disconnected)
                    }
                };
                let direct = live.process_received(&record, Instant::now()).unwrap();
                let recorded = replay.process(&record, 100).unwrap();
                assert_eq!(
                    (direct.changed, direct.recover),
                    (
                        expected.as_ref().is_ok_and(|changed| *changed),
                        expected.is_err()
                    )
                );
                assert_eq!(
                    (direct.changed, direct.recover),
                    (recorded.changed, recorded.recover)
                );
                assert_eq!(live.digest(), replay.digest());
                let report = live.view("fixture", time, 128, Quantity(1));
                let market = &report.markets[0];
                let book = headless.book(MarketId(1)).unwrap();
                assert_eq!(
                    market.sequence,
                    book.sequence().map(|value| value.0.to_string())
                );
                assert_eq!(
                    market.status,
                    match book.status() {
                        Status::Live => "live",
                        Status::AwaitingSnapshot => "awaiting snapshot",
                        Status::Invalid(_) => "invalid",
                    }
                );
                for (side, shown) in [(Side::Ask, &market.asks), (Side::Bid, &market.bids)] {
                    let actual: Vec<_> = shown
                        .iter()
                        .map(|level| (level.ticks.clone(), level.native_quantity.clone()))
                        .collect();
                    let canonical: Vec<_> = book
                        .levels(side)
                        .unwrap_or_default()
                        .iter()
                        .map(|level| (level.ticks.0.to_string(), level.qty.0.to_string()))
                        .collect();
                    assert_eq!(actual, canonical);
                }
            }
            time += 10;
            for pipeline in [&mut live, &mut replay] {
                boundary(pipeline, time, generation, false);
            }
            headless.invalidate(BookError::Unavailable);
            time += 10;
        }
        live.finish(time).unwrap();
        replay.finish(time).unwrap();
        assert_eq!(live.digest(), replay.digest());
        assert_eq!(
            live.view("live", time, 20, Quantity(1)).markets[0]
                .coverage
                .valid_ns,
            replay.view("replay", time, 20, Quantity(1)).markets[0]
                .coverage
                .valid_ns
        );
    }
}
