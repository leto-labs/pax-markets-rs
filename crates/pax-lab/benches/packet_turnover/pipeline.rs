//! Full application and display costs, separately from the lean core callback.
use super::{report, workloads::Workload};
use bytes::Bytes;
use pax_demo::{
    capture::{Record, RecordKind},
    pipeline::Pipeline,
};
use pax_types::units::{MarketId, Quantity};
use std::{hint::black_box, time::Instant};
pub fn compare(workload: &Workload, rounds: usize) {
    compare_markets(workload, rounds, 1);
    if workload.name == "public-active-2026-10-07-depth20" {
        compare_markets(workload, rounds, 64);
    }
}
fn compare_markets(workload: &Workload, rounds: usize, market_count: u64) {
    let name = if market_count == 1 {
        workload.name.clone()
    } else {
        format!(
            "synthetic-{market_count}-selected-one-active-{}",
            workload.name
        )
    };
    let mut metadata = pax_demo::runner::offline::fixture_metadata().unwrap();
    let template = metadata.markets[0].clone();
    metadata.markets.clear();
    let mut snapshots = Vec::new();
    let selected: Vec<_> = (1..=market_count).map(MarketId).collect();
    for &id in &selected {
        let mut market = template.clone();
        market.market_id = id;
        metadata.markets.push(market);
        let mut snapshot = workload.snapshot.clone();
        snapshot.market_id = id;
        snapshots.push(Bytes::from(
            serde_json::to_vec(&serde_json::json!({"type":"snapshot","data":snapshot})).unwrap(),
        ));
    }
    let events: Vec<_> = workload
        .raw
        .iter()
        .enumerate()
        .map(|(i, bytes)| Record {
            offset_ns: (i as u64 + 1) * 250_000,
            generation: 1,
            kind: RecordKind::Message(Bytes::copy_from_slice(bytes)),
        })
        .collect();
    let end = (events.len() as u64 + 1) * 250_000;
    // Include initialization in the synthetic identity: the active event bytes
    // alone do not distinguish one selected market from 64 selected markets.
    let mut input: Vec<_> = workload.raw.iter().flatten().copied().collect();
    if market_count > 1 {
        for snapshot in &snapshots {
            input.extend_from_slice(snapshot);
        }
    }
    let prepare = |sampling| {
        let mut pipeline = Pipeline::new(&metadata, &selected, sampling).unwrap();
        pipeline
            .process(
                &Record {
                    offset_ns: 0,
                    generation: 1,
                    kind: RecordKind::Connected,
                },
                0,
            )
            .unwrap();
        for snapshot in &snapshots {
            pipeline
                .process(
                    &Record {
                        offset_ns: 0,
                        generation: 1,
                        kind: RecordKind::Message(snapshot.clone()),
                    },
                    0,
                )
                .unwrap();
        }
        pipeline
    };
    let mut expected_digest = None;
    for (mode, sampling) in [
        ("application_unsampled", 0),
        ("application_sampled", 1),
        ("application_sampled_8", 8),
        ("application_sampled_10", 10),
    ] {
        if sampling != 0 && !cfg!(feature = "processing-traces") {
            continue;
        }
        let mut samples = Vec::with_capacity(rounds * events.len());
        let mut publication = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            let mut pipeline = prepare(sampling);
            for record in &events {
                let start = Instant::now();
                let result = black_box(pipeline.process(black_box(record), 0).unwrap());
                samples.push(start.elapsed());
                assert!(result.changed && !result.recover);
            }
            pipeline.finish(end).unwrap();
            let digest = accounting_digest(&pipeline, end);
            if let Some(expected) = expected_digest.as_ref() {
                assert_eq!(&digest, expected);
            } else {
                expected_digest = Some(digest);
            }
            // Worst-case publication here asks for more than represented depth,
            // exercising both size-estimate scans and actual JSON encoding.
            if market_count == 1 && sampling == 1 {
                let start = Instant::now();
                let view = pipeline.view("offline benchmark", end, 20, Quantity(100_000_000));
                let bytes = serde_json::to_vec(&view).unwrap();
                black_box(&bytes);
                publication.push(start.elapsed());
                assert!(!bytes.is_empty());
            }
        }
        #[cfg(feature = "allocation-profile")]
        {
            let mut pipeline = prepare(sampling);
            let ((), calls, requested_bytes) = super::allocations::measure(|| {
                for record in &events {
                    let result = black_box(pipeline.process(black_box(record), 0).unwrap());
                    assert!(result.changed && !result.recover);
                }
            });
            pipeline.finish(end).unwrap();
            assert_eq!(
                accounting_digest(&pipeline, end),
                *expected_digest.as_ref().unwrap()
            );
            println!(
                "{}",
                serde_json::json!({"kind":"allocations", "mode":mode,
                "workload":name, "messages":events.len(), "calls":calls,
                "requested_bytes":requested_bytes, "state_digest":accounting_digest(&pipeline, end)})
            );
        }
        report(
            mode,
            &name,
            &input,
            &mut samples,
            expected_digest.as_ref().unwrap(),
        );
        if sampling == 1 && market_count == 1 {
            report(
                "view_and_json_publish",
                &name,
                &input,
                &mut publication,
                expected_digest.as_ref().unwrap(),
            );
        }
    }
}

// Include exact weighted coverage, which the reconstruction digest deliberately
// excludes. Timing samples and trace identities cannot influence this digest.
fn accounting_digest(pipeline: &Pipeline, end: u64) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(pipeline.digest().as_bytes());
    for market in pipeline.view("benchmark", end, 0, Quantity(0)).markets {
        hash.update(&serde_json::to_vec(&market.coverage).unwrap());
    }
    hash.finalize().to_hex().to_string()
}
