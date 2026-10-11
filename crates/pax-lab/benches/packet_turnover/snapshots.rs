//! Snapshot recovery costs; reset, retained-event preparation and checks are untimed.
use super::{
    report,
    workloads::{self, Workload},
};
use pax_book::state::{Book, Outcome};
use pax_types::{
    protocol::{self, Message, snapshot::Snapshot},
    units::MarketId,
};
use std::{hint::black_box, time::Instant};

pub fn compare(workload: &Workload, rounds: usize, selected: &str) {
    let Message::Snapshot(decoded) = protocol::decode(&workload.raw_snapshot).unwrap() else {
        panic!("snapshot fixture");
    };
    assert_eq!(decoded, workload.snapshot);
    let mut initial = Book::new(MarketId(1), 1024, 100_000).unwrap();
    initial.snapshot(0, decoded).unwrap();
    let reconciled = workload.expected();
    workload.check_decoded(&reconciled);
    for mode in ["decode", "apply", "combined", "reconcile"] {
        if selected != "all" && selected != mode {
            continue;
        }
        let expected = if mode == "reconcile" {
            &reconciled
        } else {
            &initial
        };
        let digest = workloads::digest(expected);
        let mut input = Vec::new();
        if mode == "reconcile" {
            for raw in &workload.raw {
                input.extend_from_slice(raw);
            }
        }
        input.extend_from_slice(&workload.raw_snapshot);
        let mut book = Book::new(MarketId(1), 1024, 100_000).unwrap();
        let mut samples = Vec::with_capacity(rounds);
        for round in 0..rounds + 100 {
            let prepared = prepare(&mut book, workload, mode, round as u64 + 1);
            let start = Instant::now();
            let decoded = process(&mut book, workload, mode, prepared);
            let elapsed = start.elapsed();
            if round >= 100 {
                samples.push(elapsed);
            }
            if let Some(decoded) = decoded {
                assert_eq!(decoded, workload.snapshot);
            } else {
                assert_eq!(workloads::digest(&book), digest);
            }
        }
        let name = format!("snapshot_{mode}");
        #[cfg(feature = "allocation-profile")]
        {
            let generation = book.generation() + 1;
            let prepared = prepare(&mut book, workload, mode, generation);
            let (decoded, calls, requested_bytes) =
                super::allocations::measure(|| process(&mut book, workload, mode, prepared));
            if let Some(decoded) = decoded {
                assert_eq!(decoded, workload.snapshot);
            } else {
                assert_eq!(workloads::digest(&book), digest);
            }
            println!(
                "{}",
                serde_json::json!({"kind":"allocations", "mode":name,
                "workload":workload.name, "snapshots":1, "calls":calls,
                "requested_bytes":requested_bytes, "state_digest":digest})
            );
        }
        report(&name, &workload.name, &input, &mut samples, &digest);
    }
}
fn prepare(book: &mut Book, workload: &Workload, mode: &str, generation: u64) -> Option<Snapshot> {
    book.reset(generation).unwrap();
    if mode == "reconcile" {
        for event in &workload.events {
            assert_eq!(book.event(generation, event).unwrap(), Outcome::Buffered);
        }
    }
    matches!(mode, "apply" | "reconcile").then(|| workload.snapshot.clone())
}
fn process(
    book: &mut Book,
    workload: &Workload,
    mode: &str,
    prepared: Option<Snapshot>,
) -> Option<Snapshot> {
    let snapshot = prepared.unwrap_or_else(|| {
        let Message::Snapshot(snapshot) =
            protocol::decode(black_box(&workload.raw_snapshot)).unwrap()
        else {
            panic!("snapshot fixture");
        };
        snapshot
    });
    if mode == "decode" {
        Some(black_box(snapshot))
    } else {
        black_box(
            book.snapshot(book.generation(), black_box(snapshot))
                .unwrap(),
        );
        None
    }
}
