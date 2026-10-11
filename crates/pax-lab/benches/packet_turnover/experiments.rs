//! Opt-in candidates. Failed compatibility gates never become performance claims.
#[cfg(any(feature = "cputicks-experiment", feature = "simd-experiment"))]
use super::workloads::Workload;
#[cfg(any(feature = "cputicks-experiment", feature = "hotpath"))]
use pax_types::protocol::{self, Message};
#[cfg(feature = "cputicks-experiment")]
use {
    super::report,
    pax_book::state::Book,
    pax_types::units::MarketId,
    std::{hint::black_box, time::Duration},
};

#[cfg(feature = "cputicks-experiment")]
pub fn ticks(workload: &Workload, iterations: usize, digest: &str) {
    use cputicks::Ticks;
    // Calibrate outside warmup. This candidate intentionally preserves upstream
    // ordering semantics; unfenced reads are not adopted as a default timer.
    let frequency = Ticks::frequency();
    assert_ne!(frequency, 0);
    println!(
        "{}",
        serde_json::json!({"kind":"clock_candidate", "backend":Ticks::implementation(),"frequency_hz":frequency,"ordering":"upstream; not independently fenced","default":false})
    );
    let mut raw_deltas = Vec::with_capacity(iterations * workload.raw.len());
    let mut backwards = 0;
    let expected = workload.expected();
    for round in 0..iterations + 100 {
        let mut book = Book::new(MarketId(1), 1024, 100_000).unwrap();
        book.snapshot(0, workload.snapshot.clone()).unwrap();
        for raw in &workload.raw {
            let start = Ticks::now().as_raw();
            let Message::Event(event) = protocol::decode(black_box(raw)).unwrap() else {
                panic!()
            };
            black_box(book.event(0, &event).unwrap());
            let end = Ticks::now().as_raw();
            if let Some(delta) = end.checked_sub(start) {
                if round >= 100 {
                    raw_deltas.push(delta);
                }
            } else {
                backwards += 1;
            }
        }
        assert_eq!(
            super::workloads::digest(&book),
            super::workloads::digest(&expected)
        );
    }
    assert_eq!(backwards, 0, "invalid nonmonotonic counter readings");
    let mut samples: Vec<_> = raw_deltas
        .into_iter()
        .map(|ticks| {
            Duration::from_nanos(
                (u128::from(ticks) * 1_000_000_000 / u128::from(frequency))
                    .try_into()
                    .unwrap(),
            )
        })
        .collect();
    let input: Vec<_> = workload.raw.iter().flatten().copied().collect();
    report(
        "cputicks_combined",
        &workload.name,
        &input,
        &mut samples,
        digest,
    );
}

#[cfg(feature = "simd-experiment")]
pub fn simd_compatibility(workload: &Workload, iterations: usize, digest: &str) {
    use pax_book::state::Book;
    use pax_lab::measurement::simd::Decoder;
    use pax_types::units::MarketId;
    use std::{hint::black_box, time::Instant};
    let mut decoder = Decoder::default();
    for (raw, expected) in workload.raw.iter().zip(&workload.events) {
        assert_eq!(
            &decoder.decode(raw).expect("SIMD compatibility gate"),
            expected
        );
    }
    println!(
        "{}",
        serde_json::json!({"kind":"simd_compatibility", "decoder":"simd-json 0.18.1 exact adapter, 128bit", "workload":workload.name, "messages":workload.events.len(), "equivalent":workload.events.len(), "error":null, "adopted":false, "preparation":"immutable input copied to reusable scratch; reusable SIMD buffers; copy included"})
    );
    let input: Vec<_> = workload.raw.iter().flatten().copied().collect();
    for mode in ["simd_decode", "simd_combined"] {
        let mut samples = Vec::with_capacity(iterations * workload.raw.len());
        for round in 0..iterations + 100 {
            let mut book = Book::new(MarketId(1), 1024, 100_000).unwrap();
            book.snapshot(0, workload.snapshot.clone()).unwrap();
            for raw in &workload.raw {
                let start = Instant::now();
                let event = decoder.decode(black_box(raw)).unwrap();
                if mode == "simd_combined" {
                    black_box(book.event(0, &event).unwrap());
                } else {
                    black_box(event);
                }
                let elapsed = start.elapsed();
                if round >= 100 {
                    samples.push(elapsed);
                }
            }
            if mode == "simd_combined" {
                assert_eq!(super::workloads::digest(&book), digest);
            }
        }
        super::report(mode, &workload.name, &input, &mut samples, digest);
    }
    #[cfg(feature = "allocation-profile")]
    {
        let mut calls = 0;
        let mut bytes = 0;
        for raw in &workload.raw {
            let (_, count, size) = super::allocations::measure(|| {
                black_box(decoder.decode(black_box(raw)).unwrap());
            });
            calls += count;
            bytes += size;
        }
        println!(
            "{}",
            serde_json::json!({"kind":"allocations","mode":"simd_decode","workload":workload.name,"messages":workload.raw.len(),"calls":calls,"bytes":bytes})
        );
    }
}

#[cfg(feature = "hotpath")]
#[hotpath::measure]
pub fn profiled_decode(bytes: &[u8]) -> Message {
    protocol::decode(bytes).unwrap()
}
