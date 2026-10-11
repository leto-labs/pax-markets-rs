//! Offline reconstruction benchmark. Fixture loading, reset, and reporting are untimed.
#[cfg(feature = "allocation-profile")]
#[allow(unsafe_code)] // Isolated delegating GlobalAlloc hook; safety invariants are local.
#[path = "packet_turnover/allocations.rs"]
mod allocations;
#[cfg(all(feature = "mimalloc", not(feature = "allocation-profile")))]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
#[path = "packet_turnover/containers.rs"]
mod containers;
#[cfg(any(
    feature = "cputicks-experiment",
    feature = "simd-experiment",
    feature = "hotpath"
))]
#[path = "packet_turnover/experiments.rs"]
mod experiments;
#[path = "packet_turnover/options.rs"]
mod options;
#[cfg(feature = "demo-integration")]
#[path = "packet_turnover/pipeline.rs"]
mod pipeline;
#[path = "packet_turnover/session.rs"]
mod session;
#[path = "packet_turnover/snapshots.rs"]
mod snapshots;
#[path = "packet_turnover/workloads.rs"]
mod workloads;
use clap::Parser;
use pax_book::state::Book;
use pax_lab::measurement::{TurnoverSummary, measure_message};
use pax_types::{
    protocol::{self, Message, event::Side},
    units::MarketId,
};
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

#[cfg_attr(feature = "hotpath", hotpath::main)]
fn main() {
    let options = options::Options::parse();
    let smoke = options.smoke || options.test;
    let mut selected = workloads::all();
    if options.list_workloads {
        for workload in selected {
            println!("{}", workload.name);
        }
        return;
    }
    if let Some(name) = &options.workload {
        selected.retain(|workload| &workload.name == name);
        if selected.is_empty() {
            eprintln!("Unknown workload {name:?}; use --list-workloads for exact names.");
            std::process::exit(2);
        }
    }
    #[cfg(feature = "allocation-profile")]
    allocations::self_check();
    let iterations = if smoke {
        32
    } else if options.snapshot_mode.is_some() {
        1000
    } else {
        10_000
    };
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("model name"))
                .map(str::to_owned)
        })
        .unwrap_or_else(|| std::env::consts::ARCH.into());
    println!(
        "{}",
        serde_json::json!({"kind":"identity", "workload_filter":options.workload, "core_mode_filter":options.core_mode, "snapshot_mode_filter":options.snapshot_mode, "session_markets":options.session_markets, "demo_pipeline_compiled":cfg!(feature="demo-integration"), "compact_integer_decoder":"atoi_simd 0.18.1 prefix", "build":pax_lab::build_info::current_build_info(), "cpu":cpu, "architecture":std::env::consts::ARCH, "target":env!("PAX_TARGET"), "rustflags":env!("PAX_RUSTFLAGS"), "allocator":if cfg!(feature="mimalloc") { "mimalloc" } else { "system" }, "allocation_instrumentation":cfg!(feature = "allocation-profile"), "clock":"std::time::Instant", "timer_overhead":"included", "network":"excluded", "iterations":iterations})
    );
    let mut overhead = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let (_, elapsed) = measure_message(b"", |bytes| black_box(bytes).len());
        overhead.push(elapsed);
    }
    report("timer", "empty callback", &[], &mut overhead, "n/a");
    if options.workload.is_none()
        && options.core_mode.is_none()
        && options.snapshot_mode.is_none()
        && options.session_markets.is_none()
    {
        containers::compare(iterations);
    }
    for workload in selected {
        if let Some(markets) = options.session_markets {
            session::compare(&workload, iterations, usize::from(markets));
            continue;
        }
        if let Some(mode) = &options.snapshot_mode {
            snapshots::compare(&workload, iterations, mode);
            continue;
        }
        #[cfg(feature = "demo-integration")]
        if options.core_mode.is_none() {
            pipeline::compare(&workload, if smoke { 8 } else { 1000 });
        }
        let expected = workload.expected();
        workload.check_decoded(&expected);
        let digest = workloads::digest(&expected);
        #[cfg(feature = "cputicks-experiment")]
        if options.core_mode.is_none() {
            experiments::ticks(&workload, iterations, &digest);
        }
        #[cfg(feature = "simd-experiment")]
        if options.core_mode.is_none() {
            experiments::simd_compatibility(&workload, iterations, &digest);
        }
        for mode in ["decode", "apply", "combined", "instrumented"] {
            if options
                .core_mode
                .as_deref()
                .is_some_and(|selected| selected != mode)
            {
                continue;
            }
            let mut samples = Vec::with_capacity(iterations * workload.events.len());
            for round in 0..iterations + 100 {
                let mut book = Book::new(MarketId(1), 1024, 100_000).unwrap();
                book.snapshot(0, workload.snapshot.clone()).unwrap();
                for (raw, prepared) in workload.raw.iter().zip(&workload.events) {
                    let start = Instant::now();
                    match mode {
                        "decode" => {
                            #[cfg(feature = "hotpath")]
                            black_box(experiments::profiled_decode(black_box(raw)));
                            #[cfg(not(feature = "hotpath"))]
                            black_box(protocol::decode(black_box(raw)).unwrap());
                        }
                        "apply" => {
                            black_box(book.event(0, black_box(prepared)).unwrap());
                        }
                        "combined" | "instrumented" => {
                            let Message::Event(event) = protocol::decode(black_box(raw)).unwrap()
                            else {
                                panic!("event fixture");
                            };
                            if mode == "instrumented" {
                                black_box(Instant::now());
                                black_box(
                                    book.event_with_observer(0, &event, |_| {
                                        black_box(Instant::now());
                                    })
                                    .unwrap(),
                                );
                            } else {
                                black_box(book.event(0, &event).unwrap());
                            }
                        }
                        _ => unreachable!(),
                    }
                    let elapsed = start.elapsed();
                    if round >= 100 {
                        samples.push(elapsed);
                    }
                }
                if mode != "decode" {
                    assert_eq!(book.sequence(), expected.sequence());
                    for side in [Side::Ask, Side::Bid] {
                        assert_eq!(book.levels(side).unwrap(), expected.levels(side).unwrap());
                    }
                }
            }
            let input: Vec<u8> = workload.raw.iter().flatten().copied().collect();
            report(mode, &workload.name, &input, &mut samples, &digest);
        }
        if options.core_mode.is_some() {
            continue;
        }
        #[cfg(feature = "allocation-profile")]
        for mode in ["decode", "apply", "combined"] {
            let mut calls = 0;
            let mut bytes = 0;
            let mut book = Book::new(MarketId(1), 1024, 100_000).unwrap();
            book.snapshot(0, workload.snapshot.clone()).unwrap();
            for (raw, event) in workload.raw.iter().zip(&workload.events) {
                let (_, count, size) = allocations::measure(|| match mode {
                    "decode" => {
                        black_box(protocol::decode(black_box(raw)).unwrap());
                    }
                    "apply" => {
                        book.event(0, black_box(event)).unwrap();
                    }
                    _ => {
                        let Message::Event(event) = protocol::decode(black_box(raw)).unwrap()
                        else {
                            panic!()
                        };
                        book.event(0, &event).unwrap();
                    }
                });
                calls += count;
                bytes += size;
            }
            println!(
                "{}",
                serde_json::json!({"kind":"allocations", "workload":workload.name, "mode":mode, "messages":workload.events.len(), "calls":calls, "requested_bytes":bytes, "state_digest":digest})
            );
        }
        for mode in ["quantity_cached", "quantity_scan"] {
            let mut samples = Vec::with_capacity(iterations);
            let expected_quantity = expected
                .levels(Side::Ask)
                .unwrap()
                .iter()
                .map(|l| u128::from(l.qty.0))
                .sum::<u128>();
            for _ in 0..iterations {
                let started = Instant::now();
                let quantity = if mode == "quantity_cached" {
                    black_box(&expected).total_quantity(Side::Ask).unwrap()
                } else {
                    black_box(&expected)
                        .levels(Side::Ask)
                        .unwrap()
                        .iter()
                        .map(|l| u128::from(l.qty.0))
                        .sum::<u128>()
                };
                black_box(quantity);
                samples.push(started.elapsed());
                assert_eq!(quantity, expected_quantity);
            }
            report(
                mode,
                &workload.name,
                &serde_json::to_vec(&workload.snapshot).unwrap(),
                &mut samples,
                &digest,
            );
        }
        let mut invalid = workload.events[0];
        invalid.data.seq_num.0 += 1;
        let mut samples = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let mut book = Book::new(MarketId(1), 1024, 100_000).unwrap();
            book.snapshot(0, workload.snapshot.clone()).unwrap();
            let start = Instant::now();
            let result = black_box(book.event(0, black_box(&invalid)));
            samples.push(start.elapsed());
            assert!(matches!(
                result,
                Err(pax_book::state::BookError::Gap { .. })
            ));
            assert!(book.top().is_err());
        }
        report(
            "reject_gap",
            &workload.name,
            &serde_json::to_vec(&invalid).unwrap(),
            &mut samples,
            "invalid",
        );
    }
}
fn report(mode: &str, workload: &str, input: &[u8], samples: &mut [Duration], state_digest: &str) {
    let result = TurnoverSummary::from_samples(samples).unwrap();
    println!(
        "{}",
        serde_json::json!({"kind":"latency", "mode":mode, "workload":workload, "input_digest":blake3::hash(input).to_hex().to_string(), "state_digest":state_digest, "samples":result.samples, "min_ns":result.min.as_nanos(), "p50_ns":result.p50.as_nanos(), "p95_ns":result.p95.as_nanos(), "p99_ns":result.p99.as_nanos(), "max_ns":result.max.as_nanos()})
    );
}
