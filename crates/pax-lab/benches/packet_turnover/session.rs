//! Fixed-market session transitions against independently specified final ladders.
use super::{report, workloads};
use pax_book::session::Session;
use pax_types::{protocol::Message, units::MarketId};
use std::{hint::black_box, time::Instant};

pub(super) fn compare(workload: &workloads::Workload, iterations: usize, count: usize) {
    let markets: Vec<_> = (0..count)
        .map(|index| MarketId(1 + index as u64 * 1009))
        .collect();
    let expected = workload.expected();
    workload.check_decoded(&expected);
    let expected_digest = workloads::digest(&expected);
    let rounds = iterations.div_ceil(count);
    let mut samples = Vec::with_capacity(rounds * count * workload.events.len());
    // Encode remapping and order alongside the original wire fixture identity.
    let mut input = b"rotating-market-session-v1".to_vec();
    for market in &markets {
        input.extend_from_slice(&market.0.to_le_bytes());
    }
    input.extend_from_slice(&(workload.raw_snapshot.len() as u64).to_le_bytes());
    input.extend_from_slice(&workload.raw_snapshot);
    for raw in &workload.raw {
        input.extend_from_slice(&(raw.len() as u64).to_le_bytes());
        input.extend_from_slice(raw);
    }
    #[cfg(feature = "allocation-profile")]
    {
        let (_, calls, bytes) = super::allocations::measure(|| {
            black_box(Session::new(&markets, 1024, 100_000).unwrap())
        });
        println!(
            "{}",
            serde_json::json!({"kind":"session_construction_allocations","markets":count,"calls":calls,"bytes":bytes,"scope":"cold session construction; no snapshot or event"})
        );
    }
    for round in 0..rounds + 16 {
        let mut session = Session::new(&markets, 1024, 100_000).unwrap();
        session.connect(1).unwrap();
        for &market in &markets {
            let mut snapshot = workload.snapshot.clone();
            snapshot.market_id = market;
            assert!(
                session
                    .apply(1, Message::Snapshot(snapshot))
                    .unwrap()
                    .committed()
            );
        }
        for (ordinal, &prepared) in workload.events.iter().enumerate() {
            for index in 0..count {
                let mut event = prepared;
                event.order.market_id = markets[(index + ordinal) % count];
                let message = black_box(Message::Event(event));
                let started = Instant::now();
                let outcome = black_box(session.apply(1, message).unwrap());
                let elapsed = started.elapsed();
                assert!(outcome.committed());
                if round >= 16 {
                    samples.push(elapsed);
                }
            }
        }
        assert_eq!(session.books().len(), count);
        for &market in &markets {
            let book = session.book(market).unwrap();
            assert_eq!(book.market(), market);
            assert_eq!(workloads::digest(book), expected_digest);
        }
    }
    println!(
        "{}",
        serde_json::json!({"kind":"session_policy","markets":count,"rounds":rounds,"warmup_rounds":16,"events_per_market":workload.events.len(),"scope":"decoded event through validated session/book commit; setup and verification excluded; per-event clocks included"})
    );
    report(
        &format!("session-{count}"),
        &workload.name,
        &input,
        &mut samples,
        &expected_digest,
    );
}
