//! Session transitions against independent single-market books and public captures.
use pax_book::{
    session::{Session, SessionError, SessionOutcome},
    state::{Book, BookError, Phase, Status},
};
use pax_types::{
    protocol::{
        Message, decode,
        event::{Event, Side},
        snapshot::Snapshot,
    },
    units::{MarketId, Sequence},
};

fn snapshot(market: MarketId) -> Snapshot {
    let Message::Snapshot(mut value) = decode(include_bytes!(
        "../../pax-types/tests/fixtures/snapshot.json"
    ))
    .unwrap() else {
        panic!()
    };
    value.market_id = market;
    value
}
fn event(market: MarketId) -> Event {
    let Message::Event(mut value) =
        decode(include_bytes!("../../pax-types/tests/fixtures/xa.json")).unwrap()
    else {
        panic!()
    };
    value.order.market_id = market;
    value
}
fn session() -> Session {
    Session::new(&[MarketId(1), MarketId(2)], 16, 128).unwrap()
}
fn live() -> Session {
    let mut session = session();
    session.connect(1).unwrap();
    for id in [MarketId(1), MarketId(2)] {
        assert!(
            session
                .apply(1, Message::Snapshot(snapshot(id)))
                .unwrap()
                .committed()
        );
    }
    session
}
fn same_book(left: &Book, right: &Book) {
    assert_eq!(left.market(), right.market());
    assert_eq!(left.generation(), right.generation());
    assert_eq!(left.status(), right.status());
    assert_eq!(left.sequence(), right.sequence());
    for side in [Side::Ask, Side::Bid] {
        assert_eq!(left.levels(side), right.levels(side));
        assert_eq!(left.total_quantity(side), right.total_quantity(side));
    }
}

#[test]
fn invalid_construction_rejects_bounds_before_any_connection() {
    for markets in [
        vec![],
        vec![MarketId(0)],
        vec![MarketId(1), MarketId(1)],
        (1..=257).map(MarketId).collect(),
    ] {
        assert!(matches!(
            Session::new(&markets, 16, 128),
            Err(SessionError::Configuration(_))
        ));
    }
    assert!(Session::new(&[MarketId(1)], 0, 128).is_err());
    assert!(Session::new(&[MarketId(1)], 16, 0).is_err());
}

#[test]
fn buffered_reconciliation_and_duplicate_match_the_existing_book() {
    let mut session = session();
    session.connect(1).unwrap();
    let mut independent = Book::new(MarketId(1), 16, 128).unwrap();
    independent.reset(1).unwrap();
    for message in [
        Message::Event(event(MarketId(1))),
        Message::Snapshot(snapshot(MarketId(1))),
        Message::Event(event(MarketId(1))),
    ] {
        let expected = match message.clone() {
            Message::Snapshot(snapshot) => independent.snapshot(1, snapshot).unwrap(),
            Message::Event(event) => independent.event(1, &event).unwrap(),
            _ => unreachable!(),
        };
        assert_eq!(
            session.apply(1, message).unwrap(),
            SessionOutcome::Book {
                market: MarketId(1),
                sequence: independent.sequence(),
                outcome: expected
            }
        );
        same_book(session.book(MarketId(1)).unwrap(), &independent);
    }
    assert_eq!(
        session.book(MarketId(2)).unwrap().status(),
        Status::AwaitingSnapshot
    );
}

#[test]
fn rejected_message_invalidates_every_market_and_requires_a_new_generation() {
    for message in [
        Message::Error("venue failure".into()),
        Message::Unsubscribed(vec![MarketId(1)]),
        Message::Event(event(MarketId(9))),
        {
            let mut gap = event(MarketId(1));
            gap.data.seq_num = Sequence(4);
            Message::Event(gap)
        },
    ] {
        let mut session = live();
        assert!(session.apply(1, message).is_err());
        assert!(!session.is_connected());
        assert!(session.books().all(|book| book.levels(Side::Ask).is_err()));
        assert_eq!(session.connect(1), Err(SessionError::Generation));
        assert!(
            session
                .apply(1, Message::Snapshot(snapshot(MarketId(1))))
                .is_err()
        );
        session.connect(2).unwrap();
        assert_eq!(
            session
                .apply(1, Message::Snapshot(snapshot(MarketId(1))))
                .unwrap(),
            SessionOutcome::OldGeneration
        );
        assert_eq!(
            session.book(MarketId(1)).unwrap().status(),
            Status::AwaitingSnapshot
        );
        assert!(
            session
                .apply(2, Message::Snapshot(snapshot(MarketId(1))))
                .unwrap()
                .committed()
        );
    }
}

#[test]
fn future_or_disconnected_input_never_exposes_current_liquidity() {
    let mut session = session();
    assert_eq!(
        session.apply(0, Message::Pong),
        Err(SessionError::Disconnected)
    );
    session.connect(1).unwrap();
    session
        .apply(1, Message::Snapshot(snapshot(MarketId(1))))
        .unwrap();
    assert_eq!(
        session.apply(2, Message::Pong),
        Err(SessionError::Disconnected)
    );
    assert!(!session.is_connected());
    assert!(session.books().all(|book| book.top().is_err()));
}

#[test]
fn invalid_connection_boundary_has_no_partial_resets_or_observation() {
    let mut session = live();
    for generation in [0, 1, 2] {
        assert_eq!(
            session.connect_observed(generation, |_| panic!("invalid reset observer")),
            Err(SessionError::Generation)
        );
        assert_eq!(session.generation(), 1);
        assert!(session.books().all(|book| book.status() == Status::Live));
    }
    session.invalidate(BookError::Unavailable);
    assert_eq!(session.connect(0), Err(SessionError::Generation));
    session.connect(2).unwrap();
    assert!(
        session
            .books()
            .all(|book| book.generation() == 2 && book.status() == Status::AwaitingSnapshot)
    );
}

#[test]
fn controls_and_obsolete_input_do_not_run_book_observers() {
    let mut session = live();
    for message in [Message::Pong, Message::Subscribed(vec![MarketId(1)])] {
        assert_eq!(
            session
                .apply_observed(
                    1,
                    message,
                    |_| panic!("control mutation"),
                    |_| panic!("control phase")
                )
                .unwrap(),
            SessionOutcome::Control
        );
    }
    assert_eq!(
        session
            .apply_observed(
                0,
                Message::Error("old".into()),
                |_| panic!("old mutation"),
                |_| panic!("old phase")
            )
            .unwrap(),
        SessionOutcome::OldGeneration
    );
    assert!(session.is_connected());
}

#[test]
fn previous_liquidity_is_observed_before_commit_and_before_rejection() {
    let mut session = live();
    let mut observed = Vec::new();
    let mut phases = Vec::new();
    let previous = session.book(MarketId(1)).unwrap().top().unwrap();
    let result = session
        .apply_observed(
            1,
            Message::Event(event(MarketId(1))),
            |book| observed.push((book.market(), book.top(), book.sequence())),
            |phase| phases.push(phase),
        )
        .unwrap();
    assert!(result.committed());
    assert_eq!(observed, [(MarketId(1), Ok(previous), Some(Sequence(2)))]);
    assert_eq!(
        phases,
        [Phase::Preflight, Phase::Mutation, Phase::Validation]
    );
    let mut gap = event(MarketId(1));
    gap.data.seq_num = Sequence(5);
    observed.clear();
    phases.clear();
    assert!(
        session
            .apply_observed(
                1,
                Message::Event(gap),
                |book| observed.push((book.market(), book.top(), book.sequence())),
                |phase| phases.push(phase)
            )
            .is_err()
    );
    assert!(observed[0].1.is_ok()); // prior book charged before its failed transition
    assert_eq!(observed[0].2, Some(Sequence(3)));
    assert_eq!(observed.last().unwrap().0, MarketId(2));
    assert!(observed.last().unwrap().1.is_ok()); // untouched market charged before invalidation
    assert!(phases.is_empty());
    assert!(session.books().all(|book| book.top().is_err()));
}

#[test]
fn external_decoder_failure_and_reconnect_use_the_same_accounting_boundary() {
    let mut session = live();
    assert!(decode(b"malformed").is_err());
    let mut previous = Vec::new();
    session.invalidate_observed(BookError::Inconsistent("decoder continuity"), |book| {
        previous.push((book.market(), book.status()))
    });
    assert_eq!(
        previous,
        [(MarketId(1), Status::Live), (MarketId(2), Status::Live)]
    );
    previous.clear();
    session
        .connect_observed(2, |book| previous.push((book.market(), book.status())))
        .unwrap();
    assert!(
        previous
            .iter()
            .all(|(_, status)| matches!(status, Status::Invalid(_)))
    );
    assert!(
        session
            .books()
            .all(|book| book.status() == Status::AwaitingSnapshot)
    );
}

#[test]
fn retained_public_streams_agree_with_the_unmodified_single_book_engine() {
    for bytes in [
        include_str!("fixtures/public-sample.jsonl"),
        include_str!("fixtures/public-active.jsonl"),
    ] {
        let messages: Vec<_> = bytes
            .lines()
            .map(|line| decode(line.as_bytes()).unwrap())
            .collect();
        let market = messages
            .iter()
            .find_map(|message| match message {
                Message::Snapshot(snapshot) => Some(snapshot.market_id),
                _ => None,
            })
            .unwrap();
        let mut session = Session::new(&[market], 1024, 100_000).unwrap();
        session.connect(1).unwrap();
        let mut independent = Book::new(market, 1024, 100_000).unwrap();
        independent.reset(1).unwrap();
        for message in messages {
            let expected = match message.clone() {
                Message::Snapshot(snapshot) => Some(independent.snapshot(1, snapshot).unwrap()),
                Message::Event(event) => Some(independent.event(1, &event).unwrap()),
                _ => None,
            };
            let outcome = session.apply(1, message).unwrap();
            if let Some(expected) = expected {
                assert_eq!(
                    outcome,
                    SessionOutcome::Book {
                        market,
                        sequence: independent.sequence(),
                        outcome: expected
                    }
                );
            } else {
                assert_eq!(outcome, SessionOutcome::Control);
            }
            same_book(session.book(market).unwrap(), &independent);
        }
    }
}

#[test]
fn fixed_market_bounds_route_shuffled_sparse_ids_and_reconcile_independently() {
    for count in [1, 8, 64, 256] {
        let mut markets: Vec<_> = (0..count)
            .map(|index| {
                MarketId(if index == 0 {
                    1
                } else {
                    u64::MAX - index * 1009
                })
            })
            .collect();
        markets.reverse();
        let mut session = Session::new(&markets, 16, 128).unwrap();
        let mut ordered = markets.clone();
        ordered.sort_unstable();
        assert_eq!(
            session.books().map(Book::market).collect::<Vec<_>>(),
            ordered
        );
        for missing in [MarketId(0), MarketId(2), MarketId(u64::MAX)] {
            assert!(session.book(missing).is_none());
        }
        session.connect(1).unwrap();
        for (index, &market) in markets.iter().enumerate() {
            let mut independent = Book::new(market, 16, 128).unwrap();
            independent.reset(1).unwrap();
            // Neighboring books take different synchronization paths.
            let inputs = if index % 2 == 0 {
                [
                    Message::Event(event(market)),
                    Message::Snapshot(snapshot(market)),
                ]
            } else {
                [
                    Message::Snapshot(snapshot(market)),
                    Message::Event(event(market)),
                ]
            };
            for message in inputs {
                let expected = match message.clone() {
                    Message::Snapshot(value) => independent.snapshot(1, value).unwrap(),
                    Message::Event(value) => independent.event(1, &value).unwrap(),
                    _ => unreachable!(),
                };
                assert_eq!(
                    session.apply(1, message).unwrap(),
                    SessionOutcome::Book {
                        market,
                        sequence: independent.sequence(),
                        outcome: expected,
                    }
                );
                same_book(session.book(market).unwrap(), &independent);
            }
        }
        assert!(
            session
                .books()
                .all(|book| book.sequence() == Some(Sequence(3)))
        );
        let unknown = MarketId(u64::MAX);
        assert_eq!(
            session.apply(1, Message::Event(event(unknown))),
            Err(SessionError::Market(unknown))
        );
        assert!(!session.is_connected());
        assert!(session.books().all(|book| book.top().is_err()));
        session.connect(2).unwrap();
        assert!(
            session
                .books()
                .all(|book| book.generation() == 2 && book.status() == Status::AwaitingSnapshot)
        );
    }
}
