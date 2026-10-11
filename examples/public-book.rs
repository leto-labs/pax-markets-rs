//! Minimal public SDK composition. The check script supplies two offline fixtures.
//! Run with --live for a bounded public observation; no credentials are used.
use pax_book::{
    session::{Session, SessionOutcome},
    state::BookError,
};
use pax_client::{
    metadata::{METADATA_URL, MetadataClient},
    websocket::{Feed, FeedConfig, Input},
};
use pax_types::{
    protocol::decode,
    units::{MarketId, Sequence},
};
use std::{error::Error, time::Duration};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn apply(session: &mut Session, generation: u64, bytes: &[u8]) -> Result<SessionOutcome> {
    let message = decode(bytes).inspect_err(|_| {
        session.invalidate(BookError::Inconsistent("malformed input"));
    })?;
    Ok(session.apply(generation, message)?)
}

fn offline() -> Result<()> {
    let mut session = Session::new(&[MarketId(1)], 1024, 100_000)?;
    session.connect(1)?;
    assert!(session.book(MarketId(1)).unwrap().top().is_err());
    apply(&mut session, 1, include_bytes!("snapshot.json"))?;
    assert!(apply(&mut session, 1, include_bytes!("xa.json"))?.committed());
    let book = session.book(MarketId(1)).unwrap();
    let top = book.top()?;
    assert_eq!(book.sequence(), Some(Sequence(3)));
    assert_eq!((top.ask_ticks.0, top.ask_qty.0), (100015, 4500));
    assert_eq!((top.bid_ticks.0, top.bid_qty.0), (100012, 1000));

    assert!(apply(&mut session, 1, b"malformed").is_err());
    assert!(session.book(MarketId(1)).unwrap().top().is_err());
    session.connect(2)?;
    apply(&mut session, 2, include_bytes!("snapshot.json"))?;
    assert_eq!(
        session.book(MarketId(1)).unwrap().sequence(),
        Some(Sequence(2))
    );
    assert!(session.book(MarketId(1)).unwrap().top().is_ok());
    println!("offline snapshot, execute-and-rest, invalidation and recovery verified");
    Ok(())
}

async fn live() -> Result<()> {
    let metadata = MetadataClient::new(Duration::from_secs(10), 1024 * 1024)?
        .fetch(METADATA_URL)
        .await?;
    let market = metadata
        .markets
        .first()
        .ok_or("no public markets discovered")?
        .market_id;
    metadata.resolve(market)?;
    let mut session = Session::new(&[market], 1024, 100_000)?;
    let mut feed = Feed::inline(FeedConfig::public(vec![market]))?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut commits = 0;
    while let Ok(Some(input)) = tokio::time::timeout_at(deadline, feed.recv()).await {
        match input {
            Input::Connected { generation, .. } => session.connect(generation)?,
            Input::Disconnected { .. } => {
                session.invalidate(BookError::Inconsistent("disconnected"))
            }
            Input::Message {
                generation, bytes, ..
            } => match apply(&mut session, generation, &bytes) {
                Ok(outcome) => commits += u64::from(outcome.committed()),
                Err(_) => feed.request_recovery(),
            },
        }
    }
    // Keep printing and other potentially blocking application work out of ingestion.
    let top = session.book(market).unwrap().top();
    feed.shutdown().await;
    session.invalidate(BookError::Inconsistent("observation ended"));
    println!("market {market}: {commits} commits; final observation {top:?}");
    if commits == 0 {
        return Err("no validated public book observed".into());
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    match std::env::args().skip(1).collect::<Vec<_>>().as_slice() {
        [] => offline(),
        [mode] if mode == "--live" => live().await,
        _ => Err("usage: public-book [--live]".into()),
    }
}
