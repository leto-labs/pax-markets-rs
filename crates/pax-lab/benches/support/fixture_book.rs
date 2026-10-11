//! Direct SDK transitions; final state is checked against an independent ladder.
use pax_book::state::{Book, Outcome, Status};
use pax_client::test_support::fixture::Fixture;
use pax_lab::Error;
use pax_types::{
    protocol::{self, Message, event::Side, snapshot::Snapshot},
    units::MarketId,
};

pub(super) fn new() -> Result<Book, Error> {
    let mut book = Book::new(MarketId(1), 2048, 65536)?;
    book.reset(1)?;
    Ok(book)
}
pub(super) fn apply(book: &mut Book, bytes: &[u8]) -> Result<bool, Error> {
    let outcome = match protocol::decode(bytes)? {
        Message::Snapshot(snapshot) => book.snapshot(1, snapshot)?,
        Message::Event(event) => book.event(1, &event)?,
        Message::Subscribed(_) | Message::Pong => return Ok(false),
        _ => return Err("unexpected fixture application message".into()),
    };
    if !matches!(outcome, Outcome::Applied | Outcome::Synchronized) {
        return Err("fixture did not produce a validated book commit".into());
    }
    Ok(true)
}
pub(super) fn expected(fixture: &Fixture) -> Result<String, Error> {
    Ok(blake3::hash(&serde_json::to_vec(&fixture.expected)?)
        .to_hex()
        .to_string())
}
pub(super) fn verify(book: &Book, fixture: &Fixture) -> Result<String, Error> {
    if book.status() != Status::Live
        || book.sequence() != Some(fixture.expected.latest_seq_num)
        || book.levels(Side::Ask)? != fixture.expected.ask_agg_data
        || book.levels(Side::Bid)? != fixture.expected.bid_agg_data
    {
        return Err("book differs from the independently specified fixture ladder".into());
    }
    let snapshot = Snapshot {
        market_id: book.market(),
        latest_seq_num: book.sequence().ok_or("missing final sequence")?,
        ask_agg_data: book.levels(Side::Ask)?.to_vec(),
        bid_agg_data: book.levels(Side::Bid)?.to_vec(),
    };
    Ok(blake3::hash(&serde_json::to_vec(&snapshot)?)
        .to_hex()
        .to_string())
}
