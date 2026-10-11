//! Bounded read-only public-feed diagnostic; not a benchmark or automated test.
use pax_client::{
    metadata::{METADATA_URL, MetadataClient},
    websocket::{Feed, FeedConfig, Input},
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let metadata = MetadataClient::new(Duration::from_secs(10), 1024 * 1024)?
        .fetch(METADATA_URL)
        .await?;
    eprintln!("{}", serde_json::to_string(&metadata)?);
    let markets = metadata
        .markets
        .iter()
        .map(|market| market.market_id)
        .collect();
    let mut feed = Feed::spawn(FeedConfig::public(markets))?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while let Ok(Some(input)) = tokio::time::timeout_at(deadline, feed.recv()).await {
        match input {
            Input::Message { bytes, .. } => println!("{}", std::str::from_utf8(&bytes)?),
            boundary => eprintln!("{boundary:?}"),
        }
    }
    feed.shutdown().await;
    Ok(())
}
