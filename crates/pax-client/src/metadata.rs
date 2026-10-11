//! Reusable, size-bounded public HTTP metadata discovery.
use pax_types::market::{ExchangeInfo, MetadataError};
use std::time::Duration;

/// Public production metadata URL.
pub const METADATA_URL: &str = "https://api.pax.markets/v1/metadata/exchange-info";

/// Discovery/configuration failure.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Invalid bounded transport settings.
    #[error("invalid client configuration: {0}")]
    Config(&'static str),
    /// HTTP transport or status error.
    #[error("metadata HTTP failure: {0}")]
    Http(#[from] reqwest::Error),
    /// Body bytes are not UTF-8, including in otherwise ignored fields.
    #[error("metadata encoding failure: {0}")]
    Encoding(#[from] std::str::Utf8Error),
    /// Body does not match the public contract.
    #[error("metadata JSON failure: {0}")]
    Json(#[from] serde_json::Error),
    /// Metadata references or units are invalid.
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    /// A finite body bound was exceeded.
    #[error("metadata exceeds {0} bytes")]
    BodyLimit(usize),
}

/// Connection-pooling metadata client without account authentication.
#[derive(Clone, Debug)]
pub struct MetadataClient {
    client: reqwest::Client,
    limit: usize,
}
impl MetadataClient {
    /// Create a client with a total request timeout and decoded body byte limit.
    pub fn new(timeout: Duration, limit: usize) -> Result<Self, ClientError> {
        if timeout.is_zero() || limit == 0 {
            return Err(ClientError::Config("zero metadata timeout or bound"));
        }
        // Reuse Client for connection pooling; disable redirects so a venue
        // override cannot silently change the source recorded in provenance.
        // https://docs.rs/reqwest/0.13.5/reqwest/struct.ClientBuilder.html
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self { client, limit })
    }
    /// Fetch and validate exact public metadata without credentials.
    pub async fn fetch(&self, url: &str) -> Result<ExchangeInfo, ClientError> {
        let mut response = self.client.get(url).send().await?.error_for_status()?;
        if response
            .content_length()
            .is_some_and(|length| length > self.limit as u64)
        {
            return Err(ClientError::BodyLimit(self.limit));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if chunk.len() > self.limit - bytes.len() {
                return Err(ClientError::BodyLimit(self.limit));
            }
            bytes.extend_from_slice(&chunk);
        }
        // Validate the complete body, including fields that Serde will ignore.
        let text = std::str::from_utf8(&bytes)?;
        let info: ExchangeInfo = serde_json::from_str(text)?;
        info.validate()?;
        Ok(info)
    }
}
