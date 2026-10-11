//! Typed diagnostics that deliberately exclude API payloads and credentials.

/// Operator failure. AWS errors retain only a fixed service/transport category.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The explicitly selected placement is unavailable; configured alternatives may be tried.
    #[error("placement unavailable: {0}")]
    PlacementUnavailable(String),
    /// Invalid operator input.
    #[error("invalid configuration: {0}")]
    Config(String),
    /// Filesystem or child-process failure.
    #[error("operator I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Invalid AWS response shape.
    #[error("AWS response did not have the required JSON structure")]
    Json(#[from] serde_json::Error),
    /// Wrong AWS account; no mutation is permitted.
    #[error("AWS account mismatch: expected {expected}, observed {observed}")]
    Account {
        /// Requested account.
        expected: String,
        /// STS account.
        observed: String,
    },
    /// A bounded child process did not finish.
    #[error("AWS {service} {operation} exceeded its timeout")]
    Timeout {
        /// AWS service.
        service: String,
        /// AWS operation.
        operation: String,
    },
    /// AWS service or CLI transport failure, with no raw stderr/response echoed.
    #[error("AWS {service} {operation} failed ({code})")]
    Aws {
        /// AWS service.
        service: String,
        /// AWS operation.
        operation: String,
        /// Fixed, sanitized service or transport error code.
        code: String,
    },
    /// Bounded output was exceeded.
    #[error("AWS child output exceeded its configured bound")]
    OutputLimit,
    /// A remote response omitted a required field.
    #[error("AWS response is missing {0}")]
    Missing(&'static str),
    /// Cleanup could not be confirmed.
    #[error("{action} failed; cleanup remains incomplete: {cleanup}")]
    Cleanup {
        /// Original failure.
        action: String,
        /// Required cleanup failure.
        cleanup: String,
    },
}

impl Error {
    /// Match a sanitized AWS error code without inspecting credential-bearing output.
    pub fn aws_code(&self, expected: &str) -> bool {
        matches!(self, Self::Aws { code, .. } if code == expected)
    }
}
