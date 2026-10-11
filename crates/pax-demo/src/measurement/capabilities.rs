//! Resolved application and client measurement groups.
use serde::{Deserialize, Serialize};
/// Diagnostic capabilities embedded in this executable/library build.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Client feature union actually resolved by Cargo.
    pub client: pax_client::measurement::capabilities::Capabilities,
    /// Optional decoder, book and callback markers.
    pub processing_traces: bool,
    /// Optional diagnostic sidecar persistence.
    pub telemetry_export: bool,
}
/// Optional probes never control canonical receipt, heartbeat or recovery clocks.
pub const CAPABILITIES: Capabilities = Capabilities {
    client: pax_client::measurement::capabilities::CAPABILITIES,
    processing_traces: cfg!(feature = "processing-traces"),
    telemetry_export: cfg!(feature = "telemetry-export"),
};
