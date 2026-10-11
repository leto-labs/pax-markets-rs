//! Executable provenance built independently from the demo server.
use serde::{Deserialize, Serialize};
/// Resolved build groups, retained alongside each workload's runtime policy.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Capabilities {
    /// Actual client feature union.
    pub client: pax_client::measurement::capabilities::Capabilities,
    /// Compiled processing clock probes.
    pub processing_traces: bool,
    /// Compiled bounded measurement handoff.
    pub telemetry_export: bool,
}
/// Benchmark executable source and compiled capabilities.
pub type BuildInfo = pax_client::measurement::build_info::BuildInfo<Capabilities>;
/// Construct cold metadata outside the measured stream.
pub fn current_build_info() -> BuildInfo {
    pax_client::build_info!(Capabilities {
        client: pax_client::measurement::capabilities::CAPABILITIES,
        processing_traces: cfg!(feature = "processing-traces"),
        telemetry_export: cfg!(feature = "telemetry-export"),
    })
}
