//! Build metadata shared by observers, recordings and benchmarks.
use serde::{Deserialize, Serialize};

/// Build metadata with the caller's resolved measurement capabilities.
///
/// Construction and serialization belong outside ingestion. Generic capability
/// data keeps applications independent of each other's features and frameworks.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "C: Deserialize<'de>"))]
pub struct BuildInfo<C> {
    /// Executable package version.
    pub version: String,
    /// Exact source revision or an explicit unknown state.
    pub revision: String,
    /// Staged source digest; absent on older recordings and unstaged builds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_digest: Option<String>,
    /// Whether source differed from its revision at build time.
    pub dirty: bool,
    /// Compiler version.
    pub compiler: String,
    /// Cargo build profile.
    pub profile: String,
    /// Actual resolved capabilities; absent on earlier recordings.
    #[serde(default)]
    pub measurement_capabilities: Option<C>,
    /// Selected executable features; absent legacy fields remain empty.
    #[serde(default)]
    pub features: Vec<String>,
    /// Explicit optimization/profile selection, if recorded.
    #[serde(default)]
    pub optimization: String,
}

/// Construct cold build provenance using environment values at the call site.
///
/// A library's own build environment cannot describe its consuming executable.
/// This macro expands in the caller, after its build script has embedded source
/// and feature metadata. Missing metadata stays explicitly unknown.
///
/// ```
/// let identity = pax_client::build_info!(false);
/// assert!(!identity.version.is_empty());
/// ```
#[macro_export]
macro_rules! build_info {
    ($capabilities:expr) => {
        $crate::measurement::build_info::BuildInfo {
            measurement_capabilities: Some($capabilities),
            features: option_env!("PAX_FEATURES")
                .unwrap_or("")
                .split(',')
                .filter(|feature| !feature.is_empty())
                .map(str::to_owned)
                .collect(),
            version: env!("CARGO_PKG_VERSION").into(),
            revision: option_env!("PAX_REVISION").unwrap_or("unknown").into(),
            source_digest: option_env!("PAX_SOURCE_DIGEST")
                .filter(|digest| !digest.is_empty())
                .map(str::to_owned),
            dirty: option_env!("PAX_DIRTY").unwrap_or("true") == "true",
            compiler: option_env!("PAX_COMPILER").unwrap_or("unknown").into(),
            profile: option_env!("PAX_PROFILE").unwrap_or("unknown").into(),
            optimization: option_env!("PAX_OPTIMIZATION")
                .unwrap_or("unprofiled")
                .into(),
        }
    };
}
