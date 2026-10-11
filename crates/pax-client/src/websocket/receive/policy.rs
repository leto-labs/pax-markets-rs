//! Independent transport probe selection. Presets do not couple domain counters.
use crate::measurement::SampleRate;
use serde::{Deserialize, Serialize};

/// Runtime receive policy. Each rate names its own eligible operation population.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceivePolicy {
    /// Successful socket reads whose requested ancillary stamps are retained.
    pub timestamps: SampleRate,
    /// Socket read-poll work; pending polls count as eligible operations.
    pub socket: SampleRate,
    /// Linux ancillary-read internals: readiness, initialization, syscall and metadata.
    pub read_detail: SampleRate,
    /// TLS/plaintext adapter polls.
    pub tls: SampleRate,
    /// WebSocket driver polls.
    pub websocket: SampleRate,
    /// Suspended WebSocket polls whose next-poll wait is observed.
    pub pending: SampleRate,
    /// Application-message associations retained by the passive frame observer.
    /// Frame headers must still be followed while this domain is enabled; this
    /// rate limits association retention, not the required framing coverage.
    pub association: SampleRate,
}
impl ReceivePolicy {
    /// All optional domains disabled.
    pub const OFF: Self = Self {
        timestamps: SampleRate::OFF,
        socket: SampleRate::OFF,
        read_detail: SampleRate::OFF,
        tls: SampleRate::OFF,
        websocket: SampleRate::OFF,
        pending: SampleRate::OFF,
        association: SampleRate::OFF,
    };
    /// Ancillary receive timestamps without frame association or layer work.
    pub const TIMESTAMPS: Self = Self {
        timestamps: SampleRate::ALL,
        ..Self::OFF
    };
    /// Retain read timestamps and passive frame ancestry without layer-work clocks.
    /// Requires `receive-timestamps` and `receive-association` build groups.
    pub const INGRESS: Self = Self {
        timestamps: SampleRate::ALL,
        association: SampleRate::ALL,
        ..Self::OFF
    };
    /// Full observation; legacy emission sampling remains a separate control.
    pub const DETAILED: Self = Self {
        timestamps: SampleRate::ALL,
        socket: SampleRate::ALL,
        read_detail: SampleRate::ALL,
        tls: SampleRate::ALL,
        websocket: SampleRate::ALL,
        pending: SampleRate::ALL,
        association: SampleRate::ALL,
    };
    /// Validate every requested domain before changing a live policy.
    pub fn validate(self) -> Result<(), &'static str> {
        for rate in [
            self.timestamps,
            self.socket,
            self.read_detail,
            self.tls,
            self.websocket,
            self.pending,
            self.association,
        ] {
            rate.validate()?;
        }
        if self.read_detail.enabled()
            && !cfg!(all(
                target_os = "linux",
                feature = "receive-timestamps",
                feature = "receive-stages"
            ))
        {
            return Err("read-detail requires Linux timestamp and receive-stage support");
        }
        let capabilities = crate::measurement::capabilities::CAPABILITIES;
        if self.timestamps.enabled() && !capabilities.receive_timestamps {
            return Err("receive-timestamps probes compiled out");
        }
        if self.association.enabled() && !capabilities.association() {
            return Err("receive-association probes compiled out");
        }
        if [
            self.socket,
            self.read_detail,
            self.tls,
            self.websocket,
            self.pending,
        ]
        .iter()
        .any(|r| r.enabled())
            && !capabilities.receive_stages
        {
            return Err("receive-stages probes compiled out");
        }
        Ok(())
    }
    /// Whether any receive probe is requested.
    pub fn enabled(self) -> bool {
        self != Self::OFF
    }
}
impl Default for ReceivePolicy {
    fn default() -> Self {
        Self {
            read_detail: if cfg!(all(
                target_os = "linux",
                feature = "receive-timestamps",
                feature = "receive-stages"
            )) {
                SampleRate::ALL
            } else {
                SampleRate::OFF
            },
            timestamps: if cfg!(feature = "receive-timestamps") {
                SampleRate::ALL
            } else {
                SampleRate::OFF
            },
            association: if cfg!(feature = "receive-association") {
                SampleRate::ALL
            } else {
                SampleRate::OFF
            },
            ..if cfg!(feature = "receive-stages") {
                Self::DETAILED
            } else {
                Self::OFF
            }
        }
    }
}
