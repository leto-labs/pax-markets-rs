//! Resolved build capabilities; Cargo features are additive across dependents.
use serde::{Deserialize, Serialize};

/// Optional client groups compiled into this artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Connection-only Linux TCP option queries. None preserves legacy absence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket_readbacks: Option<bool>,
    /// Kernel/NIC ancillary timestamp requests and calibration.
    pub receive_timestamps: bool,
    /// Offset-model annotations, independently of the optional worker.
    /// None preserves legacy absence; older worker builds included annotations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clock_model_details: Option<bool>,
    /// Optional connection-owned driver calibration and deferred clock mapping.
    #[serde(default)]
    pub phc_background: bool,
    /// Passive frame/read association. None retains absent legacy metadata;
    /// older receive-stage builds included this support implicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receive_association: Option<bool>,
    /// Socket, TLS and WebSocket work; enables association for compatibility.
    pub receive_stages: bool,
    /// Preallocated nonblocking diagnostic publication.
    #[serde(default)]
    pub diagnostic_handoff: bool,
    /// Complete-message transport and consumer delivery markers.
    pub delivery_traces: bool,
}
impl Capabilities {
    /// Recognize explicit support or the legacy worker annotation group.
    pub const fn clock_details(self) -> bool {
        match self.clock_model_details {
            Some(available) => available,
            None => self.phc_background,
        }
    }
    /// Recognize explicit support or the legacy stage group's association.
    pub const fn association(self) -> bool {
        match self.receive_association {
            Some(available) => available,
            None => self.receive_stages,
        }
    }
}
/// The actual resolved capabilities, rather than a caller's requested feature list.
pub const CAPABILITIES: Capabilities = Capabilities {
    socket_readbacks: Some(cfg!(all(target_os = "linux", feature = "socket-readbacks"))),
    diagnostic_handoff: cfg!(feature = "diagnostic-handoff"),
    receive_timestamps: cfg!(feature = "receive-timestamps"),
    receive_association: Some(cfg!(feature = "receive-association")),
    clock_model_details: Some(cfg!(feature = "clock-model-details")),
    phc_background: cfg!(feature = "phc-background"),
    receive_stages: cfg!(feature = "receive-stages"),
    delivery_traces: cfg!(feature = "delivery-traces"),
};
