//! Socket-local timestamp requests and read-only hardware-clock discovery.
//! No NIC filters, global busy polling, clock discipline or IRQ settings change.
#[cfg(all(target_os = "linux", feature = "receive-timestamps"))]
mod linux;
use std::path::PathBuf;
mod stamp;
pub use stamp::{ClockMethod, ClockQualification, Stamp};

/// Explicit interface/clock selection. Capability is not proof of delivered stamps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HardwareConfig {
    /// OS network interface name; must own the connected socket's local address.
    pub interface: String,
    /// Corresponding PHC device, e.g. `/dev/ptp0`; opened read-only.
    pub clock: PathBuf,
}
impl HardwareConfig {
    /// Reject path traversal in sysfs interface discovery.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.interface.is_empty()
            || self.interface.len() > 64
            || !self
                .interface
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-.".contains(&c))
            || self.interface == "."
            || self.interface == ".."
        {
            return Err("invalid receive timestamp interface");
        }
        if !self.clock.is_absolute() {
            return Err("PHC clock path must be absolute");
        }
        Ok(())
    }
}
#[cfg(all(target_os = "linux", feature = "receive-timestamps"))]
pub(crate) use linux::SocketTimestamps;
#[cfg(not(all(target_os = "linux", feature = "receive-timestamps")))]
mod unsupported;
#[cfg(not(all(target_os = "linux", feature = "receive-timestamps")))]
pub(crate) use unsupported::SocketTimestamps;
