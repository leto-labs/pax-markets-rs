//! Receive collection configuration and finite limits.
use super::kernel;

/// Read-only PHC calibration ownership. The ordinary transport is unaffected.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PhcCalibration {
    /// Existing selected-read calibration, preserving the baseline clock policy.
    #[default]
    SampledRead,
    /// Connection-owned worker tries driver CLOCK_MONOTONIC brackets, falling
    /// back on unsupported kernels. Ingestion only consumes immutable samples.
    BackgroundDriver,
}

/// Finite diagnostics settings. Domain policies select observation work;
/// `sample_every` independently limits emitted application-message metadata.
/// Socket/clock sources and capacity are fixed for one connection. Live changes
/// to those fields require reconnect; probe policies and emission rates can change.
#[derive(Clone, Debug)]
pub struct ReceiveConfig {
    /// Independently selected probe work; emission sampling below is separate.
    pub policy: super::policy::ReceivePolicy,
    /// Emit metadata for every Nth application message; zero suppresses emission
    /// independently of layer collection and receive adapter work.
    pub sample_every: u64,
    /// Maximum retained completed-frame associations, 1..=65536.
    pub capacity: usize,
    /// Ask the socket for Linux kernel software timestamps.
    pub software_timestamps: bool,
    /// Optional explicitly selected NIC and read-only PHC clock.
    pub hardware: Option<kernel::HardwareConfig>,
    /// Calibration owner; background mode additionally requires phc-background.
    pub phc_calibration: PhcCalibration,
    /// Optional CPU placement for the background worker, applied inside that worker.
    /// A failed placement keeps clock intervals unavailable without blocking ingestion.
    pub phc_worker_cpu: Option<usize>,
    /// Carry the immutable selected model/read marker and resolve PHC stamps in
    /// `Stamp::resolve` after commit/export. Requires background calibration.
    pub phc_deferred_mapping: bool,
    /// Minimum time between read-only PHC calibration attempts, 0..=100 ms.
    /// Zero samples each selected read. The default 20 ms avoids burst-driven
    /// device request limits. Cached alignment retains the 100 ppm rate-error
    /// assumption and a two-second freshness bound; errors remain unavailable.
    pub phc_refresh_interval: std::time::Duration,
}
impl Default for ReceiveConfig {
    fn default() -> Self {
        Self {
            policy: super::policy::ReceivePolicy::default(),
            sample_every: 1,
            capacity: 1024,
            software_timestamps: false,
            hardware: None,
            phc_calibration: PhcCalibration::default(),
            phc_worker_cpu: None,
            phc_deferred_mapping: false,
            phc_refresh_interval: std::time::Duration::from_millis(20),
        }
    }
}
impl ReceiveConfig {
    /// Validate before any connection or OS configuration.
    pub fn validate(&self) -> Result<(), &'static str> {
        self.policy.validate()?;
        if self.phc_refresh_interval > std::time::Duration::from_millis(100) {
            return Err("PHC refresh interval must be at most 100 ms");
        }
        if (self.phc_worker_cpu.is_some() || self.phc_deferred_mapping)
            && self.phc_calibration != PhcCalibration::BackgroundDriver
        {
            return Err("PHC worker CPU/deferred mapping requires background calibration");
        }
        if self.phc_calibration == PhcCalibration::BackgroundDriver
            && (self.hardware.is_none()
                || self.phc_refresh_interval < std::time::Duration::from_millis(20)
                || !cfg!(all(
                    target_os = "linux",
                    any(target_arch = "x86_64", target_arch = "aarch64"),
                    feature = "phc-background"
                )))
        {
            return Err(
                "background PHC calibration requires phc-background, Linux x86_64/aarch64 hardware timestamps and at least 20 ms cadence",
            );
        }
        if !cfg!(feature = "receive-timestamps")
            && (self.software_timestamps || self.hardware.is_some())
        {
            return Err("receive-timestamps source support compiled out");
        }
        if !(1..=65536).contains(&self.capacity) {
            return Err("invalid receive diagnostic bounds");
        }
        if let Some(hardware) = &self.hardware {
            hardware.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn background_mode_requires_hardware_feature_and_bounded_cadence() {
        let mut config = ReceiveConfig {
            phc_calibration: PhcCalibration::BackgroundDriver,
            ..Default::default()
        };
        assert!(config.validate().is_err());
        config.hardware = Some(kernel::HardwareConfig {
            interface: "eth0".into(),
            clock: "/dev/ptp0".into(),
        });
        assert_eq!(
            config.validate().is_ok(),
            cfg!(all(
                target_os = "linux",
                any(target_arch = "x86_64", target_arch = "aarch64"),
                feature = "phc-background"
            ))
        );
        config.phc_refresh_interval = std::time::Duration::ZERO;
        assert!(config.validate().is_err());
        config = ReceiveConfig {
            phc_worker_cpu: Some(0),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
