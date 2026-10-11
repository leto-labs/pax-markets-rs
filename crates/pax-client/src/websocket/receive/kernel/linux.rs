//! Linux socket-local ancillary requests. Read mechanics and clocks have separate owners.
mod clock;
mod phc;
mod read;
#[cfg(test)]
mod tests;
use super::{HardwareConfig, Stamp};
use crate::measurement::Sampler;
use crate::websocket::receive::{
    ProbeWork, ReadDetail, ReceiveConfig,
    clock::{Calibration, InvalidClock},
    collector::ns,
    policy::ReceivePolicy,
};
#[cfg(test)]
use clock::align_stamp;
use clock::{Identity, align_stamp_with_identity, open_phc};
use nix::{
    sys::{
        socket::{ControlMessageOwned, MsgFlags, TimestampingFlag, recvmsg, setsockopt, sockopt},
        time::TimeSpec,
    },
    time::{ClockId, clock_gettime},
};
use phc::PhcClock;
use std::{
    fs::File,
    io::{self, IoSliceMut},
    os::fd::AsRawFd,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, Interest, ReadBuf},
    net::TcpStream,
};

pub(crate) struct SocketTimestamps {
    enabled: bool,
    sampler: Sampler,
    detail_sampler: Sampler,
    work: ReadDetail,
    want_software: bool,
    ancillary: Vec<u8>,
    system: Option<Calibration>,
    system_identity: Identity,
    phc: Option<PhcClock>,
    hardware_reason: &'static str,
    pub software: Stamp,
    pub hardware: Stamp,
}
impl SocketTimestamps {
    pub fn new(stream: &TcpStream, config: &ReceiveConfig) -> Self {
        let mut state = Self {
            enabled: false,
            detail_sampler: Sampler::new(config.policy.read_detail)
                .expect("validated read detail policy"),
            work: ReadDetail::default(),
            sampler: Sampler::new(config.policy.timestamps).expect("validated timestamp policy"),
            want_software: config.software_timestamps,
            ancillary: nix::cmsg_space!([TimeSpec; 3]),
            system: None,
            system_identity: Identity::new(),
            phc: None,
            hardware_reason: "not requested",
            software: Stamp::unavailable("not requested"),
            hardware: Stamp::unavailable("not requested"),
        };
        if !config.software_timestamps && config.hardware.is_none() {
            return state;
        }
        let mut flags = TimestampingFlag::empty();
        if config.software_timestamps {
            flags |= TimestampingFlag::SOF_TIMESTAMPING_RX_SOFTWARE
                | TimestampingFlag::SOF_TIMESTAMPING_SOFTWARE;
            state.system_identity.attempted();
            state.system = Calibration::system().ok();
        }
        if let Some(hardware) = &config.hardware {
            flags |= TimestampingFlag::SOF_TIMESTAMPING_RX_HARDWARE
                | TimestampingFlag::SOF_TIMESTAMPING_RAW_HARDWARE;
            match open_phc(hardware, stream) {
                Ok(file) => {
                    state.phc = Some(match config.phc_calibration {
                        super::super::config::PhcCalibration::SampledRead => {
                            PhcClock::new(file, config.phc_refresh_interval)
                        }
                        #[cfg(feature = "phc-background")]
                        super::super::config::PhcCalibration::BackgroundDriver => {
                            PhcClock::background(
                                file,
                                config.phc_refresh_interval,
                                config.phc_worker_cpu,
                                config.phc_deferred_mapping,
                                config.policy.timestamps.enabled(),
                            )
                        }
                        #[cfg(not(feature = "phc-background"))]
                        super::super::config::PhcCalibration::BackgroundDriver => {
                            unreachable!("background clock feature validated before connection")
                        }
                    });
                    state.hardware_reason =
                        "no hardware ancillary timestamp; NIC filters are not changed";
                }
                Err(reason) => state.hardware_reason = reason,
            }
            state.hardware = Stamp::unavailable(state.hardware_reason);
        }
        if setsockopt(stream, sockopt::Timestamping, &flags).is_err() {
            // No ancillary hardware data can arrive; stop a configured worker
            // rather than continuing device requests for an unavailable source.
            state.phc = None;
            if config.software_timestamps {
                state.software = Stamp::unavailable("SO_TIMESTAMPING unavailable");
            }
            if config.hardware.is_some() {
                state.hardware = Stamp::unavailable("SO_TIMESTAMPING unavailable");
            }
        } else {
            state.enabled = true;
        }
        state
    }
    pub fn counts(
        &self,
    ) -> (
        Option<crate::measurement::SelectionCounts>,
        Option<crate::measurement::SelectionCounts>,
    ) {
        (
            Some(self.sampler.counts()),
            cfg!(feature = "receive-stages").then(|| self.detail_sampler.counts()),
        )
    }
    pub fn take_work(&mut self) -> ReadDetail {
        std::mem::take(&mut self.work)
    }
    pub fn configure(&mut self, policy: ReceivePolicy) {
        if let Some(phc) = &mut self.phc {
            phc.configure(policy.timestamps.enabled());
        }
        self.detail_sampler
            .configure(policy.read_detail)
            .expect("validated read detail policy");
        self.sampler
            .configure(policy.timestamps)
            .expect("validated timestamp policy");
        self.software = Stamp::unavailable("policy changed; awaiting selected read");
        self.hardware = Stamp::unavailable("policy changed; awaiting selected read");
    }
}
