//! Linux driver brackets, separate from absolute PHC UTC accuracy.
//! ABI: Linux v6.18 include/uapi/linux/ptp_clock.h; clockid needs kernel 6.12+.
use super::super::clock::device_sample;
use super::super::{Calibration, Duration, File, Instant, InvalidClock};
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
use nix::time::{ClockId, clock_gettime};
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
use std::os::fd::AsRawFd;

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64", test))]
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct PtpTime {
    sec: i64,
    nsec: u32,
    reserved: u32,
}
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64", test))]
#[repr(C)]
struct Extended {
    samples: u32,
    clockid: i32,
    reserved: [u32; 2],
    times: [[PtpTime; 3]; 25],
}
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64", test))]
const REQUEST: nix::libc::c_ulong =
    (3 << 30) | (1216 << 16) | ((b'=' as nix::libc::c_ulong) << 8) | 9;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Method {
    Driver,
    User,
}
#[derive(Clone, Copy, Debug)]
pub(super) struct Sample {
    pub calibration: Calibration,
    pub method: Method,
}

/// One request per calibration tick. Unsupported extended requests are remembered
/// for this connection; EBUSY never triggers an additional device request.
pub(super) struct Sampler {
    driver: bool,
}
impl Sampler {
    pub fn new() -> Self {
        Self { driver: true }
    }
    pub fn sample(&mut self, file: &File) -> Result<Sample, InvalidClock> {
        if self.driver {
            match driver_sample(file) {
                Ok(calibration) => {
                    return Ok(Sample {
                        calibration,
                        method: Method::Driver,
                    });
                }
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(nix::libc::EINVAL | nix::libc::ENOTTY | nix::libc::EOPNOTSUPP)
                    ) =>
                {
                    self.driver = false
                }
                Err(error) => {
                    return Err(if error.raw_os_error() == Some(nix::libc::EBUSY) {
                        InvalidClock::Busy
                    } else {
                        InvalidClock::Unavailable
                    });
                }
            }
            // Defer fallback until the next tick to keep device request cadence
            // bounded even on old kernels. The unsupported first tick is unavailable.
            return Err(InvalidClock::Unavailable);
        }
        device_sample(file).map(|calibration| Sample {
            calibration,
            method: Method::User,
        })
    }
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64", test))]
fn ns(t: PtpTime) -> std::io::Result<i128> {
    if t.sec < 0 || t.nsec >= 1_000_000_000 || t.reserved != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid PHC timespec",
        ));
    }
    Ok(i128::from(t.sec) * 1_000_000_000 + i128::from(t.nsec))
}
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
#[allow(
    unsafe_code,
    reason = "audited read-only Linux PTP ioctl ABI; no safe wrapper in pinned nix"
)]
fn driver_sample(file: &File) -> std::io::Result<Calibration> {
    let mut request = Extended {
        samples: 1,
        clockid: nix::libc::CLOCK_MONOTONIC,
        reserved: [0; 2],
        times: [[PtpTime::default(); 3]; 25],
    };
    // SAFETY: read-only PTP_SYS_OFFSET_EXTENDED uses this exact repr(C) Linux
    // x86_64/aarch64 ABI (1216 bytes). All input/reserved bytes are initialized,
    // the writable object stays alive for the synchronous ioctl, and File owns
    // the qualified PHC descriptor. The kernel writes only within this object.
    // No clock discipline, NIC configuration or raw data pointer escapes.
    let result = unsafe { nix::libc::ioctl(file.as_raw_fd(), REQUEST, &mut request) };
    if result < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let invalid = || {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid PHC driver bracket",
        )
    };
    if request.samples != 1
        || request.clockid != nix::libc::CLOCK_MONOTONIC
        || request.reserved != [0; 2]
    {
        return Err(invalid());
    }
    let [pre, phc, post] = request.times[0];
    let (pre, phc, post) = (ns(pre)?, ns(phc)?, ns(post)?);
    // Instant is opaque. Anchor kernel CLOCK_MONOTONIC to Rust Instant after
    // the ioctl, then retain the anchor uncertainty in the driver bracket.
    let before = Instant::now();
    let mono = clock_gettime(ClockId::CLOCK_MONOTONIC).map_err(std::io::Error::from)?;
    let after = Instant::now();
    let mono = i128::from(mono.tv_sec()) * 1_000_000_000 + i128::from(mono.tv_nsec());
    let anchor = Calibration::bracket(mono, before, after).map_err(|_| invalid())?;
    let (pre, unc) = anchor
        .align(pre, after, Duration::from_secs(2), 0)
        .map_err(|_| invalid())?;
    let (post, _) = anchor
        .align(post, after, Duration::from_secs(2), 0)
        .map_err(|_| invalid())?;
    Calibration::bracket(phc, pre, post)
        .map(|c| c.with_added_uncertainty(unc))
        .map_err(|_| invalid())
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn driver_sample(_: &File) -> std::io::Result<Calibration> {
    Err(std::io::Error::from_raw_os_error(nix::libc::EOPNOTSUPP))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_linux_uapi_layout_and_invalid_values() {
        assert_eq!(std::mem::size_of::<PtpTime>(), 16);
        assert_eq!(std::mem::size_of::<Extended>(), 1216);
        assert_eq!(std::mem::offset_of!(Extended, times), 16);
        assert_eq!(REQUEST, 0xc4c03d09);
        for t in [
            PtpTime {
                sec: -1,
                nsec: 0,
                reserved: 0,
            },
            PtpTime {
                sec: 1,
                nsec: 1_000_000_000,
                reserved: 0,
            },
            PtpTime {
                sec: 1,
                nsec: 0,
                reserved: 1,
            },
        ] {
            assert!(ns(t).is_err());
        }
    }
    #[test]
    fn unsupported_request_is_not_repeated_or_followed_by_a_second_request() {
        let file = File::open("/dev/null").unwrap();
        let mut sampler = Sampler::new();
        assert!(sampler.sample(&file).is_err());
        assert!(!sampler.driver);
        assert!(sampler.sample(&file).is_err());
        assert!(!sampler.driver);
    }
}
