//! Source-clock qualification and read-only hardware clock discovery.
mod annotation;
use super::*;
pub(super) use annotation::Identity;
use std::{
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
};
fn clock_ns(clock: ClockId) -> io::Result<i128> {
    let t = clock_gettime(clock).map_err(io::Error::from)?;
    Ok(i128::from(t.tv_sec()) * 1_000_000_000 + i128::from(t.tv_nsec()))
}
pub(super) fn device_sample(file: &File) -> Result<Calibration, InvalidClock> {
    // Linux FD_TO_CLOCKID; the File remains owned throughout all clock reads.
    // https://man7.org/linux/man-pages/man3/clock_gettime.3.html
    let id = ClockId::from_raw((!file.as_raw_fd() << 3) | 3);
    let before = Instant::now();
    let source = clock_ns(id).map_err(|error| {
        if error.raw_os_error() == Some(nix::libc::EBUSY) {
            InvalidClock::Busy
        } else {
            InvalidClock::Unavailable
        }
    })?;
    Calibration::bracket(source, before, Instant::now())
}
pub(super) fn open_phc(config: &HardwareConfig, stream: &TcpStream) -> Result<File, &'static str> {
    let local = stream
        .local_addr()
        .map_err(|_| "socket local address unavailable")?
        .ip();
    let owns = nix::ifaddrs::getifaddrs()
        .map_err(|_| "interface addresses unavailable")?
        .any(|address| {
            if address.interface_name != config.interface {
                return false;
            }
            address.address.is_some_and(|a| {
                a.as_sockaddr_in()
                    .is_some_and(|a| std::net::IpAddr::V4(a.ip()) == local)
                    || a.as_sockaddr_in6()
                        .is_some_and(|a| std::net::IpAddr::V6(a.ip()) == local)
            })
        });
    if !owns {
        return Err("selected interface does not own socket local address");
    }
    let resolved = associated_path(config, Path::new("/sys/class/net"), Path::new("/dev"))?;
    // Open the resolved path and check the actual descriptor's device identity. A
    // replaced symlink/path must not silently select a different clock.
    let expected = std::fs::metadata(&resolved).map_err(|_| "PHC device identity unavailable")?;
    let file = File::open(&resolved).map_err(open_error)?;
    let opened = file
        .metadata()
        .map_err(|_| "opened PHC device identity unavailable")?;
    if !same_device(&expected, &opened) {
        return Err("opened PHC device identity mismatch");
    }
    let name = resolved.file_name().ok_or("invalid PHC device name")?;
    let device_id = std::fs::read_to_string(
        Path::new("/sys/class/net")
            .join(&config.interface)
            .join("device/ptp")
            .join(name)
            .join("dev"),
    )
    .map_err(|_| "interface PHC device identity unavailable")?;
    if !matches_sysfs_device(&opened, &device_id) {
        return Err("opened PHC does not match interface device identity");
    }
    // Recheck association after opening. This is qualification per connection,
    // never a cached association carried over after reconnect.
    if associated_path(config, Path::new("/sys/class/net"), Path::new("/dev"))? != resolved {
        return Err("PHC association changed while opening");
    }
    Ok(file)
}

fn associated_path(
    config: &HardwareConfig,
    sys_root: &Path,
    dev_root: &Path,
) -> Result<PathBuf, &'static str> {
    config.validate()?;
    let resolved = std::fs::canonicalize(&config.clock)
        .map_err(|_| "PHC device path could not be resolved")?;
    let name = resolved
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("invalid PHC device name")?;
    if !name
        .strip_prefix("ptp")
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err("PHC device path mismatch");
    }
    let device =
        std::fs::canonicalize(dev_root.join(name)).map_err(|_| "PHC device path mismatch")?;
    if resolved != device {
        return Err("PHC device path mismatch");
    }
    if !sys_root
        .join(&config.interface)
        .join("device/ptp")
        .join(name)
        .exists()
    {
        return Err("selected PHC is not associated with interface in sysfs");
    }
    Ok(resolved)
}
fn same_device(expected: &std::fs::Metadata, opened: &std::fs::Metadata) -> bool {
    expected.file_type().is_char_device()
        && opened.file_type().is_char_device()
        && expected.rdev() == opened.rdev()
        && expected.dev() == opened.dev()
        && expected.ino() == opened.ino()
}
fn matches_sysfs_device(metadata: &std::fs::Metadata, text: &str) -> bool {
    let Some((major, minor)) = text.trim().split_once(':') else {
        return false;
    };
    let (Ok(major), Ok(minor)) = (major.parse::<u64>(), minor.parse::<u64>()) else {
        return false;
    };
    metadata.file_type().is_char_device()
        && nix::sys::stat::major(metadata.rdev()) == major
        && nix::sys::stat::minor(metadata.rdev()) == minor
}
fn open_error(error: io::Error) -> &'static str {
    if error.kind() == io::ErrorKind::PermissionDenied {
        "PHC read permission denied"
    } else {
        "PHC device could not be opened"
    }
}

#[cfg(any(test, feature = "phc-background"))]
pub(super) fn align_stamp(
    time: TimeSpec,
    source: &'static str,
    model: &mut Option<Calibration>,
    next: Result<Option<Calibration>, InvalidClock>,
    read: Instant,
) -> Stamp {
    align_stamp_with_identity(time, source, model, next, read, None)
}

pub(super) fn align_stamp_with_identity(
    time: TimeSpec,
    source: &'static str,
    model: &mut Option<Calibration>,
    next: Result<Option<Calibration>, InvalidClock>,
    read: Instant,
    _identity: Option<&Identity>,
) -> Stamp {
    if time.tv_sec() == 0 && time.tv_nsec() == 0 {
        return Stamp::unavailable("requested timestamp absent in ancillary data");
    }
    if time.tv_sec() < 0 || !(0..1_000_000_000).contains(&time.tv_nsec()) {
        return Stamp::unavailable("invalid ancillary timespec");
    }
    let raw = i128::from(time.tv_sec()) * 1_000_000_000 + i128::from(time.tv_nsec());
    let cached = matches!(next, Ok(None));
    #[cfg(feature = "clock-model-details")]
    let mut mapped_at = None;
    let alignment = next.and_then(|next| {
        if let Some(next) = next {
            if let Some(previous) = model {
                previous.update(next, Duration::from_micros(10))?;
            } else {
                *model = Some(next);
            }
        }
        let model = model.as_ref().ok_or(InvalidClock::Unavailable)?;
        let now = Instant::now();
        #[cfg(feature = "clock-model-details")]
        {
            mapped_at = Some(now);
        }
        model
            .align(raw, now, Duration::from_secs(2), 100)
            .and_then(|aligned| {
                if aligned.0 <= read {
                    Ok(aligned)
                } else {
                    Err(InvalidClock::Future)
                }
            })
    });
    let (at, uncertainty, status) = match alignment {
        Ok((at, error)) => (
            Some(at),
            Some(error),
            if cached {
                "read-batch timestamp; cached PHC alignment; packet position unspecified"
            } else {
                "read-batch timestamp; packet position unspecified"
            },
        ),
        Err(InvalidClock::Stale) => (None, None, "clock alignment or observation stale"),
        Err(InvalidClock::Step) => (None, None, "clock discontinuity"),
        Err(InvalidClock::Future) => (None, None, "timestamp maps to future; interval withheld"),
        Err(InvalidClock::Busy) => (
            None,
            None,
            "PHC clock read returned EBUSY; interval withheld",
        ),
        Err(InvalidClock::WorkerAffinity) => (None, None, "PHC worker CPU placement unavailable"),
        Err(InvalidClock::CollectorStopped) => {
            (None, None, "PHC collector stopped; interval withheld")
        }
        Err(InvalidClock::PublicationLost) => (
            None,
            None,
            "PHC newest model publication unavailable; interval withheld",
        ),
        Err(InvalidClock::Warming) => (
            None,
            None,
            "PHC collector warming or timestamp policy paused",
        ),
        Err(InvalidClock::Unavailable) => (None, None, "source clock read/calibration unavailable"),
        Err(_) => (None, None, "clock alignment unavailable"),
    };
    Stamp {
        at,
        raw_ns: Some(raw),
        uncertainty_ns: uncertainty,
        #[cfg(feature = "clock-model-details")]
        qualification: at.and_then(|_| {
            _identity.and_then(|identity| identity.describe(model.as_ref().copied()?, mapped_at?))
        }),
        source,
        status,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn aliases_use_resolved_name_and_requalification_detects_changes() {
        let root = std::env::temp_dir().join(format!("pax-phc-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let result = std::panic::catch_unwind(|| {
            let dev = root.join("dev");
            let sys = root.join("sys");
            std::fs::create_dir(&dev).unwrap();
            std::fs::create_dir_all(sys.join("ena0/device/ptp/ptp0")).unwrap();
            std::fs::write(dev.join("ptp0"), b"clock fixture").unwrap();
            std::fs::write(dev.join("ptp1"), b"unrelated clock fixture").unwrap();
            let mut config = HardwareConfig {
                interface: "ena0".into(),
                clock: dev.join("ptp0"),
            };
            assert_eq!(
                associated_path(&config, &sys, &dev).unwrap(),
                dev.join("ptp0")
            );
            symlink(dev.join("ptp0"), dev.join("ptp_ena")).unwrap();
            config.clock = dev.join("ptp_ena");
            assert_eq!(
                associated_path(&config, &sys, &dev).unwrap(),
                dev.join("ptp0")
            );
            std::fs::remove_file(&config.clock).unwrap();
            symlink(dev.join("ptp1"), &config.clock).unwrap();
            assert!(associated_path(&config, &sys, &dev).is_err());
            std::fs::remove_file(&config.clock).unwrap();
            symlink(dev.join("missing"), &config.clock).unwrap();
            assert!(associated_path(&config, &sys, &dev).is_err());
            config.clock = dev.join("ptp0");
            std::fs::remove_dir(sys.join("ena0/device/ptp/ptp0")).unwrap();
            assert!(associated_path(&config, &sys, &dev).is_err());
        });
        std::fs::remove_dir_all(root).unwrap();
        result.unwrap();
    }
    #[test]
    fn opened_descriptor_requires_same_character_device() {
        let null = File::open("/dev/null").unwrap();
        let zero = File::open("/dev/zero").unwrap();
        assert!(same_device(
            &std::fs::metadata("/dev/null").unwrap(),
            &null.metadata().unwrap()
        ));
        assert!(!same_device(
            &null.metadata().unwrap(),
            &zero.metadata().unwrap()
        ));
        assert!(matches_sysfs_device(&null.metadata().unwrap(), "1:3\n"));
        assert!(!matches_sysfs_device(&null.metadata().unwrap(), "1:5"));
        assert!(!matches_sysfs_device(&null.metadata().unwrap(), "bad"));
        assert_eq!(
            open_error(io::ErrorKind::PermissionDenied.into()),
            "PHC read permission denied"
        );
        assert_eq!(
            open_error(io::ErrorKind::NotFound.into()),
            "PHC device could not be opened"
        );
    }
    #[tokio::test]
    async fn each_new_socket_rechecks_interface_ownership() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = HardwareConfig {
            interface: "pax-no-such-interface".into(),
            clock: "/dev/ptp0".into(),
        };
        for _ in 0..2 {
            let client = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (_server, _) = listener.accept().await.unwrap();
            assert_eq!(
                open_phc(&config, &client).unwrap_err(),
                "selected interface does not own socket local address"
            );
        }
    }
}
