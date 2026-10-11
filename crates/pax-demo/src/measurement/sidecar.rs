//! Versioned, independently bounded diagnostic persistence. Never canonical input.
pub mod status;
use super::receive::ReceiveTrace;
use serde_json::{Value, json};
use std::{
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, SyncSender},
    },
    thread::JoinHandle,
};
/// Optional diagnostic output. An existing file is never overwritten.
#[derive(Clone, Debug)]
pub struct SidecarConfig {
    /// Independently select records reaching this file writer; omitted records are intentional.
    pub sample_rate: pax_client::measurement::SampleRate,
    /// Destination JSONL file, independent of canonical capture completeness.
    pub path: PathBuf,
    /// Header, observations and footer byte budget, at least 4096 bytes.
    pub max_bytes: u64,
    /// Nonblocking writer queue, 1..=65536 observations.
    pub capacity: usize,
}
/// Final diagnostic coverage; loss does not invalidate canonical reconstruction.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Coverage {
    /// False if any observation was dropped or no footer was finalized.
    pub complete: bool,
    /// Received observations eligible for the independent file selection policy.
    pub eligible: u64,
    /// Observations selected for attempted writer publication.
    pub selected: u64,
    /// Successfully persisted sampled observations, not total market messages.
    pub records: u64,
    /// Observations dropped because the finite queue was full.
    pub queue_drops: u64,
    /// Optional records lost before this writer, including upstream queue loss.
    pub source_drops: u64,
    /// Observations dropped at the finite file-size bound.
    pub byte_drops: u64,
}
/// A worker owns file I/O and JSON formatting; submission never waits for space.
pub struct Sidecar {
    origin: std::time::Instant,
    sampler: pax_client::measurement::Sampler,
    sender: SyncSender<ReceiveTrace>,
    drops: Arc<AtomicU64>,
    finalized: Arc<AtomicBool>,
    source_drops: Arc<AtomicU64>,
    thread: JoinHandle<io::Result<Coverage>>,
    progress: Arc<status::Progress>,
}
impl Sidecar {
    /// Create a new file and writer. Validation and open errors occur before ingest.
    pub fn start(config: SidecarConfig) -> io::Result<Self> {
        if !cfg!(feature = "telemetry-export") {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "telemetry-export compiled out",
            ));
        }
        if config.max_bytes < 4096 || !(1..=65536).contains(&config.capacity) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid diagnostic sidecar bounds",
            ));
        }
        let sampler = pax_client::measurement::Sampler::new(config.sample_rate)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(config.path)?;
        let origin = std::time::Instant::now();
        let (sender, receiver) = mpsc::sync_channel(config.capacity);
        let drops = Arc::new(AtomicU64::new(0));
        let lost = drops.clone();
        let source_drops = Arc::new(AtomicU64::new(0));
        let source_lost = source_drops.clone();
        let finalized = Arc::new(AtomicBool::new(false));
        let finished = finalized.clone();
        let progress = Arc::new(status::Progress {
            alive: AtomicBool::new(true),
            ..status::Progress::default()
        });
        let observed = progress.clone();
        let background_cpu = crate::worker::target();
        let thread = std::thread::Builder::new()
            .name("pax-receive-writer".into())
            .spawn(move || {
                let _guard = status::Guard(observed.clone());
                let result = crate::worker::pin(background_cpu).and_then(|()| {
                    write_worker(
                        file,
                        receiver,
                        lost,
                        source_lost,
                        finished,
                        config.max_bytes,
                        config.sample_rate,
                        origin,
                        &observed,
                    )
                });
                if result.is_err() {
                    observed.failed.store(true, Ordering::Release);
                }
                result
            })?;
        Ok(Self {
            origin,
            sampler,
            sender,
            drops,
            finalized,
            source_drops,
            thread,
            progress,
        })
    }
    /// Retain writer health independently of successful output or finalization.
    pub fn status(&self) -> status::Status {
        status::Status {
            origin: self.origin,
            drops: self.drops.clone(),
            source: self.source_drops.clone(),
            progress: self.progress.clone(),
        }
    }
    /// Submit one sampled diagnostic; returns false on visible telemetry loss.
    pub fn record(&mut self, trace: ReceiveTrace) -> bool {
        let selected = self.sampler.observe();
        self.progress
            .eligible
            .store(self.sampler.eligible(), Ordering::Relaxed);
        self.progress
            .selected
            .store(self.sampler.selected(), Ordering::Relaxed);
        if !selected {
            return true;
        }
        if self.sender.try_send(trace).is_err() {
            self.drops.fetch_add(1, Ordering::Relaxed);
            false
        } else {
            true
        }
    }
    /// Mark known upstream observation loss, independently of this writer's queue.
    /// Set the final cumulative count before finalization; it cannot decrease.
    pub fn source_loss(&self, count: u64) {
        self.source_drops.fetch_max(count, Ordering::Relaxed);
    }
    /// Drain accepted observations and write the completeness footer. Call outside
    /// the ingestion path; a missing footer explicitly denotes partial diagnostics.
    pub fn finish(self) -> io::Result<Coverage> {
        self.finalized.store(true, Ordering::Release);
        drop(self.sender);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !self.thread.is_finished() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        if !self.thread.is_finished() {
            self.finalized.store(false, Ordering::Release);
            self.progress.failed.store(true, Ordering::Release);
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "diagnostic writer drain deadline exceeded",
            ));
        }
        self.thread
            .join()
            .map_err(|_| io::Error::other("diagnostic writer panicked"))?
    }
}
#[allow(clippy::too_many_arguments)]
fn write_worker(
    file: File,
    receiver: mpsc::Receiver<ReceiveTrace>,
    lost: Arc<AtomicU64>,
    source_lost: Arc<AtomicU64>,
    finished: Arc<AtomicBool>,
    max_bytes: u64,
    sample_rate: pax_client::measurement::SampleRate,
    origin: std::time::Instant,
    progress: &status::Progress,
) -> io::Result<Coverage> {
    let mut writer = BufWriter::new(file);
    let header = json!({"kind":"receive_sidecar","version":3,"measurement_schema":pax_client::measurement::schema::VERSION,"session":pax_client::measurement::schema::process_session(),"scope":"sampled receive diagnostics; not canonical input","export_policy":sample_rate,"build":crate::capture::current_build_info()});
    let header_bytes = serde_json::to_vec(&header)?.len() as u64 + 1;
    if header_bytes.saturating_add(1024) > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "sidecar header exceeds byte budget",
        ));
    }
    let mut bytes = write_line(&mut writer, &header)?;
    let mut coverage = Coverage {
        complete: false,
        records: 0,
        eligible: 0,
        selected: 0,
        queue_drops: 0,
        byte_drops: 0,
        source_drops: 0,
    };
    for trace in receiver {
        let mut encoded = serde_json::to_vec(
            &json!({"kind":"receive","observation":trace.observation(),"trace":trace}),
        )?;
        encoded.push(b'\n');
        if bytes
            .saturating_add(encoded.len() as u64)
            .saturating_add(1024)
            > max_bytes
        {
            coverage.byte_drops += 1;
            progress
                .byte_drops
                .store(coverage.byte_drops, Ordering::Relaxed);
            continue;
        }
        writer.write_all(&encoded)?;
        bytes += encoded.len() as u64;
        coverage.records += 1;
        progress
            .last_write_ns
            .store(crate::pipeline::ns(origin.elapsed()), Ordering::Relaxed);
        progress.records.store(coverage.records, Ordering::Release);
    }
    coverage.eligible = progress.eligible.load(Ordering::Relaxed);
    coverage.selected = progress.selected.load(Ordering::Relaxed);
    coverage.queue_drops = lost.load(Ordering::Relaxed);
    coverage.source_drops = source_lost.load(Ordering::Relaxed);
    coverage.complete = finished.load(Ordering::Acquire)
        && coverage.queue_drops == 0
        && coverage.byte_drops == 0
        && coverage.source_drops == 0;
    let footer = json!({"kind":"coverage","coverage":coverage});
    if bytes.saturating_add(serde_json::to_vec(&footer)?.len() as u64 + 1) > max_bytes {
        return Err(io::Error::other("sidecar footer exceeds byte budget"));
    }
    write_line(&mut writer, &footer)?;
    writer.flush()?;
    progress
        .complete
        .store(coverage.complete, Ordering::Release);
    Ok(coverage)
}
fn write_line(writer: &mut impl Write, value: &Value) -> io::Result<u64> {
    let mut encoded = serde_json::to_vec(value)?;
    encoded.push(b'\n');
    writer.write_all(&encoded)?;
    Ok(encoded.len() as u64)
}
/// Bounded inspection of optional diagnostic data. Missing file returns `None`;
/// callers replay canonical captures independently of this function's result.
pub fn inspect(path: &Path, max_bytes: u64) -> io::Result<Option<Value>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if file.metadata()?.len() > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "diagnostic sidecar exceeds bound",
        ));
    }
    let mut content = Vec::new();
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut content)?;
    if content.len() as u64 > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "diagnostic sidecar exceeds bound",
        ));
    }
    let mut lines = BufReader::new(content.as_slice()).lines();
    let header: Value = serde_json::from_str(
        &lines
            .next()
            .transpose()?
            .ok_or_else(|| io::Error::other("missing diagnostic header"))?,
    )?;
    if header["kind"] != "receive_sidecar" || !matches!(header["version"].as_u64(), Some(1..=3)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported diagnostic version",
        ));
    }
    if header["version"] == 3
        && (header["measurement_schema"] != pax_client::measurement::schema::VERSION
            || header["session"].as_str().is_none_or(str::is_empty))
    {
        return Err(io::Error::other("invalid current observation header"));
    }
    let mut records = 0u64;
    let mut footer = None;
    let mut partial = false;
    for line in lines {
        let value = match serde_json::from_str::<Value>(&line?) {
            Ok(v) => v,
            Err(_) => {
                partial = true;
                break;
            }
        };
        if footer.is_some() {
            partial = true;
            break;
        }
        match value["kind"].as_str() {
            Some("receive") => {
                if header["version"] == 3 {
                    let observation: pax_client::measurement::schema::Observation =
                        serde_json::from_value(value["observation"].clone())
                            .map_err(io::Error::other)?;
                    observation.validate().map_err(io::Error::other)?;
                    if observation.boundary
                        != pax_client::measurement::schema::Boundary::ReceiveOperation
                        || observation.population
                            != pax_client::measurement::schema::Population::Receive
                        || observation.id.is_none()
                        || serde_json::to_value(observation.id)? != value["trace"]["id"]
                        || observation.session != header["session"]
                    {
                        return Err(io::Error::other(
                            "incompatible current observation identity",
                        ));
                    }
                }
                records += 1;
            }
            Some("coverage") => footer = Some(value["coverage"].clone()),
            _ => {
                partial = true;
                break;
            }
        }
    }
    let complete = !partial
        && footer.as_ref().is_some_and(|f| {
            f["complete"] == true
                && f["records"] == records
                && f["queue_drops"] == 0
                && f["byte_drops"] == 0
                && ((f.get("selected").is_none() && f.get("eligible").is_none())
                    || f["selected"]
                        .as_u64()
                        .zip(f["eligible"].as_u64())
                        .is_some_and(|(selected, eligible)| {
                            selected == records && eligible >= selected
                        }))
                && if header["version"] == 1 && f.get("source_drops").is_none() {
                    true
                } else {
                    f["source_drops"].as_u64() == Some(0)
                }
        });
    Ok(Some(
        json!({"version":header["version"],"records":records,"complete":complete,"coverage":footer}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "pax-sidecar-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
    fn trace() -> ReceiveTrace {
        use pax_client::websocket::receive::{ReceiveTiming, ReceiveWork, kernel::Stamp};
        ReceiveTrace::new(
            crate::measurement::trace::TraceId {
                generation: 1,
                message: 1,
            },
            0,
            std::time::Instant::now(),
            ReceiveTiming {
                selection: Default::default(),
                revision: 0,
                complete_policy_window: true,
                policy: pax_client::websocket::receive::policy::ReceivePolicy::DETAILED,
                latest_read: None,
                first_observed_read: None,
                generation: 1,
                plaintext: None,
                socket: None,
                first_read: None,
                last_read: None,
                shared: false,
                buffered: true,
                coverage: "test",
                work: ReceiveWork::default(),
                lost: 0,
                sample_every: 1,
                software: Stamp::unavailable("test"),
                hardware: Stamp::unavailable("test"),
            },
        )
        .unwrap()
    }
    #[test]
    #[cfg(feature = "telemetry-export")]
    fn bounded_diagnostics_report_loss_and_missing_or_partial_files() {
        let path = path();
        assert!(inspect(&path, 4096).unwrap().is_none());
        let mut writer = Sidecar::start(SidecarConfig {
            sample_rate: pax_client::measurement::SampleRate::ALL,
            path: path.clone(),
            max_bytes: 4096,
            capacity: 2,
        })
        .unwrap();
        for _ in 0..1000 {
            writer.record(trace());
        }
        let coverage = writer.finish().unwrap();
        assert!(!coverage.complete);
        assert!(coverage.queue_drops + coverage.byte_drops > 0);
        assert!(std::fs::metadata(&path).unwrap().len() <= 4096);
        assert_eq!(inspect(&path, 4096).unwrap().unwrap()["complete"], false);
        assert!(
            Sidecar::start(SidecarConfig {
                sample_rate: pax_client::measurement::SampleRate::ALL,
                path: path.clone(),
                max_bytes: 4096,
                capacity: 1
            })
            .is_err()
        );
        std::fs::write(
            &path,
            b"{\"kind\":\"receive_sidecar\",\"version\":1}\n{\"kind\":",
        )
        .unwrap();
        assert_eq!(inspect(&path, 4096).unwrap().unwrap()["complete"], false);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    #[cfg(feature = "telemetry-export")]
    fn source_queue_loss_invalidates_new_sidecars_and_v1_remains_readable() {
        let path = path();
        let writer = Sidecar::start(SidecarConfig {
            sample_rate: pax_client::measurement::SampleRate::ALL,
            path: path.clone(),
            max_bytes: 4096,
            capacity: 1,
        })
        .unwrap();
        let status = writer.status();
        writer.source_loss(3);
        writer.source_loss(1);
        let coverage = writer.finish().unwrap();
        assert_eq!(coverage.source_drops, 3);
        assert!(!coverage.complete);
        let status = status.snapshot();
        assert!(!status.worker_alive && !status.failed && !status.complete);
        assert_eq!(status.source_drops, 3);
        assert_eq!(inspect(&path, 4096).unwrap().unwrap()["complete"], false);
        std::fs::write(&path,b"{\"kind\":\"receive_sidecar\",\"version\":1}\n{\"kind\":\"coverage\",\"coverage\":{\"complete\":true,\"records\":0,\"queue_drops\":0,\"byte_drops\":0}}\n").unwrap();
        assert_eq!(inspect(&path, 4096).unwrap().unwrap()["complete"], true);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    #[cfg(feature = "telemetry-export")]
    fn file_sampling_has_its_own_denominator_and_is_not_capture_loss() {
        let path = path();
        let mut writer = Sidecar::start(SidecarConfig {
            path: path.clone(),
            max_bytes: 1048576,
            capacity: 64,
            sample_rate: pax_client::measurement::SampleRate::every(2),
        })
        .unwrap();
        let status = writer.status();
        for _ in 0..32 {
            assert!(writer.record(trace()));
        }
        let coverage = writer.finish().unwrap();
        assert!(coverage.complete);
        assert_eq!(
            (coverage.eligible, coverage.selected, coverage.records),
            (32, 16, 16)
        );
        assert_eq!(
            (status.snapshot().eligible, status.snapshot().selected),
            (32, 16)
        );
        assert_eq!(inspect(&path, 1048576).unwrap().unwrap()["complete"], true);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn current_metadata_refuses_wrong_session_generation_and_clock() {
        let path = path();
        let sample = trace();
        let observation = sample.observation();
        for mismatch in 0..4 {
            let mut changed = observation.clone();
            match mismatch {
                0 => changed.session.push_str("-other"),
                1 => changed.id.as_mut().unwrap().generation += 1,
                2 => {
                    changed.clock.end =
                        pax_client::measurement::schema::ClockDomain::SourceMonotonic
                }
                _ => changed.boundary_version += 1,
            }
            let rows = [
                json!({"kind":"receive_sidecar","version":3,"measurement_schema":pax_client::measurement::schema::VERSION,"session":observation.session}),
                json!({"kind":"receive","trace":sample,"observation":changed}),
            ];
            std::fs::write(
                &path,
                rows.iter()
                    .map(Value::to_string)
                    .collect::<Vec<_>>()
                    .join("\n"),
            )
            .unwrap();
            assert!(inspect(&path, 1048576).is_err());
        }
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    #[cfg(feature = "telemetry-export")]
    fn finalized_empty_diagnostics_are_complete_and_versioned() {
        let path = path();
        let writer = Sidecar::start(SidecarConfig {
            sample_rate: pax_client::measurement::SampleRate::ALL,
            path: path.clone(),
            max_bytes: 4096,
            capacity: 1,
        })
        .unwrap();
        let status = writer.status();
        assert!(writer.finish().unwrap().complete);
        let status = status.snapshot();
        assert!(status.complete && !status.worker_alive && !status.failed);
        assert_eq!(inspect(&path, 4096).unwrap().unwrap()["complete"], true);
        std::fs::write(&path, b"{\"kind\":\"receive_sidecar\",\"version\":99}\n").unwrap();
        assert!(inspect(&path, 4096).is_err());
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn legacy_trace_without_first_window_metadata_remains_readable() {
        let path = path();
        let mut legacy = serde_json::to_value(trace()).unwrap();
        legacy["schema_version"] = json!(2);
        legacy
            .as_object_mut()
            .unwrap()
            .remove("first_observed_read");
        let lines = [
            json!({"kind":"receive_sidecar","version":2}),
            json!({"kind":"receive","trace":legacy}),
            json!({"kind":"coverage","coverage":{"complete":true,"records":1,
                "queue_drops":0,"byte_drops":0,"source_drops":0}}),
        ];
        let mut writer = File::create(&path).unwrap();
        for line in &lines {
            write_line(&mut writer, line).unwrap();
        }
        drop(writer);
        let report = inspect(&path, 8192).unwrap().unwrap();
        assert_eq!(report["records"], 1);
        assert_eq!(report["complete"], true);
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(all(test, feature = "telemetry-export"))]
mod abort_tests {
    use super::*;
    #[test]
    fn channel_close_without_explicit_finish_cannot_mark_complete() {
        let path = std::env::temp_dir().join(format!(
            "pax-sidecar-abort-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let Sidecar { sender, thread, .. } = Sidecar::start(SidecarConfig {
            sample_rate: pax_client::measurement::SampleRate::ALL,
            path: path.clone(),
            max_bytes: 4096,
            capacity: 1,
        })
        .unwrap();
        drop(sender);
        assert!(!thread.join().unwrap().unwrap().complete);
        assert_eq!(inspect(&path, 4096).unwrap().unwrap()["complete"], false);
        std::fs::remove_file(path).unwrap();
    }
}
