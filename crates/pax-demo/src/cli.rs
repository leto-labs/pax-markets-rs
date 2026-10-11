//! Public read-only commands. Offline replay never opens a venue connection.
mod live;
mod shutdown;
use live::{observe, serve};
#[cfg(test)]
mod tests;

use clap::{Args, Parser, Subcommand};
use pax_client::{
    metadata::{METADATA_URL, MetadataClient},
    websocket::{FeedConfig, MARKET_DATA_URL},
};
use pax_demo::{
    capture::CaptureConfig,
    pipeline::Pipeline,
    replay::Replay,
    runner::{
        self, Error, RunConfig,
        placement::{self, Placement},
    },
};
use pax_types::{
    market::ExchangeInfo,
    units::{MarketId, Quantity},
};
use std::{path::PathBuf, time::Duration};

#[derive(Parser)]
#[command(
    version,
    about = "Public Pax orderbook reconstruction, liquidity, and exact capture/replay"
)]
pub(crate) struct Cli {
    /// Tokio worker count; omit to retain Tokio's available-CPU default.
    #[arg(long, global = true, value_parser = clap::value_parser!(u16).range(1..=256))]
    runtime_workers: Option<u16>,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Show resolved optional measurement groups and build provenance.
    Capabilities,
    /// Discover public markets and exact metadata units; no API key.
    Markets {
        #[arg(long, default_value = METADATA_URL)]
        metadata_url: String,
    },
    /// Headless public observation with health, quotes, and time-weighted liquidity.
    Observe(Live),
    /// Record exact public inputs to a new directory, with finite disk and time limits.
    Capture {
        #[command(flatten)]
        live: Live,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = 268435456, value_parser = clap::value_parser!(u64).range(8..))]
        max_bytes: u64,
    },
    /// Local responsive orderbook and latency pages; no frontend build step.
    Serve(Serve),
    /// Verify a complete capture and reconstruct it offline without API access.
    Replay {
        directory: PathBuf,
        #[arg(long, default_value_t = 1073741824)]
        max_bytes: u64,
        #[arg(long, default_value_t = 100_000_000)]
        quantity: u64,
    },
}
#[derive(Args)]
struct Live {
    #[command(flatten)]
    ingestion: Ingestion,
    /// Explicit venue market IDs, discovered with `markets`.
    #[arg(long, required = true, value_delimiter = ',')]
    markets: Vec<u64>,
    #[arg(long, default_value = METADATA_URL)]
    metadata_url: String,
    #[arg(long, default_value = MARKET_DATA_URL)]
    websocket_url: String,
    /// Finite observation duration; zero observes until Ctrl-C.
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(0..=86400))]
    seconds: u64,
    /// Display rows per side, independently of reconstruction depth.
    #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u32).range(1..=1000))]
    depth: u32,
    /// Execution-estimate order size in base native units.
    #[arg(long, default_value_t = 100_000_000, value_parser = clap::value_parser!(u64).range(1..))]
    quantity: u64,
    /// Detailed timing sample interval; one samples every message.
    #[arg(long, default_value_t = u64::from(cfg!(feature="processing-traces")), value_parser = clap::value_parser!(u64).range(0..=1000000000))]
    sample_every: u64,
}
#[derive(Args)]
struct Serve {
    #[command(flatten)]
    ingestion: Ingestion,
    /// Optional bounded route probes to the configured live venue; once per minute.
    #[arg(long, conflicts_with_all = ["fixture", "replay"])]
    route_diagnostics: bool,
    /// TCP/443 usually traverses HTTPS firewalls; UDP retains the original probe.
    #[arg(long, default_value="tcp", value_parser=["tcp", "udp"], requires="route_diagnostics")]
    route_method: String,
    /// Loopback by default; explicitly choose 0.0.0.0 to expose on the LAN.
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: std::net::SocketAddr,
    /// Cycle the bundled retained public sample at an artificial cadence.
    #[arg(long, conflicts_with_all = ["replay", "markets"])]
    fixture: bool,
    /// Play a verified capture at its original cadence, without venue access.
    #[arg(long, conflicts_with_all = ["fixture", "markets"])]
    replay: Option<PathBuf>,
    /// Explicit live market IDs, discovered through metadata.
    #[arg(long, value_delimiter = ',', required_unless_present_any = ["fixture", "replay"])]
    markets: Vec<u64>,
    #[arg(long, default_value = METADATA_URL)]
    metadata_url: String,
    #[arg(long, default_value = MARKET_DATA_URL)]
    websocket_url: String,
    #[arg(long, default_value_t = 100_000_000, value_parser = clap::value_parser!(u64).range(1..))]
    quantity: u64,
    #[arg(long, default_value_t = u64::from(cfg!(feature="processing-traces")), value_parser = clap::value_parser!(u64).range(0..=1000000000))]
    sample_every: u64,
}
#[derive(Args)]
struct Ingestion {
    /// Optional independent collection and output settings (bounded JSON file).
    #[arg(long, conflicts_with = "measurement_preset")]
    measurement_config: Option<PathBuf>,
    /// Collection preset; detailed requests all compiled groups explicitly.
    #[arg(long,value_parser=["off","timestamps","detailed"],conflicts_with="measurement_config")]
    measurement_preset: Option<String>,
    /// Write sampled receive metadata to a new independent JSONL sidecar.
    #[arg(long, requires = "receive_diagnostics")]
    receive_sidecar: Option<PathBuf>,
    /// Finite diagnostic file budget; loss appears in its coverage footer.
    #[arg(long,default_value_t=16777216,value_parser=clap::value_parser!(u64).range(4096..))]
    receive_sidecar_bytes: u64,

    /// Instrument socket reads, TLS adapter and WebSocket polls (adds overhead).
    #[arg(long)]
    receive_diagnostics: bool,
    /// Emit one receive diagnostic in N messages; all reads remain observed.
    #[arg(long,default_value_t=1,value_parser=clap::value_parser!(u64).range(1..))]
    receive_sample_every: u64,
    /// Request Linux kernel software receive timestamps on this socket only.
    #[arg(long, requires = "receive_diagnostics")]
    receive_software: bool,
    /// Request hardware stamps from this explicit interface without configuring it.
    #[arg(long,requires_all=["receive_diagnostics","receive_phc"])]
    receive_interface: Option<String>,
    /// Read-only PHC device belonging to --receive-interface.
    #[arg(long, requires = "receive_interface")]
    receive_phc: Option<PathBuf>,
    /// Minimum PHC calibration interval, 0..=100 ms; zero reads every selected batch.
    #[arg(long, default_value_t=20, requires="receive_interface", value_parser=clap::value_parser!(u64).range(0..=100))]
    phc_refresh_ms: u64,
    /// Move read-only PHC calibration off ingestion using driver brackets.
    #[arg(long, requires = "receive_interface")]
    phc_background_driver: bool,
    /// Place PHC calibration on a background CPU instead of inherited ingestion affinity.
    #[arg(long, requires = "phc_background_driver")]
    phc_worker_cpu: Option<usize>,
    /// Defer PHC stamp mapping until after validated book commit.
    #[arg(long, requires = "phc_background_driver")]
    phc_deferred_mapping: bool,
    /// Per-message delivery; inline processes on the socket's receiving task.
    #[arg(long, default_value="queued", value_parser=["queued", "inline"])]
    delivery: String,
    /// Maximum queued inputs processed before returning to control/report selection.
    #[arg(long, default_value_t=1, value_parser=clap::value_parser!(u16).range(1..=256))]
    queue_drain: u16,
    /// WebSocket read chunk size; message/snapshot limits remain independent.
    #[arg(long, default_value_t=131072, value_parser=clap::value_parser!(u32).range(1024..=1048576))]
    read_buffer_bytes: u32,
    /// Disable extra transport/delivery markers while retaining processing traces.
    #[arg(long)]
    no_delivery_timing: bool,
    /// Run the public feed and book on a dedicated single-thread runtime.
    #[arg(long)]
    dedicated_ingestion: bool,
    /// Pin dedicated ingestion to this allowed Linux logical CPU ID.
    #[arg(long, requires = "dedicated_ingestion")]
    ingest_cpu: Option<usize>,
    /// Pin report/capture/diagnostic workers separately; otherwise they inherit ingestion affinity.
    #[arg(long, requires = "ingest_cpu")]
    background_cpu: Option<usize>,
}
impl Ingestion {
    fn settings(&self) -> Result<pax_demo::measurement::settings::Settings, Error> {
        use pax_demo::measurement::settings::Settings;
        use std::io::Read;
        let settings = if let Some(path) = &self.measurement_config {
            let mut data = Vec::new();
            std::fs::File::open(path)?
                .take(65537)
                .read_to_end(&mut data)?;
            if data.len() > 65536 {
                return Err("measurement settings exceed 64 KiB".into());
            }
            serde_json::from_slice(&data)?
        } else {
            match self.measurement_preset.as_deref() {
                Some("off") => Settings::off(),
                Some("timestamps") => Settings::timestamps(),
                Some("detailed") => Settings::detailed(),
                _ => Settings::default(),
            }
        };
        settings.validate()?;
        Ok(settings)
    }

    fn sidecar(&self) -> Option<pax_demo::measurement::sidecar::SidecarConfig> {
        self.receive_sidecar
            .as_ref()
            .map(|path| pax_demo::measurement::sidecar::SidecarConfig {
                sample_rate: pax_client::measurement::SampleRate::ALL,
                path: path.clone(),
                max_bytes: self.receive_sidecar_bytes,
                capacity: 1024,
            })
    }

    fn receive(&self) -> Option<pax_client::websocket::receive::ReceiveConfig> {
        self.receive_diagnostics
            .then(|| pax_client::websocket::receive::ReceiveConfig {
                sample_every: self.receive_sample_every,
                phc_calibration: if self.phc_background_driver {
                    pax_client::websocket::receive::PhcCalibration::BackgroundDriver
                } else {
                    pax_client::websocket::receive::PhcCalibration::SampledRead
                },
                phc_worker_cpu: self.phc_worker_cpu,
                phc_deferred_mapping: self.phc_deferred_mapping,
                phc_refresh_interval: Duration::from_millis(self.phc_refresh_ms),
                software_timestamps: self.receive_software,
                hardware: self
                    .receive_interface
                    .as_ref()
                    .zip(self.receive_phc.as_ref())
                    .map(|(interface, clock)| {
                        pax_client::websocket::receive::kernel::HardwareConfig {
                            interface: interface.clone(),
                            clock: clock.clone(),
                        }
                    }),
                ..Default::default()
            })
    }

    fn placement(&self) -> Result<Placement, Error> {
        if self.delivery == "inline" && self.queue_drain != 1 {
            return Err("--queue-drain greater than one requires --delivery queued".into());
        }
        let placement = if let Some(worker_cpu) = self.background_cpu {
            Placement::DedicatedWithWorkerCpu {
                cpu: self
                    .ingest_cpu
                    .ok_or("background CPU requires ingestion CPU")?,
                worker_cpu,
            }
        } else if self.dedicated_ingestion {
            Placement::Dedicated {
                cpu: self.ingest_cpu,
            }
        } else {
            Placement::Shared
        };
        placement.validate()?;
        Ok(placement)
    }
}
async fn metadata(url: &str) -> Result<ExchangeInfo, Error> {
    Ok(MetadataClient::new(Duration::from_secs(15), 1024 * 1024)?
        .fetch(url)
        .await?)
}
pub(crate) fn run() -> Result<(), Error> {
    let cli = Cli::parse();
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime.enable_all();
    if let Some(workers) = cli.runtime_workers {
        runtime.worker_threads(usize::from(workers));
    }
    runtime.build()?.block_on(dispatch(cli.command))
}
async fn dispatch(command: Command) -> Result<(), Error> {
    match command {
        Command::Capabilities => println!(
            "{}",
            serde_json::to_string_pretty(&pax_demo::capture::current_build_info())?
        ),
        Command::Markets { metadata_url } => println!(
            "{}",
            serde_json::to_string_pretty(&metadata(&metadata_url).await?)?
        ),
        Command::Observe(live) => observe(live, None).await?,
        Command::Serve(config) => serve(config).await?,
        Command::Capture {
            live,
            output,
            max_bytes,
        } => {
            if live.seconds == 0 {
                return Err("capture requires a finite --seconds duration".into());
            }
            observe(
                live,
                Some(CaptureConfig {
                    directory: output,
                    max_bytes,
                    queue_capacity: 1024,
                    max_record_bytes: 1024 * 1024,
                }),
            )
            .await?;
        }
        Command::Replay {
            directory,
            max_bytes,
            quantity,
        } => {
            if quantity == 0 {
                return Err("--quantity must be positive".into());
            }
            let mut replay = Replay::open(&directory, max_bytes)?;
            let mut pipeline = Pipeline::new(
                &replay.manifest().metadata,
                &replay.manifest().markets,
                u64::from(cfg!(feature = "processing-traces")),
            )?;
            while let Some(record) = replay.next_record()? {
                pipeline.process(&record, 0)?;
            }
            pipeline.finish(replay.manifest().end_offset_ns)?;
            let view = pipeline.view(
                "offline replay; timings are local reprocessing",
                replay.manifest().end_offset_ns,
                20,
                Quantity(quantity),
            );
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &serde_json::json!({"digest": pipeline.digest(), "capture_digest": replay.manifest().digest, "view": view})
                )?
            );
        }
    }
    Ok(())
}
