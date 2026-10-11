//! Local harness selection; the default remains the complete offline suite.
#[derive(clap::Parser)]
#[command(name = "packet-turnover", about = "Offline decode and book benchmarks")]
pub(super) struct Options {
    /// Use small sample counts for correctness checks.
    #[arg(long)]
    pub smoke: bool,
    /// Cargo's custom test-harness marker also selects a smoke run.
    #[arg(long, hide = true)]
    pub test: bool,
    /// Accept Cargo's benchmark marker.
    #[arg(long, hide = true)]
    pub bench: bool,
    /// Run only this exact workload name.
    #[arg(long)]
    pub workload: Option<String>,
    /// Run only one core phase, plus the timer baseline; omit other experiments.
    #[arg(long, value_parser = ["decode", "apply", "combined", "instrumented"])]
    pub core_mode: Option<String>,
    /// Measure snapshot decoding, loading or buffered reconciliation, instead of events.
    #[arg(long, conflicts_with = "core_mode", value_parser = ["decode", "apply", "combined", "reconcile", "all"])]
    pub snapshot_mode: Option<String>,
    /// Apply each decoded event to this many independently sequenced markets.
    #[arg(long, conflicts_with_all = ["core_mode", "snapshot_mode"], value_parser = clap::value_parser!(u16).range(1..=256))]
    pub session_markets: Option<u16>,
    /// Print the available workload names and exit.
    #[arg(long)]
    pub list_workloads: bool,
}
