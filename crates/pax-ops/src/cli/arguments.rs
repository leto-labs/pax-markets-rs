//! Typed command inputs; lifecycle operations remain in their AWS owner modules.
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Namespaced Pax demo deployment")]
pub(super) struct Cli {
    /// Required expected account for every operation except identity discovery/template.
    #[arg(long, global = true)]
    pub(super) account: Option<String>,
    #[arg(long, global = true, default_value = "ap-northeast-1")]
    pub(super) region: String,
    /// Deployment environment; --namespace selects the same scope.
    #[arg(
        long,
        visible_alias = "namespace",
        global = true,
        default_value = "pax-tokyo"
    )]
    pub(super) stack: String,
    /// Explicit private bootstrap file, only for identity/bootstrap commands.
    #[arg(long,global=true,conflicts_with_all=["admin_profile","operator_env","profile"])]
    pub(super) admin_env: Option<PathBuf>,
    #[arg(long,global=true,conflicts_with_all=["operator_env","profile"])]
    pub(super) admin_profile: Option<String>,
    /// Explicit private normal-operator file.
    #[arg(long, global = true, conflicts_with = "profile")]
    pub(super) operator_env: Option<PathBuf>,
    #[arg(long, global = true)]
    pub(super) profile: Option<String>,
    #[arg(long, global = true)]
    pub(super) deployer_role: Option<String>,
    #[command(subcommand)]
    pub(super) command: Command,
}
#[derive(Subcommand)]
pub(super) enum Command {
    /// Read identity only; account discovery has no mutation.
    Identity,
    /// Print the reviewed static host template without AWS access.
    Template,
    /// Print generated IAM policies without AWS access.
    Policies,
    /// Verify allow/deny cases through read-only IAM simulation with explicit admin access.
    VerifyAccess,
    /// IAM-only access bootstrap; does not provision compute.
    Bootstrap {
        #[arg(long, conflicts_with = "create_operator")]
        trust_principal: Option<String>,
        #[arg(long)]
        create_operator: bool,
        #[arg(long, requires = "create_operator")]
        key_file: Option<PathBuf>,
    },
    /// Resolve account-specific AZ/official AL2023 AMI and offerings, read-only.
    Resolve {
        #[arg(long)]
        zone_name: Option<String>,
        #[arg(long)]
        zone_id: Option<String>,
        /// Explicit official AL2023 image; otherwise retain the deployed literal ID.
        #[arg(long)]
        ami: Option<String>,
        /// Explicit alternative AZ IDs, tried only if requested placement is unavailable.
        #[arg(long, value_delimiter = ',')]
        fallback_zone_ids: Vec<String>,
    },
    /// Create CloudFormation planning metadata; no compute is launched.
    Plan {
        /// Previously inspected, non-secret resolved target JSON.
        #[arg(long)]
        target: PathBuf,
    },
    /// Plan HTTPS or restore public HTTP without changing the host or release.
    DemoPlan {
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        enabled: bool,
        /// Only after deployed HTTPS health has been verified independently.
        #[arg(long)]
        restrict_origin: bool,
    },
    /// Read canonical URLs, propagation state and effective origin access.
    DemoStatus,
    /// Execute a presentation-only change set; host changes are rejected.
    DemoApply {
        #[arg(long)]
        change_set: String,
        /// Allow deleting only the distribution and its query policy.
        #[arg(long)]
        allow_removal: bool,
    },
    /// Execute an inspected ready change set belonging to the owned stack.
    Apply {
        #[arg(long)]
        change_set: String,
        #[arg(long)]
        allow_replacement: bool,
    },
    /// Read owned stack state, outputs and billable resource inventory.
    Status,
    /// Inspect owned stack failures and their bounded resource diagnostics.
    Events,
    /// Read managed release/service state and capture completion without secrets.
    ReleaseStatus {
        #[arg(long)]
        instance: String,
    },
    /// Restart the current demo and verify fresh books, preserving captures.
    Restart {
        #[arg(long)]
        instance: String,
    },
    /// Deploy a verified compatible bundle through SSM, with live health and rollback.
    Deploy {
        #[arg(long)]
        instance: String,
        #[arg(long)]
        bucket: String,
        #[arg(long)]
        artifact: PathBuf,
        #[arg(long)]
        bundle: PathBuf,
        #[arg(long, value_delimiter = ',', required = true)]
        markets: Vec<u64>,
        #[arg(long)]
        ingest_cpu: Option<usize>,
        #[arg(long, requires = "ingest_cpu")]
        background_cpu: Option<usize>,
        #[arg(long, default_value_t = 60)]
        health_seconds: u32,
        #[arg(long, requires = "hardware_clock")]
        hardware_interface: Option<String>,
        #[arg(long, requires = "hardware_interface")]
        hardware_clock: Option<PathBuf>,
        /// Explicitly activate a release on a stable deployment.
        #[arg(long)]
        activate_stable: bool,
        /// Verify an unchanged healthy deployment and refuse upload/activation otherwise.
        #[arg(long)]
        require_noop: bool,
    },
    /// Resume rollback, or explicitly recreate an initial failed stack with no retained data.
    Recover {
        #[arg(long)]
        recreate_failed: bool,
    },
    /// Delete the owned stack; artifacts are retained and reported explicitly.
    Destroy {
        #[arg(long, required = true)]
        yes: bool,
    },
}
