//! CLI orchestration; bootstrap and deployer identities never share a fallback.
mod arguments;
use arguments::{Cli, Command};
use clap::Parser;
use pax_ops::{
    aws::{self, Api},
    config::{self, Identity, Scope},
    error::Error,
};
use serde_json::{Value, json};

pub(crate) fn run() -> Result<(), Error> {
    let cli = Cli::parse();
    if matches!(cli.command, Command::Template) {
        println!("{}", aws::stack::HOST_TEMPLATE);
        return Ok(());
    }
    let account = cli
        .account
        .clone()
        .or_else(|| std::env::var("PAX_AWS_ACCOUNT_ID").ok());
    let scope = account
        .map(|account| Scope::new(account, cli.region.clone(), cli.stack.clone()))
        .transpose()?;
    if matches!(cli.command, Command::Policies) {
        let scope = scope.ok_or_else(|| Error::Config("--account is required".into()))?;
        emit(
            json!({"workload_boundary":aws::access::workload_boundary(&scope),"deployer":aws::access::deployer_policy(&scope)}),
        )?;
        return Ok(());
    }
    let bootstrap = matches!(
        cli.command,
        Command::Bootstrap { .. } | Command::VerifyAccess
    );
    let discovery = matches!(cli.command, Command::Identity);
    if !bootstrap && !discovery && (cli.admin_env.is_some() || cli.admin_profile.is_some()) {
        return Err(Error::Config(
            "normal operations reject bootstrap credential selection".into(),
        ));
    }
    if bootstrap && cli.admin_env.is_none() && cli.admin_profile.is_none() {
        return Err(Error::Config(
            "bootstrap requires --admin-env or --admin-profile".into(),
        ));
    }
    let (identity, role) = if let Some(path) = &cli.admin_env {
        config::admin_file(path)?
    } else if let Some(profile) = &cli.admin_profile {
        (Identity::Profile(profile.clone()), None)
    } else if let Some(path) = &cli.operator_env {
        config::operator_file(path)?
    } else {
        (
            cli.profile
                .clone()
                .map(Identity::Profile)
                .unwrap_or(Identity::Operator),
            None,
        )
    };
    if !bootstrap && !discovery {
        validate_role_selection(role.as_deref(), cli.deployer_role.as_deref())?;
    }
    let mut api = aws::Cli::new(cli.region.clone(), identity);
    // Even before assuming a configured role, ensure the source identity's account.
    if let Some(scope) = &scope {
        api.verify_account(scope)?;
    }
    if let Some(role) = role {
        if bootstrap || cli.admin_env.is_some() {
            api.assume(&role)?;
        } else {
            assume_deployer(&mut api, scope.as_ref(), &role)?;
        }
    } else if !bootstrap && !discovery {
        let role = cli
            .deployer_role
            .clone()
            .or_else(|| std::env::var("PAX_AWS_DEPLOYER_ROLE_ARN").ok())
            .ok_or_else(|| {
                Error::Config("normal operations require an explicit deployer role".into())
            })?;
        assume_deployer(&mut api, scope.as_ref(), &role)?;
    }
    if discovery {
        emit(api.call("sts", "get-caller-identity", json!({}))?)?;
        return Ok(());
    }
    let scope = scope.ok_or_else(|| {
        Error::Config("--account is required before mutation or target resolution".into())
    })?;
    api.verify_account(&scope)?;
    let response = match cli.command {
        Command::ReleaseStatus { instance } => aws::service::status(&mut api, &scope, &instance)?,
        Command::Restart { instance } => aws::service::restart(&mut api, &scope, &instance)?,
        Command::Deploy {
            instance,
            bucket,
            artifact,
            bundle,
            markets,
            ingest_cpu,
            background_cpu,
            health_seconds,
            hardware_interface,
            hardware_clock,
            activate_stable,
            require_noop,
        } => {
            let artifact = aws::release::prepare(&artifact, &bundle)?;
            aws::release::deploy(
                &mut api,
                &scope,
                &instance,
                &bucket,
                &artifact,
                &aws::release::Options {
                    activate_stable,
                    require_noop,
                    markets,
                    ingest_cpu,
                    background_cpu,
                    health_seconds,
                    hardware_interface,
                    hardware_clock,
                },
            )?
        }
        Command::VerifyAccess => aws::access::verify_access(&mut api, &scope)?,
        Command::Bootstrap {
            trust_principal,
            create_operator,
            key_file,
        } => {
            let principal = if create_operator {
                format!(
                    "arn:aws:iam::{}:user/pax-access/pax-operator",
                    scope.account
                )
            } else {
                trust_principal.ok_or_else(|| {
                    Error::Config(
                        "bootstrap requires --trust-principal or --create-operator".into(),
                    )
                })?
            };
            let response = aws::access::bootstrap(&mut api, &scope, &principal, create_operator)?;
            if let Some(path) = key_file {
                aws::credentials::deliver(&mut api, &scope, &path)?;
            }
            response
        }
        Command::Resolve {
            zone_name,
            zone_id,
            ami,
            fallback_zone_ids,
        } => aws::stack::resolve_with_fallback(
            &mut api,
            &scope,
            zone_name.as_deref(),
            zone_id.as_deref(),
            ami.as_deref(),
            "c7a.xlarge",
            &fallback_zone_ids,
        )?,
        Command::Plan { target } => {
            let target = serde_json::from_reader(std::fs::File::open(target)?)?;
            aws::stack::plan(&mut api, &scope, &target)?
        }
        Command::DemoPlan {
            enabled,
            restrict_origin,
        } => aws::presentation::plan(&mut api, &scope, enabled, restrict_origin)?,
        Command::DemoStatus => aws::presentation::status(&mut api, &scope)?,
        Command::DemoApply {
            change_set,
            allow_removal,
        } => aws::presentation::apply(&mut api, &scope, &change_set, allow_removal)?,
        Command::Apply {
            change_set,
            allow_replacement,
        } => aws::stack::apply(&mut api, &scope, &change_set, allow_replacement)?,
        Command::Status => {
            let stack = aws::stack::status(&mut api, &scope)?;
            let resources = if stack.is_some() {
                api.call(
                    "cloudformation",
                    "list-stack-resources",
                    json!({"StackName":scope.stack}),
                )?
            } else {
                json!({})
            };
            json!({"stack":stack,"resources":resources})
        }
        Command::Events => {
            let _ = aws::stack::status(&mut api, &scope)?.ok_or(Error::Missing("owned stack"))?;
            api.call(
                "cloudformation",
                "describe-stack-events",
                json!({"StackName":scope.stack}),
            )?
        }
        Command::Recover { recreate_failed } => {
            aws::stack::recover(&mut api, &scope, recreate_failed)?
        }
        Command::Destroy { yes } => {
            if !yes {
                return Err(Error::Config("destroy requires --yes".into()));
            }
            aws::stack::destroy(&mut api, &scope)?
        }
        Command::Identity | Command::Template | Command::Policies => {
            unreachable!("handled read-only early commands")
        }
    };
    emit(response)
}
fn validate_role_selection(configured: Option<&str>, explicit: Option<&str>) -> Result<(), Error> {
    if configured.zip(explicit).is_some_and(|(a, b)| a != b) {
        return Err(Error::Config(
            "explicit deployer role conflicts with the selected credential file".into(),
        ));
    }
    Ok(())
}

fn assume_deployer(api: &mut aws::Cli, scope: Option<&Scope>, role: &str) -> Result<(), Error> {
    let scope = scope
        .ok_or_else(|| Error::Config("--account is required before deployer assumption".into()))?;
    if role != scope.deployer_arn() {
        return Err(Error::Config(
            "deployer role must match the fixed project access role in the expected account".into(),
        ));
    }
    api.assume(role)?;
    api.verify_account(scope)?;
    Ok(())
}
fn emit(value: Value) -> Result<(), Error> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn namespace_alias_selects_one_scope_without_ambiguous_overrides() {
        for flag in ["--stack", "--namespace"] {
            let cli = Cli::try_parse_from(["pax-ops", flag, "pax-research", "status"]).unwrap();
            assert_eq!(cli.stack, "pax-research");
        }
        assert!(
            Cli::try_parse_from([
                "pax-ops",
                "--stack",
                "pax-prod",
                "--namespace",
                "pax-research",
                "status"
            ])
            .is_err()
        );
    }
    #[test]
    fn explicit_role_cannot_be_silently_ignored_by_a_credential_file() {
        assert!(validate_role_selection(Some("configured"), Some("different")).is_err());
        assert!(validate_role_selection(Some("same"), Some("same")).is_ok());
        assert!(validate_role_selection(None, Some("selected")).is_ok());
    }
    #[test]
    fn credential_providers_are_mutually_exclusive() {
        for args in [
            vec![
                "pax-ops",
                "--admin-env",
                ".env",
                "--operator-env",
                ".env.operator",
                "identity",
            ],
            vec![
                "pax-ops",
                "--admin-profile",
                "bootstrap",
                "--admin-env",
                ".env",
                "identity",
            ],
            vec![
                "pax-ops",
                "--profile",
                "operator",
                "--operator-env",
                ".env.operator",
                "identity",
            ],
        ] {
            assert!(Cli::try_parse_from(args).is_err());
        }
        assert!(Cli::try_parse_from(["pax-ops", "--admin-env", ".env", "identity"]).is_ok());
    }
}
