//! One-time private operator key delivery with revocation on failure.
use super::Api;
use crate::{config::Scope, error::Error};
use serde_json::{Value, json};
use std::{fs::OpenOptions, io::Write, path::Path};

/// Create one operator key and persist it once. An existing file or AWS key is a refusal.
/// If creation succeeds but local persistence fails, revoke the key immediately.
pub fn deliver(api: &mut impl Api, scope: &Scope, path: &Path) -> Result<Value, Error> {
    deliver_with(api, scope, path, persist)
}
type Persistence = Result<(), (std::io::Error, bool)>;
fn deliver_with(
    api: &mut impl Api,
    scope: &Scope,
    path: &Path,
    save: impl FnOnce(&Path, &str, &str, &Scope) -> Persistence,
) -> Result<Value, Error> {
    super::verify_account(api, scope)?;
    let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
    if !name.starts_with(".env.") || name == ".env.example" {
        return Err(Error::Config(
            "operator key destination must be an ignored .env.* file".into(),
        ));
    }
    if path.symlink_metadata().is_ok() {
        return Err(Error::Config(
            "operator key destination already exists".into(),
        ));
    }
    let existing = api.call(
        "iam",
        "list-access-keys",
        json!({"UserName":"pax-operator"}),
    )?;
    if !existing["AccessKeyMetadata"]
        .as_array()
        .is_some_and(Vec::is_empty)
    {
        return Err(Error::Config(
            "operator already has a key; refusing another".into(),
        ));
    }
    let created = api.call(
        "iam",
        "create-access-key",
        json!({"UserName":"pax-operator"}),
    )?;
    let access = created["AccessKey"]["AccessKeyId"]
        .as_str()
        .ok_or_else(|| Error::Cleanup {
            action: "key creation returned no identifier".into(),
            cleanup: "inspect the owned operator's access-key inventory before retrying".into(),
        })?;
    let secret = created["AccessKey"]["SecretAccessKey"]
        .as_str()
        .filter(|s| !s.is_empty() && !s.contains(['\n', '\r', '\0']));
    let Some(secret) = secret else {
        if let Err(cleanup) = api.call(
            "iam",
            "delete-access-key",
            json!({"UserName":"pax-operator","AccessKeyId":access}),
        ) {
            return Err(Error::Cleanup {
                action: "created key response lacked a valid secret".into(),
                cleanup: cleanup.to_string(),
            });
        }
        return Err(Error::Missing("valid SecretAccessKey; new key revoked"));
    };
    let saved = save(path, access, secret, scope);
    if let Err((failure, opened)) = saved {
        let removal = if opened {
            std::fs::remove_file(path)
        } else {
            Ok(())
        };
        let revocation = api.call(
            "iam",
            "delete-access-key",
            json!({"UserName":"pax-operator","AccessKeyId":access}),
        );
        if removal.is_err() || revocation.is_err() {
            return Err(Error::Cleanup {
                action: format!("operator key persistence ({:?})", failure.kind()),
                cleanup: format!(
                    "local removal={}, AWS revocation={}",
                    removal.is_ok(),
                    revocation.is_ok()
                ),
            });
        }
        return Err(Error::Io(failure));
    }
    Ok(
        json!({"operator_key":"saved once to a private ignored file","deployer_role":scope.deployer_arn()}),
    )
}
fn persist(path: &Path, access: &str, secret: &str, scope: &Scope) -> Persistence {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut opened = false;
    let saved = (|| {
        let mut file = options.open(path)?;
        opened = true;
        writeln!(
            file,
            "AWS_ACCESS_KEY_ID={access}\nAWS_SECRET_ACCESS_KEY={secret}\nPAX_AWS_DEPLOYER_ROLE_ARN={}\nPAX_AWS_ACCOUNT_ID={}\nAWS_REGION={}",
            scope.deployer_arn(),
            scope.account,
            scope.region
        )?;
        file.sync_all()?;
        Ok::<(), std::io::Error>(())
    })();
    saved.map_err(|error| (error, opened))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn failed_save_revokes_without_exposing_key() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        let mut api = super::super::fake::Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(json!({"AccessKeyMetadata":[]})),
            Ok(json!({"AccessKey":{"AccessKeyId":"test-key","SecretAccessKey":"test-secret"}})),
            Ok(json!({})),
        ]);
        let path = std::env::temp_dir().join(format!(
            "pax-no-parent-{}/.env.operator",
            std::process::id()
        ));
        let error = deliver(&mut api, &scope, &path).unwrap_err();
        assert!(!format!("{error}").contains("test-key"));
        assert_eq!(api.calls.last().unwrap().1, "delete-access-key");
    }
    #[test]
    fn key_already_exists_refuses_creation() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        let mut api = super::super::fake::Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(json!({"AccessKeyMetadata":[{}]})),
        ]);
        let path = std::env::temp_dir().join(format!(".env.pax-key-test-{}", std::process::id()));
        assert!(deliver(&mut api, &scope, &path).is_err());
        assert_eq!(api.calls.len(), 2);
    }
    #[test]
    fn partial_file_is_removed_and_incomplete_revocation_is_reported() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        for revocation_ok in [true, false] {
            let path = std::env::temp_dir().join(format!(
                ".env.pax-partial-{}-{revocation_ok}",
                std::process::id()
            ));
            let mut api = super::super::fake::Fake::new([
                Ok(json!({"Account":scope.account})),
                Ok(json!({"AccessKeyMetadata":[]})),
                Ok(json!({"AccessKey":{"AccessKeyId":"test-key","SecretAccessKey":"test-secret"}})),
                if revocation_ok {
                    Ok(json!({}))
                } else {
                    Err(Error::Aws {
                        service: "iam".into(),
                        operation: "delete-access-key".into(),
                        code: "AccessDenied".into(),
                    })
                },
            ]);
            let error = deliver_with(&mut api, &scope, &path, |path, _, _, _| {
                std::fs::write(path, b"partial file").unwrap();
                Err((std::io::ErrorKind::BrokenPipe.into(), true))
            })
            .unwrap_err();
            assert_eq!(matches!(error, Error::Cleanup { .. }), !revocation_ok);
            assert!(!path.exists());
            assert_eq!(api.calls.last().unwrap().1, "delete-access-key");
            assert!(!error.to_string().contains("test-secret"));
        }
    }
    #[test]
    fn a_racing_existing_file_is_preserved_and_the_new_key_revoked() {
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        let path = std::env::temp_dir().join(format!(".env.pax-race-{}", std::process::id()));
        let mut api = super::super::fake::Fake::new([
            Ok(json!({"Account":scope.account})),
            Ok(json!({"AccessKeyMetadata":[]})),
            Ok(json!({"AccessKey":{"AccessKeyId":"test-key","SecretAccessKey":"test-secret"}})),
            Ok(json!({})),
        ]);
        assert!(
            deliver_with(&mut api, &scope, &path, |path, access, secret, scope| {
                std::fs::write(path, b"existing file").unwrap();
                persist(path, access, secret, scope)
            })
            .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"existing file");
        std::fs::remove_file(path).unwrap();
        assert_eq!(api.calls.last().unwrap().1, "delete-access-key");
    }
}
