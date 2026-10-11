//! Explicit account, region and credential selection. Dotenv is data, never code.

use crate::error::Error;
use std::{collections::BTreeMap, fmt, path::Path};

/// Owned credentials. Debug output never includes their values.
#[derive(Clone)]
pub struct Credentials {
    pub(crate) access: String,
    pub(crate) secret: String,
    pub(crate) token: Option<String>,
}
impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Credentials([redacted])")
    }
}
impl Credentials {
    /// Read STS session credentials without logging the response.
    pub fn from_sts(value: &serde_json::Value) -> Result<Self, Error> {
        let value = value
            .get("Credentials")
            .ok_or(Error::Missing("Credentials"))?;
        Ok(Self {
            access: field(value, "AccessKeyId")?,
            secret: field(value, "SecretAccessKey")?,
            token: Some(field(value, "SessionToken")?),
        })
    }
}
fn field(value: &serde_json::Value, name: &'static str) -> Result<String, Error> {
    value
        .get(name)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or(Error::Missing(name))
}

/// Isolated credential provider selection for the AWS subprocess.
#[derive(Debug, Clone)]
pub enum Identity {
    /// Explicit key or STS session credentials.
    Keys(Credentials),
    /// Explicit named AWS profile. Inherited access keys are removed.
    Profile(String),
    /// Standard AWS resolution, only for normal operator role assumption.
    Operator,
}

/// Validated non-secret deployment scope.
#[derive(Debug, Clone)]
pub struct Scope {
    /// Expected twelve-digit account ID, checked before mutation.
    pub account: String,
    /// AWS region.
    pub region: String,
    /// Owned stack name and naming prefix.
    pub stack: String,
}
impl Scope {
    /// Reject ambiguous account, region and resource names.
    pub fn new(account: String, region: String, stack: String) -> Result<Self, Error> {
        if account.len() != 12 || !account.bytes().all(|b| b.is_ascii_digit()) {
            return Err(Error::Config("account must contain twelve digits".into()));
        }
        if !region.starts_with("ap-northeast-")
            || !region
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(Error::Config(
                "an explicit ap-northeast region is required".into(),
            ));
        }
        if !stack.starts_with("pax-")
            || stack.len() > 48
            || !stack
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(Error::Config(
                "stack must be a pax- prefixed ASCII name of at most 48 characters".into(),
            ));
        }
        Ok(Self {
            account,
            region,
            stack,
        })
    }
    /// ARN of the fixed project runtime permissions boundary.
    pub fn boundary_arn(&self) -> String {
        format!(
            "arn:aws:iam::{}:policy/pax-access/pax-workload-boundary",
            self.account
        )
    }
    /// ARN of the fixed project deployer role.
    pub fn deployer_arn(&self) -> String {
        format!("arn:aws:iam::{}:role/pax-access/pax-deployer", self.account)
    }
}

/// Parse the supported dotenv subset without expansion or command evaluation.
/// Values may be unquoted or fully quoted; malformed/duplicate keys are errors.
pub fn dotenv(text: &str) -> Result<BTreeMap<String, String>, Error> {
    let mut values = BTreeMap::new();
    for (line_number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((key, value)) = line.split_once('=') else {
            return Err(Error::Config(format!(
                "dotenv line {} requires KEY=VALUE",
                line_number + 1
            )));
        };
        let key = key.trim();
        if key.is_empty()
            || !key
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err(Error::Config(format!(
                "invalid dotenv key on line {}",
                line_number + 1
            )));
        }
        let value = value.trim();
        let value = if value.starts_with(['\'', '"']) {
            let quote = value.as_bytes()[0];
            if value.len() < 2 || value.as_bytes().last() != Some(&quote) {
                return Err(Error::Config(format!(
                    "unterminated dotenv value on line {}",
                    line_number + 1
                )));
            }
            &value[1..value.len() - 1]
        } else {
            value
        };
        if values.insert(key.to_owned(), value.to_owned()).is_some() {
            return Err(Error::Config(format!(
                "duplicate dotenv key on line {}",
                line_number + 1
            )));
        }
    }
    Ok(values)
}

/// Load explicit bootstrap credentials and optional bootstrap role from a file.
/// Missing, partial or empty credential pairs are errors; no provider fallback occurs.
pub fn admin_file(path: &Path) -> Result<(Identity, Option<String>), Error> {
    let values = private_dotenv(path)?;
    let get = |name: &str| values.get(name).filter(|v| !v.is_empty()).cloned();
    let access = get("AWS_ADMIN_ACCESS_KEY_ID")
        .ok_or_else(|| Error::Config("AWS_ADMIN_ACCESS_KEY_ID is required".into()))?;
    let secret = get("AWS_ADMIN_SECRET_ACCESS_KEY")
        .ok_or_else(|| Error::Config("AWS_ADMIN_SECRET_ACCESS_KEY is required".into()))?;
    Ok((
        Identity::Keys(Credentials {
            access,
            secret,
            token: get("AWS_ADMIN_SESSION_TOKEN"),
        }),
        get("AWS_ADMIN_ROLE_ARN"),
    ))
}
/// Select a private normal-operator file. Bootstrap variable names are not accepted.
pub fn operator_file(path: &Path) -> Result<(Identity, Option<String>), Error> {
    let values = private_dotenv(path)?;
    if values.keys().any(|key| key.starts_with("AWS_ADMIN_")) {
        return Err(Error::Config(
            "bootstrap credentials cannot be used as an operator file".into(),
        ));
    }
    let get = |name: &str| values.get(name).filter(|v| !v.is_empty()).cloned();
    let access = get("AWS_ACCESS_KEY_ID")
        .ok_or_else(|| Error::Config("AWS_ACCESS_KEY_ID is required".into()))?;
    let secret = get("AWS_SECRET_ACCESS_KEY")
        .ok_or_else(|| Error::Config("AWS_SECRET_ACCESS_KEY is required".into()))?;
    Ok((
        Identity::Keys(Credentials {
            access,
            secret,
            token: get("AWS_SESSION_TOKEN"),
        }),
        get("PAX_AWS_DEPLOYER_ROLE_ARN"),
    ))
}
fn private_dotenv(path: &Path) -> Result<BTreeMap<String, String>, Error> {
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() > 65536 {
        return Err(Error::Config(
            "credential file must be a regular file of at most 64 KiB".into(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::Config(
                "bootstrap credential file must be private (0600)".into(),
            ));
        }
    }
    dotenv(&std::fs::read_to_string(path)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dotenv_is_literal_and_rejects_ambiguity() {
        let parsed = dotenv("A='$(touch nope)'\nB=\"`id`\"\n# comment\nexport C=value").unwrap();
        assert_eq!(parsed["A"], "$(touch nope)");
        assert_eq!(parsed["B"], "`id`");
        assert!(dotenv("A=a\nA=b").is_err());
        assert!(dotenv("A='unterminated").is_err());
        assert!(dotenv("invalid-key=value").is_err());
    }
    #[test]
    fn credentials_are_redacted_and_sts_tokens_required() {
        let value = serde_json::json!({"Credentials":{"AccessKeyId":"sensitive-access", "SecretAccessKey":"sensitive-secret", "SessionToken":"sensitive-token"}});
        let credentials = Credentials::from_sts(&value).unwrap();
        assert_eq!(format!("{credentials:?}"), "Credentials([redacted])");
        assert!(Credentials::from_sts(&serde_json::json!({})).is_err());
    }
    #[test]
    fn scope_cannot_escape_names() {
        assert!(
            Scope::new(
                "123456789012".into(),
                "ap-northeast-1".into(),
                "pax-tokyo".into()
            )
            .is_ok()
        );
        assert!(Scope::new("wrong".into(), "ap-northeast-1".into(), "pax-tokyo".into()).is_err());
        assert!(Scope::new("123456789012".into(), "us-east-1".into(), "other".into()).is_err());
    }
    #[cfg(unix)]
    #[test]
    fn private_provider_requires_complete_keys_and_rejects_admin_fallback() {
        use std::os::unix::fs::PermissionsExt;
        let path = std::env::temp_dir().join(format!(".env.pax-providers-{}", std::process::id()));
        std::fs::write(
            &path,
            "AWS_ADMIN_ACCESS_KEY_ID=test-access\nAWS_ADMIN_SESSION_TOKEN=test-token",
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(admin_file(&path).is_err());
        std::fs::write(&path,"AWS_ADMIN_ACCESS_KEY_ID=test-access\nAWS_ADMIN_SECRET_ACCESS_KEY=test-secret\nAWS_ADMIN_SESSION_TOKEN=test-token").unwrap();
        let (Identity::Keys(keys), _) = admin_file(&path).unwrap() else {
            panic!("explicit keys required")
        };
        assert_eq!(keys.token.as_deref(), Some("test-token"));
        assert!(operator_file(&path).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(admin_file(&path).is_err());
        std::fs::remove_file(path).unwrap();
    }
}
