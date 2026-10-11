//! Bounded JSON boundary to AWS CLI v2; credentials only enter the child environment.
pub mod access;
pub mod credentials;
mod host;
pub mod presentation;
pub mod release;
pub mod service;
pub mod stack;

use crate::{
    config::{Credentials, Identity, Scope},
    error::Error,
};
use serde_json::{Value, json};
use std::{
    io::Read,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

/// Replaceable API boundary. Implementations must not log request/response payloads.
pub trait Api {
    /// Execute one AWS JSON operation. Mutation retries belong to the owning lifecycle.
    fn call(&mut self, service: &str, operation: &str, input: Value) -> Result<Value, Error>;
    /// Transfer one artifact file. Fakes may override this without touching a network.
    fn upload(
        &mut self,
        _bucket: &str,
        _key: &str,
        _file: &std::path::Path,
    ) -> Result<Value, Error> {
        Err(Error::Config(
            "artifact upload is not implemented by this API boundary".into(),
        ))
    }
}

const TRANSPORT_CODES: &[&str] = &[
    "ReadTimeout",
    "ConnectTimeout",
    "EndpointConnection",
    "ConnectionClosed",
];

/// Retry at most three identical read-only queries on throttling or recognized
/// socket/connection failure. Hard process deadlines and unknown errors fail;
/// ambiguous mutations and credential issuance remain the owner's responsibility.
pub fn query(
    api: &mut impl Api,
    service: &str,
    operation: &str,
    input: Value,
) -> Result<Value, Error> {
    if (service == "sts" && matches!(operation, "get-session-token" | "get-federation-token"))
        || (!operation.starts_with("get-")
            && !operation.starts_with("describe-")
            && !operation.starts_with("list-"))
    {
        return Err(Error::Config(
            "query retries are restricted to read-only operations".into(),
        ));
    }
    for attempt in 0..3 {
        match api.call(service, operation, input.clone()) {
            Err(error)
                if attempt < 2
                    && ["Throttling", "ThrottlingException", "RequestLimitExceeded"]
                        .iter()
                        .chain(TRANSPORT_CODES)
                        .any(|code| error.aws_code(code)) =>
            {
                std::thread::sleep(Duration::from_millis(10 << attempt))
            }
            result => return result,
        }
    }
    unreachable!("last attempt returns")
}

/// AWS CLI v2 adapter with explicit region/credential selection and bounded output.
pub struct Cli {
    executable: std::path::PathBuf,
    region: String,
    identity: Identity,
    timeout: Duration,
}
impl Cli {
    /// Select an isolated child identity; the process-wide environment is unchanged.
    pub fn new(region: String, identity: Identity) -> Self {
        Self {
            executable: "aws".into(),
            region,
            identity,
            timeout: Duration::from_secs(60),
        }
    }
    /// Set a positive operation deadline, including bounded connection/read timeouts.
    pub fn with_timeout(mut self, timeout: Duration) -> Result<Self, Error> {
        if timeout.is_zero() {
            return Err(Error::Config("AWS timeout must be positive".into()));
        }
        self.timeout = timeout;
        Ok(self)
    }
    /// Verify STS account before an owning operation may mutate resources.
    pub fn verify_account(&mut self, scope: &Scope) -> Result<Value, Error> {
        verify_account(self, scope)
    }
    /// Assume an explicit role, then replace the child credential provider.
    pub fn assume(&mut self, role: &str) -> Result<(), Error> {
        let response = self.call(
            "sts",
            "assume-role",
            json!({"RoleArn":role,"RoleSessionName":"pax-ops","DurationSeconds":3600}),
        )?;
        self.identity = Identity::Keys(Credentials::from_sts(&response)?);
        Ok(())
    }
    fn command(&self) -> Command {
        let mut cmd = Command::new(&self.executable);
        cmd.env("AWS_PAGER", "")
            .env("AWS_CLI_AUTO_PROMPT", "off")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("AWS_MAX_ATTEMPTS", "1");
        match &self.identity {
            Identity::Operator => {}
            Identity::Keys(credentials) => {
                for key in [
                    "AWS_PROFILE",
                    "AWS_DEFAULT_PROFILE",
                    "AWS_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY",
                    "AWS_SESSION_TOKEN",
                    "AWS_ROLE_ARN",
                    "AWS_WEB_IDENTITY_TOKEN_FILE",
                    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                ] {
                    cmd.env_remove(key);
                }
                cmd.env("AWS_ACCESS_KEY_ID", &credentials.access)
                    .env("AWS_SECRET_ACCESS_KEY", &credentials.secret);
                if let Some(token) = &credentials.token {
                    cmd.env("AWS_SESSION_TOKEN", token);
                }
            }
            Identity::Profile(profile) => {
                for key in [
                    "AWS_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY",
                    "AWS_SESSION_TOKEN",
                    "AWS_DEFAULT_PROFILE",
                    "AWS_ROLE_ARN",
                    "AWS_WEB_IDENTITY_TOKEN_FILE",
                    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                ] {
                    cmd.env_remove(key);
                }
                cmd.env("AWS_PROFILE", profile);
            }
        }
        cmd
    }
}

/// Check account on a fake or live boundary before mutation.
pub fn verify_account(api: &mut impl Api, scope: &Scope) -> Result<Value, Error> {
    let response = query(api, "sts", "get-caller-identity", json!({}))?;
    let observed = response
        .get("Account")
        .and_then(Value::as_str)
        .ok_or(Error::Missing("Account"))?;
    if observed != scope.account {
        return Err(Error::Account {
            expected: scope.account.clone(),
            observed: observed.into(),
        });
    }
    Ok(response)
}

fn bounded_read(mut reader: impl Read) -> Result<Vec<u8>, Error> {
    const LIMIT: usize = 4 * 1024 * 1024;
    let mut output = Vec::new();
    let mut buffer = [0u8; 8192];
    let mut exceeded = false;
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        if output.len() + count <= LIMIT && !exceeded {
            output.extend_from_slice(&buffer[..count]);
        } else {
            exceeded = true;
        }
    }
    if exceeded {
        Err(Error::OutputLimit)
    } else {
        Ok(output)
    }
}
fn error_code(stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    // CLI v2 also emits plaintext transport failures, optionally prefixed by
    // its fixed diagnostic label. Retain only a fixed category, never endpoints.
    let transport = text.trim();
    let transport = transport
        .strip_prefix("aws: [ERROR]: ")
        .unwrap_or(transport);
    for (prefix, code) in [
        ("Read timeout on endpoint URL:", "ReadTimeout"),
        ("Connect timeout on endpoint URL:", "ConnectTimeout"),
        (
            "Could not connect to the endpoint URL:",
            "EndpointConnection",
        ),
        (
            "Connection was closed before we received a valid response from endpoint URL:",
            "ConnectionClosed",
        ),
    ] {
        if transport.starts_with(prefix) {
            return code.into();
        }
    }
    let structured = serde_json::from_slice::<Value>(stderr).ok();
    if structured.as_ref().is_some_and(|value| {
        value["Code"] == "ValidationError"
            && value["Message"].as_str().is_some_and(|message| {
                message.starts_with("Stack with id ") && message.ends_with(" does not exist")
            })
    }) {
        return "StackNotFound".into();
    }
    let candidate = structured
        .as_ref()
        .and_then(|value| value["Code"].as_str())
        .or_else(|| {
            text.split_once("An error occurred (")
                .and_then(|(_, s)| s.split_once(')').map(|(s, _)| s))
        });
    const CODES: &[&str] = &[
        "AccessDenied",
        "AccessDeniedException",
        "UnauthorizedOperation",
        "NoSuchEntity",
        "EntityAlreadyExists",
        "AlreadyExists",
        "AlreadyExistsException",
        "LimitExceeded",
        "ValidationError",
        "ResourceNotFoundException",
        "InvalidParameterValue",
        "Throttling",
        "ThrottlingException",
        "RequestLimitExceeded",
        "ExpiredToken",
        "InvalidClientTokenId",
        "InsufficientInstanceCapacity",
        "MalformedPolicyDocument",
        "InvalidInput",
        "EntityTemporarilyUnmodifiable",
        "ServiceFailure",
        "DeleteConflict",
        "MissingAuthenticationToken",
        "InvalidParameterCombination",
        "InsufficientCapabilitiesException",
        "InvocationDoesNotExist",
        "ChangeSetNotFound",
    ];
    candidate
        .filter(|s| CODES.contains(s))
        .unwrap_or("ChildFailure")
        .into()
}
impl Cli {
    fn execute(
        &mut self,
        service: &str,
        operation: &str,
        input: Value,
        file: Option<&std::path::Path>,
    ) -> Result<Value, Error> {
        let mut command = self.command();
        // Keep credential-provider descendants inside a killable process group.
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        command
            .args([
                "--region",
                if service == "pricing" {
                    "us-east-1"
                } else {
                    &self.region
                },
                "--output",
                "json",
                "--no-cli-pager",
                "--cli-error-format",
                "json",
                "--cli-connect-timeout",
                "10",
                "--cli-read-timeout",
                "30",
                service,
                operation,
                "--cli-input-json",
            ])
            .arg(serde_json::to_string(&input)?)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(file) = file {
            command.arg("--body").arg(file);
        }
        let mut child = command.spawn()?;
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let out = std::thread::spawn(move || bounded_read(stdout));
        let err = std::thread::spawn(move || bounded_read(stderr));
        let start = Instant::now();
        let mut status = None;
        loop {
            if status.is_none() {
                status = child.try_wait()?;
            }
            if status.is_some() && out.is_finished() && err.is_finished() {
                break;
            }
            if start.elapsed() >= self.timeout {
                #[cfg(unix)]
                {
                    use nix::{
                        sys::signal::{Signal, killpg},
                        unistd::Pid,
                    };
                    let _ = killpg(Pid::from_raw(child.id() as i32), Signal::SIGKILL);
                }
                let _ = child.kill();
                let _ = child.wait();
                return Err(Error::Timeout {
                    service: service.into(),
                    operation: operation.into(),
                });
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let stdout = out.join().map_err(|_| Error::OutputLimit)??;
        let stderr = err.join().map_err(|_| Error::OutputLimit)??;
        let status = status.expect("child and collectors finished");
        if !status.success() {
            return Err(Error::Aws {
                service: service.into(),
                operation: operation.into(),
                code: error_code(&stderr),
            });
        }
        if stdout.iter().all(u8::is_ascii_whitespace) {
            Ok(json!({}))
        } else {
            Ok(serde_json::from_slice(&stdout)?)
        }
    }
}
impl Api for Cli {
    fn call(&mut self, service: &str, operation: &str, input: Value) -> Result<Value, Error> {
        self.execute(service, operation, input, None)
    }
    fn upload(&mut self, bucket: &str, key: &str, file: &std::path::Path) -> Result<Value, Error> {
        self.execute(
            "s3api",
            "put-object",
            json!({"Bucket":bucket,"Key":key}),
            Some(file),
        )
    }
}

#[cfg(test)]
pub(crate) mod fake {
    use super::*;
    use std::collections::VecDeque;
    pub struct Fake {
        pub replies: VecDeque<Result<Value, Error>>,
        pub calls: Vec<(String, String, Value)>,
    }
    impl Fake {
        pub fn new(replies: impl IntoIterator<Item = Result<Value, Error>>) -> Self {
            Self {
                replies: replies.into_iter().collect(),
                calls: Vec::new(),
            }
        }
    }
    impl Api for Fake {
        fn call(&mut self, service: &str, operation: &str, input: Value) -> Result<Value, Error> {
            self.calls.push((service.into(), operation.into(), input));
            self.replies.pop_front().expect("unplanned fake API call")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn wrong_account_fails_before_mutation() {
        let mut api = fake::Fake::new([Ok(json!({"Account":"999999999999"}))]);
        let scope = Scope::new(
            "123456789012".into(),
            "ap-northeast-1".into(),
            "pax-test".into(),
        )
        .unwrap();
        assert!(matches!(
            verify_account(&mut api, &scope),
            Err(Error::Account { .. })
        ));
        assert_eq!(api.calls.len(), 1);
    }
    #[test]
    fn service_diagnostics_never_echo_secrets() {
        assert_eq!(
            error_code(br#"{"Code":"MalformedPolicyDocument","Message":"sensitive-secret"}"#),
            "MalformedPolicyDocument"
        );
        assert_eq!(
            error_code(b"An error occurred (AccessDenied) when calling x: secret"),
            "AccessDenied"
        );
        assert_eq!(
            error_code(b"credential sensitive-access sensitive-secret"),
            "ChildFailure"
        );
        assert_eq!(
            error_code(b"An error occurred (sensitive-secret) when calling x"),
            "ChildFailure"
        );
    }
    #[test]
    fn transport_categories_accept_cli_prefix_without_echoing_endpoints() {
        for (message, code) in [
            ("Read timeout on endpoint URL:", "ReadTimeout"),
            ("Connect timeout on endpoint URL:", "ConnectTimeout"),
            (
                "Could not connect to the endpoint URL:",
                "EndpointConnection",
            ),
            (
                "Connection was closed before we received a valid response from endpoint URL:",
                "ConnectionClosed",
            ),
        ] {
            for prefix in ["", "\naws: [ERROR]: "] {
                let stderr = format!("{prefix}{message} sensitive-endpoint-and-secret\n");
                assert_eq!(error_code(stderr.as_bytes()), code);
            }
        }
        for stderr in [
            "credential failure containing Read timeout on endpoint URL: secret",
            "aws: [ERROR]: SSL validation failed: secret",
            r#"{"Code":"AccessDenied","Message":"Read timeout on endpoint URL: secret"}"#,
        ] {
            assert_eq!(
                error_code(stderr.as_bytes()),
                if stderr.starts_with('{') {
                    "AccessDenied"
                } else {
                    "ChildFailure"
                }
            );
        }
    }
    #[test]
    fn transient_queries_preserve_request_identity_and_exhaust_three_attempts() {
        for code in TRANSPORT_CODES {
            let failure = || Error::Aws {
                service: "cloudformation".into(),
                operation: "describe-stacks".into(),
                code: (*code).into(),
            };
            let request = json!({"StackName":"pax-test"});
            let mut api = fake::Fake::new([Err(failure()), Err(failure()), Ok(json!({}))]);
            query(
                &mut api,
                "cloudformation",
                "describe-stacks",
                request.clone(),
            )
            .unwrap();
            assert_eq!(api.calls.len(), 3);
            assert!(api.calls.iter().all(|call| {
                call == &(
                    "cloudformation".into(),
                    "describe-stacks".into(),
                    request.clone(),
                )
            }));
            let mut api = fake::Fake::new([Err(failure()), Err(failure()), Err(failure())]);
            assert!(
                query(&mut api, "cloudformation", "describe-stacks", request)
                    .unwrap_err()
                    .aws_code(code)
            );
            assert_eq!(api.calls.len(), 3);
        }
    }
    #[test]
    fn fatal_queries_and_credential_or_mutation_requests_are_not_retried() {
        for code in ["AccessDenied", "ExpiredToken", "ChildFailure"] {
            let mut api = fake::Fake::new([Err(Error::Aws {
                service: "ec2".into(),
                operation: "describe-instances".into(),
                code: code.into(),
            })]);
            assert!(query(&mut api, "ec2", "describe-instances", json!({})).is_err());
            assert_eq!(api.calls.len(), 1);
        }
        for (service, operation) in [
            ("ssm", "send-command"),
            ("sts", "get-session-token"),
            ("sts", "get-federation-token"),
            ("sts", "assume-role"),
        ] {
            let mut api = fake::Fake::new([]);
            assert!(query(&mut api, service, operation, json!({})).is_err());
            assert!(api.calls.is_empty());
        }
    }
    #[cfg(unix)]
    #[test]
    fn actual_child_transport_retries_only_through_query() {
        use std::os::unix::fs::PermissionsExt;
        let path = std::env::temp_dir().join(format!("pax-aws-query-{}", std::process::id()));
        let count = path.with_extension("count");
        // Fixed fixture diagnostics contain a secret marker which must never
        // cross the public error boundary. The stub ignores all AWS arguments.
        std::fs::write(
            &path,
            r#"#!/bin/sh
pax_query_count=$0.count
pax_query_attempt=0
if test -f "$pax_query_count"; then pax_query_attempt=$(cat "$pax_query_count"); fi
pax_query_attempt=$((pax_query_attempt + 1))
printf '%s' "$pax_query_attempt" > "$pax_query_count"
if test "$pax_query_attempt" -lt 3; then
  printf '%s\n' 'aws: [ERROR]: Read timeout on endpoint URL: sensitive-secret' >&2
  exit 255
fi
printf '%s\n' '{"owned":true}'
"#,
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut cli = Cli::new("ap-northeast-1".into(), Identity::Operator);
        cli.executable = path.clone();
        let result = query(&mut cli, "ec2", "describe-instances", json!({}));
        let attempts = std::fs::read_to_string(&count).unwrap();
        std::fs::remove_file(&count).unwrap();
        let mutation = cli.call("ssm", "send-command", json!({}));
        let mutation_attempts = std::fs::read_to_string(&count).unwrap();
        std::fs::remove_file(&count).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(result.unwrap(), json!({"owned":true}));
        assert_eq!(attempts, "3");
        assert_eq!(mutation_attempts, "1");
        let error = mutation.unwrap_err();
        assert!(error.aws_code("ReadTimeout"));
        assert!(!error.to_string().contains("sensitive-secret"));
    }
    #[test]
    fn only_throttled_queries_are_retried() {
        let mut api = fake::Fake::new([
            Err(Error::Aws {
                service: "ec2".into(),
                operation: "describe-images".into(),
                code: "Throttling".into(),
            }),
            Ok(json!({"Images":[]})),
        ]);
        query(&mut api, "ec2", "describe-images", json!({})).unwrap();
        assert_eq!(api.calls.len(), 2);
        assert!(query(&mut api, "ec2", "run-instances", json!({})).is_err());
        assert_eq!(api.calls.len(), 2);
        let mut api = fake::Fake::new([Err(Error::Timeout {
            service: "ec2".into(),
            operation: "describe-images".into(),
        })]);
        assert!(query(&mut api, "ec2", "describe-images", json!({})).is_err());
        assert_eq!(api.calls.len(), 1);
    }
    #[cfg(unix)]
    #[test]
    fn timeout_kills_child_and_inherited_pipe_holders() {
        use std::os::unix::fs::PermissionsExt;
        let path = std::env::temp_dir().join(format!("pax-aws-timeout-{}", std::process::id()));
        std::fs::write(&path, "#!/bin/sh\nsleep 30\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut cli = Cli::new("ap-northeast-1".into(), Identity::Operator)
            .with_timeout(Duration::from_millis(50))
            .unwrap();
        cli.executable = path.clone();
        let started = Instant::now();
        let result = cli.call("sts", "get-caller-identity", json!({}));
        std::fs::remove_file(path).unwrap();
        assert!(matches!(result, Err(Error::Timeout { .. })));
        assert!(started.elapsed() < Duration::from_secs(2));
    }
    #[test]
    fn sessions_only_enter_child_environment() {
        let credentials = Credentials {
            access: "access".into(),
            secret: "secret".into(),
            token: Some("session".into()),
        };
        let cli = Cli::new("ap-northeast-1".into(), Identity::Keys(credentials));
        let command = cli.command();
        let environment: std::collections::BTreeMap<_, _> = command.get_envs().collect();
        assert_eq!(
            environment[std::ffi::OsStr::new("AWS_MAX_ATTEMPTS")],
            Some(std::ffi::OsStr::new("1"))
        );
        assert_eq!(
            environment[std::ffi::OsStr::new("AWS_SESSION_TOKEN")],
            Some(std::ffi::OsStr::new("session"))
        );
        assert!(!command.get_args().any(|arg| arg == "secret"));
        assert_eq!(environment[std::ffi::OsStr::new("AWS_PROFILE")], None);
    }
}
