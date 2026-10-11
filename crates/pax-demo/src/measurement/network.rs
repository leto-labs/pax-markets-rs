//! Optional bounded route/RTT diagnostics, isolated from market-data ingestion.
use serde::Serialize;
use std::{process::Stdio, time::Duration};
use tokio::{io::AsyncReadExt, process::Command};

/// Bounded route probe method; TCP can pass firewalls that filter UDP probes.
#[derive(Clone, Copy, Debug, Default)]
pub enum Method {
    /// Traditional numeric UDP traceroute.
    #[default]
    Udp,
    /// Half-open TCP probes to the configured HTTPS endpoint's port 443.
    Tcp,
}
/// Route configuration selected locally, never from browser input.
pub struct Config {
    /// Public WebSocket endpoint whose host will be resolved.
    pub endpoint: String,
    /// Transport used for route probes.
    pub method: Method,
}

/// Probe state, including incomplete routes and missing local tooling.
#[derive(Clone, Debug, Serialize)]
pub struct Route {
    /// UTC observation time for this diagnostic.
    pub observed_unix_seconds: u64,
    /// Configured endpoint host; never supplied by a browser request.
    pub host: String,
    /// Resolved destination address, when resolution succeeded.
    pub destination: Option<String>,
    /// Probe method; round trips are neither additive nor one-way durations.
    pub method: &'static str,
    /// Complete, incomplete, timed out, or unavailable with a diagnostic.
    pub status: String,
    /// Parsed hops; unanswered probes have no RTT.
    pub hops: Vec<Hop>,
}
/// One traceroute hop's probe round trip.
#[derive(Clone, Debug, Serialize)]
pub struct Hop {
    /// TTL position.
    pub number: u8,
    /// Numeric responding address, absent for a filtered probe.
    pub address: Option<String>,
    /// RTT in milliseconds, absent when no response arrived.
    pub rtt_ms: Option<f64>,
}
/// Resolve only the configured venue host, then run a numeric UDP route probe.
/// DNS, output, hop count, probe count, and wall time are all bounded.
pub async fn probe(endpoint: &str) -> Route {
    probe_with_method(endpoint, Method::Udp).await
}
/// Run the selected bounded diagnostic, preserving incomplete/unsupported status.
pub async fn probe_with_method(endpoint: &str, method: Method) -> Route {
    let host = endpoint
        .parse::<axum::http::Uri>()
        .ok()
        .and_then(|uri| uri.host().map(str::to_owned))
        .unwrap_or_default();
    let host = host.trim_matches(['[', ']']).to_owned();
    let mut route = Route {
        observed_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        host,
        destination: None,
        method: match method {
            Method::Udp => "UDP traceroute; one probe per hop; numeric replies",
            Method::Tcp => "TCP/443 traceroute; one probe per hop; numeric replies",
        },
        status: "unavailable".into(),
        hops: Vec::new(),
    };
    if route.host.is_empty() {
        route.status = "unavailable: invalid endpoint host".into();
        return route;
    }
    let address = {
        let addresses = tokio::time::timeout(
            Duration::from_secs(3),
            tokio::net::lookup_host((route.host.as_str(), 443)),
        )
        .await;
        match addresses {
            Ok(Ok(mut values)) => values.next().map(|value| value.ip()),
            _ => None,
        }
    };
    let Some(address) = address else {
        route.status = "unavailable: DNS failed or timed out".into();
        return route;
    };
    route.destination = Some(address.to_string());
    let mut command = Command::new("traceroute");
    command.args(["-n", "-q", "1", "-m", "24", "-w", "1"]);
    if matches!(method, Method::Tcp) {
        command.args(["-T", "-p", "443"]);
    }
    if address.is_ipv6() {
        command.arg("-6");
    }
    command.arg(address.to_string());
    match collect(command, Duration::from_secs(15)).await {
        Ok((success, text)) => {
            route.hops = parse(&text);
            route.status = if success
                && !text.contains('!')
                && route
                    .hops
                    .last()
                    .is_some_and(|h| h.address.as_deref() == route.destination.as_deref())
            {
                "complete"
            } else {
                "incomplete or filtered"
            }
            .into();
        }
        Err(error) => route.status = error,
    }
    route
}
async fn collect(mut command: Command, timeout: Duration) -> Result<(bool, String), String> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| format!("unavailable: {error}"))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or("unavailable: no probe output")?
        .take(16 * 1024 + 1);
    let mut output = Vec::new();
    let mut stderr = child
        .stderr
        .take()
        .ok_or("unavailable: no probe diagnostics")?
        .take(4097);
    let mut diagnostics = Vec::new();
    let work = async {
        tokio::try_join!(
            stdout.read_to_end(&mut output),
            stderr.read_to_end(&mut diagnostics)
        )
        .map_err(|e| e.to_string())?;
        if output.len() > 16 * 1024 || diagnostics.len() > 4096 {
            return Err("unavailable: probe output exceeded limit".into());
        }
        let status = child.wait().await.map_err(|e| e.to_string())?;
        if !status.success() {
            let reason: String = String::from_utf8_lossy(&diagnostics)
                .chars()
                .take(512)
                .collect();
            return Err(format!(
                "unavailable: probe exited {status}: {}",
                reason.trim()
            ));
        }
        Ok((
            status.success(),
            String::from_utf8_lossy(&output).into_owned(),
        ))
    };
    tokio::time::timeout(timeout, work)
        .await
        .map_err(|_| "timed out".to_owned())?
}
/// Parse numeric Linux traceroute output without treating missing replies as zero RTT.
pub fn parse(text: &str) -> Vec<Hop> {
    text.lines()
        .take(64)
        .filter_map(|line| {
            let parts: Vec<_> = line.split_whitespace().take(16).collect();
            let number: u8 = parts.first()?.parse().ok()?;
            if number == 0 || number > 24 {
                return None;
            }
            let address = parts
                .iter()
                .skip(1)
                .find_map(|p| p.parse::<std::net::IpAddr>().ok())
                .map(|ip| ip.to_string());
            let rtt_ms = parts.windows(2).find_map(|pair| {
                if pair[1] == "ms" {
                    pair[0]
                        .parse::<f64>()
                        .ok()
                        .filter(|v| v.is_finite() && *v >= 0.0)
                } else {
                    None
                }
            });
            Some(Hop {
                number,
                address,
                rtt_ms,
            })
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filtered_and_partial_route_keeps_unknown_hops() {
        let hops =
            parse("traceroute to 192.0.2.2\n 1 192.0.2.1 0.24 ms\n 2 *\n 3 192.0.2.2 12.1 ms !H\n");
        assert_eq!(hops.len(), 3);
        assert_eq!(hops[0].rtt_ms, Some(0.24));
        assert_eq!(hops[1].rtt_ms, None);
        assert_eq!(hops[1].address, None);
    }
    #[tokio::test]
    async fn missing_tool_is_a_diagnostic_not_a_panic() {
        assert!(
            collect(
                Command::new("/nonexistent/pax-route-tool"),
                Duration::from_millis(1)
            )
            .await
            .unwrap_err()
            .contains("unavailable")
        );
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn probe_process_timeout_is_bounded() {
        let mut command = Command::new("sleep");
        command.arg("5");
        assert_eq!(
            collect(command, Duration::from_millis(10))
                .await
                .unwrap_err(),
            "timed out"
        );
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn tool_failure_preserves_bounded_permission_diagnostic() {
        let mut command = Command::new("sh");
        command.args(["-c", "echo 'TCP probes require permission' >&2; exit 1"]);
        let error = collect(command, Duration::from_secs(1)).await.unwrap_err();
        assert!(error.contains("TCP probes require permission"));
    }
    #[test]
    fn hop_limit_and_nonfinite_rtts_are_enforced() {
        let hops =
            parse("0 192.0.2.1 1 ms\n24 192.0.2.2 12 ms\n25 192.0.2.3 13 ms\n2 192.0.2.4 NaN ms\n");
        assert_eq!(hops.len(), 2);
        assert_eq!(hops[0].number, 24);
        assert_eq!(hops[1].rtt_ms, None);
    }
}
