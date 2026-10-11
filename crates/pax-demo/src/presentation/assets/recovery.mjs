// Observation IDs are exact "generation:message" strings on the wire.
function observationId(value) {
    const match = typeof value === "string" && /^(\d+):(\d+)$/.exec(value);
    return match ? { generation: match[1], message: match[2] } : null;
}

export function recoveryStatus(report) {
    const rejection = report.view.last_rejection;
    if (!rejection) return null;
    const id = observationId(rejection.id);
    const recovered =
        id &&
        report.transport_connected &&
        report.view.connected &&
        report.view.markets.length > 0 &&
        report.view.markets.every((market) => {
            const commit = observationId(market.trace_id);
            return (
                market.status === "live" &&
                market.sequence != null &&
                commit &&
                commit.generation === market.generation &&
                (BigInt(commit.generation) > BigInt(id.generation) ||
                    (commit.generation === id.generation &&
                        BigInt(commit.message) > BigInt(id.message)))
            );
        });
    const state =
        report.view.mode !== "live"
            ? "recorded"
            : recovered
              ? "recovered"
              : "pending";
    const age = report.published_ns - rejection.observed_ns;
    const ageLabel =
        report.view.mode === "live" && Number.isFinite(age) && age >= 0
            ? ` · rejected ${Math.floor(age / 1e9)}s ago`
            : "";
    return {
        state,
        summary:
            state === "pending"
                ? "Book recovery pending · input rejected"
                : state === "recovered"
                  ? "Recovery history · book resynchronized"
                  : "Recovery history · recorded input rejection",
        detail: `${id ? `Generation ${id.generation}, message ${id.message}` : "Input ID unavailable"}${rejection.market != null ? `, market ${rejection.market}` : ""}${rejection.sequence != null ? `, sequence ${rejection.sequence}` : ""}${ageLabel} · ${rejection.reason}`,
    };
}

export function renderRecovery(report) {
    const host = document.getElementById("recovery-history");
    const status = recoveryStatus(report);
    host.hidden = !status;
    if (!status) {
        host.open = false;
        delete host.dataset.incident;
        delete host.dataset.state;
        return;
    }
    const incident = `${report.session}:${report.view.last_rejection.id}`;
    if (host.dataset.incident !== incident || host.dataset.state !== status.state)
        host.open = status.state === "pending";
    host.dataset.incident = incident;
    host.dataset.state = status.state;
    host.classList.toggle("rejection", status.state === "pending");
    document.getElementById("recovery-summary").textContent = status.summary;
    document.getElementById("last-rejection").textContent = status.detail;
}
