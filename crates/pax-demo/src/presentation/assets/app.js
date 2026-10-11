import {
    alignClock,
    networkEstimate,
    FirstObservation,
    SampleWindow,
} from "/timing.mjs";
import { LatencyHistory } from "/history.mjs";
import { $, chart } from "/elements.mjs";
import { books } from "/book.mjs";
import { Poller, Freshness } from "/polling.mjs";
import { createPrimary } from "/primary.mjs";
import { createDetails } from "/details.mjs";
import { renderDiagnostics } from "/diagnostics.mjs";
import { renderRecovery } from "/recovery.mjs";
const isLatency = document.body.dataset.page === "latency";
let packet,
    browser,
    clock,
    paused = false,
    lastUpdate,
    lastSession,
    lastApplied,
    coalesced = 0,
    skipped = 0,
    receivedAt = 0,
    failed = false;
let alignmentEpoch = 0,
    visibilityEpoch = 0;
function invalidateClock() {
    clock = null;
    alignmentEpoch++;
}
const observation = new FirstObservation();
const browserSampling = new SampleWindow();
const latencyHistory = new LatencyHistory();
const renderIngress = createPrimary(latencyHistory, () => browser);
const { renderRust, renderReport, renderBrowser, renderOverall, alignment } =
    createDetails(
        () => ({
            packet,
            browser,
            clock,
            receivedAt,
            browserSampling,
            coalesced,
            skipped,
            paused,
        }),
        observation,
    );
const freshness = new Freshness();
let browserPollMs = 250,
    browserCompleted = 0,
    diagnosticStatus = null,
    diagnosticPending = false;
function diagnosticHealth() {
    if (paused || document.hidden) return;
    const report = packet?.state;
    const status = freshness.status(
        performance.now(),
        report?.view.mode === "live",
        Math.max(3000, 3 * (diagnosticStatus?.settings?.publication_ms ?? 250)),
    );
    const reports = diagnosticStatus?.reports,
        transport = diagnosticStatus?.transport;
    const workerStale =
        reports &&
        (reports.failed ||
            !reports.alive ||
            reports.publication_age_ns >
                Math.max(
                    3000,
                    3 * (diagnosticStatus?.settings?.publication_ms ?? 250),
                ) *
                    1e6);
    const disconnected =
        report?.view.mode === "live" &&
        (!report.transport_connected ||
            (transport &&
                (!transport.connected ||
                    report.view.markets.some(
                        (m) => m.generation !== transport.generation,
                    ))));
    const stale = status.stale || workerStale || disconnected;
    $("display-freshness").textContent = stale
        ? "Display needs a fresh update"
        : `Display refreshed ${status.age == null ? "—" : (status.age / 1000).toFixed(1) + "s"} ago`;
    $("display-freshness").classList.toggle("warning", Boolean(stale));
    if (stale) {
        $("source").textContent = "STALE DISPLAY";
        $("health").textContent = disconnected
            ? "Feed disconnected; displayed books are not current."
            : workerStale
              ? "Report worker stopped or publication is stale."
              : status.reason;
    } else if (report) {
        $("source").textContent = report.view.mode;
        $("health").textContent =
            `${report.transport_connected ? "Feed connected" : "Replay"} · ${report.view.applied} book updates${report.view.rejected ? ` · ${report.view.rejected} rejected` : ""}${report.capture_error ? " · capture failed" : ""}`;
    }
}
async function refreshDiagnostics() {
    if (document.hidden || diagnosticPending) return;
    diagnosticPending = true;
    try {
        const response = await fetch("/api/diagnostics", {
            cache: "no-store",
            signal: AbortSignal.timeout(3000),
        });
        if (!response.ok) throw new Error("diagnostic status unavailable");
        const status = await response.json();
        diagnosticStatus = status;
        if (status.display) {
            browserPollMs = status.display.poll_ms;
            if (browserSampling.configure(status.display.sampling)) {
                browser = null;
                browserCompleted = 0;
                invalidateClock();
                observation.clear();
                if (browserSampling.rate.every) synchronize();
            }
        }
        renderDiagnostics(
            diagnosticStatus,
            packet,
            browserSampling,
            browserCompleted,
            browserPollMs,
        );
        diagnosticHealth();
    } catch {
        if ($("measurement-health"))
            $("measurement-health").textContent =
                "Independent diagnostic status unavailable; report/file health cannot be confirmed.";
    } finally {
        diagnosticPending = false;
    }
}
function render(report) {
    $("source").textContent = report.view.mode;
    $("health").textContent =
        `${report.transport_connected ? "Transport connected" : report.transport_error || "Transport offline"} · ${report.view.messages} messages · ${report.view.applied} commits · ${report.view.rejected} rejected${report.view.last_error ? ` · last recovery: ${report.view.last_error}` : ""}${report.capture_error ? ` · capture failed: ${report.capture_error}` : ""}`;
    renderRecovery(report);
    renderIngress(report, true);
    if (isLatency) {
        if ($("sdk-details").open) renderRust(report);
        if ($("presentation-details").open) {
            renderReport(report);
            renderBrowser();
        }
        renderOverall(report, $("presentation-details").open);
        alignment();
    } else books(report);
}
async function synchronize() {
    if (!browserSampling.rate.every) {
        invalidateClock();
        alignment();
        return;
    }
    if (document.hidden) {
        invalidateClock();
        return;
    }
    const epoch = ++alignmentEpoch;
    let best = null;
    try {
        for (let i = 0; i < 3; i++) {
            const start = performance.now();
            const response = await fetch("/api/clock", {
                cache: "no-store",
                signal: AbortSignal.timeout(3000),
            });
            const text = await response.text();
            const end = performance.now();
            if (!response.ok) throw new Error("clock request failed");
            const candidate = alignClock(JSON.parse(text), start, end);
            if (
                candidate &&
                (!lastSession || candidate.session === lastSession) &&
                (!best || candidate.uncertainty < best.uncertainty)
            )
                best = candidate;
        }
    } catch {
        best = null;
    }
    if (epoch !== alignmentEpoch || document.hidden) return;
    clock = best;
    alignment();
}
const poller = new Poller({
    interval: () => browserPollMs,
    load: async (signal) => {
        const sampled = browserSampling.observe();
        const started = sampled ? performance.now() : null;
        const response = await fetch("/api/state", {
            cache: "no-store",
            signal: AbortSignal.any([signal, AbortSignal.timeout(4000)]),
        });
        const text = await response.text();
        const received = sampled ? performance.now() : null;
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return { text, sampled, started, received };
    },
    receive: ({ text, sampled, started, received }) => {
        if (sampled) {
            performance.clearMarks("pax-received");
            performance.clearMarks("pax-dom");
            performance.clearMarks("pax-frame-opportunity");
            performance.mark("pax-received");
        }
        const parseStart = sampled ? performance.now() : null;
        const data = JSON.parse(text);
        const parsed = sampled ? performance.now() : null;
        const report = data.state;
        if (report.session !== lastSession) {
            lastSession = report.session;
            lastUpdate = null;
            invalidateClock();
            observation.clear();
            browser = null;
            skipped = 0;
            coalesced = 0;
            lastApplied = null;
            synchronize();
        }
        const changed = lastUpdate !== report.update_id;
        if (changed || failed) {
            if (lastUpdate !== null) {
                const delta =
                    BigInt(report.update_id) - BigInt(lastUpdate) - 1n;
                if (delta > 0n) skipped += Number(delta);
            }
            if (lastApplied !== null) {
                const delta =
                    BigInt(report.view.applied) - BigInt(lastApplied) - 1n;
                if (delta > 0n) coalesced += Number(delta);
            }
            lastApplied = report.view.applied;
            lastUpdate = report.update_id;
            packet = data;
            receivedAt = received;
            if (!sampled) {
                browser = null;
                observation.clear();
            }
            const domStart = sampled ? performance.now() : null;
            render(report);
            const domEnd = sampled ? performance.now() : null;
            if (sampled) performance.mark("pax-dom");
            if (changed && sampled) {
                const frameEpoch = visibilityEpoch;
                requestAnimationFrame(() => {
                    if (
                        paused ||
                        document.hidden ||
                        frameEpoch !== visibilityEpoch ||
                        report.session !== lastSession ||
                        report.update_id !== lastUpdate
                    )
                        return;
                    const frame = performance.now();
                    performance.mark("pax-frame-opportunity");
                    browserCompleted++;
                    browser = {
                        update_id: report.update_id,
                        setup_ns: (parseStart - received) * 1e6,
                        dispatch_ns: (domStart - parsed) * 1e6,
                        parse_ns: (parsed - parseStart) * 1e6,
                        dom_ns: (domEnd - domStart) * 1e6,
                        frame_ns: (frame - domEnd) * 1e6,
                        request_ns: (received - started) * 1e6,
                    };
                    if (isLatency) {
                        if ($("presentation-details").open) {
                            renderReport(report);
                            renderBrowser();
                        }
                        renderOverall(report, $("presentation-details").open);
                    }
                    performance.clearMarks("pax-received");
                    performance.clearMarks("pax-dom");
                    performance.clearMarks("pax-frame-opportunity");
                });
            }
        }

        failed = false;
        freshness.observe(report.session, report.update_id, performance.now());
        diagnosticHealth();
    },
    fail: (error) => {
        failed = true;
        freshness.fail();
        invalidateClock();
        observation.clear();
        browser = null;
        if (isLatency) {
            latencyHistory.gap(performance.now());
            if (packet) renderIngress(packet.state);
        }
        diagnosticHealth();
        $("health").textContent =
            `Display disconnected: ${error.message}. Last displayed book is stale.`;
    },
});
function suspendDisplay() {
    visibilityEpoch++;
    if (document.hidden) {
        invalidateClock();
        observation.clear();
        browser = null;
    }
    if (paused || document.hidden) {
        poller.suspend();
        latencyHistory.gap(performance.now());
    } else {
        poller.resume();
        if (lastSession) synchronize();
    }
}

if (isLatency) {
    $("sdk-details").addEventListener("toggle", () => {
        if ($("sdk-details").open && packet) renderRust(packet.state);
    });
    $("presentation-details").addEventListener("toggle", () => {
        if ($("presentation-details").open && packet) {
            renderReport(packet.state);
            renderBrowser();
            renderOverall(packet.state);
            alignment();
        }
    });
    $("ingress-source").addEventListener("change", () => {
        latencyHistory.reset();
        if (packet) renderIngress(packet.state, true);
    });
    $("ingress-event").addEventListener("change", () => {
        if (packet) renderIngress(packet.state);
    });
    $("ingress-breakdown").addEventListener("change", () => {
        if (packet) renderIngress(packet.state);
    });
    $("history-range").addEventListener("change", () => {
        if (packet) renderIngress(packet.state);
    });
    $("pause").addEventListener("click", () => {
        paused = !paused;
        $("pause").textContent = paused ? "Resume display" : "Pause display";
        suspendDisplay();
        $("display-freshness").textContent = paused
            ? "Display paused"
            : "Refreshing display…";
        $("source").textContent = paused
            ? "DISPLAY PAUSED"
            : packet?.state.view.mode || "Connecting";
    });
    $("trace-select").addEventListener("change", () => {
        if (packet) renderRust(packet.state);
    });
    $("trace-select").addEventListener("blur", () => {
        if (packet) renderRust(packet.state, true);
    });
    $("zoom").addEventListener("change", () => {
        if (packet) {
            renderIngress(packet.state);
            renderRust(packet.state);
            renderBrowser();
            renderOverall(packet.state);
        }
    });
}
document.addEventListener("visibilitychange", () => {
    suspendDisplay();
    if (isLatency) {
        chart("browser", [], "Browser timing");
        $("browser-meta").textContent = document.hidden
            ? "Tab hidden; frame sampling suspended."
            : "Tab visible; waiting for a fresh frame sample.";
        alignment();
        if (packet) renderOverall(packet.state);
    }
});
setInterval(() => {
    if (!document.hidden) synchronize();
}, 15000);
window.addEventListener("resize", () => {
    if (isLatency && packet) {
        renderIngress(packet.state);
        renderRust(packet.state);
        renderBrowser();
        renderOverall(packet.state);
    }
});
suspendDisplay();
refreshDiagnostics();
setInterval(diagnosticHealth, 1000);
setInterval(refreshDiagnostics, 1000);

async function routeDiagnostic() {
    if (!isLatency || document.hidden) return;
    try {
        const response = await fetch("/api/route", {
            cache: "no-store",
            signal: AbortSignal.timeout(3000),
        });
        if (!response.ok) throw new Error("route request failed");
        const route = await response.json();
        if (!route) {
            $("network-estimate").textContent =
                "Destination network estimate unavailable: route diagnostics are off or pending.";
            return;
        }
        const estimate = networkEstimate(route, Date.now() / 1000);
        $("network-estimate").textContent = estimate
            ? `Estimated one-way network time ${estimate.one_way_ms.toFixed(2)} ms = destination RTT ${estimate.rtt_ms.toFixed(2)} ms ÷ 2, assuming symmetric paths. Probe age ${estimate.age_seconds.toFixed(0)} s. This is a route model, separate from the event timeline; it does not locate or timestamp the matching engine.`
            : "Destination network estimate unavailable: requires a completed route observed within 120 seconds.";
        $("route").textContent =
            `${route.host} → ${route.destination ?? "unresolved"} · ${route.status} · observed ${new Date(route.observed_unix_seconds * 1000).toISOString()}. ${route.method}. ` +
            route.hops
                .map(
                    (h) =>
                        `${h.number}: ${h.address ?? "*"} ${h.rtt_ms == null ? "no reply" : h.rtt_ms.toFixed(2) + " ms RTT"}`,
                )
                .join(" · ") +
            ". Hop RTTs are not additive one-way spans.";
    } catch {
        if ($("network-estimate"))
            $("network-estimate").textContent =
                "Destination network estimate unavailable: diagnostic refresh failed.";
    }
}
routeDiagnostic();
setInterval(routeDiagnostic, 15000);
