import { duration, rateLabel } from "/timing.mjs";
import { $, escape } from "/elements.mjs";
export function renderDiagnostics(
    diagnosticStatus,
    packet,
    browserSampling,
    browserCompleted,
    browserPollMs,
) {
    if (document.body.dataset.page !== "latency" || !diagnosticStatus) return;
    const s = diagnosticStatus,
        c = s.capabilities,
        r = s.reports,
        q = s.receive_queue,
        w = s.sidecar,
        d = packet?.state.view.latency.diagnostics;
    $("measurement-health").textContent =
        `Build: receive timestamps ${c.client.receive_timestamps ? "available" : "omitted"}, receive stages ${c.client.receive_stages ? "available" : "omitted"}, clock models ${c.client.clock_model_details === true ? "available" : c.client.clock_model_details === false ? "omitted" : c.client.phc_background ? "available in legacy worker build" : "unreported (legacy)"}, delivery ${c.client.delivery_traces ? "available" : "omitted"}, processing ${c.processing_traces ? "available" : "omitted"}, export ${c.telemetry_export ? "available" : "omitted"}. ` +
        (r
            ? `Report ${r.mode}: ${r.failed ? "FAILED" : r.alive ? "alive" : "stopped"}, age ${r.publication_age_ns == null ? "no publication" : duration(r.publication_age_ns)}, skipped ${r.skipped}. `
            : "No live report worker. ") +
        (q
            ? `Receive queue ${q.pending}/${q.capacity}, full drops ${q.full_drops}, closed drops ${q.closed_drops}, consumer ${q.consumer_alive ? "present" : "gone"}. `
            : "Receive queue off. ") +
        (w
            ? `File writer ${w.failed ? "FAILED" : w.worker_alive ? "alive" : "stopped"}; selected ${w.selected}/${w.eligible}, buffered writes ${w.records}, last buffered age ${w.last_buffered_age_ns == null ? "none" : duration(w.last_buffered_age_ns)}, drops ${w.queue_drops + w.byte_drops + w.source_drops}, finalized complete ${w.complete}. `
            : "File export off. ") +
        (d
            ? `Receive worker handoff ${d.last_handoff_ns == null ? "unavailable" : duration(d.last_handoff_ns)}, source-observation age ${d.observation_age_ns == null ? "unavailable" : duration(d.observation_age_ns)} at last publication; ${d.window_evictions} window evictions, ${d.invalid_clock} invalid-clock samples. `
            : "") +
        `Browser completed ${browserCompleted}, selected ${browserSampling.selected}/${browserSampling.eligible} visible fetch attempts. Counters are read independently of report/file output.`;
    const rows = [];
    const processing = packet?.state.view.latency.processing;
    if (processing)
        for (const name of [
            "callback",
            "decode",
            "preflight",
            "mutation",
            "validation",
            "bookkeeping",
        ]) {
            const rate = ["preflight", "mutation", "validation"].includes(name)
                ? processing.policy.book[name]
                : processing.policy[name];
            const count = processing[name];
            rows.push([
                name,
                rateLabel(rate),
                `${count?.selected ?? 0}/${count?.eligible ?? 0} messages · revision ${processing.revision}`,
            ]);
        }
    if (s.policy) {
        const observed = packet?.state.view.latency.receive?.at(-1);
        for (const [name, rate] of Object.entries(s.policy.receive || {})) {
            const count =
                observed?.revision === s.policy.revision
                    ? observed.selection?.[name]
                    : null;
            rows.push([
                `receive ${name}`,
                rateLabel(rate),
                count
                    ? `${count.selected}/${count.eligible} at sample ${observed.id}; may lag after loss`
                    : `operation counts unavailable · revision ${s.policy.revision}`,
            ]);
        }
        rows.push([
            "delivery",
            s.policy.delivery_every ? `1/${s.policy.delivery_every}` : "off",
            "complete messages",
        ]);
    }
    rows.push([
        "browser",
        rateLabel(browserSampling.rate),
        `${browserPollMs} ms polling`,
    ]);
    if (s.settings)
        rows.push(
            [
                "publication",
                `${s.settings.publication_ms} ms`,
                "independent of collection",
            ],
            [
                "file export",
                w ? rateLabel(s.settings.sidecar) : "off",
                `${s.settings.export_poll_ms} ms worker idle polling`,
            ],
            [
                "route",
                s.display.route_seconds
                    ? `${s.display.route_seconds} s`
                    : "off",
                "destination RTT estimate",
            ],
        );
    $("measurement-policy").innerHTML = rows
        .map(
            (row) =>
                `<tr>${row.map((v) => `<td>${escape(v)}</td>`).join("")}</tr>`,
        )
        .join("");
}
