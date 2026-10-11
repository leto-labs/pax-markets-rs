import { duration, rateLabel } from "/timing.mjs";
import {
    ingressClockModels,
    ingressSummary,
    ingressParts,
    ingressContributions,
    ingressArrival,
    ingressArrivalContributions,
    preferredIngressSource,
} from "/ingress.mjs";
import { $, escape, svgNode, chart } from "/elements.mjs";
export function createPrimary(latencyHistory, getBrowser) {
    const isLatency = document.body.dataset.page === "latency";
    function card(label, value, note = "") {
        return `<div class="card"><small>${escape(label)}</small><b>${escape(value)}</b><span>${escape(note)}</span></div>`;
    }
    function cards(report, core) {
        const events = report.view.latency.populations.find(
            (p) => p.name === "event",
        );
        const note = `${core.samples}/${core.eligible} matched events${core.stale ? " · stale window" : ""}`;
        $("cards").innerHTML =
            card(
                `${core.label} p50`,
                core.p50 ? duration(core.p50.total_ns) : "Unavailable",
                core.p50 ? core.boundary_label : "No qualified samples",
            ) +
            card(
                `${core.label} p99`,
                core.p99 ? duration(core.p99.total_ns) : "Unavailable",
                core.p99
                    ? `${core.samples} samples${core.samples < 100 ? " · coarse tail" : ""}`
                    : "No qualified samples",
            );
        $("primary-status").textContent = core.p50 ? note : core.reason;
        $("secondary-cards").innerHTML =
            card(
                "Decode + book p50",
                events ? duration(events.p50_ns) : "No events",
                events
                    ? `${events.samples} sampled events / 10 s`
                    : "Snapshots counted separately",
            ) +
            card(
                "Decode + book p99",
                events ? duration(events.p99_ns) : "—",
                `Callback ${rateLabel(report.view.latency.processing?.policy.callback)}`,
            ) +
            card(
                "Heartbeat RTT",
                report.heartbeat_rtt_ns == null
                    ? "Pending"
                    : duration(report.heartbeat_rtt_ns),
                "Public application ping / pong",
            ) +
            card(
                "Browser DOM + frame",
                getBrowser()
                    ? duration(getBrowser().dom_ns + getBrowser().frame_ns)
                    : "Measuring",
                "Frame opportunity estimate",
            );
    }
    function renderIngress(report, observe = false) {
        const core = ingressSummary(
            report.view,
            preferredIngressSource(
                report.view,
                $("ingress-source")?.value ?? "auto",
            ),
        );
        cards(report, core);
        for (const model of ingressClockModels(core)) {
            const node = $(`ingress-clock-${model.percentile}`);
            if (node) node.textContent = model.text;
        }
        if (!isLatency) return;
        if (observe)
            latencyHistory.observe(performance.now(), report.session, core);
        renderHistory(core);
        const count = `${core.samples}/${core.eligible} retained accepted events qualified; source ${core.source}. ${core.boundary_label}. ${core.association_limit}`;
        const queue = report.view.latency.diagnostics?.queue;
        $("ingress-meta").textContent = core.p50
            ? `${count} p50 event ${core.p50.trace.id} ±${duration(core.p50.uncertainty_ns)}; p99 event ${core.p99.trace.id} ±${duration(core.p99.uncertainty_ns)} clock alignment uncertainty. Receive policies ${core.receive_revisions.join(", ")}, processing policies ${core.processing_revisions.join(", ")}. ${core.stale ? "STALE window. " : ""}${core.samples < 100 ? "Small sample: p99 is coarse. " : ""}Percentiles cover retained samples; consult independent loss/health below.${queue ? ` Queue drops ${queue.full_drops + queue.closed_drops}.` : ""}`
            : `${count} ${core.reason}`;
        const percent = (n) => (Number.isFinite(n) ? `${n.toFixed(2)}%` : "—");
        const arrivals = $("ingress-breakdown").value === "arrivals";
        const rows = arrivals
            ? ingressArrivalContributions(core)
            : ingressContributions(core);
        $("ingress-arrival-meta").hidden = !arrivals;
        if (arrivals)
            $("ingress-arrival-meta").textContent =
                ["p50", "p99"]
                    .map((p) => {
                        const a = ingressArrival(core[p], core.source);
                        return a.available
                            ? `${p} event ${core[p].trace.id}: associated arrival span ${duration(a.span_ns)}, raw same-clock cross-check ${duration(a.raw_span_ns)}; first/completion alignment allowances ±${duration(a.first_uncertainty_ns)} / ±${duration(a.completion_uncertainty_ns)} (span allowance ±${duration(a.span_uncertainty_ns)}). ${a.model_identity_available ? "Reported clock generation agrees." : "Clock-model identity unavailable."}`
                            : `${p}: arrival detail unavailable. ${a.reason}`;
                    })
                    .join(" ") +
                " This view keeps the original primary events and totals. Associated arrival spacing is not isolated network delay or exact packet ancestry. The raw cross-check is not an extra phase; alignment allowances do not prove absolute accuracy.";
        if (core.p50)
            rows.push({
                label: "Total ingress → book",
                p50_ns: core.p50.total_ns,
                p99_ns: core.p99.total_ns,
                p50_percent: core.p50.total_ns ? 100 : null,
                p99_percent: core.p99.total_ns ? 100 : null,
            });
        $("ingress-table").innerHTML = rows
            .map(
                (r) =>
                    `<tr><th scope="row">${escape(r.label)}</th><td>${escape(duration(r.p50_ns))}</td><td>${percent(r.p50_percent)}</td><td>${escape(duration(r.p99_ns))}</td><td>${percent(r.p99_percent)}</td></tr>`,
            )
            .join("");
        const selected = $("ingress-event").value;
        chart(
            "ingress",
            arrivals
                ? ingressArrival(core[selected], core.source).parts
                : ingressParts(core[selected], core.source),
            `${core.label} · ${selected} total event`,
        );
    }
    let historySvg = null;
    function renderHistory(core) {
        const host = $("history-chart");
        if (!host) return;
        const now = performance.now(),
            seconds = Number($("history-range").value);
        const view = latencyHistory.view(now, seconds),
            latest = view.latest;
        const current = latest?.p50 != null;
        const aligned = (name) =>
            Number.isSafeInteger(latest?.[name + "Uncertainty"])
                ? ` ±${duration(latest[name + "Uncertainty"])}`
                : "";
        $("history-meta").textContent =
            `${core.label} · ${view.points} tab observations in ${seconds / 60} min. ` +
            (current
                ? `Latest window: p50 ${duration(latest.p50)}${aligned("p50")}, p99 ${duration(latest.p99)}${aligned("p99")} · ${latest.samples} qualified events.`
                : "Current window unavailable; gaps are preserved.");
        if (!historySvg) {
            const svg = svgNode("svg", { role: "img" }),
                title = svgNode("title");
            svg.append(title);
            const levels = [0, 0.5, 1].map(() => {
                const line = svgNode("line", { stroke: "var(--line)" }),
                    text = svgNode("text", {
                        fill: "var(--muted)",
                        "font-size": "12",
                    });
                svg.append(line, text);
                return { line, text };
            });
            const left = svgNode("text", {
                    fill: "var(--muted)",
                    "font-size": "12",
                }),
                right = svgNode("text", {
                    fill: "var(--muted)",
                    "font-size": "12",
                    "text-anchor": "end",
                });
            const p50 = svgNode("path", {
                    "data-line": "p50",
                    fill: "none",
                    stroke: "var(--blue)",
                    "stroke-width": "2",
                }),
                p99 = svgNode("path", {
                    "data-line": "p99",
                    fill: "none",
                    stroke: "var(--decode)",
                    "stroke-width": "2",
                });
            const band50 = svgNode("path", {
                    "data-band": "p50",
                    fill: "var(--blue)",
                    "fill-opacity": ".13",
                }),
                band99 = svgNode("path", {
                    "data-band": "p99",
                    fill: "var(--decode)",
                    "fill-opacity": ".13",
                });
            const dot50 = svgNode("circle", { r: "3", fill: "var(--blue)" }),
                dot99 = svgNode("circle", { r: "3", fill: "var(--decode)" });
            svg.append(band99, band50, p99, p50, dot99, dot50, left, right);
            host.append(svg);
            historySvg = {
                svg,
                title,
                levels,
                left,
                right,
                p50,
                p99,
                band50,
                band99,
                dot50,
                dot99,
            };
        }
        const width = Math.max(200, host.clientWidth),
            height = 220,
            x0 = 68,
            x1 = width - 8,
            y0 = 20,
            y1 = 184,
            max = Math.max(1, view.max);
        const x = (at) =>
            x0 + ((at - (now - seconds * 1000)) / (seconds * 1000)) * (x1 - x0);
        const y = (value) => y1 - (value / max) * (y1 - y0);
        historySvg.svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
        historySvg.svg.setAttribute(
            "aria-label",
            `${core.label} history. ${$("history-meta").textContent}`,
        );
        historySvg.title.textContent = `SDK p50/p99 window history; ${seconds / 60} minutes of this tab's observations.`;
        historySvg.levels.forEach(({ line, text }, index) => {
            const value = max * (1 - index / 2),
                level = y(value);
            for (const [name, value] of Object.entries({
                x1: x0,
                x2: x1,
                y1: level,
                y2: level,
            }))
                line.setAttribute(name, value);
            text.setAttribute("x", "0");
            text.setAttribute("y", level + 4);
            text.textContent = duration(value);
        });
        for (const [node, label, pos] of [
            [historySvg.left, `−${seconds / 60} min`, x0],
            [historySvg.right, "Now", x1],
        ]) {
            node.setAttribute("x", pos);
            node.setAttribute("y", 212);
            node.textContent = label;
        }
        for (const name of ["p50", "p99"]) {
            const shading = view.bands[name]
                .map((segment) => {
                    const bounds = segment
                        .map((point) => [point.at, point.upper])
                        .concat(
                            [...segment]
                                .reverse()
                                .map((point) => [point.at, point.lower]),
                        );
                    return (
                        bounds
                            .map(
                                ([at, value], index) =>
                                    `${index ? "L" : "M"}${x(at).toFixed(2)},${y(value).toFixed(2)}`,
                            )
                            .join(" ") + " Z"
                    );
                })
                .join(" ");
            historySvg[name === "p50" ? "band50" : "band99"].setAttribute(
                "d",
                shading,
            );
            const path = view.segments
                .map((segment) =>
                    segment
                        .map(
                            (point, index) =>
                                `${index ? "L" : "M"}${x(point.at).toFixed(2)},${y(point[name]).toFixed(2)}`,
                        )
                        .join(" "),
                )
                .join(" ");
            historySvg[name].setAttribute("d", path);
            const dot = historySvg[name === "p50" ? "dot50" : "dot99"];
            dot.setAttribute("visibility", current ? "visible" : "hidden");
            if (current) {
                dot.setAttribute("cx", x(latest.at));
                dot.setAttribute("cy", y(latest[name]));
            }
        }
    }

    return renderIngress;
}
