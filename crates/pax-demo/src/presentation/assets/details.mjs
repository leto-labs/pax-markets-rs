import {
    duration,
    clockQualification,
    exclusive,
    alignedDelay,
    browserStages,
    latestBookTrace,
    firstPublication,
    deliveryStages,
    reportStages,
    bookInputAge,
    receiveStages,
    receiveBoundary,
    readStages,
} from "/timing.mjs";
import { $, chart } from "/elements.mjs";
export function createDetails(getContext, observation) {
    let traces = [],
        selected = "latest",
        traceOptionsKey,
        traceOptionsSession;
    function renderRust(report, refreshChoices = false) {
        const window = report.view.latency;
        traces = window.traces;
        const selector = $("trace-select");
        const newSession = traceOptionsSession !== report.session;
        // Keep native choices stable while the user interacts with the selector.
        // Session changes invalidate the old choices immediately; blur catches up.
        if (
            newSession ||
            refreshChoices ||
            document.activeElement !== selector
        ) {
            const choices = traces
                .slice(-128)
                .reverse()
                .map((t) => [
                    `${t.population} · market ${t.market ?? "—"} · seq ${t.sequence ?? "—"} · ${t.id}`,
                    t.id,
                ]);
            const key = JSON.stringify(choices);
            if (newSession || key !== traceOptionsKey) {
                const value = newSession ? "latest" : selector.value;
                selector.replaceChildren(
                    new Option("Latest event", "latest"),
                    new Option("Aggregate shown events", "aggregate"),
                );
                for (const [label, id] of choices)
                    selector.add(new Option(label, id));
                selector.value = [...selector.options].some(
                    (o) => o.value === value,
                )
                    ? value
                    : "latest";
                traceOptionsKey = key;
                traceOptionsSession = report.session;
            }
        }
        selected = selector.value;
        const chosen =
            selected === "aggregate"
                ? traces.filter((t) => t.population === "event")
                : selected === "latest"
                  ? [
                        traces.findLast((t) => t.population === "event") ||
                            traces.at(-1),
                    ].filter(Boolean)
                  : traces.filter((t) => t.id === selected);
        chart(
            "rust",
            exclusive(chosen),
            selected === "aggregate"
                ? `Sum of ${chosen.length} event samples`
                : "Message processing",
        );
        const ids = new Set(chosen.map((t) => t.id));
        let receive = (window.receive ?? []).filter((r) => ids.has(r.id));
        const correlatedReceive = receive.length > 0;
        if (
            !receive.length &&
            (selected === "latest" || selected === "aggregate")
        )
            receive =
                selected === "aggregate"
                    ? (window.receive ?? [])
                    : [(window.receive ?? []).at(-1)].filter(Boolean);
        chart(
            "receive",
            receiveStages(receive),
            "Active receive operations (shared batch work counted once)",
        );
        chart(
            "readwork",
            readStages(receive),
            "Observed read internals (within socket work)",
        );
        const details = receive.map((r) => r.work?.read_detail).filter(Boolean);
        $("readwork-meta").textContent = details.some(
            (d) => d.syscall?.eligible,
        )
            ? `${details.reduce((n, d) => n + d.syscall.eligible, 0)} syscall attempts · ${details.reduce((n, d) => n + d.would_block, 0)} WouldBlock · ${details.reduce((n, d) => n + d.initialized_bytes, 0)} newly initialized bytes · ${details.reduce((n, d) => n + d.copied_bytes, 0)} scratch-copy bytes. Missing phases are unobserved. These spans overlap the enclosing socket bar above.`
            : "Read internals unavailable: disabled, unsampled, buffered, or using the portable reader.";
        const waits = receive.filter((r) =>
            Number.isFinite(r.work?.pending_ns),
        );
        $("receive-meta").textContent = receive.length
            ? `${correlatedReceive ? `${receive.length}/${chosen.length} selected traces have receive metadata` : `${receive.length} independent receive samples; no selected processing correlation`} · ${window.receive_capacity_drops ?? 0} receive-window capacity evictions · ${window.receive_age_expirations ?? 0} age expirations · ${receive.filter((r) => r.buffered).length} buffered · ${receive.filter((r) => r.shared).length} share plaintext batches · pending input / scheduling wait ${waits.length ? duration(waits.reduce((s, r) => s + r.work.pending_ns, 0)) : "unavailable"} (${waits.length}/${receive.length} complete wait observations; separate from active work).`
            : "Receive diagnostics disabled, unsampled or unavailable for this trace. Existing processing timings remain available.";
        const r = receive.at(-1),
            t = chosen.find((t) => t.id === r?.id);
        $("receive-coverage").textContent = r
            ? `Trace ${r.id} · receive sample 1/${r.sample_every} · ${r.lost} association losses in this generation · ${r.coverage}. Plaintext batches ${r.plaintext ? `${r.plaintext.first}–${r.plaintext.last}` : "unavailable"}; observed socket batches ${r.socket ? `${r.socket.first}–${r.socket.last}` : "unavailable"}.`
            : "No synthetic earlier timestamps are supplied for older captures or disabled diagnostics.";
        $("receive-boundaries").textContent = r
            ? ["software", "hardware"]
                  .map((source) => {
                      const boundary = receiveBoundary(r, t, source);
                      return boundary
                          ? `${boundary.source}: batch → commit ${duration(boundary.ns)} ±${duration(boundary.uncertainty_ns)}. ${boundary.status}.`
                          : `${source}: ${r[source]?.status ?? "unavailable"}.`;
                  })
                  .join(" ")
            : "Kernel / NIC receive boundaries unavailable.";
        const qualification = clockQualification(r?.hardware?.qualification);
        if (qualification)
            $("receive-boundaries").textContent += ` ${qualification}`;
        const read = r?.latest_read;
        if (read) {
            const stamp = read.software,
                elapsed = stamp?.batch_to_message_ns - read.ended_to_message_ns;
            $("receive-boundaries").textContent +=
                ` Read batch ${read.batch}: ` +
                (Number.isFinite(stamp?.batch_to_message_ns) &&
                Number.isFinite(read.ended_to_message_ns) &&
                elapsed >= 0
                    ? `software stamp → observed read completion ${duration(elapsed)} ±${duration(stamp.uncertainty_ns)}; no proven byte association to this book commit.`
                    : `${stamp?.status ?? "timestamp unavailable"}; no message association.`);
        }
        chart(
            "delivery",
            deliveryStages(chosen),
            "Complete message → decode start",
        );
        const marked = chosen.filter((t) => t.delivery);
        const gaps = chosen.filter((t) => Number.isFinite(t.pending_gap_ns));
        const tails = chosen.filter((t) => Number.isFinite(t.bookkeeping_ns));
        $("delivery-meta").textContent =
            `Delivery markers ${marked.length}/${chosen.length} samples · queued inputs remaining at delivery: ${marked.length ? Math.max(...marked.map((t) => t.delivery.pending)) : "unavailable"} (maximum) · callback bookkeeping after commit: ${tails.length ? duration(tails.reduce((sum, t) => sum + t.bookkeeping_ns, 0)) : "unavailable"} (sum of ${tails.length}) · gap before next already-received input: ${gaps.length ? duration(gaps.reduce((sum, t) => sum + t.pending_gap_ns, 0)) : "unavailable"} (sum of ${gaps.length}). Gaps overlap handoff/preparation; do not add them again. Inline backlog is zero; unread socket data is not counted.`;
        $("trace-meta").textContent = chosen.length
            ? `${chosen.length === 1 ? `Trace ${chosen[0].id} · ${chosen[0].population} · market ${chosen[0].market ?? "—"} · sequence ${chosen[0].sequence ?? "—"}` : `${chosen.length} events, summed exclusive stage time`} · ${window.capacity_drops} capacity evictions · independent domain rates${window.stale ? " · stale window" : ""}`
            : "No fresh trace. A quiet market does not imply a broken connection.";
    }
    function renderReport(report) {
        chart(
            "report",
            reportStages(report.report_work),
            "Latest report preparation",
        );
        $("report-meta").textContent = report.report_work
            ? `${report.report_work.skipped} periodic reports skipped while the bounded worker queue was full. Only numeric snapshot extraction runs on ingestion. HTTP JSON encoding and the publication interval are separate.`
            : "Report-worker timing unavailable for this source.";
    }
    function renderBrowser() {
        const { browser, browserSampling, coalesced, skipped, paused } =
            getContext();
        if (!browser) {
            chart("browser", [], "Browser timing");
            $("browser-meta").textContent = browserSampling.rate.every
                ? "No selected frame sample for this display."
                : "Browser timing is off by policy.";
            return;
        }
        chart(
            "browser",
            browserStages(browser),
            "Browser receipt → frame opportunity",
        );
        $("browser-meta").textContent =
            `Display ${browser.update_id} · request RTT ${duration(browser.request_ns)} · tab visible · ${coalesced} book commits coalesced · ${skipped} published displays skipped${browser.frame_ns > 100e6 ? " · long frame wait; possible throttling" : ""}${paused ? " · display paused" : ""}`;
    }
    function renderOverall(report, draw = true) {
        const { packet, browser, clock, receivedAt } = getContext();
        const { market, trace } = latestBookTrace(report.view) ?? {};
        const isLive = report.view.mode === "live";
        const timed =
            trace &&
            Number.isFinite(trace.queue_ns) &&
            Number.isFinite(trace.processing_ns);
        const ages = report.view.markets
            .filter((m) => m.status === "live" && m.update_age_ns != null)
            .map((m) => `market ${m.id}: ${duration(bookInputAge(report, m))}`)
            .join(" · ");
        if (draw)
            $("overall-age").textContent =
                isLive && timed
                    ? `Book age at publication: ${duration(report.published_ns - trace.received_ns - trace.queue_ns - trace.processing_ns)} since commit. Freshness is separate from the frozen timeline.`
                    : ages
                      ? `Book input age at ${isLive ? "publication" : "snapshot"} — ${ages}. Age uses the last committed input's local receipt time${isLive ? "" : " in the recorded clock domain"}.`
                      : "";
        if (!timed || !isLive) {
            observation.clear();
            if (draw) chart("overall", [], "Server stages");
            if (draw)
                $("overall-meta").textContent = isLive
                    ? "Latest displayed sequence has no retained trace; no invented correlation."
                    : "Replay uses recorded input time and local reprocessing time. Cross-clock end-to-end spans are unavailable.";
            return;
        }
        const first = firstPublication(packet, market, trace);
        if (!first) {
            observation.clear();
            if (draw) chart("overall", [], "Server stages");
            if (draw)
                $("overall-meta").textContent =
                    "First-publication checkpoint unavailable for this commit.";
            return;
        }
        let saved = observation.select(report.session, market);
        if (!saved) {
            const delay =
                receivedAt == null
                    ? null
                    : alignedDelay(
                          clock,
                          report.session,
                          first.ready_ns,
                          receivedAt,
                          performance.now(),
                      );
            const correlated =
                browser && browser.update_id === report.update_id;
            const aligned = delay && delay.estimate >= 0 && correlated;
            const parts = [...first.parts];
            if (aligned)
                parts.push(
                    {
                        label: "First browser delivery estimate",
                        ns: delay.estimate * 1e6,
                        color: "#83b8f3",
                        note: `Includes polling and time before this browser first observes the commit; clock-model uncertainty ±${delay.uncertainty.toFixed(2)} ms.`,
                    },
                    ...browserStages(browser),
                );
            saved = {
                parts,
                title: aligned
                    ? "Local receipt → frame (first observation, estimated)"
                    : "Complete local message → first encoded display",
                meta:
                    `First observed display ${report.update_id} ← trace ${trace.id} · market ${market.id} · sequence ${market.sequence}. ` +
                    (aligned
                        ? `First encoded checkpoint → first browser receipt estimate: ${delay.lower.toFixed(2)}–${delay.upper.toFixed(2)} ms (clock uncertainty ±${delay.uncertainty.toFixed(2)} ms). Includes polling or opening this page later.`
                        : "No usable clock alignment at first observation; browser and server durations stay separate."),
            };
            if (correlated) observation.retain(saved);
        }
        if (draw) chart("overall", saved.parts, saved.title);
        if (draw) $("overall-meta").textContent = saved.meta;
    }
    function alignment() {
        const { clock } = getContext();
        if (!$("alignment")) return;
        const valid = clock && performance.now() <= clock.expires;
        $("alignment").textContent = valid
            ? `Browser/server offset estimate ${clock.offset.toFixed(2)} ms ±${clock.uncertainty.toFixed(2)} ms; clock request RTT ${clock.rtt.toFixed(2)} ms. Expires in ${Math.max(0, (clock.expires - performance.now()) / 1000).toFixed(0)} s. Model: request asymmetry bound + 1 ms timestamp allowance + 100 ppm relative drift until expiry; not a hardware accuracy guarantee.`
            : "No valid browser/server clock alignment. Cross-domain one-way durations are unavailable.";
    }

    return {
        renderRust,
        renderReport,
        renderBrowser,
        renderOverall,
        alignment,
    };
}
