// Qualified ingress-to-commit calculations. No browser or ingestion ownership.
import { clockQualification, receiveBoundary, stages } from "./timing.mjs";

const valid = (n) => Number.isSafeInteger(n) && n >= 0;
export const ingressSources = {
    software: "Kernel RX → book",
    hardware: "NIC RX → book",
    "first-software": "Kernel first observed RX → book",
    "first-hardware": "NIC first observed RX → book",
};
const clockSources = {
    software: "kernel-software/realtime",
    hardware: "nic-hardware/phc",
};
const baseSource = (source) => source.replace(/^first-/, "");
const firstWindow = (source) => source.startsWith("first-");
const validBatch = (n) =>
    typeof n === "string"
        ? /^[1-9][0-9]*$/.test(n)
        : Number.isSafeInteger(n) && n > 0;
function observation(receive, source) {
    const base = baseSource(source);
    if (!firstWindow(source))
        return {
            stamp: receive[base],
            readEnd: receive.observed_last_read_to_message_ns,
        };
    const first = receive.first_observed_read;
    if (
        !first ||
        !receive.socket ||
        ![first.batch, receive.socket.first, receive.socket.last].every(
            validBatch,
        ) ||
        BigInt(receive.socket.first) > BigInt(receive.socket.last) ||
        String(first.batch) !== String(receive.socket.first)
    )
        return null;
    return { stamp: first[base], readEnd: first.ended_to_message_ns };
}

// Prefer the earlier qualified NIC observation while preserving manual choice.
export function preferredIngressSource(view, requested = "auto") {
    if (requested !== "auto") {
        if (!Object.hasOwn(ingressSources, requested))
            throw new Error("Unknown ingress timestamp source");
        return requested;
    }
    return (
        ["first-hardware", "first-software", "hardware", "software"].find(
            (source) => ingressSummary(view, source).samples,
        ) ?? "software"
    );
}

export function ingressSummary(view, source = "software") {
    if (!Object.hasOwn(ingressSources, source))
        throw new Error("Unknown ingress timestamp source");
    const result = {
        source,
        label: ingressSources[source],
        samples: 0,
        eligible: 0,
        p50: null,
        p99: null,
        boundary_label: firstWindow(source)
            ? "First observed read-window proxy"
            : "Completion read-batch proxy",
        association_limit:
            "Earlier buffered TLS input and timestamp position within each batch are unknown; exact first contributing packet is not proven.",
        receive_revisions: [],
        processing_revisions: [],
        stale: Boolean(view.latency.stale),
        reason: "",
    };
    if (view.mode !== "live")
        return {
            ...result,
            reason: "Ingress timing unavailable for fixture/replay; processing below is local reprocessing.",
        };
    const markets = new Map(
        view.markets.filter((m) => m.status === "live").map((m) => [m.id, m]),
    );
    const receive = new Map((view.latency.receive ?? []).map((r) => [r.id, r]));
    const samples = [];
    for (const trace of new Map(
        view.latency.traces.map((t) => [t.id, t]),
    ).values()) {
        const market = markets.get(trace.market);
        const identity = /^([0-9]+):([0-9]+)$/.exec(trace.id);
        if (
            trace.population !== "event" ||
            !market ||
            market.generation !== trace.generation ||
            !identity ||
            identity[1] !== trace.generation
        )
            continue;
        result.eligible++;
        const r = receive.get(trace.id);
        if (
            !r ||
            r.complete_policy_window === false ||
            !valid(trace.received_ns) ||
            trace.received_ns !== r.received_ns
        )
            continue;
        const selected = observation(r, source),
            base = baseSource(source);
        const boundary =
            selected &&
            receiveBoundary({ id: r.id, [base]: selected.stamp }, trace, base);
        if (
            !boundary ||
            selected.stamp.source !== clockSources[base] ||
            ![
                boundary.ns,
                boundary.uncertainty_ns,
                selected.stamp.batch_to_message_ns,
                trace.queue_ns,
                trace.processing_ns,
            ].every(valid)
        )
            continue;
        samples.push({
            trace,
            receive: r,
            stamp: selected.stamp,
            read_end_to_message_ns: selected.readEnd,
            total_ns: boundary.ns,
            uncertainty_ns: boundary.uncertainty_ns,
        });
    }
    samples.sort(
        (a, b) =>
            a.total_ns - b.total_ns ||
            a.trace.received_ns - b.trace.received_ns ||
            a.trace.id.localeCompare(b.trace.id),
    );
    result.samples = samples.length;
    result.receive_revisions = [
        ...new Set(samples.map((s) => s.receive.revision ?? "unknown")),
    ];
    result.processing_revisions = [
        ...new Set(samples.map((s) => s.trace.policy_revision ?? "unknown")),
    ];
    if (!samples.length)
        return {
            ...result,
            reason: "No qualified matched ingress samples: timestamp/clock/association may be disabled, unsampled or unavailable.",
        };
    result.p50 = samples[Math.ceil(samples.length * 0.5) - 1];
    result.p99 = samples[Math.ceil(samples.length * 0.99) - 1];
    return result;
}

// Split only at associated read boundaries, never latest_read (an unassociated
// observation) or active poll spans (overlapping windows that can predate RX).
function canSplitReceive(sample, source) {
    const r = sample.receive;
    return (
        r.socket &&
        observation(r, source) &&
        valid(sample.read_end_to_message_ns) &&
        sample.read_end_to_message_ns <= sample.stamp.batch_to_message_ns
    );
}
function canSplitReadWindow(sample, source) {
    if (!firstWindow(source) || !canSplitReceive(sample, source)) return false;
    const { receive: r } = sample,
        last = r.observed_last_read_to_message_ns;
    if (!valid(last) || last > sample.read_end_to_message_ns) return false;
    // One read has one completion time. Reject contradictory legacy/partial
    // records without excluding their qualified enclosing ingress interval.
    return (
        String(r.socket.first) !== String(r.socket.last) ||
        last === sample.read_end_to_message_ns
    );
}
function hasProcessingStages(sample) {
    return (
        stages.every(([key]) => valid(sample.trace[key])) &&
        stages.reduce((sum, [key]) => sum + sample.trace[key], 0) ===
            sample.trace.processing_ns
    );
}
export function ingressParts(
    sample,
    source = "software",
    {
        splitReceive = true,
        splitReadWindow = true,
        splitProcessing = true,
    } = {},
) {
    if (!sample) return [];
    const { trace } = sample;
    const before = sample.stamp.batch_to_message_ns;
    const parts =
        splitReceive && canSplitReceive(sample, source)
            ? [
                  {
                      label: firstWindow(source)
                          ? "Ingress → first observed read end"
                          : "Ingress → observed read end",
                      ns: before - sample.read_end_to_message_ns,
                      color: "#5d95d4",
                      note: "Qualified read-batch timestamp to its observed read end; includes stack residence, scheduling and read/metadata work. Not pure kernel CPU time.",
                  },
                  {
                      label: firstWindow(source)
                          ? "First read end → complete message"
                          : "Read end → complete message",
                      ns: sample.read_end_to_message_ns,
                      color: "#9b8bd4",
                      note: "TLS, WebSocket framing, buffered residence and intervening work; read-ahead and earlier TLS bytes limit attribution.",
                  },
              ]
            : [
                  {
                      label: "Ingress → complete message",
                      ns: before,
                      color: "#5d95d4",
                      note: "Qualified read-batch interval; finer associated read boundary unavailable.",
                  },
              ];
    if (splitReceive && splitReadWindow && canSplitReadWindow(sample, source)) {
        const last = sample.receive.observed_last_read_to_message_ns;
        parts.splice(
            1,
            1,
            {
                label: "First → last observed read end",
                ns: sample.read_end_to_message_ns - last,
                color: "#a490db",
                note: "Wall time between associated reads: later input availability, transport/TLS work and scheduling. Not isolated network wait or CPU time.",
            },
            {
                label: "Last read end → complete message",
                ns: last,
                color: "#8068b5",
                note: "Remaining TLS/WebSocket work and buffered residence after the last observed read. Read-ahead and earlier buffered TLS input prevent an exact last-required-record claim.",
            },
        );
    }
    parts.push({
        label: "Message dispatch / preparation",
        ns: trace.queue_ns,
        color: "#83b8f3",
        note: "Complete message to decode start, including optional capture and diagnostics.",
    });
    if (splitProcessing && hasProcessingStages(sample)) {
        parts.push(
            ...stages.map(([key, label, color]) => ({
                label,
                color,
                ns: trace[key],
            })),
        );
    } else
        parts.push({
            label: "Decode + book processing",
            ns: trace.processing_ns,
            color: "#55b89b",
            note: "Combined measured processing; detailed stages unavailable or independently sampled.",
        });
    return parts;
}

export function ingressContributions(summary) {
    if (!summary.p50 || !summary.p99) return [];
    const options = {
        splitReceive: [summary.p50, summary.p99].every((s) =>
            canSplitReceive(s, summary.source),
        ),
        splitReadWindow: [summary.p50, summary.p99].every((s) =>
            canSplitReadWindow(s, summary.source),
        ),
        splitProcessing: [summary.p50, summary.p99].every(hasProcessingStages),
    };
    const a = ingressParts(summary.p50, summary.source, options),
        b = ingressParts(summary.p99, summary.source, options);
    return contributions(summary, a, b);
}
function contributions(summary, a, b) {
    return a.map((part, i) => ({
        label: part.label,
        p50_ns: part.ns,
        p99_ns: b[i].ns,
        p50_percent: summary.p50.total_ns
            ? (part.ns / summary.p50.total_ns) * 100
            : null,
        p99_percent: summary.p99.total_ns
            ? (b[i].ns / summary.p99.total_ns) * 100
            : null,
    }));
}

// Cold same-event diagnostic, never a replacement primary endpoint. Raw clocks
// have epoch-sized integers; convert only a checked difference, not the stamps.
export function ingressArrival(sample, source) {
    const unavailable = (reason) => ({ available: false, reason, parts: [] });
    if (
        !sample ||
        !Object.hasOwn(ingressSources, source) ||
        !firstWindow(source)
    )
        return unavailable("Requires a qualified first-window event.");
    if (
        !canSplitReadWindow(sample, source) ||
        sample.receive.complete_policy_window === false
    )
        return unavailable(
            "Associated read boundaries are missing or inconsistent.",
        );
    const { trace, receive: r, stamp: first } = sample,
        last = r[baseSource(source)];
    const expected = clockSources[baseSource(source)];
    if (
        first.source !== expected ||
        last?.source !== expected ||
        ![
            first.batch_to_message_ns,
            last?.batch_to_message_ns,
            first.uncertainty_ns,
            last?.uncertainty_ns,
        ].every(valid)
    )
        return unavailable(
            "Both same-source aligned timestamps and clock allowances are required.",
        );
    const span = first.batch_to_message_ns - last.batch_to_message_ns;
    const residence =
        last.batch_to_message_ns - r.observed_last_read_to_message_ns;
    const budget = first.uncertainty_ns + last.uncertainty_ns;
    if (![span, residence, budget].every(valid))
        return unavailable(
            "Timestamp or read chronology is reversed or inexact.",
        );
    const raw = (value) =>
        typeof value === "string" && /^(0|[1-9][0-9]{0,38})$/.test(value)
            ? BigInt(value)
            : null;
    const start = raw(first.raw_ns),
        end = raw(last.raw_ns);
    if (
        start === null ||
        end === null ||
        end < start ||
        end - start > BigInt(Number.MAX_SAFE_INTEGER)
    )
        return unavailable(
            "Exact, ordered source-clock stamps are unavailable.",
        );
    const rawSpan = Number(end - start);
    if (Math.abs(rawSpan - span) > budget)
        return unavailable(
            "Raw and aligned spans disagree beyond their clock allowances.",
        );
    const a = first.qualification?.generation,
        b = last.qualification?.generation;
    if (
        (a != null && !validBatch(a)) ||
        (b != null && !validBatch(b)) ||
        (a != null && b != null && String(a) !== String(b))
    )
        return unavailable(
            "Timestamp clock generations differ or are invalid.",
        );
    if (
        String(r.socket.first) === String(r.socket.last) &&
        (span !== 0 || rawSpan !== 0)
    )
        return unavailable(
            "One read cannot have different first and completion timestamps.",
        );
    const parts = [
        {
            label: "First → completion associated RX stamp",
            ns: span,
            color: "#d2a65a",
            note: "Interval between associated receive stamps, not isolated network delay or exact first-to-last required packet time. Includes source/route effects and read-batch association limits.",
        },
        {
            label: "Completion RX stamp → last read end",
            ns: residence,
            color: "#5d95d4",
            note: "Local elapsed residence after the completion-batch timestamp, including stack, scheduling, read and metadata work; not pure kernel CPU.",
        },
        {
            label: "Last read end → complete message",
            ns: r.observed_last_read_to_message_ns,
            color: "#8068b5",
            note: "Remaining TLS/framing and buffered residence; the read can include read-ahead.",
        },
        ...ingressParts(sample, source, { splitReceive: false }).slice(1),
    ];
    if (parts.reduce((sum, p) => sum + p.ns, 0) !== sample.total_ns)
        return unavailable(
            "Arrival phases do not preserve the primary event total.",
        );
    return {
        available: true,
        parts,
        span_ns: span,
        raw_span_ns: rawSpan,
        span_uncertainty_ns: budget,
        first_uncertainty_ns: first.uncertainty_ns,
        completion_uncertainty_ns: last.uncertainty_ns,
        completion_stamp_to_book_ns:
            last.batch_to_message_ns + trace.queue_ns + trace.processing_ns,
        model_identity_available: a != null && b != null,
        reason: "Read-window proxy; raw span is a cross-check, not an additional phase. Clock allowances do not prove absolute accuracy or packet identity.",
    };
}
export function ingressArrivalContributions(summary) {
    const a = ingressArrival(summary.p50, summary.source),
        b = ingressArrival(summary.p99, summary.source);
    if (!a.available || !b.available) return [];
    // Both columns retain identical processing granularity even if independently
    // sampled detail is incomplete in one selected event.
    if (![summary.p50, summary.p99].every(hasProcessingStages)) {
        for (const [item, sample] of [
            [a, summary.p50],
            [b, summary.p99],
        ])
            item.parts.splice(4, item.parts.length - 4, {
                label: "Decode + book processing",
                ns: sample.trace.processing_ns,
            });
    }
    return contributions(summary, a.parts, b.parts);
}

// Describe the chosen contributing stamp, never substitute completion-read details.
export function ingressClockModels(summary) {
    return ["p50", "p99"].map((percentile) => {
        const sample = summary[percentile];
        const model = clockQualification(sample?.stamp?.qualification);
        return {
            percentile,
            text: sample
                ? `${percentile} event ${sample.trace.id} · ${summary.source}: ${model ?? "Clock-model details unavailable in this record (legacy or compiled out). Reported alignment uncertainty remains separate from absolute clock accuracy and packet association."}`
                : `${percentile}: no qualified primary ingress event; clock-model details unavailable.`,
        };
    });
}
