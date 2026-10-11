// Pure clock and graph calculations; tested without a browser or venue.
export function duration(ns) {
    if (!Number.isFinite(ns) || ns < 0) return "unavailable";
    if (ns < 1000) return `${ns.toFixed(0)} ns`;
    if (ns < 1e6) return `${(ns / 1000).toFixed(2)} µs`;
    if (ns < 1e9) return `${(ns / 1e6).toFixed(2)} ms`;
    return `${(ns / 1e9).toFixed(2)} s`;
}
// Model qualification is independent of absolute PHC accuracy and packet ancestry.
// Legacy records may omit the model or its validation flag; do not invent either.
export function clockQualification(model) {
    if (!model || typeof model !== "object") return null;
    const integer = (value) =>
        Number.isSafeInteger(value) && value >= 0
            ? String(value)
            : typeof value === "string" &&
                value.length <= 20 &&
                /^(0|[1-9]\d*)$/.test(value) &&
                BigInt(value) <= 18446744073709551615n
              ? value
              : "unavailable";
    const time = (value) =>
        Number.isSafeInteger(value) && value >= 0
            ? duration(value)
            : "unavailable";
    const method =
        model.method === "driver_extended"
            ? "driver extended"
            : model.method === "user_bracket"
              ? "userspace bracket"
              : "unavailable";
    const rate =
        model.rate_validated === true
            ? "validated"
            : model.rate_validated === false
              ? "not validated"
              : "unreported (legacy)";
    const mapping =
        model.mapping_deferred === true
            ? "deferred after commit; original read retained"
            : model.mapping_deferred === false
              ? "at receive"
              : "unreported";
    return `Clock model ${method} · epoch ${integer(model.epoch)} / connection ${integer(model.generation)} · calibration age ${time(model.age_ns)} · bracket allowance ±${time(model.bracket_uncertainty_ns)} + assumed ${integer(model.assumed_drift_ppm)} ppm drift · rate ${rate} · mapping ${mapping}. Offset qualification only; PHC UTC accuracy and TCP packet association are separate.`;
}
export const stages = [
    ["decode_ns", "Decode", "#9775cf"],
    ["preflight_ns", "Preflight", "#3c947e"],
    ["mutation_ns", "Mutation", "#55b89b"],
    ["validation_ns", "Validation", "#8ed3a3"],
    ["other_ns", "Other / residual", "#70848d"],
];
export function exclusive(traces) {
    // Mixed domains are only summed within complete common observations. A null
    // field is unavailable, never a measured zero or a reason to infer a stage.
    return stages
        .filter(
            ([key]) =>
                traces.length && traces.every((t) => Number.isFinite(t[key])),
        )
        .map(([key, label, color]) => ({
            label,
            color,
            ns: traces.reduce((sum, t) => sum + t[key], 0),
        }));
}
export function alignClock({ session, received_ms, sent_ms }, started, ended) {
    const serverWork = sent_ms - received_ms;
    const wire = ended - started - serverWork;
    if (
        ![started, ended, received_ms, sent_ms].every(Number.isFinite) ||
        wire < 0 ||
        wire > 250 ||
        serverWork < 0
    )
        return null;
    return {
        session,
        offset: (received_ms + sent_ms - started - ended) / 2,
        uncertainty: wire / 2 + 1,
        at: ended,
        expires: ended + 30000,
        rtt: ended - started,
    };
}
export function alignedDelay(clock, session, serverNs, clientMs, now) {
    if (
        !clock ||
        clock.session !== session ||
        now > clock.expires ||
        now < clock.at
    )
        return null;
    // Explicit model allowance: 1 ms timestamp quantization plus 100 ppm relative
    // drift until expiry. This is an estimate, not a hardware clock-error guarantee.
    const uncertainty =
        clock.uncertainty + Math.max(0, now - clock.at) * 0.0001;
    const estimate = clientMs + clock.offset - serverNs / 1e6;
    if (!Number.isFinite(estimate) || estimate < -uncertainty) return null;
    return {
        estimate,
        lower: Math.max(0, estimate - uncertainty),
        upper: Math.max(0, estimate + uncertainty),
        uncertainty,
    };
}

export function browserStages(sample) {
    return [
        { label: "Handler setup", ns: sample.setup_ns, color: "#ac829d" },
        { label: "JSON parse", ns: sample.parse_ns, color: "#a68bd0" },
        { label: "State dispatch", ns: sample.dispatch_ns, color: "#b882a6" },
        { label: "DOM update", ns: sample.dom_ns, color: "#cc7faf" },
        {
            label: "Frame wait",
            ns: sample.frame_ns,
            color: "#e2a6c8",
            note: "Estimated frame opportunity; not physical paint.",
        },
    ];
}

// Message counters order commits across markets, independently of market order,
// receive-time ties, and whether the latest commit was selected for sampling.
export function latestBookTrace(view) {
    let latest = null;
    let message = -1n;
    for (const market of view.markets) {
        if (market.status !== "live") continue;
        const identity = /^([0-9]+):([0-9]+)$/.exec(market.trace_id ?? "");
        if (!identity || identity[1] !== market.generation) return null;
        const current = BigInt(identity[2]);
        if (current > message) {
            latest = market;
            message = current;
        }
    }
    if (!latest || latest.sequence == null) return null;
    const trace = view.latency.traces.find(
        (t) =>
            t.id === latest.trace_id &&
            t.market === latest.id &&
            t.generation === latest.generation &&
            t.sequence === latest.sequence,
    );
    return trace ? { market: latest, trace } : null;
}

// Match the first encoded publication to this exact commit, never a refresh.
export function firstPublication(packet, market, trace) {
    const first = packet.first_publications?.[market.id];
    if (
        !first ||
        trace.id !== market.trace_id ||
        first.market !== market.id ||
        first.trace_id !== trace.id ||
        first.sequence !== market.sequence
    )
        return null;
    const committed = trace.received_ns + trace.queue_ns + trace.processing_ns;
    const waiting = first.published_ns - committed;
    const encodingWait =
        first.ready_ns - first.published_ns - first.encoding_ns;
    if (
        ![
            trace.received_ns,
            trace.queue_ns,
            trace.processing_ns,
            waiting,
            encodingWait,
            first.encoding_ns,
        ].every((n) => Number.isFinite(n) && n >= 0)
    )
        return null;
    return {
        ready_ns: first.ready_ns,
        parts: [
            ...(deliveryStages([trace]).length
                ? deliveryStages([trace])
                : [
                      {
                          label: "Queue / dispatch",
                          ns: trace.queue_ns,
                          color: "#83b8f3",
                      },
                  ]),
            {
                label: "Rust processing",
                ns: trace.processing_ns,
                color: "#55b89b",
            },
            {
                label: "Commit → first publication",
                ns: waiting,
                color: "#d5a54f",
                note: "Wait for the first view containing this commit; later quiet refreshes are excluded.",
            },
            {
                label: "View / publish queue",
                ns: encodingWait,
                color: "#b08946",
            },
            {
                label: "State JSON encoding",
                ns: first.encoding_ns,
                color: "#e9c783",
            },
        ],
    };
}

// One entry bounds memory and prevents a repeated report replacing the first
// browser observation. The caller resets this on visibility or connection loss.
export class FirstObservation {
    clear() {
        this.key = null;
        this.value = null;
    }
    select(session, market) {
        const key = JSON.stringify([
            session,
            market.id,
            market.trace_id,
            market.sequence,
        ]);
        if (this.key !== key) {
            this.clear();
            this.key = key;
        }
        return this.value;
    }
    retain(value) {
        this.value ??= value;
        return this.value;
    }
}

// A model for a completed destination probe, never a measured event-origin span.
export function networkEstimate(route, nowSeconds) {
    if (
        !route ||
        route.status !== "complete" ||
        !route.destination ||
        !Array.isArray(route.hops)
    )
        return null;
    const age = nowSeconds - route.observed_unix_seconds;
    const last = route.hops.at(-1);
    if (
        !Number.isFinite(age) ||
        age < 0 ||
        age > 120 ||
        !last ||
        last.address !== route.destination ||
        !Number.isFinite(last.rtt_ms) ||
        last.rtt_ms < 0
    )
        return null;
    return {
        rtt_ms: last.rtt_ms,
        one_way_ms: last.rtt_ms / 2,
        age_seconds: age,
    };
}

// These boundaries partition the existing pre-decode interval. Missing markers
// do not become zero-valued measurements, including mixed sampled aggregates.
export function deliveryStages(traces) {
    if (
        !traces.length ||
        traces.some(
            (t) =>
                !t.delivery ||
                ![
                    t.queue_ns,
                    t.delivery.transport_ns,
                    t.delivery.handoff_ns,
                    t.delivery.preparation_ns,
                ].every((n) => Number.isFinite(n) && n >= 0) ||
                t.delivery.transport_ns +
                    t.delivery.handoff_ns +
                    t.delivery.preparation_ns !==
                    t.queue_ns,
        )
    )
        return [];
    return [
        [
            "transport_ns",
            "Transport checks",
            "#8aaaca",
            "After complete WebSocket application bytes; excludes TLS and frame assembly.",
        ],
        [
            "handoff_ns",
            "Delivery handoff",
            "#83b8f3",
            "Transport preparation to consumer delivery; includes queue/scheduling in queued mode and stream return in inline mode.",
        ],
        [
            "preparation_ns",
            "Consumer preparation",
            "#bacced",
            "Delivery marker to decode start, including record and optional capture work.",
        ],
    ].map(([key, label, color, note]) => ({
        label,
        color,
        note,
        ns: traces.reduce((sum, t) => sum + t.delivery[key], 0),
    }));
}
export function reportStages(work) {
    if (
        !work ||
        ![work.snapshot_ns, work.handoff_ns, work.preparation_ns].every(
            (n) => Number.isFinite(n) && n >= 0,
        )
    )
        return [];
    return [
        {
            label: "Numeric snapshot",
            ns: work.snapshot_ns,
            color: "#d5a54f",
            note: "Bounded extraction on ingestion; this can delay input handling.",
        },
        {
            label: "Report handoff",
            ns: work.handoff_ns,
            color: "#b08946",
            note: "Snapshot ready to report worker start; outside ingestion.",
        },
        {
            label: "Report preparation",
            ns: work.preparation_ns,
            color: "#e9c783",
            note: "Formatting, percentile sorting and health qualification on the report worker; excludes HTTP JSON encoding.",
        },
    ];
}
// Snapshot age predates report preparation. Only live reports share this clock;
// replay ages stay in the recorded input domain.
export function bookInputAge(report, market) {
    const age = market.update_age_ns;
    if (!Number.isFinite(age) || age < 0) return null;
    if (report.view.mode !== "live") return age;
    const delay = report.published_ns - report.view.observed_ns;
    return Number.isFinite(delay) && delay >= 0 ? age + delay : null;
}

// Receive work belongs to poll operations, not exclusively to returned messages.
// Do not duplicate a batch's TLS work for later buffered messages or add wait time.
export function receiveStages(receive) {
    const unique = [...new Map(receive.map((r) => [r.id, r])).values()];
    if (
        !unique.length ||
        unique.some(
            (r) =>
                r.complete_policy_window === false ||
                !r.work ||
                !["socket_ns", "tls_ns", "websocket_ns"].every(
                    (k) => Number.isFinite(r.work[k]) && r.work[k] >= 0,
                ),
        )
    )
        return [];
    return [
        [
            "socket_ns",
            "Socket reads",
            "#5d95d4",
            "Active read calls, including requested ancillary processing.",
        ],
        [
            "tls_ns",
            "TLS adapter",
            "#9b8bd4",
            "TLS stream read calls less nested socket calls; includes authentication, buffering and copying.",
        ],
        [
            "websocket_ns",
            "WebSocket / observer",
            "#69bda7",
            "WebSocket polls less nested stream calls; includes diagnostic bookkeeping.",
        ],
    ].map(([key, label, color, note]) => ({
        label,
        color,
        note,
        ns: unique.reduce((sum, r) => sum + r.work[key], 0),
    }));
}
export function receiveBoundary(receive, trace, source) {
    if (!receive || !trace || receive.id !== trace.id) return null;
    const stamp = receive[source];
    if (
        !stamp ||
        ![
            stamp.batch_to_message_ns,
            stamp.uncertainty_ns,
            trace.queue_ns,
            trace.processing_ns,
        ].every((n) => Number.isFinite(n) && n >= 0)
    )
        return null;
    return {
        ns: stamp.batch_to_message_ns + trace.queue_ns + trace.processing_ns,
        uncertainty_ns: stamp.uncertainty_ns,
        source: stamp.source,
        status: stamp.status,
    };
}

// Independent browser selection. Polling and server-side sample phases are unrelated.
export class SampleWindow {
    constructor(rate = { every: 1, width: 1, phase: 0 }) {
        this.rate = null;
        this.configure(rate);
    }
    configure(rate) {
        const { every, width, phase } = rate || {};
        if (
            ![every, width, phase].every(Number.isSafeInteger) ||
            every < 0 ||
            every > 1e9 ||
            width < 0 ||
            phase < 0 ||
            (every === 0
                ? width !== 0 || phase !== 0
                : width < 1 || width > every || phase > every - width)
        )
            throw new Error("Invalid browser measurement policy");
        if (
            this.rate &&
            [every, width, phase].every(
                (v, i) =>
                    v ===
                    [this.rate.every, this.rate.width, this.rate.phase][i],
            )
        )
            return false;
        this.rate = { every, width, phase };
        this.position = 0;
        this.eligible = 0;
        this.selected = 0;
        return true;
    }
    observe() {
        this.eligible = Math.min(Number.MAX_SAFE_INTEGER, this.eligible + 1);
        const r = this.rate;
        if (!r.every) return false;
        this.position++;
        if (this.position === r.every) this.position = 0;
        const selected =
            this.position >= r.phase && this.position - r.phase < r.width;
        if (selected)
            this.selected = Math.min(
                Number.MAX_SAFE_INTEGER,
                this.selected + 1,
            );
        return selected;
    }
}
export function rateLabel(rate) {
    if (!rate) return "unavailable";
    return rate.every === 0
        ? "off"
        : `${rate.width}/${rate.every}, phase ${rate.phase}`;
}

// Independently selected syscall internals; never sum these with enclosing socket work.
export function readStages(receive) {
    const unique = [...new Map(receive.map((r) => [r.id, r])).values()];
    const phases = [
        ["readiness", "Readiness", "#b49dda"],
        ["preparation", "Buffer preparation", "#d7a56d"],
        ["syscall", "recvmsg", "#5d95d4"],
        ["metadata", "Timestamp metadata", "#c7be74"],
        ["delivery", "Copy / advance", "#69bda7"],
    ];
    if (
        !unique.length ||
        unique.some(
            (r) => r.complete_policy_window === false || !r.work?.read_detail,
        )
    )
        return [];
    return phases.flatMap(([key, label, color]) => {
        const work = unique.map((r) => r.work.read_detail[key]);
        if (
            work.some(
                (w) =>
                    !w ||
                    w.samples !== w.eligible ||
                    !Number.isFinite(w.exclusive_ns) ||
                    w.exclusive_ns < 0,
            ) ||
            !work.some((w) => w.eligible > 0)
        )
            return [];
        return [
            {
                label,
                color,
                ns: work.reduce((sum, w) => sum + w.exclusive_ns, 0),
                note: "Disjoint work within selected socket operations; includes observer overhead and possible preemption. Missing/partially selected phases are omitted.",
            },
        ];
    });
}
