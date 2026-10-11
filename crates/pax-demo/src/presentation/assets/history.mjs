// Tab-local observations of overlapping SDK windows, never bucket percentiles.
const validNs = (value) => Number.isSafeInteger(value) && value >= 0;
const validBudget = (event) =>
    validNs(event.uncertainty_ns) &&
    validNs(event.total_ns + event.uncertainty_ns)
        ? event.uncertainty_ns
        : null;
export class LatencyHistory {
    constructor() {
        this.points = [];
        this.base = null;
        this.policy = null;
    }
    reset() {
        this.points = [];
        this.base = null;
        this.policy = null;
    }
    observe(now, session, core) {
        if (!Number.isFinite(now) || now < 0) return false;
        const valid =
            !core.stale &&
            core.samples > 0 &&
            validNs(core.p50?.total_ns) &&
            validNs(core.p99?.total_ns);
        const base = JSON.stringify([
            session,
            core.source,
            core.boundary_label,
        ]);
        const policy = valid
            ? JSON.stringify([
                  core.receive_revisions,
                  core.processing_revisions,
                  core.p50.trace.generation,
                  core.p99.trace.generation,
              ])
            : null;
        if (
            base !== this.base ||
            (valid && this.policy !== null && policy !== this.policy)
        )
            this.reset();
        this.base = base;
        if (valid) this.policy = policy;
        if (this.points.length && now < this.points.at(-1).at) {
            this.reset();
            return false;
        }
        if (!valid) return this.gap(now);
        if (this.points.length && now - this.points.at(-1).at < 1000)
            return false;
        this.points.push({
            at: now,
            p50: core.p50.total_ns,
            p99: core.p99.total_ns,
            samples: core.samples,
            p50Uncertainty: validBudget(core.p50),
            p99Uncertainty: validBudget(core.p99),
        });
        this.trim(now);
        return true;
    }
    gap(now) {
        if (!Number.isFinite(now) || now < 0) return false;
        const point = { at: now, p50: null, p99: null, samples: 0 };
        if (this.points.length && now - this.points.at(-1).at < 1000)
            this.points[this.points.length - 1] = point;
        else this.points.push(point);
        this.trim(now);
        return true;
    }
    trim(now) {
        this.points = this.points
            .filter((p) => p.at >= now - 300000)
            .slice(-320);
    }
    view(now, seconds) {
        if (seconds !== 60 && seconds !== 300)
            throw new Error("History range must be 60 or 300 seconds");
        this.trim(now);
        const points = this.points.filter((p) => p.at >= now - seconds * 1000);
        const segments = [];
        let segment = null,
            previous = null,
            max = 0;
        for (const point of points) {
            if (point.p50 === null || point.p99 === null) {
                segment = null;
                previous = null;
                continue;
            }
            if (!segment || point.at - previous.at > 2500) {
                segment = [];
                segments.push(segment);
            }
            segment.push(point);
            previous = point;
            max = Math.max(max, point.p50, point.p99);
        }
        const bands = { p50: [], p99: [] };
        for (const name of ["p50", "p99"])
            for (const line of segments) {
                let band = null;
                for (const point of line) {
                    const uncertainty = point[name + "Uncertainty"],
                        upper = point[name] + uncertainty;
                    if (!validNs(uncertainty) || !validNs(upper)) {
                        band = null;
                        continue;
                    }
                    if (!band) {
                        band = [];
                        bands[name].push(band);
                    }
                    band.push({
                        at: point.at,
                        lower: Math.max(0, point[name] - uncertainty),
                        upper,
                    });
                    max = Math.max(max, upper);
                }
            }
        return {
            segments,
            bands,
            max,
            points: points.length,
            latest: points.at(-1) ?? null,
        };
    }
}
