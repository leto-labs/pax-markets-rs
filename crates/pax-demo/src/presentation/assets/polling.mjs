// One request at a time. Suspension invalidates even a reply whose fetch ignores abort.
export class Poller {
    constructor({ load, receive, fail, interval }) {
        Object.assign(this, { load, receive, fail, interval });
        this.suspended = true;
        this.epoch = 0;
    }
    suspend() {
        this.suspended = true;
        this.epoch++;
        clearTimeout(this.timer);
        this.controller?.abort();
    }
    resume() {
        this.suspended = false;
        this.urgent = true;
        clearTimeout(this.timer);
        this.run();
    }
    async run() {
        if (this.suspended || this.controller) return;
        this.urgent = false;
        const epoch = this.epoch;
        const controller = (this.controller = new AbortController());
        const current = () => !this.suspended && epoch === this.epoch;
        try {
            const result = await this.load(controller.signal);
            if (current()) this.receive(result);
        } catch (error) {
            if (current()) this.fail(error);
        } finally {
            this.controller = null;
            if (!this.suspended) {
                if (this.urgent) this.run();
                else this.timer = setTimeout(() => this.run(), this.interval());
            }
        }
    }
}

// Browser monotonic time only: independent of book activity and clock sampling.
export class Freshness {
    observe(session, publication, now) {
        if (session !== this.session || publication !== this.publication)
            this.progress = now;
        Object.assign(this, {
            session,
            publication,
            receipt: now,
            failed: false,
        });
    }
    fail() {
        this.failed = true;
    }
    status(now, live, limit = 3000) {
        const age = this.receipt == null ? null : now - this.receipt;
        if (this.failed || age == null || age > limit)
            return {
                age,
                stale: true,
                reason: "Waiting for a fresh display response.",
            };
        if (live && now - this.progress > limit)
            return {
                age,
                stale: true,
                reason: "Server publication stopped advancing; displayed books may be stale.",
            };
        return { age, stale: false };
    }
}
