import { duration, bookInputAge } from "/timing.mjs";
import { $, escape } from "/elements.mjs";

// Keep panels and disclosure controls mounted across publications. Quiet books
// only update their age; ladder scroll positions and keyboard focus stay intact.
export function books(report) {
    const host = $("books");
    const existing = new Map(
        [...host.children].map((panel) => [panel.dataset.marketId, panel]),
    );
    for (const market of report.view.markets) {
        let panel = existing.get(String(market.id));
        if (!panel?.querySelector('[data-field="age"]')) {
            const replacement = document.createElement("section");
            replacement.className = "panel";
            replacement.dataset.marketId = market.id;
            replacement.innerHTML = `<div class="title"><h2 data-field="name"></h2><span class="badge" data-field="status"></span></div>
        <div class="book-meta"><span data-field="spread"></span><span data-field="sequence"></span><span data-field="age"></span></div>
        <div class="ladders"><div><h3>Bids</h3><table class="bid"><thead><tr><th>Price</th><th>Quantity</th></tr></thead><tbody data-field="bids"></tbody></table></div><div><h3>Asks</h3><table class="ask"><thead><tr><th>Price</th><th>Quantity</th></tr></thead><tbody data-field="asks"></tbody></table></div></div>
        <details class="depth-details"><summary>Liquidity details</summary><div class="liquidity" data-field="liquidity"></div></details>`;
            if (panel) panel.replaceWith(replacement);
            else host.append(replacement);
            panel = replacement;
        }
        existing.delete(String(market.id));
        const set = (field, value, html = false) => {
            const node = panel.querySelector(`[data-field="${field}"]`);
            if (node.dataset.value === value) return;
            node.dataset.value = value;
            if (html) node.innerHTML = value;
            else node.textContent = value;
        };
        set("name", market.name);
        set("status", market.status);
        set("spread", `Spread ${market.spread ?? "—"}`);
        set("sequence", `Seq ${market.sequence ?? "—"}`);
        const age = bookInputAge(report, market);
        set(
            "age",
            `Last book update ${age == null ? "—" : duration(age)} ${report.view.mode === "live" ? "at publication" : "at snapshot"}`,
        );
        for (const side of ["bids", "asks"])
            set(
                side,
                market[side]
                    .map(
                        (level) =>
                            `<tr><td title="${escape(level.ticks)} ticks">${escape(level.price)}</td><td title="${escape(level.native_quantity)} native units">${escape(level.quantity)}</td></tr>`,
                    )
                    .join("") ||
                    '<tr><td colspan="2">No usable levels</td></tr>',
                true,
            );
        set(
            "liquidity",
            `<div>Represented depth: ${market.bid_levels} bid / ${market.ask_levels} ask levels<br>Base native units: ${escape(market.bid_quantity)} bid / ${escape(market.ask_quantity)} ask</div>
      <div>Valid coverage: ${duration(Number(market.coverage.valid_ns))} / ${duration(Number(market.coverage.observed_ns))}<br>Time-weighted spread: ${escape(market.coverage.mean_spread_ticks)} ticks</div>
      ${market.estimates.map((estimate) => `<div>${escape(estimate.side)} ${escape(estimate.requested)} native<br>Avg price ${escape(estimate.average_price)} · shortfall ${escape(estimate.shortfall)}</div>`).join("")}`,
            true,
        );
    }
    for (const panel of existing.values()) panel.remove();
}
