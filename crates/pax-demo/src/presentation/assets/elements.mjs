import { duration } from "/timing.mjs";
export const $ = (id) => document.getElementById(id);
export const escape = (value) =>
    String(value ?? "—").replace(
        /[&<>"']/g,
        (c) =>
            ({
                "&": "&amp;",
                "<": "&lt;",
                ">": "&gt;",
                '"': "&quot;",
                "'": "&#39;",
            })[c],
    );
export function svgNode(tag, attrs = {}) {
    const node = document.createElementNS("http://www.w3.org/2000/svg", tag);
    for (const [key, value] of Object.entries(attrs))
        node.setAttribute(key, value);
    return node;
}
// Reuse stage controls so periodic reports cannot detach a focused or pressed node.
const charts = new WeakMap();
export function chart(id, parts, title) {
    const host = $(id + "-chart");
    if (!host) return;
    const scrollLeft = host.scrollLeft;
    const details = $(id + "-detail");
    const total = parts.reduce((a, p) => a + p.ns, 0);
    let selectedDetail = "Select a stage for its duration and method.";
    if (!total || parts.some((p) => !Number.isFinite(p.ns) || p.ns < 0)) {
        details.textContent = selectedDetail;
        host.textContent = "No valid measurement in this window.";
        charts.delete(host);
        return;
    }
    const width =
        Math.max(280, host.clientWidth) * Number($("zoom")?.value || 1);
    let state = charts.get(host);
    if (!state) {
        const svg = svgNode("svg", { height: 104, role: "group" });
        const parent = svgNode("rect", {
            x: 0,
            y: 4,
            height: 30,
            rx: 3,
            fill: "#344953",
        });
        const rootLabel = svgNode("text", {
            x: 10,
            y: 24,
            fill: "#fff",
            "font-size": 12,
        });
        const end = svgNode("text", {
            y: 99,
            fill: "#9aacb5",
            "text-anchor": "end",
            "font-size": 11,
        });
        svg.append(parent, rootLabel, end);
        const buttons = document.createElement("div");
        buttons.className = "controls";
        state = { svg, parent, rootLabel, end, buttons, stages: new Map() };
        charts.set(host, state);
        host.replaceChildren(svg, buttons);
    }
    state.svg.setAttribute("width", width);
    state.svg.setAttribute("aria-label", `${title}: ${duration(total)}`);
    state.parent.setAttribute("width", width);
    state.rootLabel.textContent = `${title} · ${duration(total)}`;
    state.end.setAttribute("x", width - 3);
    state.end.textContent = duration(total);
    const retained = new Set();
    let x = 0;
    for (const [index, part] of parts.entries()) {
        retained.add(part.label);
        let stage = state.stages.get(part.label);
        if (!stage) {
            const rect = svgNode("rect", {
                y: 39,
                height: 40,
                role: "button",
                tabindex: 0,
            });
            const tip = svgNode("title");
            rect.append(tip);
            const label = svgNode("text", {
                y: 63,
                fill: "#0c191e",
                "font-size": 11,
                "pointer-events": "none",
            });
            const button = document.createElement("button");
            button.type = "button";
            stage = { rect, tip, label, button, detail: "" };
            state.stages.set(part.label, stage);
            const select = () => {
                host.dataset.selectedStage = part.label;
                details.textContent = stage.detail;
            };
            rect.addEventListener("click", select);
            rect.addEventListener("keydown", (e) => {
                if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    select();
                }
            });
            button.addEventListener("click", select);
            state.svg.insertBefore(rect, state.end);
            state.svg.insertBefore(label, state.end);
        }
        const w = (width * part.ns) / total;
        const formatted = duration(part.ns);
        stage.detail = `${part.label}: ${formatted} · ${((100 * part.ns) / total).toFixed(2)}% of ${title}. ${part.note || "Local measured duration."}`;
        if (host.dataset.selectedStage === part.label)
            selectedDetail = stage.detail;
        stage.rect.setAttribute("x", x);
        stage.rect.setAttribute("width", w);
        stage.rect.setAttribute("fill", part.color);
        stage.rect.setAttribute("aria-label", `${part.label}: ${formatted}`);
        stage.tip.textContent = `${part.label}: ${formatted}`;
        stage.label.setAttribute("x", x + 5);
        stage.label.textContent =
            w > 78 ? part.label.slice(0, Math.floor((w - 10) / 6)) : "";
        stage.button.textContent = `${part.label} · ${formatted}`;
        stage.button.style.borderColor = part.color;
        const previous = state.buttons.children[index];
        if (previous !== stage.button)
            state.buttons.insertBefore(stage.button, previous || null);
        x += w;
    }
    for (const [label, stage] of state.stages)
        if (!retained.has(label)) {
            stage.rect.remove();
            stage.label.remove();
            stage.button.remove();
            state.stages.delete(label);
        }
    details.textContent = selectedDetail;
    host.scrollLeft = scrollLeft;
}
