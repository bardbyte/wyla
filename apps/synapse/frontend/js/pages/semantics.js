/** Metrics Explorer: the metrics as cards. Published first, then the
 * unreviewed bulk, then pending specs; filterable by status and by
 * data product, searchable. A card shows the name, the exact
 * calculation, the question it answers, where it is computed, its
 * grain and usual dimensions, and how much real use stands behind
 * it. A card opens the metric profile. */

import { api } from "../api.js";
import {
  card, esc, loading, statusLabel, tierChip, unavailable,
} from "../ui.js";

const STATUSES = ["", "certified", "unreviewed", "pending_certification"];
const STATUS_LABEL = {
  "": "all", certified: statusLabel("certified"),
  unreviewed: statusLabel("unreviewed"),
  pending_certification: statusLabel("pending_certification"),
};
const fmt = (n) => new Intl.NumberFormat().format(n);
const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;

export function metricCard(r) {
  const usedBy = Object.keys(r.used_by || {});
  const meta = [
    r.table ? `on <span class="mono">${esc(r.table.split(".").pop())}</span>`
            : "",
    r.grain ? `grain ${esc(r.grain)}` : "",
    (r.dimensions || []).length
      ? `by ${esc(r.dimensions.slice(0, 3).join(", "))}` : "",
    r.lob ? esc(r.lob) : "",
    r.domain ? esc(r.domain) : "",
  ].filter(Boolean);
  const stats = [
    `<span><b class="mono">${r.support}</b> uses</span>`,
    `<span><b class="mono">${r.agreement}</b> witness ${
      r.agreement === 1 ? "family" : "families"}</span>`,
    r.execution_count
      ? `<span><b class="mono">${fmt(r.execution_count)}</b> executions</span>`
      : "",
    usedBy.length
      ? `<span>used by ${esc(usedBy.slice(0, 3).join(", "))}</span>` : "",
  ].filter(Boolean);
  const guidance = r.description
    ? r.description.slice(0, 160) + (r.description.length > 160 ? "…" : "")
    : "";
  return `
    <a class="card metric-card" href="#/metric/${
      encodeURIComponent(r.id)}" title="${esc(r.label || r.id)}">
      <div class="metric-head">
        <span class="metric-name">${esc(r.label)
          || `${esc((r.fp || r.id).slice(0, 12))}… “?”`}</span>
        ${tierChip(r.tier, statusLabel(r.status_served))}
      </div>
      <code class="sqlchip" title="${esc(r.expr)}">${esc(r.expr)}</code>
      ${r.question ? `<p class="metric-question">${esc(r.question)}</p>`
                   : ""}
      ${meta.length ? `<div class="product-meta muted">${
        meta.map((m) => `<span>${m}</span>`).join("")}</div>` : ""}
      <div class="product-stats">${stats.join("")}</div>
      ${guidance ? `<p class="metric-guidance muted">${esc(guidance)}</p>`
                 : ""}
    </a>`;
}

export async function renderMetrics(outlet) {
  const state = { q: "", status: "", table: "" };
  outlet.innerHTML = `
    <div class="library-page">
      <div class="page-head">
        <h1>Metrics Explorer</h1>
        <p class="muted">Every metric the build holds, published first.
        Each card shows the exact calculation and the real use behind
        it; open one for its full definition, lineage and receipts.</p>
      </div>
      <div class="library-tools">
        <input class="search" id="search"
          placeholder="search name, calculation, question…" />
        <div class="pills" id="status-pills"></div>
        <select class="search" id="table-filter"
          title="show only metrics computed on one data product">
          <option value="">all data products</option>
        </select>
        <span class="muted" id="count"></span>
      </div>
      <div class="card-grid" id="list">${loading()}</div>
    </div>`;

  const tablesPayload = await api.tables();
  const tableSelect = outlet.querySelector("#table-filter");
  if (tablesPayload.available) {
    for (const r of tablesPayload.rows) {
      const option = document.createElement("option");
      option.value = r.physical;
      option.textContent = `${r.short || r.physical} (${r.metrics_here})`;
      tableSelect.appendChild(option);
    }
  }
  tableSelect.addEventListener("change", () => {
    state.table = tableSelect.value;
    draw();
  });

  const pills = (host, options, value, onPick, labels = {}) => {
    host.innerHTML = options.map((option) =>
      `<button class="pill ${option === value ? "on" : ""}"
        data-v="${esc(option)}">${
        esc(labels[option] ?? (option || "all"))}</button>`).join("");
    host.onclick = (e) => {
      const v = e.target?.dataset?.v;
      if (v !== undefined) onPick(v);
    };
  };

  async function draw() {
    pills(outlet.querySelector("#status-pills"), STATUSES, state.status,
      (v) => { state.status = v; draw(); }, STATUS_LABEL);
    const list = outlet.querySelector("#list");
    list.innerHTML = loading();
    const data = await api.metrics(
      { q: state.q, status: state.status, table: state.table });
    if (!list.isConnected) return;
    if (!data.available) { list.innerHTML = unavailable(data.reason); return; }
    outlet.querySelector("#count").textContent =
      `${data.shown} of ${plural(data.total, "metric")}`;
    list.innerHTML = data.rows.length
      ? data.rows.map(metricCard).join("")
      : card("", `<p class="muted">no metric matches. A narrower filter,
          or nothing here yet: nothing here means the graph holds no
          witness for it.</p>`, "empty");
  }

  let debounce = 0;
  outlet.querySelector("#search").addEventListener("input", (e) => {
    clearTimeout(debounce);
    debounce = setTimeout(() => { state.q = e.target.value; draw(); }, 250);
  });
  await draw();
}
