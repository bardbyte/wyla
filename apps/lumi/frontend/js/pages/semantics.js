/** Semantics Explorer — metrics + tables (the artboard's browse
 * surface). Rows link to real profile URLs (#/metric/…, #/table/…). */

import { api } from "../api.js";
import { card, esc, loading, tierChip, unavailable } from "../ui.js";

const STATUSES = ["", "certified", "pending_certification", "unreviewed"];

export async function renderSemantics(outlet) {
  const state = { mode: "metrics", q: "", status: "" };
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <span class="muted">Understand · Semantics Explorer</span>
    </div>
    <div class="card toolbar">
      <div class="pills" id="mode-pills"></div>
      <input class="search" id="search" placeholder="search label, SQL, question…" />
      <div class="pills" id="status-pills"></div>
      <span class="spacer"></span><span class="muted" id="count"></span>
    </div>
    <div id="list">${loading()}</div>
    <div class="legend"><span class="muted">promote / request-naming
      arrive with the steward loop (B2) — writes go through the clerk;
      this page is a projection, never a second write path</span></div>`;

  const pills = (host, options, value, onPick, labels = {}) => {
    host.innerHTML = options.map((option) =>
      `<button class="pill ${option === value ? "on" : ""}"
        data-v="${esc(option)}">${esc(labels[option] ?? (option || "all"))}
       </button>`).join("");
    host.onclick = (e) => {
      const v = e.target?.dataset?.v;
      if (v !== undefined) onPick(v);
    };
  };

  async function draw() {
    pills(outlet.querySelector("#mode-pills"), ["metrics", "tables"],
      state.mode, (v) => { state.mode = v; draw(); });
    pills(outlet.querySelector("#status-pills"), STATUSES, state.status,
      (v) => { state.status = v; draw(); });
    outlet.querySelector("#search").style.display =
      state.mode === "metrics" ? "" : "none";
    const list = outlet.querySelector("#list");
    list.innerHTML = loading();
    if (state.mode === "metrics") {
      const data = await api.metrics({ q: state.q, status: state.status });
      if (!list.isConnected) return;
      if (!data.available) { list.innerHTML = unavailable(data.reason); return; }
      outlet.querySelector("#count").textContent =
        `${data.shown} of ${data.total}`;
      list.innerHTML = data.rows.length ? `
        <div class="card table-card">
          <div class="thead cols-metrics"><span>METRIC</span>
            <span>STATUS</span><span>WITNESSES</span><span>USES</span>
            <span>USED BY</span><span>TABLE</span></div>
          ${data.rows.map((r) => `
          <div class="trow cols-metrics">
            <div class="cell-name">
              <a class="linklike" href="#/metric/${encodeURIComponent(r.id)}">${
                esc(r.label) || `${esc((r.fp || r.id).slice(0, 12))}… “?”`}</a>
              <span class="expr">${esc(r.expr)}</span>
            </div>
            ${tierChip(r.tier, r.status_served)}
            <span class="muted">${Object.entries(r.witnesses)
              .map(([w, n]) => `${esc(w)}×${n}`).join(" · ") || "—"}</span>
            <span class="mono">${r.support}</span>
            <span class="muted">${Object.entries(r.used_by)
              .map(([u, n]) => `${esc(u)} ${n}`).join(" · ") || "—"}</span>
            <span>${r.table
              ? `<a class="linklike mono" href="#/table/${
                  encodeURIComponent(r.table)}">${
                  esc(r.table.split(".").pop())}</a>`
              : `<span class="muted">—</span>`}</span>
          </div>`).join("")}
        </div>`
        : card("", `<p class="muted">no metrics match — a narrower
            filter, or nothing here yet. Nothing here means the graph
            holds no witness for it.</p>`, "empty");
    } else {
      const data = await api.tables();
      if (!list.isConnected) return;
      if (!data.available) { list.innerHTML = unavailable(data.reason); return; }
      outlet.querySelector("#count").textContent =
        `${data.rows.length} tables`;
      list.innerHTML = `
        <div class="card table-card">
          <div class="thead cols-tables"><span>TABLE</span><span>LOB</span>
            <span>COLUMNS</span><span>METRICS</span><span>JOINS</span>
            <span>TICKETS</span></div>
          ${data.rows.map((r) => `
          <div class="trow cols-tables">
            <a class="linklike mono" href="#/table/${
              encodeURIComponent(r.physical)}">${esc(r.physical)}</a>
            <span>${esc(r.lob) || `<span class="muted">unmapped</span>`}</span>
            <span class="mono">${r.columns}</span>
            <span class="mono">${r.metrics_here}</span>
            <span class="mono">${r.joins}</span>
            <span class="${r.tickets ? "warn" : "muted"}">${
              r.tickets || "—"}</span>
          </div>`).join("")}
        </div>`;
    }
  }

  let debounce = 0;
  outlet.querySelector("#search").addEventListener("input", (e) => {
    clearTimeout(debounce);
    debounce = setTimeout(() => { state.q = e.target.value; draw(); }, 250);
  });
  await draw();
}
