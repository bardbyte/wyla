/** Semantics Explorer. Metrics browse in the steward order (certified
 * first, then the unreviewed bulk, then pending specs), filterable by
 * status AND by table, searchable; every row shows the name and the
 * exact calculation. Every column header carries an ⓘ explaining
 * what the number means. Nothing gets cut off silently: long names
 * ellipsize with the full value on hover. */

import { api } from "../api.js";
import { card, esc, loading, tierChip, unavailable } from "../ui.js";

const STATUSES = ["", "certified", "unreviewed", "pending_certification"];
const STATUS_LABEL = {
  "": "all", certified: "certified", unreviewed: "unreviewed",
  pending_certification: "pending",
};

const INFO = {
  metric: "The metric's name (mined ones without a steward name show "
    + "their fingerprint) and, underneath, the exact canonical "
    + "calculation. Hover a truncated calculation to read all of it.",
  status: "certified: steward-certified in the Data Marketplace · "
    + "unreviewed: real usage evidence, no steward decision yet · "
    + "pending: a spec submitted for certification",
  witnesses: "Independent evidence families that attest this metric, "
    + "with how many real sightings each contributed",
  uses: "Support: distinct real queries observed computing exactly "
    + "this calculation",
  usedby: "Org units seen running it in the 30-day activity window",
  table: "The table this calculation reads. Click to open its profile",
  lob: "The line of business a steward mapped the table to",
  columns: "Columns BigQuery can actually serve for this table",
  metricsCol: "Witnessed metrics computed on this table",
  joins: "Join evidence touching this table (declared, measured, or "
    + "certified-SQL scoped)",
  tickets: "Open data-quality tickets from the structural census, "
    + "e.g. columns declared upstream that BigQuery cannot serve "
    + "(phantom columns). Zero is good news",
};

const th = (label, info) =>
  `<span>${esc(label)}<span class="info" title="${esc(info)}"
     aria-label="${esc(info)}">ⓘ</span></span>`;

export async function renderSemantics(outlet) {
  const state = { mode: "metrics", q: "", status: "", table: "" };
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <span class="muted">Understand · Semantics Explorer</span>
    </div>
    <div class="card toolbar">
      <div class="pills" id="mode-pills"></div>
      <input class="search" id="search"
        placeholder="search name, calculation, question…" />
      <div class="pills" id="status-pills"></div>
      <select class="search" id="table-filter" title="${esc(
        "show only metrics computed on one table")}">
        <option value="">all tables</option>
      </select>
      <span class="spacer"></span><span class="muted" id="count"></span>
    </div>
    <div id="list">${loading()}</div>
    <div class="legend"><span class="muted">promote and request-naming
      arrive with the steward loop (B2); writes go through the clerk.
      This page is a projection, never a second write path</span></div>`;

  const tablesPayload = await api.tables();
  const tableSelect = outlet.querySelector("#table-filter");
  if (tablesPayload.available) {
    for (const r of tablesPayload.rows) {
      const option = document.createElement("option");
      option.value = r.physical;
      option.textContent =
        `${r.physical} (${r.metrics_here})`;
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
    pills(outlet.querySelector("#mode-pills"), ["metrics", "tables"],
      state.mode, (v) => { state.mode = v; draw(); });
    pills(outlet.querySelector("#status-pills"), STATUSES, state.status,
      (v) => { state.status = v; draw(); }, STATUS_LABEL);
    const metricsMode = state.mode === "metrics";
    outlet.querySelector("#search").style.display =
      metricsMode ? "" : "none";
    tableSelect.style.display = metricsMode ? "" : "none";
    const list = outlet.querySelector("#list");
    list.innerHTML = loading();
    if (metricsMode) {
      const data = await api.metrics(
        { q: state.q, status: state.status, table: state.table });
      if (!list.isConnected) return;
      if (!data.available) { list.innerHTML = unavailable(data.reason); return; }
      outlet.querySelector("#count").textContent =
        `${data.shown} of ${data.total}`;
      list.innerHTML = data.rows.length ? `
        <div class="card table-card">
          <div class="thead cols-metrics">
            ${th("METRIC · CALCULATION", INFO.metric)}
            ${th("STATUS", INFO.status)}
            ${th("WITNESSES", INFO.witnesses)}
            ${th("USES", INFO.uses)}
            ${th("USED BY", INFO.usedby)}
            ${th("TABLE", INFO.table)}
          </div>
          ${data.rows.map((r) => `
          <div class="trow cols-metrics">
            <div class="cell-name">
              <a class="linklike clip" title="${esc(r.label || r.id)}"
                href="#/metric/${encodeURIComponent(r.id)}">${
                esc(r.label) || `${esc((r.fp || r.id).slice(0, 12))}… “?”`}</a>
              <span class="expr" title="${esc(r.expr)}">${esc(r.expr)}</span>
            </div>
            ${tierChip(r.tier,
              STATUS_LABEL[r.status_served] ?? r.status_served)}
            <span class="muted clip" title="${esc(
              Object.entries(r.witnesses)
                .map(([w, n]) => `${w} ×${n}`).join(", ") || "none yet")}">${
              Object.entries(r.witnesses)
                .map(([w, n]) => `${esc(w)}×${n}`).join(" · ") || "?"}</span>
            <span class="mono">${r.support}</span>
            <span class="muted clip" title="${esc(
              Object.entries(r.used_by)
                .map(([u, n]) => `${u}: ${n}`).join(", ") || "none recorded")}">${
              Object.entries(r.used_by)
                .map(([u, n]) => `${esc(u)} ${n}`).join(" · ") || "?"}</span>
            <span class="clip">
              ${r.table
                ? `<a class="linklike mono clip" title="${esc(r.table)}"
                    href="#/table/${encodeURIComponent(r.table)}">${
                    esc(r.table.split(".").pop())}</a>`
                : `<span class="muted">?</span>`}
            </span>
          </div>`).join("")}
        </div>`
        : card("", `<p class="muted">no metrics match. A narrower
            filter, or nothing here yet: nothing here means the graph
            holds no witness for it.</p>`, "empty");
    } else {
      const data = tablesPayload.available ? tablesPayload
        : await api.tables();
      if (!list.isConnected) return;
      if (!data.available) { list.innerHTML = unavailable(data.reason); return; }
      outlet.querySelector("#count").textContent =
        `${data.rows.length} tables`;
      list.innerHTML = `
        <div class="card table-card">
          <div class="thead cols-tables">
            ${th("TABLE", INFO.table)}
            ${th("LOB", INFO.lob)}
            ${th("COLUMNS", INFO.columns)}
            ${th("METRICS", INFO.metricsCol)}
            ${th("JOINS", INFO.joins)}
            ${th("DQ TICKETS", INFO.tickets)}
          </div>
          ${data.rows.map((r) => `
          <div class="trow cols-tables">
            <a class="linklike mono clip" title="${esc(r.physical)}"
              href="#/table/${encodeURIComponent(r.physical)}">${
              esc(r.physical)}</a>
            <span>${esc(r.lob) || `<span class="muted">unmapped</span>`}</span>
            <span class="mono">${r.columns}</span>
            <span class="mono">${r.metrics_here}</span>
            <span class="mono">${r.joins}</span>
            <span class="${r.tickets ? "warn" : "muted"}" title="${esc(
              INFO.tickets)}">${r.tickets || "0 ✓"}</span>
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
