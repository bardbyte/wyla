/** Semantics Explorer: the metrics browser. Steward order
 * (certified first, then the unreviewed bulk, then pending specs),
 * filterable by status AND by table, searchable; every row shows
 * the name and the exact calculation, every header carries an ⓘ.
 * Tables live on their own tab now. */

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
};

const th = (label, info) =>
  `<span>${esc(label)}<span class="info" title="${esc(info)}"
     aria-label="${esc(info)}">ⓘ</span></span>`;

export async function renderSemantics(outlet) {
  const state = { q: "", status: "", table: "" };
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <span class="muted">Understand · Semantics Explorer</span>
      <a class="linklike" href="#/tables">tables →</a>
    </div>
    <div class="card toolbar">
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
    pills(outlet.querySelector("#status-pills"), STATUSES, state.status,
      (v) => { state.status = v; draw(); }, STATUS_LABEL);
    const list = outlet.querySelector("#list");
    list.innerHTML = loading();
    {
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
    }
  }

  let debounce = 0;
  outlet.querySelector("#search").addEventListener("input", (e) => {
    clearTimeout(debounce);
    debounce = setTimeout(() => { state.q = e.target.value; draw(); }, 250);
  });
  await draw();
}
