/** Tables: the table explorer as its own tab. Search across
 * physical names and LOBs, every column explained with an ⓘ, rows
 * open the Table Profile. */

import { api } from "../api.js";
import { card, esc, loading, unavailable } from "../ui.js";

export const TABLE_INFO = {
  table: "The physical table. Click to open its profile",
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

export async function renderTables(outlet) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <span class="muted">Understand · Tables</span>
      <input class="search" id="t-search"
        placeholder="search table or line of business…" />
      <span class="spacer"></span><span class="muted" id="t-count"></span>
    </div>
    <div id="t-list">${loading()}</div>`;

  const payload = await api.tables();
  const list = outlet.querySelector("#t-list");
  if (!list) return;
  if (!payload.available) {
    list.innerHTML = unavailable(payload.reason);
    return;
  }

  const draw = (needle = "") => {
    const q = needle.trim().toLowerCase();
    const rows = payload.rows.filter((r) =>
      !q || r.physical.toLowerCase().includes(q)
      || (r.lob || "").toLowerCase().includes(q));
    outlet.querySelector("#t-count").textContent =
      `${rows.length} of ${payload.rows.length} tables`;
    list.innerHTML = rows.length ? `
      <div class="card table-card">
        <div class="thead cols-tables">
          ${th("TABLE", TABLE_INFO.table)}
          ${th("LOB", TABLE_INFO.lob)}
          ${th("COLUMNS", TABLE_INFO.columns)}
          ${th("METRICS", TABLE_INFO.metricsCol)}
          ${th("JOINS", TABLE_INFO.joins)}
          ${th("DQ TICKETS", TABLE_INFO.tickets)}
        </div>
        ${rows.map((r) => `
        <div class="trow cols-tables">
          <a class="linklike mono clip" title="${esc(r.physical)}"
            href="#/table/${encodeURIComponent(r.physical)}">${
            esc(r.physical)}</a>
          <span>${esc(r.lob) || `<span class="muted">unmapped</span>`}</span>
          <span class="mono">${r.columns}</span>
          <span class="mono">${r.metrics_here}</span>
          <span class="mono">${r.joins}</span>
          <span class="${r.tickets ? "warn" : "muted"}" title="${esc(
            TABLE_INFO.tickets)}">${r.tickets || "0 ✓"}</span>
        </div>`).join("")}
      </div>`
      : card("", `<p class="muted">no table matches
          "${esc(needle)}"</p>`, "empty");
  };

  let debounce = 0;
  outlet.querySelector("#t-search").addEventListener("input", (e) => {
    clearTimeout(debounce);
    debounce = setTimeout(() => draw(e.target.value), 200);
  });
  draw();
}
