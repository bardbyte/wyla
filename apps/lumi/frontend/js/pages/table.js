/** Table Profile: columns as servable, metrics-here, scope-honest
 * joins, the 30-day cost prior, and the served card verbatim. */

import { api } from "../api.js";
import {
  card, esc, feedbackBar, loading, unavailable,
} from "../ui.js";

export async function renderTable(outlet, physical) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <a class="linklike" href="#/semantics">← Semantics Explorer</a>
      <span class="muted">› Table Profile</span>
    </div>
    <div id="siblings"></div>
    <div id="profile">${loading()}</div>`;
  const [detail, tablesPayload] = await Promise.all(
    [api.table(physical), api.tables()]);
  const host = outlet.querySelector("#profile");
  if (!host) return;
  // hop between tables without leaving the profile
  if (tablesPayload.available && tablesPayload.rows.length > 1) {
    outlet.querySelector("#siblings").innerHTML = `
      <div class="card subnav"><span class="muted">tables:</span>
        ${tablesPayload.rows.map((r) => `
          <a class="chip mono linkchip ${
              r.physical === physical ? "on" : ""}"
            href="#/table/${encodeURIComponent(r.physical)}"
            title="${esc(r.physical)} · ${r.metrics_here} metrics">${
            esc(r.short)}</a>`).join("")}
      </div>`;
  }
  if (!detail.available) { host.innerHTML = unavailable(detail.reason); return; }
  if (!detail.found) {
    host.innerHTML = card("", `<p>${esc(physical)} is not in the
      promoted build</p>
      <a class="btn" href="#/semantics">← back</a>`, "empty");
    return;
  }
  const columns = Object.entries(detail.columns ?? {});
  const prior = detail.cost_prior;

  host.innerHTML = `
    <div class="card">
      <div class="profile-head">
        <span class="profile-title mono">${esc(physical)}</span>
        <span class="muted">${columns.length} columns servable</span>
      </div>
      ${prior ? `<div class="muted">usage prior: p50
        <span class="mono">${esc(String(prior.p50 ?? "-"))}</span> · p95
        <span class="mono">${esc(String(prior.p95 ?? "-"))}</span>
        bytes/query (30-day activity)</div>` : ""}
    </div>

    <div class="grid2">
      ${card(`METRICS ON THIS TABLE: ${
          (detail.metrics_here ?? []).length}`, `
        ${(detail.metrics_here ?? []).length === 0
          ? `<span class="muted">no witnessed metric yet: ask about
             this table and the mining will catch up</span>`
          : (detail.metrics_here ?? []).map((mh) => `
            <div class="family-row">
              <a class="linklike" href="#/metric/${
                encodeURIComponent(mh.id)}">${esc(mh.label || mh.id)}</a>
              <span class="chip">${esc(mh.status_served)}</span>
              <span class="mono muted">${mh.support}</span>
            </div>`).join("")}`)}
      ${card("JOINS: how, with evidence", `
        ${(detail.joins ?? []).length === 0
          ? `<span class="muted">no join evidence on record: co-usage
             alone is not a join</span>`
          : (detail.joins ?? []).map((j) => `
            <div class="joinrow">
              <a class="linklike mono" href="#/table/${
                encodeURIComponent(j.a === physical ? j.b : j.a)}">${
                esc(j.a === physical ? j.b : j.a)}</a>
              <span class="chip">${esc(j.source)}</span>
              ${j.scope === "scoped_only"
                ? `<span class="warn">◐ CTE-scoped: NOT raw-safe</span>`
                : ""}
              ${j.on ? `<span class="mono muted">on ${
                esc(Array.isArray(j.on) ? j.on.join(" AND ") : j.on)}
                </span>` : ""}
            </div>`).join("")}`)}
    </div>

    ${card("COLUMNS: as BigQuery can serve them",
      `<div class="colchips">${columns.map(([name, type]) =>
        `<span class="chip mono">${esc(name.split(".").pop())}
          <span class="muted">${esc(type)}</span></span>`).join("")}
       </div>`)}

    ${detail.card ? card("THE SERVED CARD: what the agent reads",
      `<pre class="block">${esc(detail.card)}</pre>`) : ""}

    <div class="legend"><span class="spacer"></span><span id="fb"></span></div>`;
  host.querySelector("#fb").replaceWith(
    feedbackBar("table_profile", physical, api.feedback));
}
