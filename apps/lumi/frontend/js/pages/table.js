/** Table Profile. Switch tables in place (searchable select plus
 * prev/next), toggle the metrics here by served status, columns as
 * servable, scope-honest joins, the 30-day cost prior — and "what
 * the agent sees": the served card in the pullout reader, exactly
 * the bytes the agent reads. */

import { api } from "../api.js";
import { createPullout } from "../pullout.js";
import {
  card, esc, feedbackBar, loading, unavailable,
} from "../ui.js";

const STATUS_LABEL = {
  certified: "certified", unreviewed: "unreviewed",
  pending_certification: "pending", team_candidate: "team",
};

export async function renderTable(outlet, physical) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <a class="linklike" href="#/tables">← Tables</a>
      <span class="muted">› Table Profile</span>
      <span class="spacer"></span>
      <button class="icon-btn" id="t-prev" title="previous table">‹</button>
      <select class="search" id="t-pick"
        title="jump to another table"></select>
      <button class="icon-btn" id="t-next" title="next table">›</button>
    </div>
    <div id="profile">${loading()}</div>`;
  const pullout = createPullout(outlet);

  const [detail, tablesPayload] = await Promise.all(
    [api.table(physical), api.tables()]);
  const host = outlet.querySelector("#profile");
  if (!host) return pullout.teardown;

  // the switcher: every table, current selected, arrows step through
  const all = tablesPayload.available
    ? tablesPayload.rows.map((r) => r.physical) : [];
  const pick = outlet.querySelector("#t-pick");
  if (all.length) {
    pick.innerHTML = all.map((t) => `
      <option value="${esc(t)}" ${t === physical ? "selected" : ""}>${
      esc(t)}</option>`).join("");
    const jump = (t) => { location.hash = `#/table/${
      encodeURIComponent(t)}`; };
    pick.addEventListener("change", () => jump(pick.value));
    const at = all.indexOf(physical);
    outlet.querySelector("#t-prev").addEventListener("click", () =>
      jump(all[(at - 1 + all.length) % all.length]));
    outlet.querySelector("#t-next").addEventListener("click", () =>
      jump(all[(at + 1) % all.length]));
  } else {
    outlet.querySelector("#t-prev").hidden = true;
    outlet.querySelector("#t-next").hidden = true;
    pick.hidden = true;
  }

  if (!detail.available) {
    host.innerHTML = unavailable(detail.reason);
    return pullout.teardown;
  }
  if (!detail.found) {
    host.innerHTML = card("", `<p>${esc(physical)} is not in the
      promoted build</p>
      <a class="btn" href="#/tables">← back</a>`, "empty");
    return pullout.teardown;
  }
  const columns = Object.entries(detail.columns ?? {});
  const prior = detail.cost_prior;
  const metricsHere = detail.metrics_here ?? [];
  const statuses = [...new Set(metricsHere.map((m) =>
    m.status_served))];

  host.innerHTML = `
    <div class="card">
      <div class="profile-head">
        <span class="profile-title mono">${esc(physical)}</span>
        <span class="muted">${columns.length} columns servable</span>
        <span class="spacer"></span>
        ${detail.card ? `<button class="btn" id="agent-card">what
          the agent sees →</button>` : ""}
      </div>
      ${prior ? `<div class="muted">usage prior: p50
        <span class="mono">${esc(String(prior.p50 ?? "-"))}</span> · p95
        <span class="mono">${esc(String(prior.p95 ?? "-"))}</span>
        bytes/query (30-day activity)</div>` : ""}
    </div>

    <div class="grid2">
      <div class="card">
        <div class="card-label">METRICS ON THIS TABLE: ${
          metricsHere.length}</div>
        ${statuses.length > 1 ? `
          <div class="pills" id="mh-pills"></div>` : ""}
        <div id="mh-list"></div>
      </div>
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

    <div class="legend"><span class="spacer"></span><span id="fb"></span></div>`;

  // metrics-here list, toggled by served status
  let mhFilter = "";
  const mhList = host.querySelector("#mh-list");
  const mhPills = host.querySelector("#mh-pills");
  const drawMetrics = () => {
    const rows = metricsHere.filter((m) =>
      !mhFilter || m.status_served === mhFilter);
    mhList.innerHTML = rows.length === 0
      ? `<span class="muted">${metricsHere.length === 0
          ? `no witnessed metric yet: ask about this table and the
             mining will catch up`
          : "none with this status"}</span>`
      : rows.map((mh) => `
        <div class="family-row">
          <a class="linklike" href="#/metric/${
            encodeURIComponent(mh.id)}">${esc(mh.label || mh.id)}</a>
          <span class="chip">${esc(
            STATUS_LABEL[mh.status_served] ?? mh.status_served)}</span>
          <span class="mono muted">${mh.support}</span>
        </div>`).join("");
    if (mhPills) {
      mhPills.innerHTML = ["", ...statuses].map((s) => `
        <button class="pill ${mhFilter === s ? "on" : ""}"
          data-s="${esc(s)}">${
          s ? esc(STATUS_LABEL[s] ?? s) : "all"}</button>`).join("");
    }
  };
  if (mhPills) {
    mhPills.addEventListener("click", (e) => {
      const s = e.target?.dataset?.s;
      if (s === undefined) return;
      mhFilter = s;
      drawMetrics();
    });
  }
  drawMetrics();

  // the served card opens in the pullout: the agent's exact bytes
  const agentBtn = host.querySelector("#agent-card");
  if (agentBtn) {
    agentBtn.addEventListener("click", () => {
      pullout.open({
        title: physical, kind: "served card",
        sub: "the exact bytes the agent reads for this table",
        html: `<pre class="md-code">${esc(detail.card)}</pre>`,
        raw: detail.card,
      });
    });
  }

  host.querySelector("#fb").replaceWith(
    feedbackBar("table_profile", physical, api.feedback));
  return pullout.teardown;
}
