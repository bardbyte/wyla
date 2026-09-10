/** Data Product profile. Switch products in place (searchable select
 * plus prev/next); what the product is and who stands behind it; the
 * metrics computed on it, toggled by status; the joins on record with
 * their evidence; and the columns as a searchable list — the first
 * twelve open, the rest a search away — each one expandable to what
 * it is (the description on record, the MDM's supplementary meaning,
 * sensitivity, agreement) and where it is used (joins, metrics).
 * "What the agent sees" opens the served card, exactly the bytes the
 * agent reads. */

import { api } from "../api.js";
import { createPullout } from "../pullout.js";
import {
  card, esc, feedbackBar, loading, statusLabel, unavailable,
} from "../ui.js";

const FIRST = 12;                 // columns open before a search
const fmt = (n) => (n === null || n === undefined || n === "")
  ? "" : new Intl.NumberFormat().format(n);
const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;
const escapeRe = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const mentions = (text, name) =>
  new RegExp(`(^|[^A-Za-z0-9_])${escapeRe(name)}($|[^A-Za-z0-9_])`)
    .test(String(text || ""));

// a column row: the head is the one-line summary, the detail opens
// under it with everything the build knows about the column
function columnRow(c, uses) {
  const desc = c.description || "";
  const shortDesc = desc
    || (c.ungoverned ? "no business meaning on record"
                     : "no description on record");
  const facts = [
    `type <span class="mono">${esc(c.type || "?")}</span>${
      c.type_source ? ` from ${esc(c.type_source)}` : ""}`,
    c.agreement > 1 ? `${c.agreement} sources agree` : "one source",
    c.sensitive ? `sensitive${(c.sensitivity_sources || []).length
      ? ` (${esc(c.sensitivity_sources.join(", "))})` : ""}` : "",
    c.ungoverned ? "ungoverned: in BigQuery, in no catalog" : "",
    (c.flags || []).length ? `flags ${esc(c.flags.join(", "))}` : "",
  ].filter(Boolean);
  return `
    <div class="col-row" data-name="${esc(c.name)}">
      <button class="col-head" type="button" aria-expanded="false">
        <span class="caret" aria-hidden="true">▸</span>
        <span class="col-name mono">${esc(c.name)}</span>
        <span class="col-flags">
          <span class="chip col-type">${esc(c.type || "?")}</span>
          ${c.sensitive ? `<span class="chip warn">sensitive</span>` : ""}
        </span>
        <span class="col-desc ${desc ? "" : "muted"}">${esc(shortDesc)}</span>
      </button>
      <div class="col-detail" hidden>
        ${desc ? `<p>${esc(desc)}${c.description_source
          ? ` <span class="muted">· from ${esc(c.description_source)}</span>`
          : ""}</p>`
          : `<p class="muted">${esc(shortDesc)}: the archive and the
             stewards have not described this column.</p>`}
        ${c.supplementary ? `<p><b>MDM:</b> ${esc(c.supplementary)}</p>` : ""}
        ${c.business_name ? `<p>business name: <b>${esc(c.business_name)}</b></p>`
                          : ""}
        <div class="col-facts muted">${facts.join(" · ")}</div>
        ${uses.joins.length ? `<div class="col-uses">
          <span class="muted">in joins:</span>${uses.joins.map((j) => `
            <a class="pill" href="#/product/${encodeURIComponent(j.other)}"
              title="${esc(j.on)}">${esc(j.other.split(".").pop())}</a>`)
            .join("")}</div>` : ""}
        ${uses.metrics.length ? `<div class="col-uses">
          <span class="muted">in metrics:</span>${uses.metrics.map((m) => `
            <a class="pill" href="#/metric/${encodeURIComponent(m.id)}"
              title="${esc(m.expr || "")}">${esc(m.label || m.id)}</a>`)
            .join("")}</div>` : ""}
      </div>
    </div>`;
}

export async function renderTable(outlet, physical) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <a class="linklike" href="#/products">← Data Products</a>
      <span class="muted">› Data Product</span>
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
      <option value="${esc(t)}" ${t === physical ? "selected" : ""}
        title="${esc(t)}">${esc(t.split(".").pop())}</option>`).join("");
    const jump = (t) => { location.hash = `#/product/${
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
      <a class="btn" href="#/products">← back</a>`, "empty");
    return pullout.teardown;
  }
  // every servable column with its meaning: the detail rows when the
  // build carries them, else the bare schema
  const columns = (detail.columns_detail || []).length
    ? detail.columns_detail
    : Object.entries(detail.columns ?? {}).map(([name, type]) =>
        ({ name, type }));
  const prior = detail.cost_prior;
  const metricsHere = detail.metrics_here ?? [];
  const statuses = [...new Set(metricsHere.map((m) =>
    m.status_served))];
  const meta = [
    detail.business_unit ? `business unit ${esc(detail.business_unit)}` : "",
    detail.owner ? `owner ${esc(detail.owner)}` : "",
    detail.layer ? `${esc(detail.layer)} layer` : "",
    detail.rows ? `${fmt(detail.rows)} rows` : "",
    detail.latest_partition ? `data to ${esc(detail.latest_partition)}` : "",
    detail.lifecycle && detail.lifecycle !== "unknown"
      ? esc(detail.lifecycle) : "",
    (detail.primary_key || []).length
      ? `key <span class="mono">${esc(detail.primary_key.join(", "))}</span>`
      : "",
  ].filter(Boolean);

  host.innerHTML = `
    <div class="card">
      <div class="profile-head">
        <span class="profile-title mono" title="${esc(physical)}">${
          esc(physical.split(".").pop())}</span>
        ${detail.lob ? `<span class="chip" title="${esc(detail.lob_name
          || detail.lob)}">${esc(detail.lob)}</span>` : ""}
        <span class="muted">${columns.length} columns servable</span>
        <span class="spacer"></span>
        ${detail.card ? `<button class="btn" id="agent-card">what
          the agent sees →</button>` : ""}
      </div>
      <p class="product-desc-full ${detail.description ? "" : "muted"}">${
        esc(detail.description
          || "No description on record yet: the archive and the "
             + "stewards have not described this table.")}</p>
      ${meta.length ? `<div class="product-meta muted">${
        meta.map((m) => `<span>${m}</span>`).join("")}</div>` : ""}
      ${prior ? `<div class="muted">usage prior: p50
        <span class="mono">${esc(String(prior.p50 ?? "-"))}</span> · p95
        <span class="mono">${esc(String(prior.p95 ?? "-"))}</span>
        bytes/query (30-day activity)</div>` : ""}
    </div>

    ${card(`COLUMNS: ${columns.length} servable`, `
      <div class="col-tools">
        <input class="search" id="col-search"
          placeholder="search ${columns.length} columns by name or meaning…" />
        <span class="muted" id="col-count"></span>
      </div>
      <div class="col-list" id="col-list"></div>
      <div class="col-more" id="col-more"></div>`)}

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
              <a class="linklike mono" href="#/product/${
                encodeURIComponent(j.a === physical ? j.b : j.a)}"
                title="${esc(j.a === physical ? j.b : j.a)}">${
                esc((j.a === physical ? j.b : j.a).split(".").pop())}</a>
              <span class="chip">${esc(j.source)}</span>
              ${j.scope === "scoped_only"
                ? `<span class="warn">◐ CTE-scoped: NOT raw-safe</span>`
                : ""}
              ${j.on ? `<span class="mono muted">on ${
                esc(Array.isArray(j.on) ? j.on.join(" AND ") : j.on)}
                </span>` : ""}
            </div>`).join("")}`)}
    </div>

    <div class="legend"><span class="spacer"></span><span id="fb"></span></div>`;

  // ── the columns: the first twelve, then a search; a row opens to
  //    its meaning and its uses ──
  const usesOf = (name) => ({
    joins: (detail.joins ?? []).flatMap((j) => {
      const on = Array.isArray(j.on) ? j.on.join(" AND ") : (j.on || "");
      return mentions(on, name)
        ? [{ other: j.a === physical ? j.b : j.a, on }] : [];
    }),
    metrics: metricsHere.filter((m) => mentions(m.expr, name)),
  });
  const colList = host.querySelector("#col-list");
  const colCount = host.querySelector("#col-count");
  const colMore = host.querySelector("#col-more");
  const colState = { q: "", all: false, open: new Set() };
  const drawColumns = () => {
    const q = colState.q.trim().toLowerCase();
    const hits = q
      ? columns.filter((c) => [c.name, c.type, c.description,
                                c.supplementary, c.business_name]
          .some((v) => (v || "").toLowerCase().includes(q)))
      : columns;
    const shown = (q || colState.all) ? hits : hits.slice(0, FIRST);
    colList.innerHTML = shown.length
      ? shown.map((c) => columnRow(c, usesOf(c.name))).join("")
      : `<p class="muted">no column matches "${esc(colState.q)}"</p>`;
    for (const row of colList.querySelectorAll(".col-row")) {
      if (colState.open.has(row.dataset.name)) toggleRow(row, true);
    }
    colCount.textContent = q
      ? `${plural(hits.length, "match")}`
      : (shown.length < hits.length
          ? `the first ${shown.length} of ${hits.length} · search for the rest`
          : `all ${hits.length}`);
    colMore.innerHTML = (!q && hits.length > FIRST)
      ? `<button class="btn" id="col-toggle">${colState.all
          ? `show the first ${FIRST}` : `show all ${hits.length} columns`}
        </button>` : "";
  };
  const toggleRow = (row, open) => {
    const on = open ?? !row.classList.contains("open");
    row.classList.toggle("open", on);
    row.querySelector(".col-head").setAttribute("aria-expanded", String(on));
    row.querySelector(".col-detail").hidden = !on;
    if (on) colState.open.add(row.dataset.name);
    else colState.open.delete(row.dataset.name);
  };
  colList.addEventListener("click", (e) => {
    if (e.target.closest("a")) return;          // a use link navigates
    const head = e.target.closest(".col-head");
    if (head) toggleRow(head.closest(".col-row"));
  });
  colMore.addEventListener("click", (e) => {
    if (e.target.closest("#col-toggle")) {
      colState.all = !colState.all;
      drawColumns();
    }
  });
  let colDebounce = 0;
  host.querySelector("#col-search").addEventListener("input", (e) => {
    clearTimeout(colDebounce);
    colDebounce = setTimeout(() => {
      colState.q = e.target.value;
      drawColumns();
    }, 150);
  });
  drawColumns();

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
          <span class="chip">${esc(statusLabel(mh.status_served))}</span>
          <span class="mono muted">${mh.support}</span>
        </div>`).join("");
    if (mhPills) {
      mhPills.innerHTML = ["", ...statuses].map((s) => `
        <button class="pill ${mhFilter === s ? "on" : ""}"
          data-s="${esc(s)}">${
          s ? esc(statusLabel(s)) : "all"}</button>`).join("");
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
