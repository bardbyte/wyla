/** Home — the capabilities page (Lumi Home artboard, real build).
 * Hero → promises → LIVE PROOF → network planes → diff → the Sources
 * rail (the ledger as trust centerpiece) → exclusions → legend. */

import { api } from "../api.js";
import { card, esc, kv, loading, unavailable } from "../ui.js";

const PROMISES = [
  ["continues your question", "“same for Canada” edits, never restarts"],
  ["asks, never guesses", "one crisp question when words are ambiguous"],
  ["never invents a metric", "unregistered gets a door, not a proxy"],
  ["explains every answer", "definition, filters, source, grain, SQL"],
  ["shows what exists", "lifecycle status with the owner of next action"],
  ["right access, automatically", "entitled users never file tickets"],
];

export async function renderHome(outlet) {
  outlet.innerHTML = `
    <div class="hero">
      <h1>One company. One number.</h1>
      <p>Ask in plain words. Get governed numbers with receipts —
         every answer carries its meridian line.</p>
      <div class="doors">
        <a class="btn primary" href="#/semantics">Explore semantics</a>
        <a class="btn" href="#/cosmos">Open the cosmos</a>
        <a class="btn" href="#/operate" id="door-operate">Operate</a>
      </div>
    </div>
    <div id="home-body">${loading()}</div>`;

  const [home, sources, planes] = await Promise.all(
    [api.home(), api.sources(), api.planes()]);
  const body = outlet.querySelector("#home-body");
  if (!body) return;

  const promises = card(
    "WHAT IT DOES — six promises from user research",
    `<div class="promises">${PROMISES.map(([lead, rest]) =>
      `<span>· <b>${esc(lead)}</b> — ${esc(rest)}</span>`).join("")}
     </div>`);

  if (!home.available) {
    body.innerHTML =
      `<div class="grid2">${promises}${unavailable(home.reason)}</div>`;
    return;
  }

  const door = outlet.querySelector("#door-operate");
  if (door) door.textContent = `Operate · ${home.open_reviews} open`;

  const stack = Object.entries(home.metrics_by_status)
    .sort((a, b) => b[1] - a[1])
    .map(([status, n]) =>
      `<span class="chip">${esc(status)} <b class="mono">${n}</b></span>`)
    .join("");
  const readiness = Object.entries(home.readiness).map(([lob, r]) => `
    <div>
      <div class="readiness-head"><span>readiness · ${esc(lob)}</span>
        <span><b>${r.pct}%</b> <span class="muted">${r.witnessed}/${
          r.tables} tables witnessed</span></span></div>
      <div class="bar"><i style="width:${r.pct}%"></i></div>
    </div>`).join("");
  const proof = card("LIVE PROOF — the system status, in the open", `
    <div class="proof-counts">
      <span><b>${home.counts.tables ?? "—"}</b> tables${
        home.excluded_tables.length
          ? ` <span class="muted">(+${home.excluded_tables.length
            } excluded — on record)</span>` : ""}</span>
      <span><b>${home.counts.metrics ?? "—"}</b> metrics</span>
      <span><b>${home.counts.vocab ?? "—"}</b> vocab</span>
    </div>
    <div class="stack">${stack}</div>
    <div class="muted">joins <b>${home.joins.total}</b>${
      home.joins.scoped_only
        ? ` <span class="warn">· ${home.joins.scoped_only} CTE-scoped ◐ —
           evidence the relationship exists, not that raw tables join
           safely</span>` : ""}</div>
    ${readiness}
    <div class="muted">open reviews <b>${home.open_reviews}</b> ·
      sources <b>${home.sources_count}</b></div>`);

  const planesCard = card("NETWORK PLANES — what this machine carries", `
    <div class="muted">BQ (PSC, direct): key ${
      planes?.bq?.key ? "✓" : "—"} · project ${
      planes?.bq?.project ? "✓" : "—"} · endpoint ${
      planes?.bq?.endpoint ? "✓" : "default"}</div>
    <div class="muted">Vertex (proxy): key ${
      planes?.vertex?.key ? "✓" : "—"} · project ${
      planes?.vertex?.project ? "✓" : "—"} · model
      <span class="mono">${esc(planes?.vertex?.model ?? "—")}</span></div>
    <div class="muted">booleans only — configured or not, never
      values; enrichment and dry-runs stay with laptop.py</div>`);

  const diff = home.diff ? card(
    "SINCE LAST BUILD — the diff, verbatim",
    `<pre class="block">${esc(home.diff.split("\n").slice(0, 14)
      .join("\n"))}</pre>`) : "";

  const shelf = sources.available ? card(
    "SOURCES — everything the company handed Meridian, and proof we read it",
    `<div class="sources">${sources.sources.map((s) => `
      <div class="source">
        <div><span class="chip acc">${esc(s.chip)}</span>${
          s.sub ? ` <span class="muted">· ${esc(s.sub)}</span>` : ""}</div>
        <div class="name">${esc(s.display)}</div>
        <div class="counts mono">${
          Object.entries(s.contributes.nodes)
            .map(([k, n]) => `${esc(k)} ${n}`).join(" · ") || "—"}</div>
        ${Object.keys(s.ledger).length ? `<div class="ledger">ledger: ${
          Object.entries(s.ledger)
            .map(([k, n]) => `${n} ${esc(k)}`).join(" · ")}</div>` : ""}
      </div>`).join("")}</div>`)
    : card("SOURCES", `<p class="muted">${
        esc(sources.reason ?? "loading the shelf…")}</p>`);

  const excluded = home.excluded_tables.length ? card(
    "EXCLUDED — with the reason on record",
    home.excluded_tables.map((t) =>
      `<div class="mono muted">${esc(t.physical)} — ${
        esc(t.intentionally_excluded || t.reason)}</div>`).join(""))
    : "";

  body.innerHTML = `
    <div class="grid2">${promises}${proof}</div>
    <div class="grid2">${planesCard}${diff || ""}</div>
    ${shelf}${excluded}
    <div class="legend">legend:
      <span class="chip tier-ha">● human</span>
      <span class="chip tier-gr">◆ grounded</span>
      <span class="chip tier-in">◐ inferred</span>
      <span class="chip tier-gu">○ guessed</span>
      <span class="spacer"></span>
      <span>every write records an actor · every read is scoped by a
        principal</span>
    </div>`;
}
