/** Home. Hero doors, the six promises, LIVE PROOF, the Sources rail
 * (one card per source family, the ledger as the trust line), and
 * the exclusions card. The diff lives in Operate; network planes are
 * an operator detail, not a landing claim. */

import { api } from "../api.js";
import { card, esc, loading, prose, unavailable } from "../ui.js";

const PROMISES = [
  ["continues your question", "“same for Canada” edits, never restarts"],
  ["asks, never guesses", "one crisp question when words are ambiguous"],
  ["never invents a metric", "unregistered gets a door, not a proxy"],
  ["explains every answer", "definition, filters, source, grain, SQL"],
  ["shows what exists", "lifecycle status with the owner of next action"],
  ["right access, automatically", "entitled users never file tickets"],
];

/* one shelf card per FAMILY: merged counts, merged ledger, the
 * sub-sources named small. The enrichment loop is an internal stage
 * (it lives in Operate), not a company source; it never shelves. */
function groupByFamily(sources) {
  const families = new Map();
  for (const s of sources) {
    if (s.family === "enrichment") continue;
    const entry = families.get(s.family) ?? {
      family: s.family, display: s.display, chip: s.chip,
      subs: [], nodes: {}, ledger: {},
    };
    if (s.sub) entry.subs.push(s.sub);
    for (const [k, n] of Object.entries(s.contributes?.nodes ?? {}))
      entry.nodes[k] = (entry.nodes[k] ?? 0) + n;
    for (const [k, n] of Object.entries(s.ledger ?? {}))
      entry.ledger[k] = (entry.ledger[k] ?? 0) + n;
    families.set(s.family, entry);
  }
  return [...families.values()];
}

export async function renderHome(outlet) {
  outlet.innerHTML = `
    <div class="hero">
      <h1>Ask in plain words. Get governed numbers with receipts.</h1>
      <p>Every answer carries its meridian line.</p>
      <div class="doors">
        <a class="btn primary" href="#/semantics">Explore semantics</a>
        <a class="btn" href="#/cosmos">Open the cosmos</a>
        <a class="btn" href="#/artifacts">Knowledge files</a>
        <a class="btn" href="#/operate" id="door-operate">Operate</a>
      </div>
    </div>
    <div id="home-body">${loading()}</div>`;

  const [home, sources, artifacts] = await Promise.all(
    [api.home(), api.sources(), api.artifacts()]);
  const body = outlet.querySelector("#home-body");
  if (!body) return;

  const promises = card(
    "WHAT IT DOES · six promises from user research",
    `<div class="promises">${PROMISES.map(([lead, rest]) =>
      `<span>· <b>${esc(lead)}</b>: ${esc(rest)}</span>`).join("")}
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
  const proof = card("LIVE PROOF · the system status, in the open", `
    <div class="proof-counts">
      <span><b>${home.counts.tables ?? "?"}</b> tables${
        home.excluded_tables.length
          ? ` <span class="muted">(+${home.excluded_tables.length
            } excluded, on record)</span>` : ""}</span>
      <span><b>${home.counts.metrics ?? "?"}</b> metrics</span>
      <span><b>${home.counts.vocab ?? "?"}</b> vocab</span>
    </div>
    <div class="stack">${stack}</div>
    <div class="muted">joins <b>${home.joins.total}</b>${
      home.joins.scoped_only
        ? ` <span class="warn">· ${home.joins.scoped_only} CTE-scoped ◐,
           evidence the relationship exists, not that raw tables join
           safely</span>` : ""}</div>
    ${readiness}
    <div class="muted">open reviews <b>${home.open_reviews}</b> ·
      sources <b>${home.sources_count}</b></div>`);

  const knowledgeFiles = (artifacts?.files ?? [])
    .filter((f) => !f.staged);
  const shelf = sources.available ? card(
    "SOURCES · everything the company handed Meridian, and proof we read it",
    `<div class="sources">${groupByFamily(sources.sources).map((s) => `
      <div class="source">
        <div><span class="chip acc">${esc(s.chip)}</span>${
          s.subs.length
            ? ` <span class="muted">${s.subs.map(esc).join(" · ")}</span>`
            : ""}</div>
        <div class="name">${esc(s.display)}</div>
        <div class="counts mono">${
          Object.entries(s.nodes)
            .map(([k, n]) => `${esc(k)} ${n}`).join(" · ")
          || "no served nodes (held out by design)"}</div>
        ${Object.keys(s.ledger).length ? `<div class="ledger">ledger: ${
          Object.entries(s.ledger)
            .map(([k, n]) => `${n} ${esc(k)}`).join(" · ")}</div>` : ""}
        ${s.family === "knowledge" ? (knowledgeFiles.length ? `
          <div class="ledger">${knowledgeFiles.length} files on the
            shelf: ${knowledgeFiles.slice(0, 6)
              .map((f) => esc(f.name)).join(" · ")}${
            knowledgeFiles.length > 6 ? " · …" : ""}</div>
          <a class="linklike" href="#/artifacts">browse them →</a>`
          : `<div class="ledger">${
              prose(artifacts?.files_reason
                ?? "shelf not visible from this machine")}</div>`) : ""}
      </div>`).join("")}</div>`)
    : card("SOURCES", `<p class="muted">${
        esc(sources.reason ?? "loading the shelf…")}</p>`);

  const excluded = home.excluded_tables.length ? card(
    "EXCLUDED · with the reason on record",
    home.excluded_tables.map((t) =>
      `<div class="mono muted">${esc(t.physical)}: ${
        prose(t.intentionally_excluded || t.reason)}</div>`).join(""))
    : "";

  body.innerHTML = `
    <div class="grid2">${promises}${proof}</div>
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
