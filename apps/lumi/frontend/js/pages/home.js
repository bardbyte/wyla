/** Home. One flowing page: hero doors, the six promises, LIVE
 * PROOF, the sources flow story, exclusions. Open sections with
 * quiet labels; no card boxes on the landing narrative. */

import { api } from "../api.js";
import { esc, loading, prose, unavailable } from "../ui.js";

/* Home is one flowing page: open sections with quiet labels, not a
 * stack of card boxes. Cards stay a component for dense surfaces. */
const section = (label, body, cls = "") =>
  `<section class="page-sec ${cls}">
     <h2 class="sec-label">${label}</h2>${body}</section>`;

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

/* the sources space, told as a story anyone can read: four streams
 * of company knowledge flow into one shared map. Real names, real
 * read-counts: the animation is decoration, the numbers are not. */
const BUCKETS = [
  ["The official records",
   "what the company has written down about its data",
   ["atlas_catalog", "atlas_mdm", "acropedia"]],
  ["What stewards have approved",
   "definitions people signed their names to",
   ["marketplace", "domain_map", "steward"]],
  ["What actually runs each day",
   "the warehouse itself, and the questions people really ask it",
   ["warehouse", "activity", "query_mining", "snippets"]],
  ["What the teams know",
   "the know-how each business unit keeps in its files",
   ["knowledge"]],
];

function flowRail(families, home, knowledgeFiles, artifacts) {
  const byFamily = new Map(families.map((f) => [f.family, f]));
  const claimed = new Set(BUCKETS.flatMap(([, , keys]) => keys));
  const leftovers = families.filter((f) =>
    !claimed.has(f.family) && f.family !== "gold");
  const reduced = matchMedia("(prefers-reduced-motion: reduce)").matches;

  const bucketCards = BUCKETS.map(([title, sub, keys]) => {
    const members = keys.map((k) => byFamily.get(k)).filter(Boolean);
    const read = members.reduce(
      (n, m) => n + (m.ledger.consumed ?? 0), 0);
    const names = [...new Set(members.map((m) => m.display))];
    const isKnowledge = keys.includes("knowledge");
    return `
      <div class="flow-bucket">
        <div class="flow-bucket-title">${esc(title)}</div>
        <div class="muted">${esc(sub)}</div>
        <div class="flow-chips">${names.map((n) =>
          `<span class="chip">${esc(n)}</span>`).join("")}</div>
        <div class="ledger">${read
          ? `we read ${read} files from here`
          : "read with every build"}</div>
        ${isKnowledge ? (knowledgeFiles.length ? `
          <div class="ledger">${knowledgeFiles.length} files today:
            ${knowledgeFiles.slice(0, 4).map((f) => esc(f.name))
              .join(" · ")}${knowledgeFiles.length > 4 ? " · …" : ""}
            · <a class="linklike" href="#/artifacts">browse →</a></div>`
          : `<div class="ledger">${prose(artifacts?.files_reason
              ?? "shelf not visible from this machine")}</div>`) : ""}
      </div>`;
  }).join("");

  // four streams meet in the middle: SVG stretches with the column,
  // the travelling dots ride the same paths (SMIL, offline, no lib)
  const ys = [50, 150, 250, 350];
  const lane = `
    <svg class="flow-lane" viewBox="0 0 150 400"
         preserveAspectRatio="none" aria-hidden="true">
      ${ys.map((y, i) => `
        <path id="flow-p${i}" d="M 2 ${y} C 80 ${y}, 70 200, 148 200"
          fill="none" stroke="currentColor" stroke-width="1.4"
          vector-effect="non-scaling-stroke" opacity="0.3" />`).join("")}
      ${reduced ? "" : ys.map((_, i) => [0, 1].map((k) => `
        <circle class="flow-dot" r="3.2" opacity="0.85">
          <animateMotion dur="3.6s" begin="${(i * 0.9 + k * 1.8).toFixed(1)}s"
            repeatCount="indefinite" rotate="0">
            <mpath href="#flow-p${i}"></mpath>
          </animateMotion>
        </circle>`).join("")).join("")}
    </svg>`;

  const fused = `
    <div class="flow-fusion">
      <div class="flow-bucket-title">One shared map</div>
      <p class="muted">Every fact arrives with its receipt: where it
        came from and when we saw it. When two sources describe the
        same thing, they back each other up. When they disagree, the
        disagreement stays visible until a person settles it.</p>
      <div class="proof-counts">
        <span><b>${home.sources_count}</b> sources</span>
        <span><b>${home.counts.tables ?? 0}</b> tables</span>
        <span><b>${home.counts.metrics ?? 0}</b> metrics</span>
        <span><b>${home.counts.vocab ?? 0}</b> business terms</span>
      </div>
      <div class="doors">
        <a class="linklike" href="#/cosmos">see it as a sky →</a>
        <a class="linklike" href="#/semantics">browse what it knows →</a>
      </div>
    </div>`;

  const footnotes = [
    "plus an answer key we keep to one side, only ever used to test ourselves",
    ...leftovers.map((f) => `also read: ${f.display}`),
  ];
  return section(
    "HOW THE MAP GETS BUILT · four streams, one picture",
    `<div class="flow">
       <div class="flow-buckets">${bucketCards}</div>
       ${lane}
       ${fused}
     </div>
     <div class="ledger">${footnotes.map(esc).join(" · ")}</div>`,
    "ruled");
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

  const promises = section(
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
  const proof = section("LIVE PROOF · the system status, in the open", `
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
  const shelf = sources.available
    ? flowRail(groupByFamily(sources.sources), home, knowledgeFiles,
        artifacts)
    : section("SOURCES", `<p class="muted">${
        esc(sources.reason ?? "loading the shelf…")}</p>`, "ruled");

  const excluded = home.excluded_tables.length ? section(
    "EXCLUDED · with the reason on record",
    home.excluded_tables.map((t) =>
      `<div class="mono muted">${esc(t.physical)}: ${
        prose(t.intentionally_excluded || t.reason)}</div>`).join(""),
    "ruled")
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
