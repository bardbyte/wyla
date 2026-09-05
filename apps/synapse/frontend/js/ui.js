/** Shared render helpers. Every dynamic string passes through esc();
 * tiers render through tierChip (shape + word, never color alone). */

export const esc = (s) =>
  String(s ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;",
    '"': "&quot;", "'": "&#39;",
  }[c]));

// display-plane typography for served PROSE (reasons, blurbs):
// em dashes never reach the page. Data artifacts shown verbatim
// (SQL, cards, diffs) do not pass through here.
export const prose = (s) =>
  esc(String(s ?? "").replace(/\s+—\s+/g, ": ").replace(/—/g, "-"));

export const TIER = {
  ha: { glyph: "●", word: "human-asserted" },
  gr: { glyph: "◆", word: "grounded" },
  in: { glyph: "◐", word: "inferred" },
  gu: { glyph: "○", word: "unverified" },
};

export const tierChip = (tier, label = "") => {
  const t = TIER[tier] ?? TIER.gu;
  return `<span class="chip tier-${esc(tier)}">${
    esc(label || t.word)} ${t.glyph}</span>`;
};

// a metric's status as the product says it: the graph's value stays
// the CSS class, the filter key and the API word; the chip says the
// label. "published" is what the Data Marketplace calls a steward-
// approved definition.
export const STATUS_LABEL = {
  certified: "published", pending_certification: "pending",
  pending: "pending", unreviewed: "unreviewed", team_candidate: "team",
  rejected: "rejected", deprecated: "deprecated",
};
export const statusLabel = (s) => STATUS_LABEL[s] ?? String(s ?? "");

export const card = (label, body, cls = "") =>
  `<div class="card ${cls}"><div class="card-label">${esc(label)}</div>${body}</div>`;

export const unavailable = (reason) =>
  card("NO COMPILED BUILD ON THIS MACHINE",
    `<p>${prose(reason)}</p>
     <p class="muted">The product renders only the real promoted
     build: nothing is mocked. Compile on this machine and this page
     fills itself.</p>`, "empty");

export const loading = () =>
  `<div class="card"><p class="muted">loading…</p></div>`;

export const kv = (entries, sep = " · ") =>
  entries.map(([k, v]) => `${esc(k)} <b class="mono">${esc(v)}</b>`)
    .join(sep);

export const feedbackBar = (screen, objectId, send) => {
  const host = document.createElement("span");
  host.className = "legend";
  host.innerHTML = `
    <button class="linklike" data-vote="up" title="This reads right">👍</button>
    <button class="linklike" data-vote="down"
      title="Something is off: a steward will see it">👎</button>`;
  host.addEventListener("click", (e) => {
    const vote = e.target?.dataset?.vote;
    if (!vote) return;
    send({ screen, object_id: objectId, vote });
    host.innerHTML =
      `<span class="muted">noted: a steward will see it</span>`;
  });
  return host;
};
