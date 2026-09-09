/** Skills: what Synapse already knows how to do, browsable. The
 * doctrine packs it loads by itself when a question calls for one
 * (intent routing), the slash command that loads one on purpose,
 * and the full text of each pack a click away. Built-in packs ship
 * with the assistant; a markdown briefing dropped in graph/skills/
 * appears beside them wearing "unreviewed" — it can steer, never
 * assert facts. Nothing here is selected: it is read. */

import { api } from "../api.js";
import { renderMarkdown } from "../md.js";
import { esc, loading } from "../ui.js";

const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;

// the one-line "what it is for", from the pack's own first lines
function gist(text) {
  const lines = String(text || "").split("\n").map((l) => l.trim());
  const body = lines.filter((l) => l && !l.startsWith("#"));
  return body.length ? body.slice(0, 2).join(" ") : "";
}

export function skillCard(s) {
  const origin = (s.origin || "").replace(/[^a-z]/g, "");
  const headings = [...String(s.text || "").matchAll(/^##\s+(.+)$/gm)]
    .map((m) => m[1].trim()).slice(0, 6);
  return `
    <div class="card skill-card" data-name="${esc(s.name)}">
      <div class="skill-card-head">
        <b>${esc(s.title || s.name)}</b>
        <span class="origin-tag o-${esc(origin)}">${
          esc(s.origin === "builtin" ? "built-in" : (s.origin || ""))}</span>
        <span class="spacer"></span>
        <code class="slash-name" title="type this in the chat to load the pack on purpose">/${esc(s.name)}</code>
      </div>
      <p class="skill-snippet">${esc(s.description || gist(s.text))}</p>
      ${headings.length ? `<div class="skill-moves">${headings.map((h) =>
        `<span class="pill">${esc(h)}</span>`).join("")}</div>` : ""}
      <div class="skill-actions">
        <button class="btn primary skill-use" data-name="${esc(s.name)}"
          title="open a new chat with /${esc(s.name)} in the composer">Use in chat</button>
        <details class="skill-read">
          <summary>read the doctrine</summary>
          <div class="md skill-doc">${renderMarkdown(s.text || "", "md")}</div>
        </details>
      </div>
    </div>`;
}

export async function renderSkills(outlet) {
  outlet.innerHTML = `
    <div class="library-page">
      <div class="page-head">
        <h1>Skills</h1>
        <p class="muted">What Synapse already knows how to do. A skill
        is a doctrine pack: the moves for a kind of ask, the checks
        each move must run, the shape of the deliverable. Synapse loads
        the matching pack by itself when a question calls for it and
        says so in the activity trail; type <code>/</code> in the chat
        to load one on purpose. Drop a markdown briefing in
        <code>graph/skills/</code> to add your own: it appears here as
        <b>unreviewed</b> and can steer, never assert facts.</p>
      </div>
      <div class="skills-how">
        <div><b>On its own</b><span>finds the definition in the graph,
          proves the query with a dry run, hands it over or runs it
          under the limits, checks the rows, charts and dashboards
          with the receipts, remembers what you settle.</span></div>
        <div><b>With a pack</b><span>the moves and the checks for that
          kind of ask, applied to this turn, named in the trail.</span></div>
        <div><b>On purpose</b><span>a slash command in the composer
          loads a pack for one message: <code>/executive-summary what
          moved in Q2</code>.</span></div>
      </div>
      <div class="library-tools">
        <span class="muted" id="skills-count"></span>
      </div>
      <div class="skills-grid" id="skills-grid">${loading()}</div>
    </div>`;

  const grid = outlet.querySelector("#skills-grid");
  const got = await api.chatSkills().catch(() => ({}));
  if (!grid.isConnected) return;
  const packs = got.available ? got.skills || [] : [];
  outlet.querySelector("#skills-count").textContent = packs.length
    ? `${plural(packs.length, "skill")} on the shelf: ${
        packs.filter((s) => s.origin === "builtin").length} built-in, ${
        packs.filter((s) => s.origin !== "builtin").length} unreviewed`
    : "";
  if (!packs.length) {
    grid.innerHTML = `<p class="muted">${esc(got.reason
      || "no skills on file")}</p>`;
    return;
  }
  grid.innerHTML = packs.map(skillCard).join("");
  // Use in chat: a new chat opens with the slash command in the
  // composer, the cursor after it, ready for the ask
  grid.addEventListener("click", (e) => {
    const btn = e.target.closest(".skill-use");
    if (!btn) return;
    try {
      sessionStorage.setItem("synapse.prefill", `/${btn.dataset.name} `);
    } catch { /* storage refused: the slash menu still works */ }
    location.hash = "#/chat/new";
  });
}
