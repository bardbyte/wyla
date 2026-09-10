/** The Skills page (V2.7): the doctrine shelf, browsable.
 *
 * Nobody selects skills any more — Synapse loads the matching pack
 * itself when a task calls for it (intent routing). This page is
 * where people READ the shelf: what each pack teaches, in a line,
 * with the full doctrine a click away. Built-in packs ship with the
 * assistant; anything the analyst drops in graph/skills/ appears
 * beside them wearing "unreviewed".
 */

import { api } from "../api.js";
import { renderMarkdown } from "../md.js";
import { esc } from "../ui.js";

export async function renderSkills(outlet) {
  outlet.innerHTML = `
    <div class="skills-page">
      <div class="page-head">
        <h1>Skills</h1>
        <p class="muted">Doctrine packs Synapse applies on its own:
        when a question matches a pack, it loads the pack into the
        turn and says so in the activity trail — nothing to select.
        Drop a markdown briefing in <code>graph/skills/</code> to
        add your own; it appears here as <b>unreviewed</b> and can
        steer, never assert facts.</p>
      </div>
      <div class="skills-grid" id="skills-grid">
        <p class="muted">loading…</p>
      </div>
    </div>`;

  const grid = outlet.querySelector("#skills-grid");
  const got = await api.chatSkills().catch(() => ({}));
  const packs = got.available ? got.skills || [] : [];
  if (!packs.length) {
    grid.innerHTML = `<p class="muted">${esc(got.reason
      || "no skills on file")}</p>`;
    return;
  }
  grid.innerHTML = packs.map((s) => `
    <div class="skill-card">
      <div class="skill-card-head">
        <b>${esc(s.title || s.name)}</b>
        <span class="origin-tag o-${esc((s.origin || "")
          .replace(/[^a-z]/g, ""))}">${esc(s.origin || "")}</span>
      </div>
      <p class="skill-snippet">${esc(s.description || "")}</p>
      <details>
        <summary>read the doctrine</summary>
        <div class="md skill-doc">${renderMarkdown(s.text || "",
                                                   "md")}</div>
      </details>
    </div>`).join("");
}
