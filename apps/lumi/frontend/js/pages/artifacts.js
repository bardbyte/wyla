/** Artifacts — the Knowledge Files shelf: known (with ledger lines),
 * staged (waiting for the loader), and the door to add one for a
 * business unit. Staging is a source drop, never a graph write. */

import { api } from "../api.js";
import { card, esc, loading } from "../ui.js";

export async function renderArtifacts(outlet) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <span class="muted">Understand · Artifacts (Knowledge Files)</span>
    </div>
    <div id="shelf">${loading()}</div>
    ${card("ADD A KNOWLEDGE FILE — a source drop, named from the domain", `
      <div class="legend">
        <input class="search" id="a-bu" placeholder="business unit (e.g. USCS)" />
        <input class="search" id="a-name"
          placeholder="artifact name (e.g. lending vocabulary)" />
      </div>
      <textarea class="input" id="a-content"
        placeholder="the knowledge itself — definitions, joins, vocabulary, guardrails for this business unit"></textarea>
      <div class="legend">
        <button class="btn primary" id="a-stage" disabled>Stage artifact</button>
        <span class="muted">lands in sources/artifacts/ · ingested once
          the Knowledge Files loader ships · recorded as you</span>
      </div>
      <p class="muted" id="a-result"></p>`)}`;

  const drawShelf = async () => {
    const payload = await api.artifacts();
    const shelf = outlet.querySelector("#shelf");
    if (!shelf) return;
    const known = (payload.known ?? []).map((s) => `
      <div class="source">
        <div><span class="chip acc">${esc(s.chip)}</span></div>
        <div class="name">${esc(s.display)}</div>
        <div class="counts mono">${Object.entries(s.contributes?.nodes ?? {})
          .map(([k, n]) => `${esc(k)} ${n}`).join(" · ") || "—"}</div>
        ${Object.keys(s.ledger ?? {}).length
          ? `<div class="ledger">ledger: ${Object.entries(s.ledger)
              .map(([k, n]) => `${n} ${esc(k)}`).join(" · ")}</div>` : ""}
      </div>`).join("");
    shelf.innerHTML = `
      ${card("WHAT MERIDIAN ALREADY KNOWS — curated knowledge files",
        known || `<p class="muted">no knowledge file has reached the
          graph yet${payload.reason ? ` — ${esc(payload.reason)}` : ""}</p>`)}
      ${card("STAGED — waiting for the loader",
        (payload.staged ?? []).length
          ? payload.staged.map((f) =>
              `<div class="mono muted">${esc(f)}
                <span class="chip">staged</span></div>`).join("")
          : `<p class="muted">nothing staged — add a knowledge file
             below and it appears here, honestly labeled, until the
             Knowledge Files loader ingests it</p>`)}`;
  };
  await drawShelf();

  const bu = outlet.querySelector("#a-bu");
  const name = outlet.querySelector("#a-name");
  const content = outlet.querySelector("#a-content");
  const stageBtn = outlet.querySelector("#a-stage");
  const result = outlet.querySelector("#a-result");
  const check = () => {
    stageBtn.disabled = !(/^[A-Za-z0-9_-]{1,40}$/.test(bu.value)
      && name.value.trim() && content.value.trim());
  };
  [bu, name, content].forEach((el) => el.addEventListener("input", check));
  stageBtn.addEventListener("click", async () => {
    result.textContent = "";
    const body = await api.stageArtifact({
      business_unit: bu.value, name: name.value, content: content.value });
    if (body.ok && body.staged) {
      result.textContent = `staged as ${body.file} — ${body.note}`;
      bu.value = ""; name.value = ""; content.value = "";
      check();
      await drawShelf();
    } else {
      result.textContent = body.reason
        ?? (body.detail ? JSON.stringify(body.detail) : "refused");
    }
  });
}
