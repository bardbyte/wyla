/** Artifacts: the knowledge shelf as a real file browser. Left, every
 * knowledge file grouped by its skill area (CFR, Finance, TLS, …);
 * click one and it opens on the right, Markdown rendered, code files
 * shown as code. Staged files sit in their own group, honestly
 * labeled until the loader ingests them. The staging door lives at
 * the bottom. */

import { api } from "../api.js";
import { renderMarkdown } from "../md.js";
import { card, esc, loading } from "../ui.js";

const fmtSize = (n) =>
  n >= 1024 ? `${(n / 1024).toFixed(1)} KB` : `${n} B`;

export async function renderArtifacts(outlet) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <span class="muted">Understand · Artifacts (Knowledge Files)</span>
      <span class="spacer"></span><span class="muted" id="a-count"></span>
    </div>
    <div class="artifact-browser">
      <div class="artifact-list card" id="a-list">${loading()}</div>
      <div class="artifact-view card" id="a-view">
        <div class="card-label">READER</div>
        <p class="muted">pick a file on the left. Markdown renders as
          a page; specs and SQL render as code.</p>
      </div>
    </div>
    <div id="a-stage-card"></div>`;

  const payload = await api.artifacts();
  const listHost = outlet.querySelector("#a-list");
  const viewHost = outlet.querySelector("#a-view");
  if (!listHost) return;

  const files = payload.files ?? [];
  outlet.querySelector("#a-count").textContent =
    `${files.length} files on the shelf`;

  if (!files.length) {
    listHost.innerHTML = card("THE SHELF", `<p class="muted">${
      esc(payload.files_reason ?? payload.reason
        ?? "no knowledge files found")}</p>
      <p class="muted">Stage one below and it appears here.</p>`,
      "empty");
  } else {
    const groups = new Map();
    for (const f of files) {
      const key = f.staged ? "staged (awaiting the loader)" : f.area;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(f);
    }
    listHost.innerHTML = `
      <div class="card-label">THE SHELF · grouped by skill area</div>
      ${[...groups.entries()].map(([area, members]) => `
        <div class="artifact-group">
          <div class="artifact-area">${esc(area)}</div>
          ${members.map((f) => `
            <button class="artifact-file" data-rel="${esc(f.rel)}"
              title="${esc(f.rel)}">
              <span class="artifact-name clip">${esc(f.name)}</span>
              <span class="chip">${esc(f.kind)}</span>
              <span class="muted mono">${fmtSize(f.size)}</span>
            </button>`).join("")}
        </div>`).join("")}`;
    listHost.addEventListener("click", async (e) => {
      const button = e.target.closest?.(".artifact-file");
      if (!button) return;
      listHost.querySelectorAll(".artifact-file.on")
        .forEach((el) => el.classList.remove("on"));
      button.classList.add("on");
      viewHost.innerHTML = loading();
      const file = await api.artifactFile(button.dataset.rel);
      if (!file.found) {
        viewHost.innerHTML = `
          <div class="card-label">READER</div>
          <p class="muted">${esc(file.reason ?? "could not open it")}</p>`;
        return;
      }
      viewHost.innerHTML = `
        <div class="artifact-view-head">
          <span class="profile-title">${esc(file.name)}</span>
          <span class="chip">${esc(file.kind)}</span>
          <span class="muted mono clip" title="${esc(file.rel)}">${
            esc(file.rel)}</span>
        </div>
        <div class="md">${renderMarkdown(file.content, file.kind)}</div>`;
    });
  }

  outlet.querySelector("#a-stage-card").innerHTML = card(
    "ADD A KNOWLEDGE FILE · a source drop, named from the domain", `
    <div class="legend">
      <input class="search" id="a-bu" placeholder="business unit (e.g. USCS)" />
      <input class="search" id="a-name"
        placeholder="artifact name (e.g. lending vocabulary)" />
    </div>
    <textarea class="input" id="a-content"
      placeholder="the knowledge itself: definitions, joins, vocabulary, guardrails for this business unit (markdown welcome)"></textarea>
    <div class="legend">
      <button class="btn primary" id="a-stage" disabled>Stage artifact</button>
      <span class="muted">lands in sources/artifacts/ · ingested once
        the Knowledge Files loader ships · recorded as you</span>
    </div>
    <p class="muted" id="a-result"></p>`);

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
      result.textContent = `staged as ${body.file}. ${body.note ?? ""}`;
      renderArtifacts(outlet);            // re-list with the new file
    } else {
      result.textContent = body.reason
        ?? (body.detail ? JSON.stringify(body.detail) : "refused");
    }
  });
}
