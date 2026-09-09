/** Knowledge: the knowledge files as a Finder-style folder tree —
 * the staged drops and the reference docs the graph is built from
 * (skills have their own page). Folders open and close with
 * disclosure triangles; clicking a file slides the pullout reader in
 * from the right. The creator below turns your material into a
 * knowledge file in the house format with the model's help — you
 * read it before it is staged — or takes typed knowledge and dumped
 * files as they are; the SharePoint MCP connector door stays honest,
 * other connectors sit visibly disabled, never fake. */

import { api } from "../api.js";
import { renderMarkdown } from "../md.js";
import { createPullout } from "../pullout.js";
import { card, esc, loading } from "../ui.js";
import { creatorPanel, wireCreator } from "./skills.js";

const fmtSize = (n) =>
  n >= 1024 ? `${(n / 1024).toFixed(1)} KB` : `${n} B`;

const TEXT_EXT = ["md", "txt", "csv", "json", "yaml", "yml", "sql"];

/* rel paths → a nested folder tree (Finder-style, folders first) */
function buildTree(files) {
  const root = { folders: new Map(), files: [] };
  for (const f of files) {
    const parts = f.rel.split("/");
    let node = root;
    for (const part of parts.slice(0, -1)) {
      if (!node.folders.has(part))
        node.folders.set(part, { folders: new Map(), files: [] });
      node = node.folders.get(part);
    }
    node.files.push(f);
  }
  return root;
}

function renderTree(node, depth = 0) {
  const folders = [...node.folders.entries()].map(([name, child]) => `
    <details class="tree-folder" ${depth < 1 ? "open" : ""}>
      <summary><span class="tri">▸</span><span class="folder-ico">▣</span>
        ${esc(name)}<span class="muted tree-count">${
        countFiles(child)}</span></summary>
      <div class="tree-children">${renderTree(child, depth + 1)}</div>
    </details>`).join("");
  const files = node.files.map((f) => `
    <button class="artifact-file tree-file" data-rel="${esc(f.rel)}"
      title="${esc(f.rel)}">
      <span class="artifact-name clip">${esc(f.name)}</span>
      ${f.staged ? '<span class="chip">staged</span>' : ""}
      <span class="chip">${esc(f.kind)}</span>
      <span class="muted mono">${fmtSize(f.size)}</span>
    </button>`).join("");
  return folders + files;
}

function countFiles(node) {
  return node.files.length + [...node.folders.values()]
    .reduce((n, child) => n + countFiles(child), 0);
}

export async function renderKnowledge(outlet) {
  outlet.innerHTML = `
    <div class="library-page">
      <div class="page-head">
        <h1>Knowledge</h1>
        <p class="muted">The knowledge files the graph is built from:
        the reference docs and the files people stage here. A staged
        file enters the graph on the next build, with who staged it
        and for which business unit. Skills have their own page.</p>
      </div>
      <div class="library-tools">
        <span class="muted" id="a-count"></span>
      </div>
      <div class="artifact-list card" id="a-list">${loading()}</div>
      <div id="a-draft-card"></div>
      <div id="a-stage-card"></div>
    </div>`;
  const pullout = createPullout(outlet);

  const payload = await api.artifacts();
  const listHost = outlet.querySelector("#a-list");
  if (!listHost) return;

  // the skills live on their own page: the tree shows the rest
  const files = (payload.files ?? []).filter((f) =>
    !String(f.rel || "").startsWith("skills/"));
  outlet.querySelector("#a-count").textContent =
    `${files.length} knowledge files on the shelf`;

  if (!files.length) {
    listHost.innerHTML = card("THE SHELF", `<p class="muted">${
      esc(payload.files_reason ?? payload.reason
        ?? "no knowledge files found")}</p>
      <p class="muted">Stage one below and it appears here.</p>`,
      "empty");
  } else {
    listHost.innerHTML = `
      <div class="card-label">THE SHELF · folders open like a
        finder</div>
      <div class="tree">${renderTree(buildTree(files))}</div>`;
    listHost.addEventListener("click", async (e) => {
      const button = e.target.closest?.(".artifact-file");
      if (!button) return;
      listHost.querySelectorAll(".artifact-file.on")
        .forEach((el) => el.classList.remove("on"));
      button.classList.add("on");
      const file = await api.artifactFile(button.dataset.rel);
      const clear = () => button.classList.remove("on");
      if (!file.found) {
        pullout.open({ title: "not available", kind: "!",
          sub: button.dataset.rel,
          raw: file.reason ?? "could not open it",
          onClose: clear });
        return;
      }
      pullout.open({
        title: file.name, kind: file.kind, sub: file.rel,
        html: renderMarkdown(file.content, file.kind),
        raw: file.content, onClose: clear });
    });
  }

  // ── the creator with the model: material in, a knowledge file in
  //    the house format out, staged only after the person read it ──
  outlet.querySelector("#a-draft-card").innerHTML = creatorPanel("knowledge");
  wireCreator(outlet, "knowledge", async (name, text, fields) => {
    const unit = (fields.business_unit || "").trim();
    if (!/^[A-Za-z0-9_-]{1,40}$/.test(unit)) {
      return { ok: false, reason: "a business unit names the file: letters, digits, dashes" };
    }
    const body = await api.stageArtifact({
      business_unit: unit, name: name || fields.title || "knowledge",
      content: text, ext: "md" });
    if (body.ok && body.staged) {
      return { ok: true, note: `staged as ${body.file}. ${body.note ?? ""}` };
    }
    return { ok: false, reason: body.reason
      ?? (body.detail ? JSON.stringify(body.detail) : "refused") };
  }, () => { pullout.teardown(); renderKnowledge(outlet); });

  // ── the plain creator: type it, dump files, or connect a system ──
  outlet.querySelector("#a-stage-card").innerHTML = card(
    "OR STAGE IT AS IT IS · a source drop, named from the domain", `
    <div class="legend">
      <input class="search" id="a-bu" placeholder="business unit (e.g. USCS)" />
      <input class="search" id="a-name"
        placeholder="artifact name (e.g. lending vocabulary)" />
    </div>
    <textarea class="input" id="a-content"
      placeholder="the knowledge itself: definitions, joins, vocabulary, guardrails for this business unit (markdown welcome)"></textarea>
    <div class="legend">
      <button class="btn primary" id="a-stage" disabled>Stage artifact</button>
      <label class="btn" for="a-files" title="Drop text files
        (md, txt, csv, json, yaml, sql) straight onto the shelf">
        dump files…</label>
      <input type="file" id="a-files" multiple hidden
        accept=".md,.txt,.csv,.json,.yaml,.yml,.sql" />
      <span class="muted">lands in sources/artifacts/ · ingested once
        the Knowledge Files loader ships · recorded as you</span>
    </div>
    <div class="legend connectors">
      <span class="muted">or connect a system:</span>
      <button class="btn" id="a-sharepoint">Connect SharePoint ·
        MCP connector</button>
      <button class="btn" disabled
        title="Not wired yet">Google Drive</button>
      <button class="btn" disabled
        title="Not wired yet">Confluence</button>
      <button class="btn" disabled title="Not wired yet">S3</button>
    </div>
    <p class="muted" id="a-connector-note" hidden>The SharePoint MCP
      connector wires in next: pick a site and its documents sync to
      this shelf with their provenance. Until that lands this button
      only tells the truth: nothing is mocked.</p>
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
      pullout.teardown();
      renderKnowledge(outlet);            // re-list with the new file
    } else {
      result.textContent = body.reason
        ?? (body.detail ? JSON.stringify(body.detail) : "refused");
    }
  });

  // dump files: each text file stages as its own artifact
  outlet.querySelector("#a-files").addEventListener("change",
    async (e) => {
      const unit = /^[A-Za-z0-9_-]{1,40}$/.test(bu.value)
        ? bu.value : "DROP";
      const outcomes = [];
      for (const f of e.target.files) {
        const ext = (f.name.split(".").pop() || "").toLowerCase();
        if (!TEXT_EXT.includes(ext)) {
          outcomes.push(`${f.name}: skipped (not a text format)`);
          continue;
        }
        if (f.size > 200_000) {
          outcomes.push(`${f.name}: skipped (over 200 KB)`);
          continue;
        }
        const text = await f.text();
        const stem = f.name.replace(/\.[^.]+$/, "");
        const body = await api.stageArtifact({
          business_unit: unit, name: stem, content: text, ext });
        outcomes.push(body.ok && body.staged
          ? `${f.name} → staged as ${body.file}`
          : `${f.name}: ${body.reason ?? "refused"}`);
      }
      result.textContent = outcomes.join(" · ");
      if (outcomes.some((o) => o.includes("staged as"))) {
        pullout.teardown();
        renderKnowledge(outlet);
      }
    });

  const spBtn = outlet.querySelector("#a-sharepoint");
  spBtn.addEventListener("click", () => {
    const note = outlet.querySelector("#a-connector-note");
    note.hidden = !note.hidden;
  });

  return pullout.teardown;
}
