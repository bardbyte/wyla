/** Artifacts: the knowledge shelf. The full-width shelf lists every
 * knowledge file grouped by skill area (CFR, CFR/TLS, …, any
 * nesting); clicking one opens a Claude-style pullout panel from the
 * right — rendered Markdown (code files as code), copy to clipboard,
 * close (button or Esc). The creator below takes typed knowledge,
 * dumped files, or — next — a SharePoint MCP connector; other
 * connectors sit visibly disabled, never fake. */

import { api } from "../api.js";
import { renderMarkdown } from "../md.js";
import { card, esc, loading } from "../ui.js";

const fmtSize = (n) =>
  n >= 1024 ? `${(n / 1024).toFixed(1)} KB` : `${n} B`;

const TEXT_EXT = ["md", "txt", "csv", "json", "yaml", "yml", "sql"];

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    // headless / permission-less fallback
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    let ok = false;
    try { ok = document.execCommand("copy"); } catch { ok = false; }
    ta.remove();
    return ok;
  }
}

export async function renderArtifacts(outlet) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <span class="muted">Understand · Artifacts (Knowledge Files)</span>
      <span class="spacer"></span><span class="muted" id="a-count"></span>
    </div>
    <div class="artifact-list card" id="a-list">${loading()}</div>
    <div id="a-stage-card"></div>
    <aside class="artifact-panel" id="a-panel" aria-label="Reader"
      hidden>
      <div class="artifact-panel-head">
        <span class="profile-title clip" id="a-panel-title"></span>
        <span class="chip" id="a-panel-kind"></span>
        <span class="spacer"></span>
        <button class="btn" id="a-copy" title="Copy the file to the
          clipboard">copy</button>
        <button class="icon-btn" id="a-close"
          aria-label="Close the reader">✕</button>
      </div>
      <div class="muted mono clip" id="a-panel-rel"></div>
      <div class="artifact-panel-body md" id="a-panel-body"></div>
    </aside>`;

  const payload = await api.artifacts();
  const listHost = outlet.querySelector("#a-list");
  const panel = outlet.querySelector("#a-panel");
  if (!listHost) return;

  const files = payload.files ?? [];
  outlet.querySelector("#a-count").textContent =
    `${files.length} files on the shelf`;

  // ── the pullout reader ──
  let openContent = "";
  const closePanel = () => {
    panel.classList.remove("open");     // slide out ...
    setTimeout(() => { panel.hidden = true; }, 260);  // ... then hide
    listHost.querySelectorAll(".artifact-file.on")
      .forEach((el) => el.classList.remove("on"));
  };
  const openPanel = (file) => {
    openContent = file.content;
    outlet.querySelector("#a-panel-title").textContent = file.name;
    outlet.querySelector("#a-panel-kind").textContent = file.kind;
    outlet.querySelector("#a-panel-rel").textContent = file.rel;
    outlet.querySelector("#a-panel-body").innerHTML =
      renderMarkdown(file.content, file.kind);
    panel.hidden = false;
    requestAnimationFrame(() => panel.classList.add("open"));
  };
  outlet.querySelector("#a-close").addEventListener("click", closePanel);
  const onKey = (e) => { if (e.key === "Escape") closePanel(); };
  window.addEventListener("keydown", onKey);
  const copyBtn = outlet.querySelector("#a-copy");
  copyBtn.addEventListener("click", async () => {
    const ok = await copyText(openContent);
    copyBtn.textContent = ok ? "copied ✓" : "copy failed";
    setTimeout(() => { copyBtn.textContent = "copy"; }, 1600);
  });

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
      <div class="artifact-groups">
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
        </div>`).join("")}
      </div>`;
    listHost.addEventListener("click", async (e) => {
      const button = e.target.closest?.(".artifact-file");
      if (!button) return;
      listHost.querySelectorAll(".artifact-file.on")
        .forEach((el) => el.classList.remove("on"));
      button.classList.add("on");
      const file = await api.artifactFile(button.dataset.rel);
      if (!file.found) {
        openPanel({ name: "not available", kind: "!",
          rel: button.dataset.rel,
          content: file.reason ?? "could not open it" });
        return;
      }
      openPanel(file);
    });
  }

  // ── the creator: type it, dump files, or connect a system ──
  outlet.querySelector("#a-stage-card").innerHTML = card(
    "ADD KNOWLEDGE · a source drop, named from the domain", `
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
      window.removeEventListener("keydown", onKey);
      renderArtifacts(outlet);            // re-list with the new file
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
        window.removeEventListener("keydown", onKey);
        renderArtifacts(outlet);
      }
    });

  const spBtn = outlet.querySelector("#a-sharepoint");
  spBtn.addEventListener("click", () => {
    const note = outlet.querySelector("#a-connector-note");
    note.hidden = !note.hidden;
  });

  return () => window.removeEventListener("keydown", onKey);
}
