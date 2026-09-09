/** Skills: what Synapse already knows how to do, and what you teach
 * it. Built-in packs ship with the assistant; your own packs live in
 * graph/skills/users/<you>/ and load for you alone; shared packs in
 * graph/skills/ load for everyone — both wearing "unreviewed": they
 * steer where the agent looks, they never assert facts. Synapse
 * loads the matching pack by itself when a question calls for one
 * and says so in the activity trail; type / in the chat to load one
 * on purpose. The creator below turns your material — notes, a
 * runbook, a pasted email, a workbook — into a pack in the house
 * format with the model's help; you read it before it is saved. */

import { api } from "../api.js";
import { renderMarkdown } from "../md.js";
import { esc, loading } from "../ui.js";

const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;
const TEXT_EXT = ["md", "txt", "csv", "tsv", "json", "yaml", "yml", "sql",
                  "xml", "html", "py"];

function gist(text) {
  const lines = String(text || "").split("\n").map((l) => l.trim());
  const body = lines.filter((l) => l && !l.startsWith("#"));
  return body.length ? body.slice(0, 2).join(" ") : "";
}

export function skillCard(s) {
  const origin = s.mine ? "mine" : (s.origin || "").replace(/[^a-z]/g, "");
  const tag = s.mine ? "yours" : s.origin === "built-in" ? "built-in"
    : (s.origin || "");
  const headings = [...String(s.text || "").matchAll(/^##\s+(.+)$/gm)]
    .map((m) => m[1].trim()).slice(0, 6);
  return `
    <div class="card skill-card" data-name="${esc(s.name)}">
      <div class="skill-card-head">
        <b>${esc(s.title || s.name)}</b>
        <span class="origin-tag o-${esc(origin)}">${esc(tag)}</span>
        <span class="spacer"></span>
        <code class="slash-name" title="type this in the chat to load the pack on purpose">/${esc(s.name)}</code>
      </div>
      <p class="skill-snippet">${esc(s.description || gist(s.text))}</p>
      ${headings.length ? `<div class="skill-moves">${headings.map((h) =>
        `<span class="pill">${esc(h)}</span>`).join("")}</div>` : ""}
      <div class="skill-actions">
        <button class="btn primary skill-use" data-name="${esc(s.name)}"
          title="open a new chat with /${esc(s.name)} in the composer">Use in chat</button>
        ${s.mine ? `<button class="btn skill-delete" data-name="${esc(s.name)}"
          title="remove this pack of yours">Delete</button>` : ""}
        <details class="skill-read">
          <summary>read the doctrine</summary>
          <div class="md skill-doc">${renderMarkdown(s.text || "", "md")}</div>
        </details>
      </div>
    </div>`;
}

// the creator: material in, a draft in the house format out, saved
// only after the person has read it
export function creatorPanel(kind) {
  const isSkill = kind === "skill";
  return `
    <div class="card creator" id="creator">
      <div class="card-label">${isSkill
        ? "TEACH SYNAPSE A SKILL · your material, in the house format"
        : "ADD A KNOWLEDGE FILE · your material, in the house format"}</div>
      <p class="muted creator-note">${isSkill
        ? `Paste what you know — how your team reads a metric, the steps
           you take for a kind of question, a runbook — or add a file.
           The model rewrites it as a pack: the moves and the checks, no
           facts invented. You read the draft, edit it, then save it. It
           loads for you alone until a steward shares it.`
        : `Paste the reference — definitions, which tables and columns
           mean what, the metrics and their calculations, the traps —
           or add a file. The model rewrites it in the house format
           with nothing invented; you read the draft, edit it, then
           stage it. It enters the graph on the next build.`}</p>
      <div class="creator-row">
        <input class="search" id="cr-title" placeholder="${isSkill
          ? "title (e.g. Churn triage)" : "title (e.g. Lending vocabulary)"}" />
        <input class="search" id="cr-hint" placeholder="${isSkill
          ? "what it is for, in a line (e.g. why did approvals move)"
          : "what it covers, in a line"}" />
        ${isSkill ? "" : `<input class="search" id="cr-bu"
          placeholder="business unit (e.g. GMNS)" maxlength="40" />`}
      </div>
      <textarea class="input creator-material" id="cr-material"
        placeholder="the material: your notes, in your words (markdown welcome)"></textarea>
      <div class="creator-row">
        <label class="btn" for="cr-file" title="a text file as it is; a workbook, a Word file or a deck converted to text">add a file…</label>
        <input type="file" id="cr-file" hidden
          accept=".md,.txt,.csv,.tsv,.json,.yaml,.yml,.sql,.xml,.html,.py,.xlsx,.docx,.pptx" />
        <button class="btn primary" id="cr-draft" disabled>Draft with the model</button>
        <span class="muted" id="cr-status"></span>
      </div>
      <div class="creator-draft" id="cr-preview" hidden>
        <div class="creator-row">
          <input class="search mono" id="cr-name" placeholder="name (a slug)" maxlength="40" />
          <span class="muted" id="cr-chars"></span>
          <span class="spacer"></span>
          <button class="btn" id="cr-toggle" aria-pressed="false">edit the text</button>
          <button class="btn primary" id="cr-save">${isSkill
            ? "Save as my skill" : "Stage as a knowledge file"}</button>
        </div>
        <ul class="creator-notes muted" id="cr-notes"></ul>
        <div class="md creator-rendered" id="cr-rendered"></div>
        <textarea class="input creator-text" id="cr-text" hidden></textarea>
      </div>
      <p class="muted" id="cr-result"></p>
    </div>`;
}

const readB64 = (file) => new Promise((resolve, reject) => {
  const reader = new FileReader();
  reader.onload = () => resolve(String(reader.result).split(",")[1] || "");
  reader.onerror = () => reject(reader.error);
  reader.readAsDataURL(file);
});

// wires a creator panel: returns nothing; onSave(name, text, fields)
// does the saving and returns {ok, reason?, note?}
export function wireCreator(host, kind, onSave, onSaved) {
  const $ = (id) => host.querySelector(`#${id}`);
  const material = $("cr-material");
  const draftBtn = $("cr-draft");
  const status = $("cr-status");
  const result = $("cr-result");
  const preview = $("cr-preview");
  const rendered = $("cr-rendered");
  const textArea = $("cr-text");
  const nameInput = $("cr-name");
  const check = () => { draftBtn.disabled = !material.value.trim(); };
  material.addEventListener("input", check);
  $("cr-file").addEventListener("change", async (e) => {
    const file = e.target.files[0];
    e.target.value = "";
    if (!file) return;
    const ext = (file.name.split(".").pop() || "").toLowerCase();
    status.textContent = `reading ${file.name}…`;
    let text = "";
    if (TEXT_EXT.includes(ext)) {
      text = await file.text();
    } else {
      const got = await api.chatFileText(file.name, await readB64(file))
        .catch((err) => ({ available: false, reason: String(err) }));
      if (!got.available) { status.textContent = got.reason || "refused"; return; }
      text = got.text;
    }
    material.value = (material.value.trim()
      ? material.value.trim() + "\n\n" : "") + `<<< ${file.name} >>>\n${text}`;
    status.textContent = `${file.name} added to the material`;
    check();
  });
  const showDraft = (text) => {
    textArea.value = text;
    rendered.innerHTML = renderMarkdown(text, "md");
    $("cr-chars").textContent = `${text.length.toLocaleString()} characters`;
    preview.hidden = false;
  };
  draftBtn.addEventListener("click", async () => {
    draftBtn.disabled = true;
    status.textContent = "drafting… the model rewrites your material";
    result.textContent = "";
    const got = await api.chatDraft({
      kind, title: $("cr-title").value, hint: $("cr-hint").value,
      material: material.value }).catch((err) =>
      ({ available: false, reason: String(err) }));
    draftBtn.disabled = false;
    if (!got.available) {
      status.textContent = "";
      result.textContent = `no draft: ${got.reason || "the model did not answer"}. You can still write the text yourself below.`;
      nameInput.value = nameInput.value || slugOf($("cr-title").value);
      showDraft(textArea.value || `# ${$("cr-title").value || "Untitled"}\n\n`);
      return;
    }
    const d = got.draft;
    nameInput.value = d.name || slugOf(d.title || $("cr-title").value);
    $("cr-notes").innerHTML = (d.notes || []).map((n) =>
      `<li>${esc(n)}</li>`).join("");
    showDraft(d.text);
    status.textContent = "read the draft; edit it if you like; then save";
  });
  $("cr-toggle").addEventListener("click", () => {
    const editing = textArea.hidden;
    textArea.hidden = !editing;
    rendered.hidden = editing;
    $("cr-toggle").setAttribute("aria-pressed", String(editing));
    $("cr-toggle").textContent = editing ? "preview" : "edit the text";
    if (!editing) rendered.innerHTML = renderMarkdown(textArea.value, "md");
  });
  textArea.addEventListener("input", () => {
    $("cr-chars").textContent = `${textArea.value.length.toLocaleString()} characters`;
  });
  $("cr-save").addEventListener("click", async () => {
    const got = await onSave(nameInput.value.trim(), textArea.value, {
      title: $("cr-title").value, business_unit: ($("cr-bu") || {}).value });
    if (!got.ok) { result.textContent = got.reason || "not saved"; return; }
    result.textContent = got.note || "saved";
    if (onSaved) onSaved(got);
  });
}

export function slugOf(text) {
  return String(text || "").toLowerCase().replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "").slice(0, 40);
}

export async function renderSkills(outlet) {
  outlet.innerHTML = `
    <div class="library-page">
      <div class="page-head">
        <h1>Skills</h1>
        <p class="muted">What Synapse already knows how to do, and what
        you teach it. A skill is a doctrine pack: the moves for a kind
        of ask, the checks each move must run, the shape of the
        deliverable. Synapse loads the matching pack by itself when a
        question calls for it and says so in the activity trail; type
        <code>/</code> in the chat to load one on purpose. Yours load for
        you alone, wearing <b>unreviewed</b>: they steer, never assert
        facts.</p>
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
      <div id="skills-mine-head" class="shelf-head" hidden>Yours</div>
      <div class="skills-grid" id="skills-mine" hidden></div>
      <div class="shelf-head">Built in</div>
      <div class="skills-grid" id="skills-grid">${loading()}</div>
      <div id="skills-shared-head" class="shelf-head" hidden>Shared</div>
      <div class="skills-grid" id="skills-shared" hidden></div>
      ${creatorPanel("skill")}
    </div>`;

  const grid = outlet.querySelector("#skills-grid");
  async function list() {
    const got = await api.chatSkills().catch(() => ({}));
    if (!grid.isConnected) return;
    const packs = got.available ? got.skills || [] : [];
    const mine = packs.filter((s) => s.mine);
    const builtin = packs.filter((s) => !s.mine && s.origin === "built-in");
    const shared = packs.filter((s) => !s.mine && s.origin !== "built-in");
    outlet.querySelector("#skills-count").textContent = packs.length
      ? `${plural(packs.length, "skill")} on the shelf: ${builtin.length} built-in, ${
          mine.length} yours, ${shared.length} shared`
      : "";
    grid.innerHTML = builtin.length ? builtin.map(skillCard).join("")
      : `<p class="muted">${esc(got.reason || "no skills on file")}</p>`;
    const mineHost = outlet.querySelector("#skills-mine");
    mineHost.hidden = outlet.querySelector("#skills-mine-head").hidden
      = mine.length === 0;
    mineHost.innerHTML = mine.map(skillCard).join("");
    const sharedHost = outlet.querySelector("#skills-shared");
    sharedHost.hidden = outlet.querySelector("#skills-shared-head").hidden
      = shared.length === 0;
    sharedHost.innerHTML = shared.map(skillCard).join("");
  }
  await list();
  // Use in chat: a new chat opens with the slash command in the
  // composer; Delete: a pack of yours, gone from the shelf
  outlet.addEventListener("click", async (e) => {
    const use = e.target.closest(".skill-use");
    if (use) {
      try {
        sessionStorage.setItem("synapse.prefill", `/${use.dataset.name} `);
      } catch { /* storage refused: the slash menu still works */ }
      location.hash = "#/chat/new";
      return;
    }
    const del = e.target.closest(".skill-delete");
    if (del) {
      await api.chatDeleteSkill(del.dataset.name).catch(() => ({}));
      await list();
    }
  });
  wireCreator(outlet, "skill", async (name, text) => {
    const got = await api.chatSaveSkill(name, text)
      .catch((err) => ({ available: false, reason: String(err) }));
    if (!got.available) return { ok: false, reason: got.reason };
    return { ok: true, note: `saved as /${got.skill.name}${
      got.skill.replaced ? " (replaced your earlier version)" : ""}: it loads for you from the next chat on` };
  }, () => list());
}
