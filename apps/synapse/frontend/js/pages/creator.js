/** The creator: material in, a draft in the house format out, saved
 * only after the person has read it. One panel for a skill and for a
 * knowledge file (creatorPanel(kind)); wireCreator hands the save to
 * the page. A file added to the material rides as text: a text file
 * as it is, a workbook or a Word file converted by the server. */

import { api } from "../api.js";
import { renderMarkdown } from "../md.js";
import { esc } from "../ui.js";

const TEXT_EXT = ["md", "txt", "csv", "tsv", "json", "yaml", "yml", "sql",
                  "xml", "html", "py"];

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
export function wireCreator(host, kind, onSave, onSaved, opts = {}) {
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
  // writing by hand: the editor opens at once with the house template
  if (opts.prefill) {
    showDraft(opts.prefill);
    textArea.hidden = false;
    rendered.hidden = true;
    $("cr-toggle").setAttribute("aria-pressed", "true");
    $("cr-toggle").textContent = "preview";
    status.textContent = "write the pack, name it, then save";
  }
}

export function slugOf(text) {
  return String(text || "").toLowerCase().replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "").slice(0, 40);
}

