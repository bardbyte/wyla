/** Add a skill: one pop-up, three ways in — bring a file, write one
 * from the house template, or Draft with Radix from your material —
 * and one door out: every way ends as a submission to your manager
 * (the PRD's single-level approval). The person gives the name, a
 * description and the intended purpose; the system records the
 * submitter, the date, the version and the status; Synapse reads the
 * file for the manager. Nothing loads until the manager approves. */

import { api } from "../api.js";
import { renderMarkdown } from "../md.js";
import { esc } from "../ui.js";

export function slugOf(text) {
  return String(text || "").toLowerCase().replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "").slice(0, 40);
}

const TEXT_EXT = ["md", "txt", "csv", "tsv", "json", "yaml", "yml", "sql",
                  "xml", "html", "py", "markdown"];
const KNOWLEDGE_EXT = ["md", "txt", "csv", "json", "yaml", "yml", "sql"];

export const SKILL_TEMPLATE = `# <Title: the kind of ask this is for>

<One line: what this pack is for and when it applies.>

## <A move: the first thing to do for this kind of ask>
1. <A step, naming the tool when one is implied: search(...),
   run_sql(mode="dry_run"), check(kind=...), propose_sql, artifact.>
2. <The next step.>

## Never
- <A rule the material implies, stated as a prohibition.>
`;

const readB64 = (file) => new Promise((resolve, reject) => {
  const reader = new FileReader();
  reader.onload = () => resolve(String(reader.result).split(",")[1] || "");
  reader.onerror = () => reject(reader.error);
  reader.readAsDataURL(file);
});

const firstLine = (text) => {
  let title = "";
  for (const raw of String(text || "").split("\n")) {
    const line = raw.trim();
    if (!line) continue;
    if (line.startsWith("#") && !title) { title = line.replace(/^#+\s*/, ""); continue; }
    if (line.startsWith("<!--")) continue;
    return { title, description: line.slice(0, 160) };
  }
  return { title, description: "" };
};

// opens the pop-up on a tab: "upload" | "write" | "draft"
// onSubmitted(submission) runs after a submission, the pop-up closes
// itself; approver names who will review (the board's approver)
export function openAddSkill(tab = "upload", onSubmitted = null, approver = null) {
  document.getElementById("addskill")?.remove();
  const to = approver?.name || "your manager";
  const root = document.createElement("div");
  root.id = "addskill";
  root.className = "modal-root";
  root.innerHTML = `
    <div class="modal" role="dialog" aria-modal="true" aria-label="Add a skill">
      <div class="modal-head">
        <b>Add a skill</b>
        <span class="muted">it goes to ${esc(to)} for approval before it loads</span>
        <span class="spacer"></span>
        <button class="icon-btn" data-x title="Close">✕</button>
      </div>
      <div class="modal-tabs" role="tablist">
        <button role="tab" data-tab="upload">Bring a file</button>
        <button role="tab" data-tab="write">Write a skill</button>
        <button role="tab" data-tab="draft">Draft with Radix</button>
      </div>
      <div class="modal-body">
        <section data-pane="upload">
          <p class="muted">A markdown file of yours becomes a skill (doctrine:
            the moves for a kind of ask) or a knowledge file (reference for
            a business unit). The first heading is its title, the first line
            after it the one line the agent reads when deciding to load it.</p>
          <div class="as-row kind-toggle" role="radiogroup" aria-label="What kind of file">
            <button class="btn on" data-kind="skill" role="radio" aria-checked="true">Skill</button>
            <button class="btn" data-kind="knowledge" role="radio" aria-checked="false">Knowledge file</button>
            <input class="search" id="as-upload-bu" placeholder="business unit (e.g. TLS)" maxlength="40" hidden />
          </div>
          <div class="as-row">
            <input class="search" id="as-upload-description" placeholder="description, in a line (blank: the file's first line)" maxlength="400" />
          </div>
          <div class="as-row">
            <input class="search" id="as-upload-purpose" placeholder="intended purpose: what it is for, who asks (required)" maxlength="600" />
          </div>
          <label class="drop" for="as-file">
            <input type="file" id="as-file" multiple accept=".md,.txt,.markdown,.csv,.json,.yaml,.yml,.sql" hidden />
            <span class="drop-glyph">⇧</span>
            <span>Drop files here, or click to choose — each one is submitted for approval</span>
          </label>
          <ul class="as-outcomes muted" id="as-upload-outcomes"></ul>
        </section>
        <section data-pane="write" hidden>
          <p class="muted">The house template: a title, the one line, the
            moves as numbered steps naming the tool each implies, and the
            rules. Steer; never assert a fact about the data.</p>
          <div class="as-row">
            <input class="search mono" id="as-write-name" placeholder="name (a slug, e.g. churn-triage)" maxlength="40" />
            <span class="spacer"></span>
            <button class="btn" id="as-write-preview" aria-pressed="false">preview</button>
          </div>
          <div class="as-row">
            <input class="search" id="as-write-purpose" placeholder="intended purpose: what it is for, who asks (required)" maxlength="600" />
          </div>
          <textarea class="input as-editor" id="as-write-text"></textarea>
          <div class="md as-rendered" id="as-write-rendered" hidden></div>
          <div class="as-row">
            <span class="muted" id="as-write-note"></span>
            <span class="spacer"></span>
            <button class="btn primary" id="as-write-save">Submit for approval</button>
          </div>
        </section>
        <section data-pane="draft" hidden>
          <ol class="as-steps">
            <li data-step="1" class="on"><b>Material</b> what you know, in your words</li>
            <li data-step="2"><b>Draft</b> Radix rewrites it as a pack</li>
            <li data-step="3"><b>Read, edit, submit</b> to ${esc(to)} for approval</li>
          </ol>
          <div data-draft="1">
            <div class="as-row">
              <input class="search" id="as-title" placeholder="title (e.g. Churn triage)" />
              <input class="search" id="as-hint" placeholder="intended purpose: what it is for, in a line (e.g. why did approvals move)" />
            </div>
            <textarea class="input as-material" id="as-material"
              placeholder="paste your notes, a runbook, an email — how your team reads a metric, the steps you take for a kind of question"></textarea>
            <div class="as-row">
              <label class="btn" for="as-material-file" title="a text file as it is; a workbook, a Word file or a deck converted to text">add a file…</label>
              <input type="file" id="as-material-file" hidden
                accept=".md,.txt,.csv,.tsv,.json,.yaml,.yml,.sql,.xml,.html,.py,.xlsx,.docx,.pptx" />
              <span class="muted" id="as-material-note"></span>
              <span class="spacer"></span>
              <button class="btn primary" id="as-draft-go" disabled>Draft with Radix</button>
            </div>
          </div>
          <div data-draft="2" hidden>
            <p class="muted as-wait"><span class="think-orb">✳</span> Radix is rewriting your material in the house format…</p>
          </div>
          <div data-draft="3" hidden>
            <ul class="as-notes muted" id="as-notes"></ul>
            <div class="as-row">
              <input class="search mono" id="as-draft-name" placeholder="name (a slug)" maxlength="40" />
              <span class="muted" id="as-draft-chars"></span>
              <span class="spacer"></span>
              <button class="btn" id="as-draft-toggle" aria-pressed="false">edit the text</button>
            </div>
            <div class="md as-rendered" id="as-draft-rendered"></div>
            <textarea class="input as-editor" id="as-draft-text" hidden></textarea>
            <div class="as-row">
              <button class="btn" id="as-draft-back">← material</button>
              <span class="muted" id="as-draft-note"></span>
              <span class="spacer"></span>
              <button class="btn primary" id="as-draft-save">Submit for approval</button>
            </div>
          </div>
          <p class="muted" id="as-draft-error"></p>
        </section>
      </div>
    </div>`;
  document.body.appendChild(root);
  const $ = (id) => root.querySelector(`#${id}`);
  const close = () => { root.remove(); document.removeEventListener("keydown", onKey); };
  const onKey = (e) => { if (e.key === "Escape") close(); };
  document.addEventListener("keydown", onKey);
  root.addEventListener("click", (e) => {
    if (e.target === root || e.target.closest("[data-x]")) close();
  });
  const submitted = (submission) => {
    if (onSubmitted) onSubmitted(submission);
    close();
  };
  // one door out: the submission, with what the PRD asks for
  const submit = (body) => api.chatSubmitReview(body)
    .catch((err) => ({ available: false, reason: String(err) }));

  // ── tabs ──
  function show(name) {
    for (const t of root.querySelectorAll("[role=tab]")) {
      t.classList.toggle("on", t.dataset.tab === name);
      t.setAttribute("aria-selected", String(t.dataset.tab === name));
    }
    for (const pane of root.querySelectorAll("[data-pane]")) {
      pane.hidden = pane.dataset.pane !== name;
    }
  }
  root.querySelector(".modal-tabs").addEventListener("click", (e) => {
    const t = e.target.closest("[role=tab]");
    if (t) show(t.dataset.tab);
  });
  show(tab);

  // ── bring a file: a skill, or a knowledge file for a business unit ──
  let uploadKind = "skill";
  root.querySelector(".kind-toggle").addEventListener("click", (e) => {
    const b = e.target.closest("[data-kind]");
    if (!b) return;
    uploadKind = b.dataset.kind;
    for (const o of root.querySelectorAll("[data-kind]")) {
      o.classList.toggle("on", o === b);
      o.setAttribute("aria-checked", String(o === b));
    }
    $("as-upload-bu").hidden = uploadKind !== "knowledge";
    $("as-file").accept = uploadKind === "knowledge"
      ? KNOWLEDGE_EXT.map((k) => `.${k}`).join(",") : ".md,.txt,.markdown";
  });
  $("as-file").addEventListener("change", async (e) => {
    const picked = [...e.target.files];
    e.target.value = "";
    const list = $("as-upload-outcomes");
    const purpose = $("as-upload-purpose").value.trim();
    if (!purpose) {
      list.innerHTML = "<li>say what it is for first: the intended purpose is part of the submission</li>";
      $("as-upload-purpose").focus();
      return;
    }
    let last = null;
    for (const file of picked) {
      const text = await file.text();
      const ext = (file.name.split(".").pop() || "md").toLowerCase();
      const stem = file.name.replace(/\.[^.]+$/, "");
      const got = await submit({
        kind: uploadKind, name: slugOf(stem), title: firstLine(text).title || stem,
        description: $("as-upload-description").value.trim(),
        purpose, text, business_unit: $("as-upload-bu").value.trim(),
        ext: KNOWLEDGE_EXT.includes(ext) ? ext : "md" });
      const li = document.createElement("li");
      li.textContent = got.available
        ? `${file.name} → /${got.submission.name} submitted to ${
            got.submission.approver?.name || to} for approval${
            got.resubmitted ? ` (v${got.submission.version}: a new version of your earlier one)` : ""}`
        : `${file.name}: ${got.reason || "refused"}`;
      list.appendChild(li);
      if (got.available) last = { ...got.submission, resubmitted: got.resubmitted };
    }
    if (last) setTimeout(() => submitted(last), 600);
  });
  const drop = root.querySelector(".drop");
  drop.addEventListener("dragover", (e) => { e.preventDefault(); drop.classList.add("over"); });
  drop.addEventListener("dragleave", () => drop.classList.remove("over"));
  drop.addEventListener("drop", (e) => {
    e.preventDefault(); drop.classList.remove("over");
    const input = $("as-file");
    input.files = e.dataTransfer.files;
    input.dispatchEvent(new Event("change"));
  });

  // ── write a skill ──
  $("as-write-text").value = SKILL_TEMPLATE;
  $("as-write-preview").addEventListener("click", () => {
    const editing = !$("as-write-rendered").hidden;
    $("as-write-rendered").hidden = editing;
    $("as-write-text").hidden = !editing;
    $("as-write-preview").setAttribute("aria-pressed", String(!editing));
    $("as-write-preview").textContent = editing ? "preview" : "edit";
    if (!editing) $("as-write-rendered").innerHTML = renderMarkdown($("as-write-text").value, "md");
  });
  $("as-write-save").addEventListener("click", async () => {
    const text = $("as-write-text").value;
    const head = firstLine(text);
    const name = $("as-write-name").value.trim() || slugOf(head.title);
    const purpose = $("as-write-purpose").value.trim();
    if (!purpose) { $("as-write-note").textContent = "say what it is for: the intended purpose is part of the submission"; $("as-write-purpose").focus(); return; }
    const got = await submit({ kind: "skill", name, title: head.title, description: head.description,
                               purpose, text });
    $("as-write-note").textContent = got.available
      ? `submitted to ${got.submission.approver?.name || to} for approval` : (got.reason || "not submitted");
    if (got.available) setTimeout(() => submitted({ ...got.submission, resubmitted: got.resubmitted }), 400);
  });

  // ── Draft with Radix ──
  const step = (n) => {
    for (const li of root.querySelectorAll(".as-steps li")) {
      li.classList.toggle("on", Number(li.dataset.step) === n);
      li.classList.toggle("done", Number(li.dataset.step) < n);
    }
    for (const pane of root.querySelectorAll("[data-draft]")) {
      pane.hidden = Number(pane.dataset.draft) !== n;
    }
  };
  const material = $("as-material");
  material.addEventListener("input", () => { $("as-draft-go").disabled = !material.value.trim(); });
  $("as-material-file").addEventListener("change", async (e) => {
    const file = e.target.files[0];
    e.target.value = "";
    if (!file) return;
    const ext = (file.name.split(".").pop() || "").toLowerCase();
    $("as-material-note").textContent = `reading ${file.name}…`;
    let text = "";
    if (TEXT_EXT.includes(ext)) text = await file.text();
    else {
      const got = await api.chatFileText(file.name, await readB64(file))
        .catch((err) => ({ available: false, reason: String(err) }));
      if (!got.available) { $("as-material-note").textContent = got.reason || "refused"; return; }
      text = got.text;
    }
    material.value = (material.value.trim() ? material.value.trim() + "\n\n" : "")
      + `<<< ${file.name} >>>\n${text}`;
    $("as-material-note").textContent = `${file.name} added to the material`;
    $("as-draft-go").disabled = false;
  });
  const showDraft = (text) => {
    $("as-draft-text").value = text;
    $("as-draft-rendered").innerHTML = renderMarkdown(text, "md");
    $("as-draft-chars").textContent = `${text.length.toLocaleString()} characters`;
  };
  $("as-draft-go").addEventListener("click", async () => {
    $("as-draft-error").textContent = "";
    step(2);
    const got = await api.chatDraft({ kind: "skill", title: $("as-title").value,
      hint: $("as-hint").value, material: material.value })
      .catch((err) => ({ available: false, reason: String(err) }));
    if (!got.available) {
      step(1);
      $("as-draft-error").textContent = `no draft: ${got.reason || "Radix did not answer"}. Write it yourself on the Write tab, or try again.`;
      return;
    }
    const d = got.draft;
    $("as-draft-name").value = d.name || slugOf(d.title || $("as-title").value);
    $("as-notes").innerHTML = (d.notes || []).map((n) => `<li>${esc(n)}</li>`).join("");
    showDraft(d.text);
    $("as-draft-note").textContent = "read it; edit if you like; then submit";
    step(3);
  });
  $("as-draft-toggle").addEventListener("click", () => {
    const editing = $("as-draft-text").hidden;
    $("as-draft-text").hidden = !editing;
    $("as-draft-rendered").hidden = editing;
    $("as-draft-toggle").setAttribute("aria-pressed", String(editing));
    $("as-draft-toggle").textContent = editing ? "preview" : "edit the text";
    if (!editing) showDraft($("as-draft-text").value);
  });
  $("as-draft-back").addEventListener("click", () => step(1));
  $("as-draft-save").addEventListener("click", async () => {
    const text = $("as-draft-text").value;
    const head = firstLine(text);
    const purpose = $("as-hint").value.trim();
    if (!purpose) { $("as-draft-note").textContent = "say what it is for on the material step: the intended purpose is part of the submission"; return; }
    const got = await submit({ kind: "skill", name: $("as-draft-name").value.trim(),
                               title: head.title || $("as-title").value, description: head.description,
                               purpose, text });
    $("as-draft-note").textContent = got.available
      ? `submitted to ${got.submission.approver?.name || to} for approval` : (got.reason || "not submitted");
    if (got.available) setTimeout(() => submitted({ ...got.submission, resubmitted: got.resubmitted }), 400);
  });
  return close;
}
