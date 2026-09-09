/** Skills: the shelf, the way a settings page lists it. One list of
 * what Synapse knows how to do (the doctrine packs: built in, yours,
 * shared) and what it knows about (the knowledge files the graph is
 * built from: the folders under graph/skills/ such as CFR/ and TLS/,
 * the staged drops, the reference docs), each with when it was last
 * written and who wrote it — Synapse for what ships with the
 * assistant, You for your own, a file's own author line, the
 * business unit a staged file was dropped for, or its folder. Search
 * from the toolbar; Browse and Add open one pop-up with three ways
 * in: bring a file, write one from the house template, or Draft with
 * Synapse from your material. A row opens in the reader on the right,
 * with Use in chat for a pack. */

import { api } from "../api.js";
import { renderMarkdown } from "../md.js";
import { createPullout } from "../pullout.js";
import { esc, loading } from "../ui.js";
import { openAddSkill } from "./addskill.js";

const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;
const when = (iso) => {
  const t = Date.parse(iso || "");
  return Number.isFinite(t)
    ? new Date(t).toLocaleDateString(undefined,
        { year: "2-digit", month: "numeric", day: "numeric" }) : "";
};
const KIND_LABEL = { skill: "skill", knowledge: "knowledge",
                     reference: "reference" };


// the rows of the shelf, from the two shelves the server keeps
function shelfRows(packs, files) {
  const rows = [];
  for (const p of packs) {
    rows.push({
      id: `pack:${p.name}`, kind: "skill", name: p.title || p.name,
      sub: `/${p.name}${p.description ? ` · ${p.description}` : ""}`,
      updated: p.updated || "", author: p.author || "", pack: p,
      mine: !!p.mine, sort: p.mine ? 0 : p.origin === "built-in" ? 1 : 2,
    });
  }
  for (const f of files) {
    if (f.family === "pack") continue;      // listed through the packs
    rows.push({
      id: `file:${f.rel}`,
      kind: f.family === "reference" ? "reference" : "knowledge",
      name: f.title || f.name,
      sub: `${f.rel}${f.staged ? " · staged" : ""}`,
      updated: f.updated || "", author: f.author || "", file: f,
      mine: false, sort: 3,
    });
  }
  return rows.sort((a, b) => a.sort - b.sort
    || String(b.updated).localeCompare(String(a.updated))
    || a.name.localeCompare(b.name));
}

function rowHtml(r) {
  return `
    <tr class="shelf-row" data-id="${esc(r.id)}" tabindex="0">
      <td><span class="shelf-name">${esc(r.name)}</span>
        <span class="shelf-sub" title="${esc(r.sub)}">${esc(r.sub)}</span></td>
      <td class="shelf-kind"><span class="chip">${esc(KIND_LABEL[r.kind])}</span></td>
      <td class="shelf-when" title="${esc(r.updated)}">${esc(when(r.updated))}</td>
      <td class="shelf-author">${esc(r.author)}</td>
    </tr>`;
}

export async function renderSkills(outlet) {
  outlet.innerHTML = `
    <div class="library-page skills-page">
      <div class="shelf-toolbar">
        <h1>Skills</h1>
        <span class="muted" id="sk-count"></span>
        <span class="spacer"></span>
        <button class="icon-btn" id="sk-search-btn" title="search"
          aria-expanded="false">⌕</button>
        <input class="search" id="sk-search"
          placeholder="search skills and knowledge…" hidden />
        <button class="btn" id="sk-browse"
          title="bring a markdown file of your own: it becomes a skill of yours">Browse</button>
        <button class="btn primary" id="sk-add">Add</button>
      </div>
      <p class="muted shelf-intro">What Synapse knows how to do, and what
        it knows about. A <b>skill</b> is doctrine: the moves for a kind
        of ask, loaded by itself when a question calls for it (type
        <code>/</code> in the chat to load one on purpose); yours load for
        you alone. A <b>knowledge</b> file is reference the graph is built
        from; one you add enters the graph on the next build.</p>
      <table class="shelf-table">
        <thead><tr><th>Skill</th><th>Kind</th><th>Last updated</th><th>Author</th></tr></thead>
        <tbody id="sk-rows"><tr><td colspan="4">${loading()}</td></tr></tbody>
      </table>
      <p class="muted" id="sk-note"></p>
    </div>`;
  const pullout = createPullout(outlet);
  const $ = (id) => outlet.querySelector(`#${id}`);
  const tbody = $("sk-rows");
  const note = $("sk-note");
  let rows = [];
  let needle = "";

  function paint() {
    const q = needle.trim().toLowerCase();
    const shown = rows.filter((r) => !q || [r.name, r.sub, r.author, r.kind]
      .some((v) => String(v || "").toLowerCase().includes(q)));
    tbody.innerHTML = shown.length ? shown.map(rowHtml).join("")
      : `<tr><td colspan="4" class="shelf-empty">${q
          ? `nothing matches "${esc(needle)}"`
          : "nothing on the shelf yet: Add one, or Browse for a file"}</td></tr>`;
    const skills = rows.filter((r) => r.kind === "skill").length;
    const knowledge = rows.filter((r) => r.kind === "knowledge").length;
    const reference = rows.filter((r) => r.kind === "reference").length;
    $("sk-count").textContent = rows.length
      ? `${plural(skills, "skill")} · ${plural(knowledge, "knowledge file")}${
          reference ? ` · ${plural(reference, "reference doc")}` : ""}` : "";
  }
  async function load() {
    const [packs, shelf] = await Promise.all([
      api.chatSkills().catch(() => ({})),
      api.artifacts().catch(() => ({})),
    ]);
    if (!tbody.isConnected) return;
    rows = shelfRows(packs.available ? packs.skills || [] : [],
                     shelf.files || []);
    if (!rows.length && (shelf.files_reason || packs.reason)) {
      note.textContent = shelf.files_reason || packs.reason || "";
    }
    paint();
  }

  // ── the reader: a row opens on the right, a pack with its doors ──
  async function openRow(id) {
    const r = rows.find((x) => x.id === id);
    if (!r) return;
    for (const tr of tbody.querySelectorAll(".shelf-row.on")) tr.classList.remove("on");
    tbody.querySelector(`.shelf-row[data-id="${CSS.escape(id)}"]`)?.classList.add("on");
    const clear = () => tbody.querySelector(".shelf-row.on")?.classList.remove("on");
    if (r.pack) {
      const p = r.pack;
      pullout.open({
        title: p.title || p.name, kind: r.mine ? "your skill" : `skill · ${p.author}`,
        sub: `/${p.name}`,
        html: `
          <div class="shelf-actions">
            <button class="btn primary sk-use" data-name="${esc(p.name)}">Use in chat</button>
            ${r.mine ? `<button class="btn sk-delete" data-name="${esc(p.name)}">Delete</button>` : ""}
          </div>
          <div class="md">${renderMarkdown(p.text || "", "md")}</div>`,
        raw: p.text || "", onClose: clear });
      return;
    }
    const file = await api.artifactFile(r.file.rel);
    if (!file.found) {
      pullout.open({ title: "not available", kind: "!", sub: r.file.rel,
                     raw: file.reason ?? "could not open it", onClose: clear });
      return;
    }
    pullout.open({
      title: r.name, kind: `${KIND_LABEL[r.kind]} · ${r.author || "unsigned"}`,
      sub: r.file.rel,
      html: renderMarkdown(file.content, file.kind), raw: file.content,
      onClose: clear });
  }
  tbody.addEventListener("click", (e) => {
    const tr = e.target.closest(".shelf-row");
    if (tr) openRow(tr.dataset.id);
  });
  tbody.addEventListener("keydown", (e) => {
    const tr = e.target.closest(".shelf-row");
    if (tr && (e.key === "Enter" || e.key === " ")) {
      e.preventDefault(); openRow(tr.dataset.id);
    }
  });
  outlet.addEventListener("click", async (e) => {
    const use = e.target.closest(".sk-use");
    if (use) {
      try { sessionStorage.setItem("synapse.prefill", `/${use.dataset.name} `); }
      catch { /* storage refused: the slash menu still works */ }
      location.hash = "#/chat/new";
      return;
    }
    const del = e.target.closest(".sk-delete");
    if (del) {
      await api.chatDeleteSkill(del.dataset.name).catch(() => ({}));
      pullout.close();                    // the reader was showing it
      await load();
    }
  });

  // ── search ──
  $("sk-search-btn").addEventListener("click", () => {
    const box = $("sk-search");
    box.hidden = !box.hidden;
    $("sk-search-btn").setAttribute("aria-expanded", String(!box.hidden));
    if (!box.hidden) box.focus();
    else { needle = ""; box.value = ""; paint(); }
  });
  $("sk-search").addEventListener("input", (e) => { needle = e.target.value; paint(); });

  // ── Browse and Add: one pop-up, three ways in ──
  const afterSave = (skill) => {
    note.textContent = `saved as /${skill.name}: it loads for you from the next chat on`;
    load();
  };
  $("sk-browse").addEventListener("click", () => openAddSkill("upload", afterSave));
  $("sk-add").addEventListener("click", () => openAddSkill("draft", afterSave));

  await load();
  return pullout.teardown;
}
