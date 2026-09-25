/** Synapse v3 (docs/specs/synapse_v3_harness.md §6): the
 * conversational surface — a streamed conversation with ONE live
 * activity line that collapses into a friendly summary, and an
 * artifact panel that opens only when the model puts something in it.
 *
 * A pure consumer of the assistant event stream: nothing here calls
 * a model, holds a key, or invents a value. Artifacts render exactly
 * what the validator stored — including the EXPLORATORY watermark —
 * and exports carry the provenance footer. No harness words reach
 * the user: no transcript dump, no JSON, no tool ids.
 */

import { api } from "../api.js";
import { mountDepthKnob, mountModelPicker } from "../knobs.js";
import { renderMarkdown } from "../md.js";
import { esc, prose, statusLabel } from "../ui.js";

// the italic line under every number, artifact and proposal: who
// prepared it and how far to trust it. The definition line the graph
// gave stays one hover away (the title) and in the receipts.
const disclaimer = (prov) => (prov && prov.status === "certified")
  ? "Prepared by Radix from a certified definition. Check the receipts "
    + "before you act on a number."
  : "Prepared by Radix. Exploratory until a check stands behind it: "
    + "verify before you rely on it.";

const SESSION_KEY = "synapse-chat-session";
const PALETTE = ["#2f6feb", "#e8710a", "#1a9850", "#9970ab",
                 "#d6604d", "#35978f"];

export async function renderChat(outlet, wanted = "") {
  outlet.innerHTML = `
    <div class="chatv2 empty" id="chatv2">
      <div class="chat-main">
        <div class="chat-scroll" id="chat-scroll">
        <div class="chat-masthead">
          <button class="chat-title-btn" id="chat-title"
            title="Rename this chat">
            <span class="chat-title-text">New chat</span>
            <span class="chev">⌄</span></button>
          <span class="muted chat-build" id="chat-build" hidden></span>
          <span class="spacer"></span>
          <span class="muted" id="chat-meter" hidden></span>
          <button class="btn" id="chat-memory-btn"
            aria-expanded="false">⊚ memory</button>
          <div id="chat-memory-pop" class="skills-pop" hidden></div>
          <button class="btn" id="chat-share"
            title="Copy a link to this chat">Share</button>
        </div>
        <div class="chat-hero" id="chat-hero">
          <span class="think-orb hero-orb" aria-hidden="true">✳</span>
          <h1 class="chat-greet" id="chat-greet"></h1>
        </div>
        <div class="chat-thread" id="chat-thread"></div>
        <div class="chat-chiprow" id="chat-chiprow"></div>
        <div class="chat-composer">
          <div class="chat-box">
            <textarea id="chat-input" rows="1"
              placeholder="Type / for skills"></textarea>
            <div class="chat-slash" id="chat-slash" hidden></div>
            <div class="chat-files" id="chat-files" hidden></div>
            <input type="file" id="chat-file-input" multiple hidden />
            <div class="chat-actions">
              <button class="icon-btn chat-plus" id="chat-plus"
                title="More" aria-expanded="false">+</button>
              <div class="chat-plus-pop" id="chat-plus-pop" hidden></div>
              <div class="chat-skills" id="chat-skills" hidden
                aria-label="Skills on this chat"></div>
              <span class="spacer"></span>
              <select id="chat-model" class="chat-depth chat-plane" hidden
                title="Which model answers this chat"></select>
              <select id="chat-depth" class="chat-depth" hidden
                title="How deeply Radix thinks on this ask">
                <option value="minimal">Minimal</option>
                <option value="quick">Quick</option>
                <option value="standard" selected>Standard</option>
                <option value="deep">Deep</option>
                <option value="max">Extra deep</option>
              </select>
              <button class="chat-pill" id="chat-model-btn" type="button"
                aria-haspopup="listbox" aria-expanded="false"
                title="Which model answers this chat"><span class="pill-label">Model</span><span class="chev">⌄</span></button>
              <div class="knob-pop model-pop" id="chat-model-pop" hidden role="listbox"
                aria-label="Model"></div>
              <button class="chat-pill" id="chat-depth-btn" type="button"
                aria-haspopup="dialog" aria-expanded="false"
                title="How deeply Radix thinks on this ask"><span class="pill-label">Thinking effort</span><span class="chev">⌄</span></button>
              <div class="knob-pop depth-pop" id="chat-depth-pop" hidden role="dialog"
                aria-label="Thinking effort"></div>
              <button class="icon-btn chat-help" id="chat-help"
                title="What the thinking levels and the models mean"
                aria-label="Explain the dials" aria-expanded="false">?</button>
              <div class="chat-help-pop" id="chat-help-pop" hidden></div>
              <button class="btn primary chat-send chat-stop" id="chat-stop" type="button"
                hidden aria-label="Stop" title="Stop"><span class="stop-glyph"
                aria-hidden="true"></span></button>
              <button class="btn primary chat-send" id="chat-send"
                title="Send · Enter">↑</button>
            </div>
          </div>
          <div class="chat-foot">Radix is AI and can make mistakes.
            Check the receipts before you act on a number.</div>
        </div>
        </div>
        <button class="chat-jump" id="chat-jump" type="button" hidden
          title="Jump to the latest">↓ Latest</button>
      </div>
    </div>`;

  const el = (id) => outlet.querySelector("#" + id);
  const thread = el("chat-thread");
  const input = el("chat-input");
  const state = { session: null, source: null, turns: new Map(),
                  running: false, seq: 0, artifacts: new Map() };

  // ── the whole main pane is the scroll surface (#chat-scroll): the
  //    thread scrolls under the masthead and past the chips, the
  //    composer stays docked at the bottom of the pane, and the wheel
  //    works in the gutters beside the column too — nothing between
  //    the masthead and the composer traps it. The thread follows new
  //    content only while the reader is at the bottom. Scrolling up to
  //    reread unsticks it, so a thinking delta, an answer token or the
  //    one-second heartbeat never yanks the view back down; sending a
  //    message re-sticks it; while new content lands out of view a
  //    "Latest" pill offers the way back ──
  const NEAR_BOTTOM = 48;
  const scroller = el("chat-scroll");
  const composer = outlet.querySelector(".chat-composer");
  const jump = el("chat-jump");
  const atBottom = () => scroller.scrollHeight - scroller.scrollTop
    - scroller.clientHeight <= NEAR_BOTTOM;
  let stuck = true;
  const scroll = (force = false) => {
    if (force) stuck = true;
    if (stuck) {
      scroller.scrollTop = scroller.scrollHeight;
      jump.hidden = true;
      return;
    }
    // the pill sits just above the docked composer, whatever its
    // height is at the moment
    jump.style.bottom = `${composer.offsetHeight + 12}px`;
    jump.hidden = false;
  };
  scroller.addEventListener("scroll", () => {
    stuck = atBottom();
    if (stuck) jump.hidden = true;
  }, { passive: true });
  jump.addEventListener("click", () => scroll(true));
  const say = (html, cls = "") => {
    const div = document.createElement("div");
    div.className = `ask-note ${cls}`;
    div.innerHTML = html;
    thread.appendChild(div);
    scroll();
  };

  // ── boot: reopen or create ("new" always starts fresh) ───
  let boot = null;
  const stored = wanted === "new" ? ""
    : wanted || localStorage.getItem(SESSION_KEY);
  if (stored) {
    boot = await api.chatSession(stored);
    if (!boot.available) boot = null;
  }
  if (!boot && wanted === "new") {
    // an empty chat already exists: reuse it rather than pile them up
    const recent = await api.chatSessions(10).catch(() => ({}));
    const empty = (recent.sessions || []).find((r) => r.messages === 0);
    if (empty) {
      boot = await api.chatSession(empty.id);
      if (!boot.available) boot = null;
    }
  }
  if (!boot) {
    const created = await api.chatNewSession();
    if (!created.available) {
      say(`<b>Chat is not available.</b> ${esc(created.reason ?? "")}`,
          "error");
      return () => {};
    }
    boot = await api.chatSession(created.session.id);
  }
  state.session = boot.session;
  localStorage.setItem(SESSION_KEY, state.session.id);
  if (wanted !== state.session.id) {
    // settle the URL on the real session without re-routing
    history.replaceState(null, "", `#/chat/${state.session.id}`);
  }
  el("chat-build").textContent =
    `build ${state.session.build_id || "?"}`;

  // ── the empty state: a greeting and the composer, nothing else;
  //    the first message turns it into the conversation ──────
  const shell = el("chatv2");
  const hour = new Date().getHours();
  const dayPart = hour < 12 ? "Morning"
    : hour < 17 ? "Afternoon" : "Evening";
  const first = String(boot.user_name || "").trim().split(/\s+/)[0];
  el("chat-greet").textContent = first
    ? `${dayPart}, ${first}.` : `${dayPart}, how are things?`;
  // ── the dials, explained: one catalog from the backend fills the
  //    model knob, the thinking knob and the "?" popover — the same
  //    source the admin console reads. This surface carries no
  //    chat/autopilot switch: the one mode (below). state.plane holds
  //    the model choice: a plane (its default model) or plane:model
  const planeSel = el("chat-model");
  state.plane = boot.choice || boot.plane || "";
  planeSel.innerHTML = `<option value="${esc(state.plane)}" selected>${
    esc(boot.model || "")}</option>`;
  const modelKnob = mountModelPicker({
    button: el("chat-model-btn"), pop: el("chat-model-pop"), select: planeSel,
    below: () => shell.classList.contains("empty") });
  const depthKnob = mountDepthKnob({
    button: el("chat-depth-btn"), pop: el("chat-depth-pop"), select: el("chat-depth"),
    below: () => shell.classList.contains("empty") });
  const helpPop = el("chat-help-pop");
  const helpRow = (label, text, fact = "") => `
    <div class="help-row"><b>${esc(label)}</b><span>${esc(text)}${
      fact ? `<i class="help-fact">${esc(fact)}</i>` : ""}</span></div>`;
  async function loadDials() {
    let dials = null;
    try { dials = await api.chatDials(); } catch { dials = null; }
    if (!dials || !dials.available) return;
    if (!el("chat-depth") || !planeSel.isConnected) return;    // page left
    const models = (dials.models || []).length ? dials.models
      : (dials.planes || []).map((p) => ({ ...p, plane: p.id }));
    modelKnob.setModels(models);
    depthKnob.setDepths(dials.depths || []);
    const notes = dials.notes || {};
    // the "?" explains the thinking levels and the models
    helpPop.innerHTML = `
      <div class="help-group">
        <div class="help-head">Thinking effort <span>how much Radix thinks before each step</span></div>
        ${(dials.depths || []).map((d) => helpRow(d.label, d.means)).join("")}
      </div>
      <div class="help-group">
        <div class="help-head">Model <span>${esc(notes.plane || "")}</span></div>
        ${models.map((m) => helpRow(m.label, m.fit ? `${m.fit} ${m.means}` : m.means, [
          m.available ? (m.default ? "available · where a new chat starts" : "available")
                      : `not available here: ${m.reason}`,
          m.facts || ""].filter(Boolean).join(" · "))).join("")}
      </div>`;
  }
  loadDials();
  // the model switch is remembered on the chat and rides the next
  // message; a model this machine cannot ride is refused with the
  // reason and the knob goes back to the one that works
  planeSel.addEventListener("change", async () => {
    const wanted = planeSel.value;
    if (!state.session || !state.session.id) return;
    const got = await api.chatSetModel(state.session.id, wanted);
    if (!got.available) {
      setEmpty(false);                 // the refusal must be seen
      say(`<b>model not switched.</b> ${esc(got.reason || "")}`, "error");
      planeSel.value = state.plane;
      modelKnob.refresh();
      return;
    }
    state.plane = got.choice || got.plane || wanted;
    modelKnob.refresh();
    if (!shell.classList.contains("empty")) {
      const shown = planeSel.selectedOptions[0]?.textContent || got.model || wanted;
      say(`Switched to <b>${esc(shown)}</b> from the next message on.`);
    }
  });
  // above the composer when it is docked at the bottom, below it while
  // the chat is empty and the composer sits mid-screen; never past the
  // edge of the window — it scrolls inside instead
  function placeHelp() {
    const box = (helpPop.offsetParent || helpPop.parentElement)
      .getBoundingClientRect();
    const below = shell.classList.contains("empty");
    helpPop.classList.toggle("below", below);
    const room = below ? window.innerHeight - box.bottom - 16 : box.top - 16;
    helpPop.style.maxHeight =
      `${Math.max(180, Math.min(room, window.innerHeight * 0.8))}px`;
  }
  el("chat-help").addEventListener("click", () => {
    helpPop.hidden = !helpPop.hidden;
    el("chat-help").setAttribute("aria-expanded", String(!helpPop.hidden));
    if (!helpPop.hidden) placeHelp();
  });
  function setEmpty(empty) {
    shell.classList.toggle("empty", empty);
    input.placeholder = empty ? "Type / for skills" : "Write a message…";
  }
  const titleText = el("chat-title").querySelector(".chat-title-text");
  function setTitle(title) {
    titleText.textContent = (title || "").trim() || "New chat";
    state.session.title = title || "";
  }
  setTitle(boot.session.title);
  async function refreshTitle() {
    const got = await api.chatSession(state.session.id)
      .catch(() => ({}));
    if (got.available) setTitle(got.session.title);
  }
  // rename: the title is a button; a click makes it an input
  el("chat-title").addEventListener("click", () => {
    if (el("chat-title").hidden) return;
    const box = document.createElement("input");
    box.className = "chat-title-input";
    box.value = titleText.textContent === "New chat"
      ? "" : titleText.textContent;
    box.placeholder = "Name this chat";
    el("chat-title").hidden = true;
    el("chat-title").after(box);
    box.focus();
    let settled = false;
    const done = async (save) => {
      if (settled) return;
      settled = true;
      const title = box.value.trim();
      box.remove();
      el("chat-title").hidden = false;
      if (save && title && title !== state.session.title) {
        await api.chatRename(state.session.id, title);
        setTitle(title);
        pingShelf();
      }
    };
    box.addEventListener("keydown", (e) => {
      if (e.key === "Enter") done(true);
      if (e.key === "Escape") done(false);
    });
    box.addEventListener("blur", () => done(true));
  });
  // share: a link to this chat, on the clipboard
  el("chat-share").addEventListener("click", async () => {
    const link = location.href;
    try {
      await navigator.clipboard.writeText(link);
      el("chat-share").textContent = "Link copied";
    } catch {
      el("chat-share").textContent = link;
    }
    setTimeout(() => { el("chat-share").textContent = "Share"; }, 1800);
  });
  // the + menu: the real doors, never a dead control
  const plusPop = el("chat-plus-pop");
  plusPop.innerHTML = `
    <button class="plus-item" id="plus-files">⊕ Add files</button>
    <span class="plus-note muted" id="plus-files-note">PDF, images, text,
      CSV, JSON, workbooks, Word files, decks</span>`;
  el("chat-plus").addEventListener("click", () => {
    plusPop.hidden = !plusPop.hidden;
    el("chat-plus").setAttribute("aria-expanded",
                                 String(!plusPop.hidden));
  });
  // ── files: added from the plus menu, shown as chips, riding the
  //    next message; a refused file says why and leaves nothing behind
  const fileInput = el("chat-file-input");
  const filesRow = el("chat-files");
  state.files = [];                       // pending, in order
  const fmtSize = (n) => n >= 1048576 ? `${(n / 1048576).toFixed(1)} MB`
    : n >= 1024 ? `${Math.round(n / 1024)} KB` : `${n} B`;
  function paintFiles() {
    filesRow.hidden = state.files.length === 0;
    filesRow.innerHTML = state.files.map((f) => `
      <span class="file-chip" data-id="${esc(f.id)}" title="${esc(
        f.note || `${f.family} · rides ${f.rides === "inline" ? "as itself"
                   : "as text"}`)}">
        <span class="file-kind">${esc(f.suffix)}</span>
        <span class="file-name">${esc(f.name)}</span>
        <span class="muted">${fmtSize(f.size)}</span>
        <button class="file-x" type="button" title="remove">×</button>
      </span>`).join("");
  }
  api.chatFileSupport().then((got) => {
    if (got && got.available) {
      const kinds = [...new Set(got.accepted.map((a) => a.suffix))];
      el("plus-files-note").textContent =
        `${kinds.join(", ")} · up to ${got.max_file_mb} MB each`;
      fileInput.accept = kinds.map((k) => `.${k}`).join(",");
    }
  }).catch(() => {});
  plusPop.querySelector("#plus-files").addEventListener("click", () => {
    plusPop.hidden = true;
    fileInput.click();
  });
  const readB64 = (file) => new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result).split(",")[1] || "");
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(file);
  });
  fileInput.addEventListener("change", async () => {
    const picked = [...fileInput.files];
    fileInput.value = "";
    for (const file of picked) {
      let got;
      try {
        got = await api.chatUpload(state.session.id, file.name,
                                   await readB64(file));
      } catch (e) {
        got = { available: false, reason: String(e) };
      }
      if (!got.available) {
        setEmpty(false);
        say(`<b>not attached.</b> ${esc(got.reason || file.name)}`, "error");
        continue;
      }
      state.files.push(got.file);
      paintFiles();
    }
  });
  filesRow.addEventListener("click", async (e) => {
    const btn = e.target.closest(".file-x");
    if (!btn) return;
    const chip = btn.closest(".file-chip");
    const id = chip.dataset.id;
    state.files = state.files.filter((f) => f.id !== id);
    paintFiles();
    api.chatRemoveFile(state.session.id, id).catch(() => {});
  });
  // files uploaded before a reload are still pending on the chat
  for (const f of (boot.files || [])) {
    if (!f.sent_turn) state.files.push(f);
  }
  paintFiles();
  // the mode (§5) is fixed on this surface: Chat hands every query
  // over for the person to run. The run-and-build-without-stopping
  // mode stays a runtime mode the admin console can pick; there is
  // no switch here, and no remembered preference can steer it
  const MODE = "chat";
  state.mode = MODE;
  // "/" lists the skills: pick one and it rides the chat as a chip
  // where the mode pill used to be — pinned on the session, so every
  // turn loads it until the × — the way an attached image sits in a
  // composer. Typing "/name and the words" still works as text.
  const slash = el("chat-slash");
  const skillsRow = el("chat-skills");
  state.skills = [...(boot.session.skills || [])];
  let packs = null;
  async function loadPacks() {
    if (packs) return packs;
    const got = await api.chatSkills().catch(() => ({}));
    packs = got.available ? got.skills || [] : [];
    return packs;
  }
  const packTitle = (name) => {
    const p = (packs || []).find((x) => x.name === name);
    return p ? (p.title || p.name) : name;
  };
  function paintSkills() {
    skillsRow.hidden = state.skills.length === 0;
    skillsRow.innerHTML = state.skills.map((name) => `
      <span class="skill-chip" data-name="${esc(name)}"
        title="/${esc(name)} rides every message of this chat">
        <span class="skill-glyph" aria-hidden="true">✦</span>
        <span class="skill-name">${esc(packTitle(name))}</span>
        <button class="skill-x" type="button" title="remove from this chat"
          aria-label="remove ${esc(name)}">×</button>
      </span>`).join("");
  }
  async function pinSkills(names) {
    const got = await api.chatSetSkills(state.session.id, names)
      .catch(() => ({ available: false, reason: "no answer" }));
    if (!got.available || got.ok === false) {
      setEmpty(false);
      say(`<b>skill not added.</b> ${esc(got.reason || "")}`, "error");
      return false;
    }
    state.skills = got.skills || names;
    paintSkills();
    return true;
  }
  async function pickSlash(name) {
    slash.hidden = true;
    input.value = input.value.replace(/^\/[A-Za-z0-9_\-]*\s*/, "");
    await loadPacks();
    if (!state.skills.includes(name)) {
      if (state.skills.length >= 4) {
        setEmpty(false);
        say("<b>four skills at most</b> on one chat: remove one first.", "error");
      } else {
        await pinSkills([...state.skills, name]);
      }
    }
    input.focus();
  }
  skillsRow.addEventListener("click", (e) => {
    const x = e.target.closest(".skill-x");
    if (!x) return;
    const name = x.closest(".skill-chip").dataset.name;
    pinSkills(state.skills.filter((n) => n !== name));
  });
  loadPacks().then(paintSkills);
  // the status the Skills page shows, on the menu too: Built in for
  // what ships with Synapse, Published for a pack of yours the manager
  // approved, Shared for the shelf's shared packs
  const packStatus = (p) => p.origin === "built-in"
    ? ["s-builtin", "Built in"] : p.mine
      ? ["s-published", "Published"] : ["s-shared", "Shared"];
  const packRank = (p) => (p.mine ? 0 : p.origin === "built-in" ? 1 : 2);
  // below the composer while the chat is empty and the composer sits
  // mid-screen, above it once docked; never past the window's edge —
  // it scrolls inside instead
  function placeSlash() {
    const box = (slash.offsetParent || slash.parentElement)
      .getBoundingClientRect();
    const below = shell.classList.contains("empty");
    slash.classList.toggle("below", below);
    const room = below ? window.innerHeight - box.bottom - 16 : box.top - 16;
    slash.style.maxHeight =
      `${Math.max(160, Math.min(room, window.innerHeight * 0.7))}px`;
  }
  async function paintSlash() {
    const m = input.value.match(/^\/([A-Za-z0-9_\-]*)$/);
    if (!m) { slash.hidden = true; return; }
    // the whole shelf, not a first few: yours first, then the built-in
    // packs, then the shared ones; the menu scrolls past the fold
    const rows = (await loadPacks()).filter((p) =>
      String(p.name || "").toLowerCase().startsWith(m[1].toLowerCase()))
      .sort((a, b) => packRank(a) - packRank(b)
        || String(a.name).localeCompare(String(b.name)));
    slash.innerHTML = rows.length ? rows.map((p) => {
      const [cls, label] = packStatus(p);
      return `
      <button class="slash-item" data-name="${esc(p.name)}">
        <b>/${esc(p.name)}</b>
        <span class="muted slash-title">${esc(p.title || "")}</span>
        <span class="sub-status slash-status ${cls}">${label}</span></button>`;
    }).join("")
      : `<div class="muted slash-none">No skill starts with “/${
          esc(m[1])}” — the Skills tab lists them all.</div>`;
    slash.hidden = false;
    placeSlash();
    for (const b of slash.querySelectorAll(".slash-item")) {
      b.addEventListener("click", () => pickSlash(b.dataset.name));
    }
  }
  input.addEventListener("input", paintSlash);
  // the Skills page hands a pack over (Use in chat): a bare "/name"
  // becomes the chip; anything else lands in the composer as text
  let prefill = "";
  try {
    prefill = sessionStorage.getItem("synapse.prefill") || "";
    sessionStorage.removeItem("synapse.prefill");
  } catch { prefill = ""; }
  if (prefill && !state.running) {
    const bare = prefill.match(/^\/([A-Za-z0-9_\-]+)\s*$/);
    if (bare) {
      pickSlash(bare[1]);
    } else {
      input.value = prefill;
      input.focus();
      input.setSelectionRange(input.value.length, input.value.length);
      paintSlash();
    }
  }
  document.addEventListener("click", (e) => {
    if (!plusPop.hidden && !plusPop.contains(e.target)
        && e.target !== el("chat-plus")) plusPop.hidden = true;
    if (!helpPop.hidden && !helpPop.contains(e.target)
        && e.target !== el("chat-help")) {
      helpPop.hidden = true;
      el("chat-help").setAttribute("aria-expanded", "false");
    }
    if (!slash.hidden && !slash.contains(e.target)
        && e.target !== input) slash.hidden = true;
  });

  // the chat list lives in the shell nav now — one nav, not two;
  // this just tells the shelf something changed
  const pingShelf = () =>
    window.dispatchEvent(new CustomEvent("synapse:sessions"));

  // ── §9: the handoff banner ───────────────────────────────
  if (boot.session.handoff && (boot.messages || []).length) {
    const h = boot.session.handoff;
    say(`<b>Where you left off</b> — ${esc(h.say || "")}
      ${h.checked?.length ? `<div class="muted">checked: ${
        esc(h.checked.join(", "))}</div>` : ""}
      ${h.chips?.length ? `<div class="muted">next: ${
        esc(h.chips.join(" · "))}</div>` : ""}`, "handoff-note");
  }

  // ── §8: the memory panel — disclosed, retirable ──────────
  const memBtn = el("chat-memory-btn");
  const memPop = el("chat-memory-pop");
  async function paintMemories() {
    const got = await api.chatMemories(
      state.session.project_id || "").catch(() => ({}));
    const rows = got.available ? got.memories || [] : [];
    memBtn.textContent = rows.length ? `⊚ memory · ${rows.length}`
                                     : "⊚ memory";
    memPop.innerHTML = rows.length ? rows.map((m) => `
      <div class="memory-row">
        <span>${esc(m.text)}
          ${m.scope !== "global" ? `<span class="origin-tag
            o-unreviewed">project</span>` : ""}</span>
        <button class="row-btn" data-mem="${esc(m.id)}"
          title="retire">×</button>
      </div>`).join("")
      : `<div class="muted" style="padding:6px">Nothing remembered
         yet. Memory is on: when you settle a preference in chat
         ("by spend I mean acquirer net spend"), Radix keeps it,
         says so inline with an undo, and lists it here — never a
         metric definition.</div>`;
    for (const btn of memPop.querySelectorAll("[data-mem]")) {
      btn.addEventListener("click", async () => {
        await api.chatRetireMemory(btn.dataset.mem);
        paintMemories();
      });
    }
  }
  memBtn.addEventListener("click", () => {
    memPop.hidden = !memPop.hidden;
    memBtn.setAttribute("aria-expanded", String(!memPop.hidden));
    if (!memPop.hidden) paintMemories();
  });
  document.addEventListener("click", (e) => {
    if (!memPop.hidden && !memPop.contains(e.target)
        && e.target !== memBtn) memPop.hidden = true;
  });
  paintMemories();      // the count on the button, from the start

  // skills need no picker: the agent loads packs itself by intent;
  // the Skills tab in the nav is where people browse them

  // ── the artifact panel ───────────────────────────────────
  function statusChip(prov) {
    if (!prov || !prov.status) return "";
    return `<span class="status-chip s-${esc(prov.status)}">${
      esc(statusLabel(prov.status))}</span>`;
  }

  function chartSVG(spec, width = 520, height = 280) {
    // one x axis for every series: the union of their labels in order
    // of first appearance (chronological when they are dates), each
    // point placed by its label — a forecast over Jul–Dec lands on
    // Jul–Dec, never over the actuals; a null, or a label a series
    // lacks, is a gap in the line
    const cats = [];
    for (const s of spec.series || []) {
      for (const p of s.points || []) {
        const label = String(p[0]);
        if (!cats.includes(label)) cats.push(label);
      }
    }
    if (cats.length > 1 && cats.every(isDate)) cats.sort();
    const series = (spec.series || []).map((s) => {
      const by = new Map((s.points || []).map((p) => [String(p[0]),
        p[1] === null || p[1] === undefined ? NaN : Number(p[1])]));
      return { name: s.name,
               // a forecast, a projection or a target reads dashed
               dashed: !!s.dashed || /forecast|projection|projected|estimate|target|\bplan\b/i
                 .test(String(s.name || "")),
               values: cats.map((c) => (by.has(c) ? by.get(c) : NaN)) };
    });
    const all = series.flatMap((s) => s.values).filter(Number.isFinite);
    if (!all.length) return "<svg></svg>";
    const yMin = Math.min(0, ...all);
    const yMax = Math.max(...all) || 1;
    // numbers read as numbers on the axis: compact past ten thousand,
    // separators below, never a raw 4000000 hanging off the left edge
    const tickFmt = (v) => Math.abs(v) >= 1e4
      ? new Intl.NumberFormat(undefined, { notation: "compact",
          maximumFractionDigits: 1 }).format(v)
      : new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 })
          .format(Number(v.toPrecision(3)));
    const ticks = [0, 1, 2, 3, 4].map((g) => yMin + ((yMax - yMin) * g) / 4);
    const textW = (s) => String(s).length * 6.2;     // 10.5px, roughly
    const many = series.length > 1;
    const isBar = spec.kind === "bar";
    const n = cats.length;
    // the frame fits its labels: the left margin from the widest tick,
    // the bottom from the category labels — rotated when a bar chart's
    // names would collide, thinned when even that would
    const padL = Math.ceil(Math.max(...ticks.map((t) => textW(tickFmt(t)))) + 14);
    const plotW = width - padL - 12;
    const widest = Math.max(8, ...cats.map(textW));
    const slot = plotW / Math.max(1, n);
    const rotate = isBar && widest > slot - 6 && n <= 30;
    const step = rotate ? Math.max(1, Math.ceil(14 / slot))
      : Math.max(1, Math.ceil((widest + 10) / slot));
    const pad = { l: padL, r: 12, t: many ? 30 : 16,
                  b: rotate ? Math.min(96, 24 + Math.ceil(widest * 0.7)) : 34 };
    const px = (i) => isBar
      ? pad.l + ((i + 0.5) * plotW) / Math.max(1, n)
      : pad.l + (n < 2 ? 0 : (i * plotW) / (n - 1));
    const py = (v) => pad.t + (height - pad.t - pad.b)
      * (1 - (v - yMin) / (yMax - yMin || 1));
    // the runs of consecutive values a line is drawn through: a gap
    // ends one run and starts the next
    const runs = (values) => {
      const out = [];
      let cur = [];
      values.forEach((v, i) => {
        if (Number.isFinite(v)) cur.push(i);
        else if (cur.length) { out.push(cur); cur = []; }
      });
      if (cur.length) out.push(cur);
      return out;
    };
    let body = "";
    for (const v of ticks) {
      const y = py(v);
      body += `<line x1="${pad.l}" y1="${y}" x2="${width - pad.r}"
        y2="${y}" class="grid"/>
        <text x="${pad.l - 6}" y="${y + 4}" class="tick"
        text-anchor="end">${esc(tickFmt(v))}</text>`;
    }
    series.forEach((s, si) => {
      const color = PALETTE[si % PALETTE.length];
      const dash = s.dashed ? ' stroke-dasharray="6 4"' : "";
      if (isBar) {
        const group = slot * 0.72;
        const bw = Math.max(3, group / series.length - 2);
        s.values.forEach((v, i) => {
          if (!Number.isFinite(v)) return;
          const x = px(i) - group / 2 + si * (group / series.length) + 1;
          body += `<rect class="chart-bar" x="${x}" y="${
            Math.min(py(v), py(0))}"
            width="${bw}" height="${Math.max(0.5, Math.abs(py(v) - py(0)))}"
            fill="${color}" opacity="${s.dashed ? 0.55 : 0.85}"
            style="animation-delay:${i * 12}ms"><title>${esc(cats[i])}: ${
            esc(fmtNum(v))}</title></rect>`;
        });
      } else {
        for (const run of runs(s.values)) {
          const path = run.map((i, k) =>
            `${k ? "L" : "M"}${px(i)},${py(s.values[i])}`).join(" ");
          if (spec.kind === "area" && run.length > 1) {
            body += `<path class="fill" d="${path} L${
              px(run[run.length - 1])},${py(yMin)} L${px(run[0])},${
              py(yMin)} Z" fill="${color}"/>`;
          }
          if (spec.kind !== "scatter" && run.length > 1) {
            body += `<path class="line" d="${path}" fill="none"
              stroke="${color}" stroke-width="2"${dash}/>`;
          }
        }
        s.values.forEach((v, i) => {
          if (!Number.isFinite(v)) return;
          body += `<circle class="dot" cx="${px(i)}" cy="${py(v)}"
            r="2.6" fill="${color}"><title>${esc(cats[i])}: ${
            esc(fmtNum(v))}</title></circle>`;
        });
      }
    });
    // the category labels: every one when they fit, every k-th when
    // not, the last always on a line chart so the range reads
    cats.forEach((label, i) => {
      // the last label of a line chart always, and none within a step
      // of it, so the range reads without two labels colliding
      const drawn = isBar ? i % step === 0
        : (i === n - 1 || (i % step === 0 && n - 1 - i >= step));
      if (!drawn) return;
      const x = px(i);
      if (rotate) {
        body += `<text x="${x}" y="${height - pad.b + 14}" class="tick"
          text-anchor="end" transform="rotate(-35 ${x} ${height - pad.b + 14})"
          >${esc(label)}</text>`;
      } else {
        const anchor = isBar ? "middle" : (i === 0 ? "start"
          : i === n - 1 ? "end" : "middle");
        body += `<text x="${x}" y="${height - 12}" class="tick"
          text-anchor="${anchor}">${esc(label)}</text>`;
      }
    });
    // the legend only when there is something to tell apart, above the
    // plot where it covers nothing; the unit top-right either way
    if (many) {
      let lx = pad.l;
      series.forEach((s, si) => {
        body += `<text x="${lx}" y="${12}" class="tick"><tspan fill="${
          PALETTE[si % PALETTE.length]}">${s.dashed ? "┄" : "■"}</tspan> ${
          esc(s.name)}</text>`;
        lx += textW(s.name) + 22;
      });
    }
    if (spec.unit) {
      body += `<text x="${width - pad.r}" y="${12}" class="tick"
        text-anchor="end">${esc(spec.unit)}</text>`;
    }
    if (spec.watermark) {
      body += `<text x="${width / 2}" y="${height / 2}"
        class="watermark" text-anchor="middle"
        transform="rotate(-18 ${width / 2} ${height / 2})">${
        esc(spec.watermark)}</text>`;
    }
    return `<svg viewBox="0 0 ${width} ${height}"
      class="chartv2" xmlns="http://www.w3.org/2000/svg">${body}</svg>`;
  }

  // numbers read as numbers: separators, two decimals at most, and a
  // compact form for the big ones with the exact value on hover
  const fmtNum = (n) => new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 2 }).format(n);
  const compact = (n) => Math.abs(n) >= 1e5
    ? new Intl.NumberFormat(undefined, { notation: "compact",
        maximumFractionDigits: 1 }).format(n)
    : fmtNum(n);
  const isNum = (v) => v !== null && v !== undefined && v !== ""
    && typeof v !== "boolean" && Number.isFinite(Number(v));
  const isDate = (v) => typeof v === "string"
    && /^\d{4}-\d{2}(-\d{2})?([T ].*)?$/.test(v);

  function columnKinds(cols, rows) {
    return cols.map((c) => {
      const vals = rows.map((r) => r[c.key])
        .filter((v) => v !== null && v !== undefined && v !== "");
      const nums = vals.filter(isNum).length;
      const dates = vals.filter(isDate).length;
      const kind = vals.length && nums / vals.length >= 0.8 ? "num"
        : vals.length && dates / vals.length >= 0.8 ? "date" : "text";
      return { key: c.key, label: c.label, kind };
    });
  }

  function sparkline(values, w = 140, h = 28) {
    const nums = values.map(Number).filter(Number.isFinite);
    if (nums.length < 2) return "";
    const min = Math.min(...nums);
    const span = (Math.max(...nums) - min) || 1;
    const pts = nums.map((v, i) => [
      (i / (nums.length - 1)) * w,
      h - 2 - ((v - min) / span) * (h - 4)]);
    const line = pts.map((pt) => pt.map((n) => n.toFixed(1)).join(","))
      .join(" ");
    return `<svg class="stat-spark" viewBox="0 0 ${w} ${h}"
      preserveAspectRatio="none" aria-hidden="true">
      <polygon class="area" points="0,${h} ${line} ${w},${h}"/>
      <polyline points="${line}"/></svg>`;
  }

  // the summary strip: the rows and their span, then each numeric
  // column's total, range and mean with its shape — every value
  // computed from the artifact's own rows, nothing estimated
  function reportStrip(spec, kinds) {
    const rows = spec.rows || [];
    const cols = spec.columns || [];
    const stats = [];
    const first = { label: "rows", value: fmtNum(rows.length),
      sub: `${cols.length} column${cols.length === 1 ? "" : "s"}` };
    const dateCol = kinds.find((k) => k.kind === "date");
    if (dateCol) {
      const ds = rows.map((r) => String(r[dateCol.key] || ""))
        .filter(Boolean).sort();
      const today = new Date().toISOString().slice(0, 10);
      const future = ds.filter((d) => d.slice(0, 10) > today).length;
      if (ds.length) first.sub += ` · ${ds[0]} → ${ds[ds.length - 1]}`;
      if (future) {
        first.warn = true;
        first.sub += ` · ${future} dated after today`;
      }
    }
    if (spec.watermark) first.sub += ` · ${spec.watermark}`;
    stats.push(first);
    for (const k of kinds.filter((x) => x.kind === "num").slice(0, 4)) {
      const nums = rows.map((r) => Number(r[k.key]))
        .filter(Number.isFinite);
      if (!nums.length) continue;
      const total = nums.reduce((a, b) => a + b, 0);
      stats.push({ label: k.label, value: compact(total), exact: total,
        sub: `min ${compact(Math.min(...nums))} · max ${
          compact(Math.max(...nums))} · avg ${
          compact(total / nums.length)}`,
        spark: sparkline(nums) });
    }
    return `<div class="report-strip">${stats.map((st, i) => `
      <div class="stat${st.warn ? " warn" : ""}" style="--i:${i}">
        <div class="stat-label" title="${esc(st.label)}">${
          esc(st.label)}</div>
        <div class="stat-value"${typeof st.exact === "number"
          ? ` data-n="${st.exact}" data-fmt="compact" title="${
              esc(fmtNum(st.exact))}"` : ""}>${esc(st.value)}</div>
        <div class="stat-sub">${esc(st.sub)}</div>
        ${st.spark || ""}
      </div>`).join("")}</div>`;
  }

  function tableReport(spec, opts = {}) {
    const { summary = true, limit = 50 } = opts;
    const cols = spec.columns || [];
    const rows = spec.rows || [];
    const kinds = columnKinds(cols, rows);
    const strip = summary && rows.length ? reportStrip(spec, kinds) : "";
    const head = kinds.map((k) =>
      `<th class="${k.kind}" data-key="${esc(k.key)}">${esc(k.label)}${
        (cols.find((c) => c.key === k.key) || {}).status
          ? ` <span class="muted">(${esc(cols.find((c) =>
              c.key === k.key).status)})</span>` : ""}</th>`).join("");
    const cell = (k, v) => k.kind === "num" && isNum(v)
      ? `<td class="num">${esc(fmtNum(Number(v)))}</td>`
      : `<td class="${k.kind}">${esc(String(v ?? ""))}</td>`;
    const body = rows.map((r, i) =>
      `<tr${i >= limit ? " hidden" : ""}>${
        kinds.map((k) => cell(k, r[k.key])).join("")}</tr>`).join("");
    const more = rows.length > limit ? `<div class="table-more">
        <span>Showing the first ${limit} of ${fmtNum(rows.length)} rows</span>
        <button class="btn table-all" data-limit="${limit}">Show all</button>
      </div>` : "";
    return `${strip}<div class="tablev3"><table class="sortable">
      <thead><tr>${head}</tr></thead>
      <tbody>${body}</tbody></table></div>${more}`;
  }

  function tableHTML(spec) {
    return tableReport(spec, { summary: false, limit: 20 });
  }

  function bindTable(container) {
    for (const btn of container.querySelectorAll(".table-all")) {
      btn.addEventListener("click", () => {
        const wrap = btn.closest(".table-more").previousElementSibling;
        const all = btn.textContent === "Show all";
        wrap.querySelectorAll("tbody tr").forEach((tr, i) => {
          tr.hidden = !all && i >= Number(btn.dataset.limit);
        });
        btn.textContent = all ? "Show fewer" : "Show all";
      });
    }
  }

  // numbers count up to their value once, never past it; reduced
  // motion shows the value at once
  function animateNumbers(container) {
    const reduce = window.matchMedia
      && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    for (const node of container.querySelectorAll("[data-n]")) {
      const target = Number(node.dataset.n);
      const final = node.textContent;
      if (!Number.isFinite(target) || reduce) continue;
      const fmt = node.dataset.fmt === "compact" ? compact : fmtNum;
      const start = performance.now();
      const step = (now) => {
        const t = Math.min(1, (now - start) / 700);
        const eased = 1 - Math.pow(1 - t, 3);
        node.textContent = t < 1 ? fmt(target * eased) : final;
        if (t < 1) requestAnimationFrame(step);
      };
      requestAnimationFrame(step);
    }
  }

  function kpiTile(spec) {
    const delta = typeof spec.delta === "number"
      ? `<span class="kpi-delta ${spec.delta >= 0 ? "up" : "down"}">${
          spec.delta >= 0 ? "▲" : "▼"} ${
          esc(String(Math.abs(spec.delta)))}</span>` : "";
    const numeric = typeof spec.value === "number"
      && Number.isFinite(spec.value);
    return `<div class="kpi-tile">
      ${spec.label ? `<div class="kpi-label">${esc(spec.label)}</div>`
                   : ""}
      <div class="kpi-value"${numeric
        ? ` data-n="${spec.value}" data-fmt="plain"` : ""}>${
        numeric ? esc(fmtNum(spec.value))
                : esc(String(spec.value ?? "—"))}${
        spec.unit ? `<span class="kpi-unit">${esc(spec.unit)}</span>`
                  : ""}${delta}</div>
    </div>`;
  }

  function tileFooter(spec) {
    const prov = spec.provenance;
    return `<div class="tile-footer">
      ${statusChip(prov)}
      ${spec.watermark ? `<span class="status-chip s-exploratory">${
        esc(spec.watermark)}</span>` : ""}
      ${prov ? `<span class="meridian"
        title="${esc(prov.meridian_line || "")}">${
        esc(disclaimer(prov))}</span>` : ""}
    </div>`;
  }

  const TIER_STROKE = { certified: ["#2e7d32", ""],
                        witnessed: ["#8a6d1a", "6 3"] };

  function diagramSVG(spec, width = 640) {
    const nodes = spec.nodes || [];
    const lanes = { metric: [], concept: [], table: [] };
    for (const n of nodes) {
      (lanes[n.kind] || lanes.concept).push(n);
    }
    const cols = [lanes.metric, lanes.concept, lanes.table]
      .filter((lane) => lane.length);
    const rowH = 54;
    const height = Math.max(...cols.map((c) => c.length), 1)
      * rowH + 30;
    const pos = new Map();
    cols.forEach((lane, ci) => {
      const x = 90 + ci * ((width - 180) / Math.max(cols.length - 1,
                                                    1));
      lane.forEach((n, ri) => {
        const y = 30 + ri * rowH
          + (height - 40 - lane.length * rowH) / 2;
        pos.set(n.id, [cols.length === 1 ? width / 2 : x, y]);
      });
    });
    let body = "";
    for (const e of spec.edges || []) {
      const a = pos.get(e.a); const b = pos.get(e.b);
      if (!a || !b) continue;
      const [stroke, dash] = TIER_STROKE[e.tier]
        || ["#9a938a", "2 4"];
      body += `<line x1="${a[0]}" y1="${a[1]}" x2="${b[0]}"
        y2="${b[1]}" stroke="${stroke}" stroke-width="1.6"
        ${dash ? `stroke-dasharray="${dash}"` : ""}>
        <title>${esc(e.rel || "")}${e.tier ? ` (${esc(e.tier)})`
                                           : ""}</title></line>`;
    }
    const DOT = { certified: "#2e7d32", pending: "#b07d1e" };
    for (const n of nodes) {
      const p = pos.get(n.id);
      if (!p) continue;
      const label = n.label.length > 26
        ? n.label.slice(0, 25) + "…" : n.label;
      body += `<g class="dg-node dg-${esc(n.kind || "other")}">
        <rect x="${p[0] - 78}" y="${p[1] - 15}" width="156"
          height="30" rx="8"/>
        ${n.status ? `<circle cx="${p[0] - 66}" cy="${p[1]}" r="4"
          fill="${DOT[n.status] || "#9a938a"}"><title>${
          esc(statusLabel(n.status))}</title></circle>` : ""}
        <text x="${p[0] + (n.status ? 6 : 0)}" y="${p[1] + 4}"
          text-anchor="middle"><title>${esc(n.id)}</title>${
          esc(label)}</text></g>`;
    }
    return `<svg viewBox="0 0 ${width} ${height}" class="diagramv2"
      xmlns="http://www.w3.org/2000/svg">${body}</svg>`;
  }

  function panelBody(type, spec) {
    const mark = spec.watermark
      ? `<div class="watermark-band">${esc(spec.watermark)}</div>`
      : "";
    if (type === "chart") return mark + chartSVG(spec, 460, 230);
    if (type === "table") return mark + tableHTML(spec);
    if (type === "kpi") return mark + kpiTile(spec);
    if (type === "document") {
      return mark + `<div class="md docview">${
        renderMarkdown(spec.markdown || "", "md")}</div>`;
    }
    return "";
  }

  function renderArtifactBody(row) {
    const spec = row.spec || {};
    const prov = spec.provenance;
    const footer = `
      <div class="artifact-footer">
        ${statusChip(prov)}
        ${spec.watermark && !prov
          ? `<span class="status-chip s-exploratory">${
              esc(spec.watermark)}</span>` : ""}
        ${prov ? `<span class="meridian"
          title="${esc(prov.meridian_line || "")}">${
          esc(disclaimer(prov))}</span>` : ""}
        <span class="muted">build ${esc(spec.build_id || "?")}
          · v${row.version}</span>
      </div>`;
    if (row.type === "chart") {
      return chartSVG(spec) + footer;
    }
    if (row.type === "table") {
      const mark = spec.watermark
        ? `<div class="watermark-band">${esc(spec.watermark)}</div>`
        : "";
      return mark + tableReport(spec, { summary: true, limit: 50 })
        + footer;
    }
    if (row.type === "document") {
      const mark = spec.watermark
        ? `<div class="watermark-band">${esc(spec.watermark)}</div>`
        : "";
      return mark + `<div class="md docview">${
        renderMarkdown(spec.markdown || "", "md")}</div>` + footer;
    }
    if (row.type === "kpi") {
      return kpiTile(spec) + footer;
    }
    if (row.type === "dashboard") {
      const filters = (spec.filters || []).map((f) => `
        <span class="dash-filter" data-slot="${esc(f.slot)}">
          <span class="muted">${esc(f.label || f.slot)}:</span>
          ${f.options.map((o) => `<button class="filter-opt${
            o === f.active ? " active" : ""}" data-slot="${
            esc(f.slot)}" data-value="${esc(o)}">${esc(o)}</button>`)
            .join("")}
        </span>`).join("");
      const panels = (spec.panels || []).map((p, i) => `
        <div class="dash-panel dash-${esc(p.type)}" style="--i:${i}">
          ${p.title ? `<div class="dash-panel-title">${
            esc(p.title)}</div>` : ""}
          ${panelBody(p.type, p.spec || {})}
          ${tileFooter(p.spec || {})}
        </div>`).join("");
      return `${filters ? `<div class="dash-filters">${filters}
        </div>` : ""}
        <div class="dash-grid">${panels}</div>
        ${spec.notes ? `<div class="dash-notes md">${
          renderMarkdown(spec.notes, "md")}</div>` : ""}${footer}`;
    }
    if (row.type === "diagram") {
      if (spec.kind === "mermaid") {
        return `<pre class="mermaid-src">${esc(spec.source || "")
          }</pre><div class="muted" style="font-size:12px">mermaid
          source — export .mmd to render elsewhere</div>` + footer;
      }
      return diagramSVG(spec) + footer;
    }
    return `<pre>${esc(JSON.stringify(spec, null, 1))}</pre>`;
  }

  function bindDashboardFilters(container, row) {
    for (const btn of container.querySelectorAll(".filter-opt")) {
      btn.addEventListener("click", () => {
        if (btn.classList.contains("active")) return;
        // rule 3 in the UI: a filter pick is a whatif REQUEST in
        // the conversation, never a hidden client-side query
        send(`Set ${btn.dataset.slot} to ${btn.dataset.value} on `
             + `"${row.title}" and update it`);
      });
    }
  }

  function download(name, mime, content) {
    const a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob([content], { type: mime }));
    a.download = name;
    a.click();
    URL.revokeObjectURL(a.href);
  }

  function provenanceLine(row) {
    const prov = (row.spec || {}).provenance;
    return `build ${row.spec?.build_id || "?"} · v${row.version}`
      + (prov ? ` · ${statusLabel(prov.status)} · ${prov.meridian_line}`
              : "")
      + (row.spec?.watermark ? ` · ${row.spec.watermark}` : "");
  }

  function exportButtons(row, host) {
    const box = host.querySelector(".artifact-export");
    const body = host.querySelector(".artifact-body");
    const slug = (row.title || row.type).toLowerCase()
      .replace(/[^a-z0-9]+/g, "-").slice(0, 40);
    const buttons = [];
    if (row.type === "chart") {
      buttons.push(["SVG", () => download(`${slug}.svg`,
        "image/svg+xml",
        body.querySelector("svg").outerHTML)]);
      buttons.push(["PNG", () => {
        const svg = body.querySelector("svg");
        const img = new Image();
        img.onload = () => {
          const canvas = document.createElement("canvas");
          canvas.width = 1040; canvas.height = 560;
          const ctx = canvas.getContext("2d");
          ctx.fillStyle = "#ffffff";
          ctx.fillRect(0, 0, canvas.width, canvas.height);
          ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
          canvas.toBlob((blob) => {
            const a = document.createElement("a");
            a.href = URL.createObjectURL(blob);
            a.download = `${slug}.png`;
            a.click();
          });
        };
        img.src = "data:image/svg+xml;base64," + btoa(
          unescape(encodeURIComponent(svg.outerHTML)));
      }]);
    }
    if (row.type === "table") {
      buttons.push(["CSV", () => {
        const cols = row.spec.columns || [];
        const lines = [`# ${provenanceLine(row)}`,
          cols.map((c) => JSON.stringify(c.label)).join(",")];
        for (const r of row.spec.rows || []) {
          lines.push(cols.map((c) =>
            JSON.stringify(r[c.key] ?? "")).join(","));
        }
        download(`${slug}.csv`, "text/csv", lines.join("\n"));
      }]);
    }
    if (row.type === "document") {
      buttons.push(["MD", () => download(`${slug}.md`,
        "text/markdown",
        (row.spec.markdown || "")
        + `\n\n---\n${provenanceLine(row)}\n`)]);
    }
    if (row.type === "dashboard") {
      buttons.push(["HTML", () => {
        const styles = [...document.styleSheets].map((sheet) => {
          try {
            return [...sheet.cssRules].map((r) => r.cssText).join("\n");
          } catch { return ""; }
        }).join("\n");
        download(`${slug}.html`, "text/html",
          `<!doctype html><meta charset="utf-8"><title>${
            esc(row.title)}</title><style>${styles}</style>` +
          `<body style="max-width:900px;margin:24px auto;` +
          `font-family:sans-serif"><h2>${esc(row.title)}</h2>` +
          body.innerHTML +
          `<p style="font-size:11px;color:#777">${
            esc(provenanceLine(row))}</p></body>`);
      }]);
    }
    if (row.type === "diagram") {
      if (row.spec.kind === "mermaid") {
        buttons.push(["MMD", () => download(`${slug}.mmd`,
          "text/plain",
          (row.spec.source || "") + `\n%% ${provenanceLine(row)}\n`)]);
      } else {
        buttons.push(["SVG", () => download(`${slug}.svg`,
          "image/svg+xml",
          body.querySelector("svg").outerHTML)]);
      }
    }
    // every type exports a deck server-side: one panel per slide,
    // meridian lines riding in the slide notes
    buttons.push(["PPTX", () => {
      const a = document.createElement("a");
      a.href = api.chatPptxUrl(row.artifact_id)
        + `?version=${row.version}`;
      a.download = "";
      a.click();
    }]);
    box.innerHTML = "";
    for (const [label, fn] of buttons) {
      const b = document.createElement("button");
      b.className = "btn";
      b.textContent = label;
      b.addEventListener("click", fn);
      box.appendChild(b);
    }
  }

  // artifacts publish inside the chat: the block renders the artifact
  // where the turn made it, with its exports and its versions;
  // reopening an old chat renders every artifact again in place
  async function showArtifact(block, artifactId, version = null) {
    const got = await api.chatArtifact(artifactId, version);
    if (!got.available || !block.isConnected) return;
    const row = got.artifact;
    state.artifacts.set(artifactId, row);
    const body = block.querySelector(".artifact-body");
    block.querySelector(".artifact-name").textContent = row.title;
    body.innerHTML = renderArtifactBody(row);
    bindTable(body);
    animateNumbers(body);
    if (row.type === "dashboard") bindDashboardFilters(body, row);
    exportButtons(row, block);
    const versions = await api.chatArtifactVersions(artifactId);
    const select = block.querySelector(".artifact-version");
    const list = versions.versions || [];
    select.hidden = list.length < 2;
    select.innerHTML = list.map((v) =>
      `<option value="${v.version}"${v.version === row.version
        ? " selected" : ""}>v${v.version}</option>`).join("");
    select.onchange = () =>
      showArtifact(block, artifactId, Number(select.value));
  }

  // ── the stream ───────────────────────────────────────────
  function turnFor(turnId) {
    let turn = state.turns.get(turnId);
    if (turn) return turn;
    const div = document.createElement("div");
    div.className = "chat-turn";
    div.innerHTML = `${activityHTML()}
      <div class="chat-prose md"></div>
      <div class="chat-extras"></div>`;
    thread.appendChild(div);
    turn = { el: div, ...activityParts(div) };
    rememberThinking(turn.activity);
    state.turns.set(turnId, turn);
    scroll();
    return turn;
  }

  // ── the thinking block, the way a chat assistant shows it: closed
  //    by default — one compact line ("Radix is thinking… 12s") while
  //    the model works; a click on it, or Enter on the summary, opens
  //    the streamed thoughts and the steps; that choice is kept per
  //    browser, so the next turn (and the next chat) opens the way the
  //    person left it; the answer folds it to "Thought for 12s" unless
  //    they had it open ──
  const THINK_KEY = "synapse-thinking-open";
  const thinkOpen = () => {
    try { return localStorage.getItem(THINK_KEY) === "1"; } catch { return false; }
  };
  function rememberThinking(details) {
    // the click lands before the toggle, so the state after it is the
    // opposite of the one now; Enter and Space on the summary click too
    details.querySelector("summary").addEventListener("click", () => {
      try { localStorage.setItem(THINK_KEY, details.open ? "0" : "1"); } catch {}
    });
  }
  // the block's markup, the same on a turn and on a task row: the
  // compact line, the folded title, the tokens so far beside them
  const activityHTML = () => `
      <details class="tool-activity" hidden>
        <summary title="">
          <span class="tri">▸</span>
          <span class="thinking-line">
            <span class="think-orb">✳</span>
            <span class="think-text">Radix is thinking…</span></span>
          <span class="tool-title" hidden></span>
          <span class="think-usage" hidden></span>
        </summary>
        <div class="tool-steps"></div>
      </details>`;
  const activityParts = (root) => ({
    activity: root.querySelector(".tool-activity"),
    toolTitle: root.querySelector(".tool-title"),
    toolSteps: root.querySelector(".tool-steps"),
    thinking: root.querySelector(".thinking-line"),
    thinkText: root.querySelector(".think-text"),
    liveUsage: root.querySelector(".think-usage"),
    prose: root.querySelector(".chat-prose"),
    extras: root.querySelector(".chat-extras"),
    buffer: "", steps: 0, rows: new Map(), verbs: [],
    thoughts: "", done: false, tick: null, tickLabel: "",
    tickStart: 0, seg: null, segText: "", thought: false,
    settled: false, startedAt: 0 });

  // ── usage: what a turn spent, live beside the thinking line (from
  //    budget_tick: the turn's tokens in and out so far) and as a
  //    footer under the answer (from turn_done, replayed from the
  //    stored message's payload); a cost only when a rate is set ──
  const fmtInt = (n) => new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 0 }).format(Math.max(0, Number(n) || 0));
  const usageOf = (event) => {
    // a stream record (the turn's own split, turn_*) or a stored
    // message's usage (the same numbers, plain names)
    const num = (...keys) => {
      for (const k of keys) {
        const v = Number(event[k]);
        if (event[k] !== null && event[k] !== undefined && Number.isFinite(v)) return v;
      }
      return 0;
    };
    const cost = event.turn_cost_usd ?? event.cost_usd;
    return { tokensIn: num("turn_tokens_in", "tokens_in"),
             tokensOut: num("turn_tokens_out", "tokens_out"),
             tokens: num("turn_tokens", "tokens"),
             calls: num("model_calls", "calls", "turn_calls"),
             secs: num("elapsed_ms") / 1000,
             cost: typeof cost === "number" && Number.isFinite(cost) ? cost : null };
  };
  const tokensText = (u) => `${fmtInt(u.tokens)} tokens (${fmtInt(u.tokensIn)} in · ${
    fmtInt(u.tokensOut)} out)`;
  const costText = (u) => u.cost === null ? ""
    : ` · $${u.cost.toFixed(u.cost < 0.01 ? 4 : 2)}`;
  const secsText = (s) => `${s >= 10 ? s.toFixed(0) : s.toFixed(1)}s`;
  function liveUsage(turn, event) {
    const u = usageOf(event);
    if (!turn.liveUsage || turn.done) return;
    turn.liveUsage.textContent = `· ${tokensText(u)}${costText(u)}`;
    turn.liveUsage.hidden = false;
  }
  // the footer: "12.4s · 8,210 tokens (7,900 in · 310 out) · 2 model calls"
  function usageFooter(container, u) {
    if (!u) return;
    let foot = container.querySelector(":scope > .turn-usage");
    if (!foot) {
      foot = document.createElement("div");
      foot.className = "turn-usage";
      container.appendChild(foot);
    }
    const calls = `${fmtInt(u.calls)} model call${u.calls === 1 ? "" : "s"}`;
    foot.textContent = u.tokens > 0 || u.calls > 0
      ? `${secsText(u.secs)} · ${tokensText(u)} · ${calls}${costText(u)}`
      : `${secsText(u.secs)} · no model call`;
    foot.title = "What this turn cost: wall time, tokens in and out, model calls"
      + (u.cost === null ? " (no rate configured: SYNAPSE_COST_IN and SYNAPSE_COST_OUT)"
                         : ", and the estimate at the configured rate");
  }

  // what Radix is doing, in the user's words: the model's own
  // thought summary when it narrates, a plain verb for the tool
  // otherwise — never a tool name, an id, or raw output
  const argOf = (event, key) => {
    const m = String(event.args || "").match(
      new RegExp(`"${key}":\\s*"([^"]{1,60})`));
    return m ? m[1] : "";
  };
  const VERBS = {
    search: (e) => {
      const q = argOf(e, "query");
      const kind = argOf(e, "kind");
      if (kind === "list") return `Listing the ${q || ""} metrics`;
      if (kind === "exact") return `Scanning the cards for ${q}`;
      return q ? `Searching the graph for ${q}` : "Searching the graph";
    },
    read: (e) => {
      const id = argOf(e, "id");
      if (!id) return "Collecting what was used";
      const [kind, name] = id.split(":");
      return name ? `Reading the ${name} ${kind} card` : `Reading ${id}`;
    },
    sample_values: (e) => {
      const col = argOf(e, "column");
      return col ? `Sampling real values of ${col}` : "Sampling real values";
    },
    run_sql: (e) => ["snapshot", "run"].includes(argOf(e, "mode"))
      ? "Running the query" : "Checking the query",
    propose_sql: () => "Writing the query for you to run",
    chart: () => "Drawing the rows",
    python: () => "Computing",
    check: (e) => ({ part_whole: "Checking the parts add up",
                     crosscheck: "Cross-checking two routes",
                     coverage: "Checking coverage",
                     fanout: "Checking the join is safe",
                     reconcile: "Reconciling against the published "
                                + "definition",
                     answer: "Verifying the answer" }[argOf(e, "kind")]
                   || "Running a check"),
    artifact: (e) => argOf(e, "artifact_id")
      ? "Updating the artifact" : "Building the artifact",
    ask: () => "Preparing a question",
    load_skill: (e) => `Loading the ${argOf(e, "name") || "matching"} skill`,
    remember: () => "Keeping a preference",
    note: () => "Noting the thread",
    suggest_next: () => "Lining up next steps",
  };
  // the past tense for the collapsed summary, deduplicated
  const PAST = {
    search: "searched the graph", read: "read the cards",
    sample_values: "sampled real values", run_sql: "ran the query",
    propose_sql: "wrote the query", chart: "drew the chart",
    python: "computed", check: "ran the checks",
    artifact: "built the artifact", ask: "asked a question",
    load_skill: "loaded a skill", remember: "kept a preference",
    note: "took a note",
  };
  const QUIET = new Set(["suggest_next"]);     // not work: no row

  function friendly(event) {
    const verb = VERBS[event.tool];
    return verb ? verb(event) : "Working";
  }

  // a step that did not go through reads as a snag, never a stack
  // trace: what was being tried, a plain reason, and — once the model
  // takes its next step — that it moved on. The raw error stays one
  // hover away, on the row.
  const SNAGS = [
    [/configuration, not the query/i,
     "a setup problem on the server side, not the question"],
    [/timed? ?out|deadline/i, "the warehouse took too long to answer"],
    [/permission|access denied|forbidden|\b403\b|not authori[sz]ed/i,
     "no access to that data"],
    [/ceiling|too (much|large|many)|over the|exceed/i,
     "it would have read too much data"],
    [/not found|no such|unknown (table|column|metric|tool)|does not exist|missing/i,
     "nothing by that name in the graph"],
    [/syntax|invalid|unrecognized|could not (run|parse|compile)|\b400\b|bad request/i,
     "the query was not accepted as written"],
    [/truncat/i, "the result was too large to bring back whole"],
  ];
  function snagReason(summary) {
    const raw = String(summary).replace(/^ERROR:\s*/, "");
    for (const [re, words] of SNAGS) if (re.test(raw)) return words;
    const first = raw.split(/[—;\n]/)[0].replace(/[.:\s]+$/, "").trim();
    return first ? first.charAt(0).toLowerCase() + first.slice(1, 90)
      : "it did not go through";
  }
  function snagRow(row, event, summary, secs = "") {
    row.classList.add("failed");
    row.querySelector(".mark").textContent = "!";
    row.title = String(summary).replace(/^ERROR:\s*/, "").slice(0, 400);
    row.querySelector(".step-text").innerHTML =
      `${prose(friendly(event))} <span class="snag">hit a snag</span>
       <span class="muted">— ${esc(snagReason(summary))}${secs}</span>
       <span class="muted snag-next" hidden> · trying another way</span>`;
  }
  function movedOn(turn, another) {
    const row = turn.snagged;
    if (!row) return;
    if (another) row.querySelector(".snag-next").hidden = false;
    turn.snagged = null;
  }

  function lastLine(text) {
    const lines = String(text).replace(/\*\*/g, "").split("\n")
      .map((l) => l.trim()).filter((l) => l && !/^#+\s*$/.test(l));
    const line = (lines[lines.length - 1] || "").replace(/^#+\s*/, "");
    return line.length > 120 ? line.slice(0, 118) + "…" : line;
  }

  function showThinking(turn, text) {
    if (turn.done) return;          // never after the turn landed
    turn.thinkText.textContent = text;
    turn.thinking.hidden = false;
    scroll();
  }

  // the live line keeps a visible heartbeat: the label, then the
  // seconds — a long model call never looks like a stuck tool, and a
  // stuck tool never looks like thinking
  function stopPulse(turn) {
    if (turn.tick) { clearInterval(turn.tick); turn.tick = null; }
  }

  // the live line's words: the assistant by name while it thinks, the
  // plain verb while a tool runs, "Stopping…" on the stop; the seconds
  // count from the first one, and past twenty the line says "still"
  const wording = (label) => label === "Thinking…" ? "Radix is thinking…" : label;
  const still = (base) => base.startsWith("Radix is ")
    ? `Radix is still ${base.slice(9)}`
    : `Still ${base.charAt(0).toLowerCase()}${base.slice(1)}`;
  function pulse(turn, label, sinceIso = "") {
    stopPulse(turn);
    turn.tickLabel = wording(label);
    // the clock starts when the event happened, not when it was
    // seen — a replayed turn shows its true seconds
    const since = Date.parse(sinceIso || "");
    turn.tickStart = Number.isFinite(since)
      ? Math.min(since, Date.now()) : Date.now();
    showThinking(turn, turn.tickLabel);
    turn.tick = setInterval(() => {
      if (turn.done) { stopPulse(turn); return; }
      const secs = Math.round((Date.now() - turn.tickStart) / 1000);
      if (secs < 1) return;
      const base = turn.tickLabel.replace(/…$/, "");
      showThinking(turn, secs >= 20
        ? `${still(base)} · ${secs}s`
        : `${base}… ${secs}s`);
    }, 1000);
  }

  // ── the block's contents: the model's own thought summaries in the
  // order they happen, interleaved with the steps — folded behind the
  // compact line while it works, "Thought for 34s" when the answer
  // lands, yours to expand; new work brings the live line back
  function doneLabel(thought, elapsedMs, verbs) {
    const secs = Math.max(0, (elapsedMs || 0) / 1000);
    const head = `${thought ? "Thought" : "Worked"} for ${
      secs >= 10 ? secs.toFixed(0) : secs.toFixed(1)}s`;
    return verbs.length ? `${head} · ${verbs.join(", ")}` : head;
  }

  function openBlock(turn) {
    turn.activity.hidden = false;
    turn.activity.open = thinkOpen();       // closed unless they keep it open
    turn.settled = false;
    turn.toolTitle.hidden = true;
    turn.thinking.hidden = false;
  }

  function thoughtSegment(turn) {
    if (!turn.seg) {
      const seg = document.createElement("div");
      seg.className = "think-seg md";
      turn.toolSteps.appendChild(seg);
      turn.seg = seg;
      turn.segText = "";
      turn.thought = true;
    }
    return turn.seg;
  }

  function settleBlock(turn, elapsedMs) {
    stopPulse(turn);
    turn.thinking.hidden = true;
    if (turn.toolSteps.children.length === 0) {
      turn.activity.hidden = true;        // nothing to show, no block
      return;
    }
    turn.toolTitle.textContent = doneLabel(
      turn.thought,
      elapsedMs ?? (turn.startedAt ? Date.now() - turn.startedAt : 0),
      [...new Set(turn.verbs)]);
    turn.toolTitle.hidden = false;
    // the answer folds the block — unless the person keeps it open
    turn.activity.open = thinkOpen();
    turn.settled = true;
  }

  function doneThinking(turn, elapsedMs) {
    turn.done = true;
    settleBlock(turn, elapsedMs);
    if (turn.liveUsage) turn.liveUsage.hidden = true;   // the footer takes over
  }

  // one row per call: announced when the call starts, settled when
  // it returns — the row is the receipt, the live line is the pulse
  function toolStart(turn, event) {
    if (QUIET.has(event.tool)) return;
    turn.activity.hidden = false;
    turn.steps += 1;
    const row = document.createElement("div");
    row.className = "theater-step pending";
    row.innerHTML = `<span class="mark">·</span>
      <span class="step-body">
        <span class="step-text">${prose(friendly(event))}…</span>
      </span>`;
    attachInput(row, event.input);
    turn.toolSteps.appendChild(row);
    turn.rows.set(event.n, row);
    if (PAST[event.tool]) turn.verbs.push(PAST[event.tool]);
    scroll();
  }

  // the call's input — the SQL, the code — sits under the row, shown
  // on a click, the way a chat assistant shows what a tool was given
  function attachInput(row, input) {
    if (!input || row.querySelector(".step-input")) return;
    const pre = document.createElement("pre");
    pre.className = "step-input";
    pre.hidden = true;
    pre.textContent = input;
    row.querySelector(".step-body").appendChild(pre);
    row.classList.add("has-input");
    row.querySelector(".step-text").addEventListener("click", () => {
      pre.hidden = !pre.hidden;
    });
  }

  function toolDone(turn, event) {
    if (QUIET.has(event.tool)) return;
    let row = turn.rows.get(event.n);
    if (!row) {
      toolStart(turn, event);
      row = turn.rows.get(event.n);
    }
    row.classList.remove("pending");
    attachInput(row, event.input);
    const summary = String(event.summary || "");
    const outcome = summary.split("\n")[0].slice(0, 120);
    const secs = event.elapsed_ms >= 1000
      ? ` · ${(event.elapsed_ms / 1000).toFixed(1)}s` : "";
    if (outcome.startsWith("ERROR")) {
      snagRow(row, event, summary, secs);
      turn.snagged = row;
    } else {
      row.querySelector(".step-text").innerHTML =
        `${prose(friendly(event))}${outcome
          ? ` <span class="muted">— ${prose(outcome)}${secs}</span>` : ""}`;
    }
    scroll();
  }

  function userBubble(text, before = null, files = []) {
    const div = document.createElement("div");
    div.className = "chat-user";
    div.textContent = text;
    if (files && files.length) {
      const row = document.createElement("div");
      row.className = "chat-user-files";
      row.innerHTML = files.map((f) => `<span class="file-chip sent"
        title="${esc(f.rides === "convert" ? "converted to text" : f.family || "")}">
        <span class="file-kind">${esc(f.suffix || (f.name || "").split(".").pop())}</span>
        <span class="file-name">${esc(f.name)}</span></span>`).join("");
      div.appendChild(row);
    }
    if (before) thread.insertBefore(div, before);
    else thread.appendChild(div);
    scroll();
  }

  // a chip is a follow-up the person taps: plain text becomes the
  // next message; an action chip ({label, action: "chart", saved_as})
  // calls the model-free step instead
  function chipRow(suggestions, clarify) {
    const box = el("chat-chiprow");
    const items = clarify
      ? (clarify.options || []).map((o) => ({
          label: o.label, hint: o.evidence || o.why || "" }))
      : (suggestions || []).slice(0, 3).map((c) =>
          typeof c === "string" ? { label: c, hint: "" }
            : { label: c.label || "", hint: c.hint || "",
                action: c.action || "", payload: c });
    if (clarify) {
      say(`<b>${prose(clarify.question)}</b>`);
    }
    box.innerHTML = items.map((c, i) => `
      <button class="chip-choice${c.action ? " act" : ""}" data-i="${i}">
        <b>${esc(c.label)}</b>
        ${c.hint ? `<span class="muted">${prose(c.hint)}</span>` : ""}
      </button>`).join("");
    scroll();        // the chips sit in the scroll flow, under the answer
    for (const b of box.querySelectorAll(".chip-choice")) {
      b.addEventListener("click", async () => {
        const item = items[Number(b.dataset.i)];
        box.innerHTML = "";
        if (item.action === "chart") {
          if (state.running) return;
          const accepted = await api.chatChart(state.session.id, {
            saved_as: item.payload.saved_as || "" });
          if (!accepted.available) {
            say(`<b>not charted.</b> ${esc(accepted.reason || "")}`,
                "error");
          }
          return;
        }
        send(item.label);
      });
    }
  }

  function artifactCard(container, row, live) {
    const div = document.createElement("div");
    div.className = "artifact-block";
    div.dataset.artifact = row.artifact_id;
    div.innerHTML = `
      <div class="artifact-head">
        <span class="glyph">${{ chart: "📊", table: "▦",
          document: "🗎", dashboard: "▥", diagram: "✦",
          kpi: "◉" }[row.type] || "▣"}</span>
        <b class="artifact-name" title="${esc(row.title)}">${
          esc(row.title)}</b>
        <span class="muted">${esc(row.type)}${
          row.spec?.watermark
            ? ` · ${esc(row.spec.watermark)}` : ""}</span>
        <select class="artifact-version" title="Version" hidden></select>
        <span class="spacer"></span>
        <span class="artifact-export"></span>
      </div>
      <div class="artifact-body"><p class="muted">rendering…</p></div>`;
    container.closest(".chat-turn")?.classList.add("has-artifact");
    container.appendChild(div);
    // published in the chat, where the turn made it: no drawer, no
    // scrolling up to find it
    showArtifact(div, row.artifact_id).then(() => { if (live) scroll(); });
    scroll();
  }

  const bytes = (n) => {
    if (n === null || n === undefined || n === "") return "an unknown amount";
    let v = Number(n);
    if (!Number.isFinite(v)) return "an unknown amount";
    for (const u of ["B", "KB", "MB", "GB", "TB"]) {
      if (v < 1000 || u === "TB") {
        return u === "B" ? `${v.toFixed(0)} B` : `${v.toFixed(1)} ${u}`;
      }
      v /= 1000;
    }
    return "";
  };

  // the handover (§5): the query first, priced and disclosed; the
  // rows on a tap — Run query executes under the limits with no
  // model call, Run + build dashboard chains the build after it
  function proposalCard(container, proposal, meta) {
    const div = document.createElement("div");
    div.className = "card proposal-card";
    const schema = (proposal.result_schema || [])
      .map((c) => c && c.name).filter(Boolean);
    const written = String(proposal.sql_written || proposal.sql || "");
    div.innerHTML = `
      <div class="proposal-head">
        <span class="glyph">⌘</span>
        <b>${esc(proposal.title || "Proposed query")}</b>
        ${statusChip({ status: proposal.status })}
      </div>
      ${proposal.why
        ? `<div class="proposal-why">${prose(proposal.why)}</div>` : ""}
      <pre class="proposal-sql" spellcheck="false">${esc(written)}</pre>
      <div class="proposal-meta muted">would scan ${
        esc(bytes(proposal.bytes_processed))}${schema.length
          ? ` · ${schema.length} columns: ${
              esc(schema.slice(0, 6).join(", "))}` : ""}${
        ` · <span class="meridian" title="${esc(proposal.meridian_line || "")
          }">${esc(disclaimer(proposal))}</span>`}</div>
      ${proposal.over_ceiling
        ? `<div class="proposal-warn">⚠ over the ${
            esc(bytes(proposal.scan_ceiling_bytes))} ceiling for live
            runs: Run will be refused unless the query is narrowed
            (Edit SQL: a filter on the partition column) or the ceiling
            is raised in the silo settings</div>` : ""}
      ${(proposal.warnings || []).length
        ? `<div class="proposal-warn">${(proposal.warnings || [])
            .slice(0, 2).map((w) => `<div>⚠ ${esc(w)}</div>`).join("")}</div>`
        : ""}
      <div class="proposal-actions">
        <button class="btn primary" data-run="query">Run query</button>
        <button class="btn" data-run="dashboard">Run + build dashboard</button>
        <button class="btn" data-edit="1">Edit SQL</button>
      </div>`;
    const pre = div.querySelector(".proposal-sql");
    const editBtn = div.querySelector("[data-edit]");
    editBtn.addEventListener("click", () => {
      const on = pre.contentEditable !== "true";
      pre.contentEditable = on ? "true" : "false";
      pre.classList.toggle("editing", on);
      editBtn.textContent = on ? "Done editing" : "Edit SQL";
      if (on) pre.focus();
    });
    for (const b of div.querySelectorAll("[data-run]")) {
      b.addEventListener("click", async () => {
        if (state.running) return;
        const current = pre.textContent.trim();
        const edited = current !== written.trim();
        const dashboard = b.dataset.run === "dashboard";
        el("chat-chiprow").innerHTML = "";
        userBubble(`Run: ${proposal.title || "the query"}${
          edited ? " (edited)" : ""}${
          dashboard ? " and build a dashboard" : ""}`);
        const accepted = await api.chatRun(state.session.id, {
          message_id: meta.message_id || "", sql: edited ? current : "",
          dashboard, depth: el("chat-depth").value });
        if (!accepted.available) {
          say(`<b>not run.</b> ${esc(accepted.reason || "")}`, "error");
        }
      });
    }
    container.appendChild(div);
    scroll();
  }

  function memoryNote(turn, text, memoryId) {
    const div = document.createElement("div");
    div.className = "memory-note";
    div.innerHTML = `<span>Remembered: ${esc(text)}</span>
      <button class="btn" title="retire this memory">undo</button>`;
    div.querySelector("button").addEventListener("click", async () => {
      if (memoryId) await api.chatRetireMemory(memoryId);
      div.classList.add("retired");
      div.querySelector("button").remove();
      paintMemories();
    });
    turn.extras.appendChild(div);
    paintMemories();
  }

  // ── a compound ask: the task board (docs/multi-task-turns.md). The
  //    plan draws one row per task under the person's message; every
  //    event a task's sub-turn emits carries the task id and lands
  //    under its row, which has the same shape as a turn so the
  //    helpers above (steps, thoughts, cards, the pulse) work on it
  //    unchanged. The running row is open, a finished one folds, and
  //    the transcript replays the same board from the stored plan ──
  const TASK_MARKS = { planned: "·", running: "◐", done: "✓",
                       partial: "◔", failed: "✕", stopped: "■" };
  function boardFor(turn) {
    if (turn.board) return turn.board;
    const div = document.createElement("div");
    div.className = "task-board";
    div.innerHTML = `<div class="task-board-head"></div>
      <div class="task-rows"></div>`;
    turn.el.insertBefore(div, turn.el.firstChild);
    turn.board = { el: div, head: div.querySelector(".task-board-head"),
                   rows: div.querySelector(".task-rows"),
                   tasks: new Map() };
    return turn.board;
  }
  function boardHead(turn, count, pool, stopped) {
    const board = boardFor(turn);
    board.head.textContent = `Split into ${count} task${count === 1 ? "" : "s"}`
      + (pool > 1 ? ` · up to ${pool} side by side` : "")
      + (stopped ? " · stopped" : "");
  }
  function taskTurnFor(turn, id, goal = "", deps = []) {
    const board = boardFor(turn);
    let task = board.tasks.get(id);
    if (task) {
      if (goal && task.goalEl.textContent === id) task.goalEl.textContent = goal;
      return task;
    }
    const row = document.createElement("details");
    row.className = "task-row planned";
    row.dataset.task = id;
    row.innerHTML = `
      <summary>
        <span class="task-mark">·</span>
        <span class="task-goal"></span>
        <span class="task-after"></span>
        <span class="task-status">planned</span>
        <span class="task-cost"></span>
      </summary>
      <div class="task-body">
        ${activityHTML()}
        <div class="chat-prose md"></div>
        <div class="chat-extras"></div>
        <div class="task-note muted" hidden></div>
      </div>`;
    row.querySelector(".task-goal").textContent = goal || id;
    if (deps && deps.length) {
      row.querySelector(".task-after").textContent = `after ${deps.join(", ")}`;
    }
    board.rows.appendChild(row);
    task = { task: id, parent: turn, el: row,
             goalEl: row.querySelector(".task-goal"),
             statusEl: row.querySelector(".task-status"),
             costEl: row.querySelector(".task-cost"),
             noteEl: row.querySelector(".task-note"),
             ...activityParts(row) };
    rememberThinking(task.activity);
    board.tasks.set(id, task);
    scroll();
    return task;
  }
  function taskStatus(task, status, cost, reason = "") {
    task.el.className = `task-row ${status || "planned"}`;
    task.statusEl.textContent = status || "planned";
    task.el.querySelector(".task-mark").textContent =
      TASK_MARKS[status] || "·";
    if (cost) {
      const secs = (cost.elapsed_ms || 0) / 1000;
      task.costEl.textContent = `${cost.model_calls ?? 0} call${
        cost.model_calls === 1 ? "" : "s"} · ${
        secs >= 10 ? secs.toFixed(0) : secs.toFixed(1)}s`;
    }
    if (reason) {
      task.noteEl.textContent = reason;
      task.noteEl.hidden = false;
    }
    // the running row is open; a finished one folds until tapped
    task.el.open = status === "running";
  }
  // where an event lands: a tagged record under its task's row on the
  // parent turn's board (<parent>.<task>), everything else on the turn
  function homeOf(event) {
    if (!event.task) return turnFor(event.turn_id || "loose");
    const parent = String(event.turn_id || "").split(".")[0] || "loose";
    return taskTurnFor(turnFor(parent), event.task);
  }

  function handle(event) {
    const turn = homeOf(event);
    switch (event.ev) {
      case "plan_made": {
        boardHead(turn, (event.tasks || []).length, event.pool || 1, false);
        for (const t of event.tasks || []) {
          taskTurnFor(turn, t.id, t.goal, t.depends_on || []);
        }
        pulse(turn, `Working through ${(event.tasks || []).length} tasks…`,
              event.ts);
        break;
      }
      case "task_started": {
        const task = taskTurnFor(turn, event.task, event.goal,
                                 event.depends_on || []);
        task.startedAt = Date.parse(event.ts || "") || Date.now();
        taskStatus(task, "running");
        break;
      }
      case "task_done": {
        const task = taskTurnFor(turn, event.task);
        taskStatus(task, event.status, event.cost, event.reason || "");
        doneThinking(task, (event.cost || {}).elapsed_ms);
        break;
      }
      case "skills_loaded":
        // the loader record (per skill: whole, sectioned or refused,
        // with the characters sent) rides the transcript for Operate;
        // a refusal already shows as the error card that follows it
        break;
      case "turn_started": {
        if (turn.task) {            // a task's own start: its row only
          turn.startedAt = Date.parse(event.ts || "") || Date.now();
          openBlock(turn);
          pulse(turn, "Thinking…", event.ts);
          break;
        }
        setRunning(true);
        state.liveTurn = turn;             // the turn a stop would end
        setEmpty(false);
        // a turn this page did not send — the build chained after a
        // run, or an ask from another tab — still shows as the
        // person's turn; one the page sent (or a /skill ask, shown
        // in the person's own words) is already on screen
        const bubbles = thread.querySelectorAll(".chat-user");
        const last = bubbles.length
          ? bubbles[bubbles.length - 1].textContent.trim() : "";
        const text = String(event.text || "").trim();
        if (text && !(last === text || last.endsWith(text))) {
          userBubble(text, turn.el);       // above the turn it opens
        }
        turn.startedAt = Date.parse(event.ts || "") || Date.now();
        openBlock(turn);
        pulse(turn, event.planning ? "Sorting out the asks…" : "Thinking…",
              event.ts);
        pingShelf();                       // the shelf marks it working
        break;
      }
      case "model_prompt":
        // each model call restarts the clock and starts a new thought
        // segment: after a tool returns the line says Thinking, never
        // the tool's name
        if (event.kind === "call") {
          turn.seg = null;
          if (turn.settled) openBlock(turn);
          pulse(turn, "Thinking…", event.ts);
        }
        break;            // the transcript record lives in Operate
      case "thinking": {
        // the model's own summary of what it is thinking, live: in
        // the block as it streams, its latest line on the header
        if (turn.settled) openBlock(turn);
        const seg = thoughtSegment(turn);
        turn.segText += event.delta || "";
        seg.innerHTML = renderMarkdown(turn.segText, "md");
        // the latest line of the thought rides the compact line's
        // hover; the line itself stays "Radix is thinking… 12s"
        const line = lastLine(turn.segText);
        if (line) turn.activity.querySelector("summary").title = line;
        scroll();
        break;
      }
      case "tool_call":
        if (turn.settled) openBlock(turn);
        movedOn(turn, true);             // after a snag: the next step
        pulse(turn, `${friendly(event)}…`, event.ts);
        toolStart(turn, event);
        break;
      case "tool_step":
        toolDone(turn, event);
        break;
      case "tool_result":
        if (event.tool === "remember") {
          let got = {};
          try { got = JSON.parse(event.content || "{}"); } catch {}
          if (got.ok) {
            memoryNote(turn, argOf({ args: event.content }, "text")
              || "a preference", got.memory_id);
          }
        }
        break;
      case "say_token":
        movedOn(turn, false);            // the answer is the next step
        if (!turn.settled) settleBlock(turn);   // the answer: fold it
        turn.buffer += event.delta || "";
        turn.prose.innerHTML = renderMarkdown(turn.buffer, "md");
        scroll();
        break;
      case "artifact":
        state.artifacts.set(event.artifact_id, event);
        artifactCard(turn.extras, event, true);
        pingShelf();
        break;
      case "proposal":
        if (!turn.settled) settleBlock(turn);
        proposalCard(turn.extras, event.proposal || {},
                     { message_id: event.message_id || "" });
        break;
      case "chips":
        if (turn.task) break;         // a task's follow-ups stay with it
        if (event.clarify) chipRow(null, event.clarify);
        else chipRow(event.suggestions || []);
        break;
      case "budget_tick":
        el("chat-meter").textContent =
          `${event.tokens ?? 0} tokens · ${event.calls ?? 0} calls`;
        // the turn's tokens so far, on the line of the turn that
        // spends them: a task's tick counts on the parent's line
        liveUsage(turn.task ? turn.parent : turn, event);
        break;
      case "turn_done":
        if (turn.task) {              // a task's own end: its row settles
          doneThinking(turn, event.elapsed_ms);
          break;
        }
        setRunning(false);
        doneThinking(turn, event.elapsed_ms);
        usageFooter(turn.el, usageOf(event));      // what the turn cost
        if (!state.session.title) refreshTitle();
        if (event.status === "partial"
            || event.status === "stopped") {
          turn.el.classList.add("partial");
        }
        pingShelf();
        break;
      case "error":
        doneThinking(turn, 0);
        if (event.code !== "trace") {
          say(`${prose(event.message || "Something went wrong.")}
            ${(event.next_actions || []).map((a) =>
              `<div class="muted">→ ${esc(a)}</div>`).join("")}`,
            "error");
        }
        setRunning(false);
        break;
      default:
        break;
    }
  }

  function subscribe() {
    if (state.source) state.source.close();
    const source = new EventSource(
      api.chatStreamUrl(state.session.id, state.seq));
    for (const name of [
      "turn_started", "skills_loaded", "plan_made", "task_started", "task_done",
      "model_prompt", "thinking", "tool_call",
      "tool_step", "tool_result", "say_token", "artifact", "proposal",
      "chips", "budget_tick", "turn_done", "error"]) {
      source.addEventListener(name, (message) => {
        let event;
        try { event = JSON.parse(message.data); } catch { return; }
        state.seq = event.seq || state.seq;
        handle(event);
      });
    }
    state.source = source;
  }

  // the thinking block of a past turn, from the trace kept with the
  // message: folded, expandable, the same shape as the live one
  function traceBlock(container, trace, elapsedMs) {
    const entries = (trace || []).filter((t) => t.kind === "thought"
      ? String(t.text || "").trim() : !QUIET.has(t.tool));
    if (!entries.length) return;
    const verbs = [...new Set(entries.filter((t) => t.kind === "tool")
      .map((t) => PAST[t.tool]).filter(Boolean))];
    const thought = entries.some((t) => t.kind === "thought");
    const details = document.createElement("details");
    details.className = "tool-activity";
    details.innerHTML = `<summary><span class="tri">▸</span>
      <span class="tool-title">${esc(doneLabel(thought, elapsedMs, verbs))
      }</span></summary><div class="tool-steps"></div>`;
    rememberThinking(details);          // folded; a click is a choice too
    const steps = details.querySelector(".tool-steps");
    for (const t of entries) {
      const el = document.createElement("div");
      if (t.kind === "thought") {
        el.className = "think-seg md";
        el.innerHTML = renderMarkdown(t.text || "", "md");
      } else {
        el.className = "theater-step";
        const summary = String(t.summary || "");
        const outcome = summary.split("\n")[0].slice(0, 120);
        el.innerHTML = `<span class="mark">·</span>
          <span class="step-body"><span class="step-text">${
            prose(friendly(t))}${outcome
            ? ` <span class="muted">— ${prose(outcome)}</span>` : ""}</span>
          </span>`;
        if (outcome.startsWith("ERROR")) {
          snagRow(el, t, summary);
          // a past turn already knows whether another step followed
          if (entries.slice(entries.indexOf(t) + 1)
                .some((n) => n.kind === "tool")) {
            el.querySelector(".snag-next").hidden = false;
          }
        }
        attachInput(el, t.input);
      }
      steps.appendChild(el);
    }
    container.prepend(details);
  }

  // a task run's messages: one per task (payload.task) under the
  // board, then the synthesis (payload.plan) as the turn's answer
  // with every row's status and cost — the same board as live
  function replayTaskMessage(message, last) {
    const p = message.payload || {};
    const parentId = String(message.turn_id || "").split(".")[0] || "loose";
    const turn = turnFor(parentId);
    turn.done = true;
    turn.activity.hidden = true;
    if (p.task) {
      const task = taskTurnFor(turn, p.task.id, p.task.goal,
                               p.task.depends_on || []);
      task.done = true;
      task.activity.hidden = true;
      task.prose.innerHTML = renderMarkdown(message.text || "", "md");
      traceBlock(task.el.querySelector(".task-body"), p.trace, p.elapsed_ms);
      for (const id of p.artifacts || []) {
        const row = state.artifacts.get(id);
        if (row) artifactCard(task.extras, row, false);
      }
      if (p.proposal) {
        proposalCard(task.extras, p.proposal, { message_id: message.id });
      }
      return;
    }
    const plan = p.plan || {};
    const tasks = plan.tasks || [];
    boardHead(turn, tasks.length, plan.pool || 1, !!plan.stopped);
    for (const t of tasks) {
      const task = taskTurnFor(turn, t.id, t.goal, t.depends_on || []);
      task.done = true;
      taskStatus(task, t.status, t.cost, t.reason || "");
    }
    turn.prose.innerHTML = renderMarkdown(message.text || "", "md");
    const box = document.createElement("div");
    traceBlock(box, p.trace, p.elapsed_ms);
    turn.el.insertBefore(box, turn.prose);
    for (const id of p.artifacts || []) {
      const row = state.artifacts.get(id);
      if (row) artifactCard(turn.extras, row, false);
    }
    if (p.chips?.length && last) chipRow(p.chips);
    if (p.usage) usageFooter(turn.el, usageOf(p.usage));
  }

  // ── history replay from the store ────────────────────────
  for (const row of boot.artifacts || []) {
    state.artifacts.set(row.artifact_id, row);
  }
  for (const message of boot.messages || []) {
    if (message.role === "user") {
      userBubble(message.text, null, (message.payload || {}).files || []);
    }
    else if (boot.running && boot.turn_id
             && String(message.turn_id || "").split(".")[0] === boot.turn_id) {
      // the in-flight turn's own messages (a task run's finished
      // tasks): the event replay below draws them
    }
    else if (message.payload?.task || message.payload?.plan) {
      replayTaskMessage(message,
                        message === boot.messages[boot.messages.length - 1]);
    }
    else {
      const div = document.createElement("div");
      div.className = "chat-turn";
      div.innerHTML = `<div class="chat-prose md">${
        renderMarkdown(message.text || "", "md")}</div>
        <div class="chat-extras"></div>`;
      traceBlock(div, message.payload?.trace, message.payload?.elapsed_ms);
      thread.appendChild(div);
      // a card in the transcript reopens the artifact; the panel
      // itself stays shut until the model puts something new in it
      for (const id of message.payload?.artifacts || []) {
        const row = state.artifacts.get(id);
        if (row) artifactCard(div.querySelector(".chat-extras"),
                              row, false);
      }
      if (message.payload?.proposal) {
        proposalCard(div.querySelector(".chat-extras"),
                     message.payload.proposal, { message_id: message.id });
      }
      const chips = message.payload?.chips;
      if (chips?.length
          && message === boot.messages[boot.messages.length - 1]) {
        chipRow(chips);
      }
      // the footer the stream drew, from the stored usage
      if (message.payload?.usage) {
        usageFooter(div, usageOf(message.payload.usage));
      }
    }
  }
  setEmpty(!(boot.messages || []).length);
  state.seq = boot.head || 0;
  // a turn runs on the server, not in this tab: coming back to a
  // session mid-turn replays the in-flight turn from its first event
  // and keeps following it — switching chats or tabs loses nothing
  if (boot.running && boot.turn_after !== null
      && boot.turn_after !== undefined) {
    state.seq = boot.turn_after;
    setRunning(true);
  }
  scroll();

  // ── sending ──────────────────────────────────────────────
  function setRunning(running) {
    state.running = running;
    const sendBtn = el("chat-send");
    const stopBtn = el("chat-stop");
    sendBtn.disabled = running;
    sendBtn.hidden = running;            // the stop takes its place
    stopBtn.hidden = !running;
    if (!running) {                      // the turn ended: the composer is back
      state.stopping = false;
      stopBtn.disabled = false;
      stopBtn.classList.remove("stopping");
      stopBtn.title = "Stop";
    }
  }
  // the stop: pressed once, the button locks and the live line says
  // "Stopping…" until the server's turn_done (status stopped) lands
  // through the stream and setRunning(false) restores the composer
  async function stop() {
    if (!state.running || state.stopping) return;
    state.stopping = true;
    const stopBtn = el("chat-stop");
    stopBtn.disabled = true;
    stopBtn.classList.add("stopping");
    stopBtn.title = "Stopping…";
    const turn = state.liveTurn;
    if (turn && !turn.done) pulse(turn, "Stopping…");
    try {
      await api.chatStop(state.session.id);
    } catch {
      // the stream's turn_done restores the composer either way
    }
  }
  async function send(text) {
    if (!text.trim() || state.running) return;
    el("chat-chiprow").innerHTML = "";
    slash.hidden = true;
    setEmpty(false);
    const files = state.files.slice();
    userBubble(text, null, files);
    scroll(true);                        // sending re-sticks the thread
    input.value = "";
    const accepted = await api.chatSend(state.session.id, text,
                                        el("chat-depth").value,
                                        state.mode, "",   // the chat's own plane
                                        files.map((f) => f.id));
    if (!accepted.available) {
      say(`<b>not sent.</b> ${esc(accepted.reason || "")}`, "error");
      return;
    }
    state.files = [];                    // they rode this message
    paintFiles();
  }
  el("chat-send").addEventListener("click", () => send(input.value));
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      const pick = slash.hidden ? null : slash.querySelector(".slash-item");
      if (pick) { pickSlash(pick.dataset.name); return; }
      send(input.value);
    }
    if (e.key === "Escape" && !slash.hidden) slash.hidden = true;
    else if (e.key === "Escape" && state.running) stop();
  });
  el("chat-stop").addEventListener("click", stop);

  subscribe();
  pingShelf();
  return () => { if (state.source) state.source.close(); };
}
