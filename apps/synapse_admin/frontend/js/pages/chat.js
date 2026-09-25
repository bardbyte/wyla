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
import { createArtifactRenderer } from "../artifacts-render.js";

const SESSION_KEY = "synapse-chat-session";

export async function renderChat(outlet, wanted = "") {
  outlet.innerHTML = `
    <div class="chatv2 empty" id="chatv2">
      <div class="chat-main">
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
        <button class="chat-jump" id="chat-jump" type="button" hidden
          title="Jump to the latest">↓ Latest</button>
        <div class="chat-chiprow" id="chat-chiprow"></div>
        <div class="chat-composer">
          <div class="chat-box">
            <textarea id="chat-input" rows="1"
              placeholder="Type / for skills"></textarea>
            <div class="chat-slash" id="chat-slash" hidden></div>
            <div class="chat-actions">
              <button class="icon-btn chat-plus" id="chat-plus"
                title="More" aria-expanded="false">+</button>
              <div class="chat-plus-pop" id="chat-plus-pop" hidden></div>
              <div class="chat-modes" role="radiogroup"
                aria-label="How Radix works this ask">
                <button class="chat-mode on" data-mode="chat"
                  role="radio" aria-checked="true"
                  title="Radix writes the query and hands it over; you run it">Chat</button>
                <button class="chat-mode" data-mode="autopilot"
                  role="radio" aria-checked="false"
                  title="Radix runs the query under the limits and builds the deliverable">Autopilot</button>
              </div>
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
                title="What Chat, Autopilot, the thinking levels and the models mean"
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
      <aside class="chat-panel" id="chat-panel" hidden>
        <div class="chat-panel-head">
          <b class="panel-title" id="panel-title"></b>
          <select id="panel-version" title="Version"></select>
          <button class="btn" id="panel-close" title="Close">✕</button>
          <div class="panel-actions">
            <span id="panel-export"></span>
          </div>
        </div>
        <div class="chat-panel-body" id="panel-body"></div>
      </aside>
    </div>`;

  const el = (id) => outlet.querySelector("#" + id);
  const thread = el("chat-thread");
  const input = el("chat-input");
  const state = { session: null, source: null, turns: new Map(),
                  running: false, seq: 0, artifacts: new Map(),
                  panelId: "" };

  // ── the thread follows new content only while the reader is at the
  //    bottom. Scrolling up to reread unsticks it, so a thinking delta,
  //    an answer token or the one-second heartbeat never yanks the
  //    view back down; sending a message re-sticks it; while new
  //    content lands out of view a "Latest" pill offers the way back ──
  const NEAR_BOTTOM = 48;
  const jump = el("chat-jump");
  const atBottom = () => thread.scrollHeight - thread.scrollTop
    - thread.clientHeight <= NEAR_BOTTOM;
  let stuck = true;
  const scroll = (force = false) => {
    if (force) stuck = true;
    if (stuck) {
      thread.scrollTop = thread.scrollHeight;
      jump.hidden = true;
      return;
    }
    // the pill sits just above the thread's bottom edge, whatever the
    // composer's height is at the moment
    const main = thread.parentElement;
    jump.style.bottom = `${Math.max(0, main.clientHeight - thread.offsetTop
      - thread.offsetHeight) + 12}px`;
    jump.hidden = false;
  };
  thread.addEventListener("scroll", () => {
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
  // ── the dials, explained: the model switch shows the chat's model
  //    now; one catalog from the backend then fills the switch (every
  //    model on every plane, grouped by plane), the option titles and
  //    the "?" popover — one source for both surfaces. state.plane holds
  //    the choice id: a plane (its default model) or plane:model
  const planeSel = el("chat-model");
  state.plane = boot.choice || boot.plane || "";
  planeSel.innerHTML = `<option value="${esc(state.plane)}" selected>${
    esc(boot.model || "")}</option>`;
  // the two knobs: pills over the hidden selects; the dials catalog
  // fills them below, the selects' own options stand in until then
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
    if (!planeSel.isConnected || !el("chat-depth")) return;   // page left
    const planes = dials.planes || [];
    // the models, grouped by plane; a plane with one model is one option
    const models = (dials.models || []).length ? dials.models
      : planes.map((p) => ({ ...p, plane: p.id, plane_name: p.plane_name }));
    const groups = [];
    for (const m of models) {
      let g = groups.find((x) => x.plane === m.plane);
      if (!g) { g = { plane: m.plane, name: m.plane_name || m.plane, rows: [] }; groups.push(g); }
      g.rows.push(m);
    }
    const option = (m) => `
      <option value="${esc(m.id)}"${m.id === state.plane ? " selected" : ""}${
        m.available ? "" : " disabled"} title="${
        esc(m.available ? m.means : m.reason)}">${esc(m.label)}${
        m.available ? "" : " · not configured"}</option>`;
    planeSel.innerHTML = groups.map((g) => g.rows.length > 1
      ? `<optgroup label="${esc(g.name)}">${g.rows.map(option).join("")}</optgroup>`
      : g.rows.map(option).join("")).join("");
    modelKnob.setModels(models);
    depthKnob.setDepths(dials.depths || []);
    for (const b of document.querySelectorAll(".chat-mode")) {
      const m = (dials.modes || []).find((x) => x.id === b.dataset.mode);
      if (m) b.title = m.means;
    }
    const notes = dials.notes || {};
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
  // the switch is remembered on the chat and rides the next message;
  // a plane this machine cannot ride is refused with the reason and
  // the switch goes back to the one that works
  planeSel.addEventListener("change", async () => {
    const wanted = planeSel.value;
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
    // before the first message the select itself is the confirmation;
    // mid-conversation the thread says so, where the person is looking
    if (!shell.classList.contains("empty")) {
      const shown = planeSel.selectedOptions[0]?.textContent || got.model || wanted;
      say(`Switched to <b>${esc(shown)}</b> from the next message on.`);
    }
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
    <a class="plus-item" href="#/skills">✦ Browse skills</a>
    <button class="plus-item" id="plus-memory">⊚ Memory</button>
    <a class="plus-item" href="#/chat/new">✳ New chat</a>`;
  el("chat-plus").addEventListener("click", () => {
    plusPop.hidden = !plusPop.hidden;
    el("chat-plus").setAttribute("aria-expanded",
                                 String(!plusPop.hidden));
  });
  plusPop.querySelector("#plus-memory").addEventListener("click", () => {
    plusPop.hidden = true;
    el("chat-memory-btn").click();
  });
  // the mode (§5): Chat hands queries over for you to run; Autopilot
  // runs and builds without stopping. Kept per browser.
  const MODE_KEY = "synapse-chat-mode";
  state.mode = localStorage.getItem(MODE_KEY) === "autopilot"
    ? "autopilot" : "chat";
  function paintMode() {
    for (const b of outlet.querySelectorAll(".chat-mode")) {
      const on = b.dataset.mode === state.mode;
      b.classList.toggle("on", on);
      b.setAttribute("aria-checked", String(on));
    }
  }
  for (const b of outlet.querySelectorAll(".chat-mode")) {
    b.addEventListener("click", () => {
      state.mode = b.dataset.mode;
      localStorage.setItem(MODE_KEY, state.mode);
      paintMode();
    });
  }
  paintMode();
  // "/" lists the skills: pick one and the turn loads that pack
  const slash = el("chat-slash");
  let packs = null;
  async function loadPacks() {
    if (packs) return packs;
    const got = await api.chatSkills().catch(() => ({}));
    packs = got.available ? got.skills || [] : [];
    return packs;
  }
  function pickSlash(name) {
    input.value = `/${name} `;
    slash.hidden = true;
    input.focus();
  }
  async function paintSlash() {
    const m = input.value.match(/^\/([A-Za-z0-9_\-]*)$/);
    if (!m) { slash.hidden = true; return; }
    const rows = (await loadPacks()).filter((p) =>
      String(p.name || "").toLowerCase().startsWith(m[1].toLowerCase()))
      .slice(0, 8);
    slash.innerHTML = rows.length ? rows.map((p) => `
      <button class="slash-item" data-name="${esc(p.name)}">
        <b>/${esc(p.name)}</b>
        <span class="muted">${esc(p.title || "")}</span></button>`).join("")
      : `<div class="muted slash-none">No skill starts with “/${
          esc(m[1])}” — the Skills tab lists them all.</div>`;
    slash.hidden = false;
    for (const b of slash.querySelectorAll(".slash-item")) {
      b.addEventListener("click", () => pickSlash(b.dataset.name));
    }
  }
  input.addEventListener("input", paintSlash);
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
  // the rendering — charts, tables, tiles, dashboards, diagrams and
  // the strip under every number — is js/artifacts-render.js, one
  // module identical on both surfaces; what differs per surface
  // (the words under a number) arrives as a hook
  const { statusChip, renderArtifactBody, bindTable, animateNumbers }
    = createArtifactRenderer({
    esc, prose, statusLabel, renderMarkdown,
    // this console shows the definition line itself, in the open
    meridian: (prov) => (prov && prov.meridian_line
      ? `<span class="meridian" title="${esc(prov.meridian_line)}">${
          prose(prov.meridian_line)}</span>` : ""),
  });

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

  function exportButtons(row) {
    const box = el("panel-export");
    const slug = (row.title || row.type).toLowerCase()
      .replace(/[^a-z0-9]+/g, "-").slice(0, 40);
    const buttons = [];
    if (row.type === "chart") {
      buttons.push(["SVG", () => download(`${slug}.svg`,
        "image/svg+xml",
        el("panel-body").querySelector("svg").outerHTML)]);
      buttons.push(["PNG", () => {
        const svg = el("panel-body").querySelector("svg");
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
          el("panel-body").innerHTML +
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
          el("panel-body").querySelector("svg").outerHTML)]);
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

  async function openArtifact(artifactId, version = null) {
    const got = await api.chatArtifact(artifactId, version);
    if (!got.available) return;
    const row = got.artifact;
    state.panelId = artifactId;
    state.artifacts.set(artifactId, row);
    const panel = el("chat-panel");
    panel.hidden = false;
    el("panel-title").textContent = row.title;
    el("panel-body").innerHTML = renderArtifactBody(row);
    el("panel-body").scrollTop = 0;       // the artifact, from its top
    bindTable(el("panel-body"));
    animateNumbers(el("panel-body"));
    if (row.type === "dashboard") {
      bindDashboardFilters(el("panel-body"), row);
    }
    exportButtons(row);
    // the drawer slides in from the right edge and moves the chat to
    // the middle; the card it came from lights up
    requestAnimationFrame(() => {
      panel.classList.add("open");
      shell.classList.add("panel-open");
    });
    for (const card of thread.querySelectorAll(".artifact-inline")) {
      card.classList.toggle("on", card.dataset.artifact === artifactId);
    }
    const versions = await api.chatArtifactVersions(artifactId);
    const select = el("panel-version");
    select.innerHTML = (versions.versions || []).map((v) =>
      `<option value="${v.version}"${v.version === row.version
        ? " selected" : ""}>v${v.version}</option>`).join("");
    select.onchange = () =>
      openArtifact(artifactId, Number(select.value));
  }
  el("panel-close").addEventListener("click", () => {
    const panel = el("chat-panel");
    panel.classList.remove("open");
    shell.classList.remove("panel-open");
    state.panelId = "";
    for (const card of thread.querySelectorAll(".artifact-inline.on")) {
      card.classList.remove("on");
    }
    // hidden once it has slid out, so nothing off-screen stays live
    setTimeout(() => {
      if (!panel.classList.contains("open")) panel.hidden = true;
    }, 340);
  });

  // ── the stream ───────────────────────────────────────────
  function turnFor(turnId) {
    let turn = state.turns.get(turnId);
    if (turn) return turn;
    const div = document.createElement("div");
    div.className = "chat-turn";
    div.innerHTML = `
      <details class="tool-activity" hidden open>
        <summary>
          <span class="tri">▸</span>
          <span class="thinking-line">
            <span class="think-orb">✳</span>
            <span class="think-text">Thinking…</span></span>
          <span class="tool-title" hidden></span>
        </summary>
        <div class="tool-steps"></div>
      </details>
      <div class="chat-prose md"></div>
      <div class="chat-extras"></div>`;
    thread.appendChild(div);
    turn = { el: div,
             activity: div.querySelector(".tool-activity"),
             toolTitle: div.querySelector(".tool-title"),
             toolSteps: div.querySelector(".tool-steps"),
             thinking: div.querySelector(".thinking-line"),
             thinkText: div.querySelector(".think-text"),
             prose: div.querySelector(".chat-prose"),
             extras: div.querySelector(".chat-extras"),
             buffer: "", steps: 0, rows: new Map(), verbs: [],
             thoughts: "", done: false, tick: null, tickLabel: "",
             tickStart: 0, seg: null, segText: "", thought: false,
             settled: false, startedAt: 0 };
    state.turns.set(turnId, turn);
    scroll();
    return turn;
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

  function pulse(turn, label, sinceIso = "") {
    stopPulse(turn);
    turn.tickLabel = label;
    // the clock starts when the event happened, not when it was
    // seen — a replayed turn shows its true seconds
    const since = Date.parse(sinceIso || "");
    turn.tickStart = Number.isFinite(since)
      ? Math.min(since, Date.now()) : Date.now();
    showThinking(turn, label);
    turn.tick = setInterval(() => {
      if (turn.done) { stopPulse(turn); return; }
      const secs = Math.round((Date.now() - turn.tickStart) / 1000);
      if (secs < 4) return;
      const base = turn.tickLabel.replace(/…$/, "");
      showThinking(turn, secs >= 20
        ? `Still ${base.charAt(0).toLowerCase()}${base.slice(1)} · ${secs}s`
        : `${base}… ${secs}s`);
    }, 1000);
  }

  // ── the thinking block, the way a chat assistant shows it: the model's own
  // thought summaries in the order they happen, interleaved with the
  // steps — open while it works, "Thought for 34s" when the answer
  // lands, yours to expand; new work reopens it
  function doneLabel(thought, elapsedMs, verbs) {
    const secs = Math.max(0, (elapsedMs || 0) / 1000);
    const head = `${thought ? "Thought" : "Worked"} for ${
      secs >= 10 ? secs.toFixed(0) : secs.toFixed(1)}s`;
    return verbs.length ? `${head} · ${verbs.join(", ")}` : head;
  }

  function openBlock(turn) {
    turn.activity.hidden = false;
    turn.activity.open = true;
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
    turn.activity.open = false;
    turn.settled = true;
  }

  function doneThinking(turn, elapsedMs) {
    turn.done = true;
    settleBlock(turn, elapsedMs);
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

  function userBubble(text, before = null) {
    const div = document.createElement("div");
    div.className = "chat-user";
    div.textContent = text;
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
    div.className = "card artifact-inline";
    div.dataset.artifact = row.artifact_id;
    div.innerHTML = `
      <span class="glyph">${{ chart: "📊", table: "▦",
        document: "🗎", dashboard: "▥", diagram: "✦",
        kpi: "◉" }[row.type] || "▣"}</span>
      <span class="artifact-name" title="${esc(row.title)}">${
        esc(row.title)}</span>
      <span class="muted">${esc(row.type)} · v${row.version}${
        row.spec?.watermark
          ? ` · ${esc(row.spec.watermark)}` : ""}</span>
      <button class="btn">open</button>`;
    div.querySelector("button").addEventListener("click", () =>
      openArtifact(row.artifact_id));
    container.appendChild(div);
    // the panel is model-invoked: it opens on an artifact in THIS
    // interaction, never on reopening an old chat
    if (live) openArtifact(row.artifact_id);
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
        proposal.meridian_line
          ? ` · <span class="meridian">${esc(proposal.meridian_line)}</span>`
          : ""}</div>
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
        <details class="tool-activity" hidden open>
          <summary>
            <span class="tri">▸</span>
            <span class="thinking-line">
              <span class="think-orb">✳</span>
              <span class="think-text">Thinking…</span></span>
            <span class="tool-title" hidden></span>
          </summary>
          <div class="tool-steps"></div>
        </details>
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
             activity: row.querySelector(".tool-activity"),
             toolTitle: row.querySelector(".tool-title"),
             toolSteps: row.querySelector(".tool-steps"),
             thinking: row.querySelector(".thinking-line"),
             thinkText: row.querySelector(".think-text"),
             prose: row.querySelector(".chat-prose"),
             extras: row.querySelector(".chat-extras"),
             buffer: "", steps: 0, rows: new Map(), verbs: [],
             thoughts: "", done: false, tick: null, tickLabel: "",
             tickStart: 0, seg: null, segText: "", thought: false,
             settled: false, startedAt: 0 };
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
      case "skills_loaded":
        // the loader record (per skill: whole, sectioned or refused,
        // with the characters sent) rides the transcript for Operate;
        // a refusal already shows as the error card that follows it
        break;
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
        const line = lastLine(turn.segText);
        if (line) { turn.tickLabel = line; showThinking(turn, line); }
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
        break;
      case "turn_done":
        if (turn.task) {              // a task's own end: its row settles
          doneThinking(turn, event.elapsed_ms);
          break;
        }
        setRunning(false);
        doneThinking(turn, event.elapsed_ms);
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
  }

  // ── history replay from the store ────────────────────────
  for (const row of boot.artifacts || []) {
    state.artifacts.set(row.artifact_id, row);
  }
  for (const message of boot.messages || []) {
    if (message.role === "user") userBubble(message.text);
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
    userBubble(text);
    scroll(true);                        // sending re-sticks the thread
    input.value = "";
    const accepted = await api.chatSend(state.session.id, text,
                                        el("chat-depth").value,
                                        state.mode, state.plane);
    if (!accepted.available) {
      say(`<b>not sent.</b> ${esc(accepted.reason || "")}`, "error");
    }
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
