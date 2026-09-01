/** Synapse v2 (docs/specs/synapse_v2.md §10): the Claude-shaped
 * surface — sidebar of chats, a streamed conversation with collapsed
 * tool activity, and an artifact panel on the right for the outputs
 * the user keeps.
 *
 * A pure consumer of the assistant event stream: nothing here calls
 * a model, holds a key, or invents a value. Artifacts render exactly
 * what the validator stored — including the EXPLORATORY watermark —
 * and exports carry the provenance footer.
 */

import { api } from "../api.js";
import { renderMarkdown } from "../md.js";
import { esc, prose } from "../ui.js";

const SESSION_KEY = "synapse-chat-session";
const PALETTE = ["#2f6feb", "#e8710a", "#1a9850", "#9970ab",
                 "#d6604d", "#35978f"];

export async function renderChat(outlet, wanted = "") {
  outlet.innerHTML = `
    <div class="chatv2">
      <aside class="chat-side">
        <button class="btn primary" id="chat-new">＋ New chat</button>
        <input id="chat-search" placeholder="Search chats…">
        <div class="chat-recents" id="chat-recents"></div>
      </aside>
      <div class="chat-main">
        <div class="chat-masthead">
          <span class="muted" id="chat-build"></span>
          <span class="spacer"></span>
          <span class="muted" id="chat-meter"></span>
          <button class="btn" id="chat-skills-btn"
            aria-expanded="false">⊕ skills</button>
          <span id="chat-skill-chips"></span>
          <div id="chat-skills-pop" class="skills-pop" hidden></div>
        </div>
        <div class="chat-thread" id="chat-thread"></div>
        <div class="chat-chiprow" id="chat-chiprow"></div>
        <div class="chat-composer">
          <textarea id="chat-input" rows="1"
            placeholder="Ask anything about your data…"></textarea>
          <div class="ask-actions">
            <span class="muted">Enter to send · Esc to stop</span>
            <span class="spacer"></span>
            <button class="btn" id="chat-stop" hidden>stop</button>
            <button class="btn primary" id="chat-send">Send</button>
          </div>
        </div>
      </div>
      <aside class="chat-panel" id="chat-panel" hidden>
        <div class="chat-panel-head">
          <b id="panel-title"></b>
          <span class="spacer"></span>
          <select id="panel-version"></select>
          <span id="panel-export"></span>
          <button class="btn" id="panel-close">✕</button>
        </div>
        <div class="chat-panel-body" id="panel-body"></div>
      </aside>
    </div>`;

  const el = (id) => outlet.querySelector("#" + id);
  const thread = el("chat-thread");
  const input = el("chat-input");
  const state = { session: null, source: null, turns: new Map(),
                  running: false, seq: 0, artifacts: new Map(),
                  panelId: "", skillsAvailable: [],
                  skillsLoaded: new Set() };

  const scroll = () => { thread.scrollTop = thread.scrollHeight; };
  const say = (html, cls = "") => {
    const div = document.createElement("div");
    div.className = `ask-note ${cls}`;
    div.innerHTML = html;
    thread.appendChild(div);
    scroll();
  };

  // ── boot: reopen or create ───────────────────────────────
  let boot = null;
  const stored = wanted || localStorage.getItem(SESSION_KEY);
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
  el("chat-build").textContent =
    `build ${state.session.build_id || "?"}`;
  state.skillsLoaded = new Set(boot.session.skills || []);

  // ── sidebar ──────────────────────────────────────────────
  async function refreshRecents() {
    const got = await api.chatSessions();
    const rows = got.available ? got.sessions : [];
    const needle = el("chat-search").value.trim().toLowerCase();
    el("chat-recents").innerHTML = rows
      .filter((r) => !needle
        || (r.title || "untitled").toLowerCase().includes(needle))
      .map((r) => `
        <div class="recent-row${r.id === state.session.id
          ? " on" : ""}" data-id="${esc(r.id)}">
          <span class="recent-title">${esc(r.title
            || "New chat")}</span>
          <span class="muted">${esc((r.updated_at
            || "").slice(5, 16).replace("T", " "))}</span>
        </div>`).join("")
      || `<div class="muted" style="padding:8px">No chats yet.</div>`;
    for (const row of outlet.querySelectorAll(".recent-row")) {
      row.addEventListener("click", () => {
        localStorage.setItem(SESSION_KEY, row.dataset.id);
        location.hash = `#/chat/${row.dataset.id}`;
      });
    }
  }
  el("chat-search").addEventListener("input", refreshRecents);
  el("chat-new").addEventListener("click", async () => {
    const created = await api.chatNewSession();
    if (created.available) {
      localStorage.setItem(SESSION_KEY, created.session.id);
      location.hash = `#/chat/${created.session.id}`;
    }
  });

  // ── skills (same picker contract as Ask) ─────────────────
  const skillsBtn = el("chat-skills-btn");
  const skillsPop = el("chat-skills-pop");
  const skillChips = el("chat-skill-chips");
  async function refreshSkills() {
    try {
      const got = await api.askSkills();
      state.skillsAvailable = got.available ? (got.skills || []) : [];
    } catch { state.skillsAvailable = []; }
    paintSkillChips();
  }
  function paintSkillChips() {
    const loaded = [...state.skillsLoaded];
    skillChips.innerHTML = loaded.map((n) =>
      `<button class="skill-chip" data-skill="${esc(n)}"
        title="click to unload">${esc(n)} ×</button>`).join(" ");
    skillsBtn.textContent = loaded.length
      ? `⊕ skills (${loaded.length})` : "⊕ skills";
    for (const chip of skillChips.querySelectorAll(".skill-chip")) {
      chip.addEventListener("click", () =>
        toggleSkill(chip.dataset.skill));
    }
  }
  function paintSkillsPop() {
    skillsPop.innerHTML = (state.skillsAvailable.length
      ? state.skillsAvailable.map((s) => `
        <label class="skills-row">
          <input type="checkbox" data-skill="${esc(s.name)}"
            ${state.skillsLoaded.has(s.name) ? "checked" : ""}>
          <span><b>${esc(s.title || s.name)}</b>
            <span class="muted">${esc(s.description || "")}</span>
          </span></label>`).join("")
      : `<div class="muted" style="padding:6px">No skills yet:
         drop a markdown briefing in <code>graph/skills/</code>.
         </div>`);
    for (const box of skillsPop.querySelectorAll("input")) {
      box.addEventListener("change", () =>
        toggleSkill(box.dataset.skill));
    }
  }
  async function toggleSkill(name) {
    const next = new Set(state.skillsLoaded);
    if (next.has(name)) next.delete(name); else next.add(name);
    const saved = await api.chatSetSkills(state.session.id, [...next]);
    if (saved.available && saved.ok) state.skillsLoaded = next;
    else say(`<b>skills:</b> ${esc(saved.reason || "not saved")}`,
             "error");
    paintSkillChips();
    if (!skillsPop.hidden) paintSkillsPop();
  }
  skillsBtn.addEventListener("click", () => {
    skillsPop.hidden = !skillsPop.hidden;
    skillsBtn.setAttribute("aria-expanded", String(!skillsPop.hidden));
    if (!skillsPop.hidden) paintSkillsPop();
  });
  document.addEventListener("click", (e) => {
    if (!skillsPop.hidden && !skillsPop.contains(e.target)
        && e.target !== skillsBtn) skillsPop.hidden = true;
  });

  // ── the artifact panel ───────────────────────────────────
  function statusChip(prov) {
    if (!prov || !prov.status) return "";
    return `<span class="status-chip s-${esc(prov.status)}">${
      esc(prov.status)}</span>`;
  }

  function chartSVG(spec, width = 520, height = 280) {
    const pad = { l: 46, r: 12, t: 14, b: 34 };
    const series = spec.series || [];
    const all = series.flatMap((s) => s.points.map((p) => p[1]));
    if (!all.length) return "<svg></svg>";
    const yMin = Math.min(0, ...all);
    const yMax = Math.max(...all) || 1;
    const n = Math.max(...series.map((s) => s.points.length));
    const px = (i) => pad.l + (n < 2 ? 0
      : (i * (width - pad.l - pad.r)) / (n - 1));
    const py = (v) => pad.t + (height - pad.t - pad.b)
      * (1 - (v - yMin) / (yMax - yMin || 1));
    let body = "";
    // y gridlines + labels
    for (let g = 0; g <= 4; g++) {
      const v = yMin + ((yMax - yMin) * g) / 4;
      const y = py(v);
      body += `<line x1="${pad.l}" y1="${y}" x2="${width - pad.r}"
        y2="${y}" class="grid"/>
        <text x="${pad.l - 6}" y="${y + 4}" class="tick"
        text-anchor="end">${esc(Number(v.toPrecision(3)))}</text>`;
    }
    series.forEach((s, si) => {
      const color = PALETTE[si % PALETTE.length];
      if (spec.kind === "bar") {
        const bw = Math.max(4, (width - pad.l - pad.r)
          / (n * series.length) - 4);
        s.points.forEach((p, i) => {
          const x = px(i) - (bw * series.length) / 2 + si * bw;
          body += `<rect x="${x}" y="${Math.min(py(p[1]), py(0))}"
            width="${bw}" height="${Math.abs(py(p[1]) - py(0))}"
            fill="${color}" opacity="0.85"/>`;
        });
      } else {
        const path = s.points.map((p, i) =>
          `${i ? "L" : "M"}${px(i)},${py(p[1])}`).join(" ");
        if (spec.kind === "area") {
          body += `<path d="${path} L${px(s.points.length - 1)},${
            py(yMin)} L${px(0)},${py(yMin)} Z" fill="${color}"
            opacity="0.15"/>`;
        }
        if (spec.kind !== "scatter") {
          body += `<path d="${path}" fill="none" stroke="${color}"
            stroke-width="2"/>`;
        }
        s.points.forEach((p, i) => {
          body += `<circle cx="${px(i)}" cy="${py(p[1])}" r="2.6"
            fill="${color}"><title>${esc(String(p[0]))}: ${
            esc(String(p[1]))}</title></circle>`;
        });
      }
    });
    const first = series[0]?.points || [];
    if (first.length) {
      body += `<text x="${px(0)}" y="${height - 12}" class="tick"
        >${esc(String(first[0][0]))}</text>
        <text x="${px(first.length - 1)}" y="${height - 12}"
        class="tick" text-anchor="end">${esc(String(
          first[first.length - 1][0]))}</text>`;
    }
    const legend = series.map((s, si) =>
      `<tspan fill="${PALETTE[si % PALETTE.length]}">■</tspan> ${
        esc(s.name)} `).join(" ");
    body += `<text x="${pad.l}" y="${pad.t - 2}" class="tick"
      >${legend}${spec.unit ? " · " + esc(spec.unit) : ""}</text>`;
    if (spec.watermark) {
      body += `<text x="${width / 2}" y="${height / 2}"
        class="watermark" text-anchor="middle"
        transform="rotate(-18 ${width / 2} ${height / 2})">${
        esc(spec.watermark)}</text>`;
    }
    return `<svg viewBox="0 0 ${width} ${height}"
      class="chartv2" xmlns="http://www.w3.org/2000/svg">${body}</svg>`;
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
        ${prov ? `<span class="meridian">${
          prose(prov.meridian_line)}</span>` : ""}
        <span class="muted">build ${esc(spec.build_id || "?")}
          · v${row.version}</span>
      </div>`;
    if (row.type === "chart") {
      return chartSVG(spec) + footer;
    }
    if (row.type === "table") {
      const cols = spec.columns || [];
      const head = cols.map((c) =>
        `<th data-key="${esc(c.key)}">${esc(c.label)}${
          c.status ? ` <span class="muted">(${esc(c.status)})</span>`
                   : ""}</th>`).join("");
      const body = (spec.rows || []).map((r) =>
        `<tr>${cols.map((c) =>
          `<td>${esc(String(r[c.key] ?? ""))}</td>`).join("")}</tr>`)
        .join("");
      const mark = spec.watermark
        ? `<div class="watermark-band">${esc(spec.watermark)}</div>`
        : "";
      return `${mark}<div class="tablewrap"><table class="sortable">
        <thead><tr>${head}</tr></thead>
        <tbody>${body}</tbody></table></div>${footer}`;
    }
    if (row.type === "document") {
      const mark = spec.watermark
        ? `<div class="watermark-band">${esc(spec.watermark)}</div>`
        : "";
      return mark + `<div class="md docview">${
        renderMarkdown(spec.markdown || "", "md")}</div>` + footer;
    }
    return `<pre>${esc(JSON.stringify(spec, null, 1))}</pre>`;
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
      + (prov ? ` · ${prov.status} · ${prov.meridian_line}` : "")
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
    el("chat-panel").hidden = false;
    el("panel-title").textContent = row.title;
    el("panel-body").innerHTML = renderArtifactBody(row);
    exportButtons(row);
    const versions = await api.chatArtifactVersions(artifactId);
    const select = el("panel-version");
    select.innerHTML = (versions.versions || []).map((v) =>
      `<option value="${v.version}"${v.version === row.version
        ? " selected" : ""}>v${v.version}</option>`).join("");
    select.onchange = () =>
      openArtifact(artifactId, Number(select.value));
  }
  el("panel-close").addEventListener("click", () => {
    el("chat-panel").hidden = true;
    state.panelId = "";
  });

  // ── the stream ───────────────────────────────────────────
  function turnFor(turnId) {
    let turn = state.turns.get(turnId);
    if (turn) return turn;
    const div = document.createElement("div");
    div.className = "chat-turn";
    div.innerHTML = `
      <details class="tool-activity" hidden>
        <summary><span class="tri">▸</span>
          <span class="tool-title">working…</span></summary>
        <div class="tool-steps"></div>
      </details>
      <div class="chat-prose md"></div>
      <div class="chat-extras"></div>`;
    thread.appendChild(div);
    turn = { el: div,
             activity: div.querySelector(".tool-activity"),
             toolTitle: div.querySelector(".tool-title"),
             toolSteps: div.querySelector(".tool-steps"),
             prose: div.querySelector(".chat-prose"),
             extras: div.querySelector(".chat-extras"),
             buffer: "", steps: 0, saw: null };
    state.turns.set(turnId, turn);
    scroll();
    return turn;
  }

  function toolRow(turn, text) {
    turn.activity.hidden = false;
    turn.steps += 1;
    const row = document.createElement("div");
    row.className = "theater-step";
    row.innerHTML = `<span class="mark">·</span>
      <span>${prose(text)}</span>`;
    turn.toolSteps.appendChild(row);
    turn.toolTitle.textContent =
      `${turn.steps} look${turn.steps > 1 ? "s" : ""} · ${text}`
      .slice(0, 90);
    scroll();
  }

  function userBubble(text) {
    const div = document.createElement("div");
    div.className = "chat-user";
    div.textContent = text;
    thread.appendChild(div);
    scroll();
  }

  function chipRow(suggestions, clarify) {
    const box = el("chat-chiprow");
    const items = clarify
      ? (clarify.options || []).map((o) => ({
          label: o.label, hint: o.evidence || o.why || "" }))
      : (suggestions || []).map((c) => ({ label: c, hint: "" }));
    if (clarify) {
      say(`<b>${prose(clarify.question)}</b>`);
    }
    box.innerHTML = items.map((c, i) => `
      <button class="chip-choice" data-i="${i}">
        <b>${esc(c.label)}</b>
        ${c.hint ? `<span class="muted">${prose(c.hint)}</span>` : ""}
      </button>`).join("");
    for (const b of box.querySelectorAll(".chip-choice")) {
      b.addEventListener("click", () => {
        const item = items[Number(b.dataset.i)];
        box.innerHTML = "";
        send(item.label);
      });
    }
  }

  function artifactCard(turn, event) {
    const div = document.createElement("div");
    div.className = "card artifact-inline";
    div.innerHTML = `
      <span class="glyph">${{ chart: "📊", table: "▦",
        document: "🗎" }[event.type] || "▣"}</span>
      <b>${esc(event.title)}</b>
      <span class="muted">v${event.version}${
        event.spec?.watermark
          ? ` · ${esc(event.spec.watermark)}` : ""}</span>
      <button class="btn">open</button>`;
    div.querySelector("button").addEventListener("click", () =>
      openArtifact(event.artifact_id));
    turn.extras.appendChild(div);
    openArtifact(event.artifact_id);   // fresh work opens the panel
    scroll();
  }

  function sawButton(turn) {
    if (!turn.saw || turn.sawBtn) return;
    const btn = document.createElement("button");
    btn.className = "btn saw-toggle";
    btn.textContent = "what the model saw";
    btn.addEventListener("click", () => {
      let panel = turn.el.querySelector(".saw-panel");
      if (panel) { panel.hidden = !panel.hidden; return; }
      panel = document.createElement("div");
      panel.className = "saw-panel";
      let html = `<details><summary>system prompt (${
        turn.saw.system.length} chars)</summary><pre>${
        esc(turn.saw.system)}</pre></details>`;
      for (const s of turn.saw.steps) {
        const promptEv = turn.saw.prompts.find((q) => q.n === s.n);
        const res = (turn.saw.results || {})[s.ref];
        html += `<details><summary>look ${s.n} · ${esc(s.tool)}
          <span class="muted">${esc(s.think || "")}</span></summary>
          ${promptEv ? `<div class="saw-label">the model saw</div>
            <pre>${esc(promptEv.content)}</pre>` : ""}
          <div class="saw-label">the tool returned</div>
          <pre>${esc(res ? res.content : s.summary || "")}</pre>
          </details>`;
      }
      panel.innerHTML = html;
      turn.el.appendChild(panel);
    });
    turn.extras.appendChild(btn);
    turn.sawBtn = btn;
  }

  function handle(event) {
    const turn = turnFor(event.turn_id || "loose");
    switch (event.ev) {
      case "turn_started":
        setRunning(true);
        turn.saw = { system: "", prompts: [], steps: [],
                     results: {} };
        break;
      case "model_prompt":
        if (!turn.saw) break;
        if (event.kind === "system") turn.saw.system = event.content;
        else turn.saw.prompts.push(event);
        break;
      case "tool_step":
        if (turn.saw) turn.saw.steps.push(event);
        toolRow(turn, `${event.tool}: ${
          String(event.summary || "").split("\n")[0]}`);
        break;
      case "tool_result":
        if (turn.saw) turn.saw.results[event.ref] = event;
        break;
      case "say_token":
        turn.buffer += event.delta || "";
        turn.prose.innerHTML = renderMarkdown(turn.buffer, "md");
        scroll();
        break;
      case "artifact":
        artifactCard(turn, event);
        refreshRecents();
        break;
      case "chips":
        if (event.clarify) chipRow(null, event.clarify);
        else chipRow(event.suggestions || []);
        break;
      case "budget_tick":
        el("chat-meter").textContent =
          `${event.tokens ?? 0} tokens · ${event.calls ?? 0} calls`;
        break;
      case "turn_done":
        setRunning(false);
        sawButton(turn);
        if (event.status === "partial"
            || event.status === "stopped") {
          turn.el.classList.add("partial");
        }
        refreshRecents();
        break;
      case "error":
        if (event.code !== "trace") {
          say(`<b>${esc(event.code || "error")}</b> ${
            prose(event.message || "")}
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
      "turn_started", "model_prompt", "tool_step", "tool_result",
      "say_token", "artifact", "chips", "budget_tick", "turn_done",
      "error"]) {
      source.addEventListener(name, (message) => {
        let event;
        try { event = JSON.parse(message.data); } catch { return; }
        state.seq = event.seq || state.seq;
        handle(event);
      });
    }
    state.source = source;
  }

  // ── history replay from the store ────────────────────────
  for (const message of boot.messages || []) {
    if (message.role === "user") userBubble(message.text);
    else {
      const div = document.createElement("div");
      div.className = "chat-turn";
      div.innerHTML = `<div class="chat-prose md">${
        renderMarkdown(message.text || "", "md")}</div>`;
      thread.appendChild(div);
      const chips = message.payload?.chips;
      if (chips?.length
          && message === boot.messages[boot.messages.length - 1]) {
        chipRow(chips);
      }
    }
  }
  for (const row of boot.artifacts || []) {
    state.artifacts.set(row.artifact_id, row);
  }
  if (boot.artifacts?.length) {
    openArtifact(boot.artifacts[boot.artifacts.length - 1]
      .artifact_id);
  }
  state.seq = boot.head || 0;
  scroll();

  // ── sending ──────────────────────────────────────────────
  function setRunning(running) {
    state.running = running;
    el("chat-send").disabled = running;
    el("chat-stop").hidden = !running;
  }
  async function send(text) {
    if (!text.trim() || state.running) return;
    el("chat-chiprow").innerHTML = "";
    userBubble(text);
    input.value = "";
    const accepted = await api.chatSend(state.session.id, text);
    if (!accepted.available) {
      say(`<b>not sent.</b> ${esc(accepted.reason || "")}`, "error");
    }
  }
  el("chat-send").addEventListener("click", () => send(input.value));
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      send(input.value);
    }
    if (e.key === "Escape" && state.running) {
      api.chatStop(state.session.id);
    }
  });
  el("chat-stop").addEventListener("click", () =>
    api.chatStop(state.session.id));

  subscribe();
  refreshRecents();
  refreshSkills();
  return () => { if (state.source) state.source.close(); };
}
