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
import { renderMarkdown } from "../md.js";
import { esc, prose } from "../ui.js";

const SESSION_KEY = "synapse-chat-session";
const PALETTE = ["#2f6feb", "#e8710a", "#1a9850", "#9970ab",
                 "#d6604d", "#35978f"];

export async function renderChat(outlet, wanted = "") {
  outlet.innerHTML = `
    <div class="chatv2 empty" id="chatv2">
      <div class="chat-main">
        <div class="chat-masthead">
          <button class="chat-title-btn" id="chat-title"
            title="Rename this chat">
            <span class="chat-title-text">New chat</span>
            <span class="chev">⌄</span></button>
          <span class="muted chat-build" id="chat-build"></span>
          <span class="spacer"></span>
          <span class="muted" id="chat-meter"></span>
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
            <div class="chat-actions">
              <button class="icon-btn chat-plus" id="chat-plus"
                title="More" aria-expanded="false">+</button>
              <div class="chat-plus-pop" id="chat-plus-pop" hidden></div>
              <div class="chat-modes" role="radiogroup"
                aria-label="How Synapse works this ask">
                <button class="chat-mode on" data-mode="chat"
                  role="radio" aria-checked="true"
                  title="Synapse writes the query and hands it over; you run it">Chat</button>
                <button class="chat-mode" data-mode="autopilot"
                  role="radio" aria-checked="false"
                  title="Synapse runs the query under the limits and builds the deliverable">Autopilot</button>
              </div>
              <span class="spacer"></span>
              <span class="chat-model" id="chat-model"></span>
              <select id="chat-depth" class="chat-depth"
                title="How deeply Synapse thinks on this ask">
                <option value="quick">Quick</option>
                <option value="standard" selected>Standard</option>
                <option value="deep">Deep</option>
              </select>
              <button class="btn" id="chat-stop" hidden>stop</button>
              <button class="btn primary chat-send" id="chat-send"
                title="Send · Enter">↑</button>
            </div>
          </div>
          <div class="chat-foot">Synapse is AI and can make mistakes.
            Check the receipts before you act on a number.</div>
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
                  panelId: "" };

  const scroll = () => { thread.scrollTop = thread.scrollHeight; };
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
  el("chat-model").textContent = boot.model || "";
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
         ("by spend I mean acquirer net spend"), Synapse keeps it,
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

  function tableHTML(spec) {
    const cols = spec.columns || [];
    const head = cols.map((c) =>
      `<th data-key="${esc(c.key)}">${esc(c.label)}${
        c.status ? ` <span class="muted">(${esc(c.status)})</span>`
                 : ""}</th>`).join("");
    const body = (spec.rows || []).map((r) =>
      `<tr>${cols.map((c) =>
        `<td>${esc(String(r[c.key] ?? ""))}</td>`).join("")}</tr>`)
      .join("");
    return `<div class="tablewrap"><table class="sortable">
      <thead><tr>${head}</tr></thead>
      <tbody>${body}</tbody></table></div>`;
  }

  function kpiTile(spec) {
    const delta = typeof spec.delta === "number"
      ? `<span class="kpi-delta ${spec.delta >= 0 ? "up" : "down"}">${
          spec.delta >= 0 ? "▲" : "▼"} ${
          esc(String(Math.abs(spec.delta)))}</span>` : "";
    return `<div class="kpi-tile">
      ${spec.label ? `<div class="kpi-label">${esc(spec.label)}</div>`
                   : ""}
      <div class="kpi-value">${esc(String(spec.value ?? "—"))}${
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
      ${prov && prov.meridian_line ? `<span class="meridian"
        title="${esc(prov.meridian_line)}">${
        prose(prov.meridian_line)}</span>` : ""}
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
          esc(n.status)}</title></circle>` : ""}
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
        ${prov ? `<span class="meridian">${
          prose(prov.meridian_line)}</span>` : ""}
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
      return mark + tableHTML(spec) + footer;
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
      const panels = (spec.panels || []).map((p) => `
        <div class="dash-panel dash-${esc(p.type)}">
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
    el("chat-panel").hidden = false;
    el("panel-title").textContent = row.title;
    el("panel-body").innerHTML = renderArtifactBody(row);
    if (row.type === "dashboard") {
      bindDashboardFilters(el("panel-body"), row);
    }
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

  // what Synapse is doing, in the user's words: the model's own
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
                     reconcile: "Reconciling against the certified "
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

  // ── the thinking block, the way Claude shows it: the model's own
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
  // on a click, the way Claude shows what a tool was given
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
    const outcome = String(event.summary || "").split("\n")[0]
      .slice(0, 120);
    const failed = outcome.startsWith("ERROR");
    const secs = event.elapsed_ms >= 1000
      ? ` · ${(event.elapsed_ms / 1000).toFixed(1)}s` : "";
    row.querySelector(".step-text").innerHTML =
      `${prose(friendly(event))}${outcome
        ? ` <span class="muted">— ${prose(failed
            ? outcome.replace(/^ERROR:\s*/, "did not work: ")
            : outcome)}${secs}</span>` : ""}`;
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
    div.innerHTML = `
      <span class="glyph">${{ chart: "📊", table: "▦",
        document: "🗎", dashboard: "▥", diagram: "✦",
        kpi: "◉" }[row.type] || "▣"}</span>
      <b>${esc(row.title)}</b>
      <span class="muted">v${row.version}${
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

  function handle(event) {
    const turn = turnFor(event.turn_id || "loose");
    switch (event.ev) {
      case "turn_started": {
        setRunning(true);
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
        pulse(turn, "Thinking…", event.ts);
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
        const line = lastLine(turn.segText);
        if (line) { turn.tickLabel = line; showThinking(turn, line); }
        scroll();
        break;
      }
      case "tool_call":
        if (turn.settled) openBlock(turn);
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
        if (event.clarify) chipRow(null, event.clarify);
        else chipRow(event.suggestions || []);
        break;
      case "budget_tick":
        el("chat-meter").textContent =
          `${event.tokens ?? 0} tokens · ${event.calls ?? 0} calls`;
        break;
      case "turn_done":
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
      "turn_started", "model_prompt", "thinking", "tool_call",
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
        const outcome = String(t.summary || "").split("\n")[0]
          .slice(0, 120);
        el.innerHTML = `<span class="mark">·</span>
          <span class="step-body"><span class="step-text">${
            prose(friendly(t))}${outcome
            ? ` <span class="muted">— ${prose(outcome.replace(
                /^ERROR:\s*/, "did not work: "))}</span>` : ""}</span>
          </span>`;
        attachInput(el, t.input);
      }
      steps.appendChild(el);
    }
    container.prepend(details);
  }

  // ── history replay from the store ────────────────────────
  for (const row of boot.artifacts || []) {
    state.artifacts.set(row.artifact_id, row);
  }
  for (const message of boot.messages || []) {
    if (message.role === "user") userBubble(message.text);
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
    el("chat-send").disabled = running;
    el("chat-stop").hidden = !running;
  }
  async function send(text) {
    if (!text.trim() || state.running) return;
    el("chat-chiprow").innerHTML = "";
    slash.hidden = true;
    setEmpty(false);
    userBubble(text);
    input.value = "";
    const accepted = await api.chatSend(state.session.id, text,
                                        el("chat-depth").value,
                                        state.mode);
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
    else if (e.key === "Escape" && state.running) {
      api.chatStop(state.session.id);
    }
  });
  el("chat-stop").addEventListener("click", () =>
    api.chatStop(state.session.id));

  subscribe();
  pingShelf();
  return () => { if (state.source) state.source.close(); };
}
