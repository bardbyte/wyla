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
  state.skillsLoaded = new Set(boot.session.skills || []);

  // the chat list lives in the shell nav now — one nav, not two;
  // this just tells the shelf something changed
  const pingShelf = () =>
    window.dispatchEvent(new CustomEvent("synapse:sessions"));

  // ── skills (same picker contract as Ask) ─────────────────
  const skillsBtn = el("chat-skills-btn");
  const skillsPop = el("chat-skills-pop");
  const skillChips = el("chat-skill-chips");
  async function refreshSkills() {
    try {
      const got = await api.chatSkills();
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
            ${s.origin ? `<span class="origin-tag o-${esc(s.origin
              .replace(/[^a-z]/g, ""))}">${esc(s.origin)}</span>` : ""}
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
        document: "🗎", dashboard: "▥", diagram: "✦",
        kpi: "◉" }[event.type] || "▣"}</span>
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
        pingShelf();
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
        pingShelf();
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
  pingShelf();
  refreshSkills();
  return () => { if (state.source) state.source.close(); };
}
