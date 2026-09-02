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
    <div class="chatv2">
      <div class="chat-main">
        <div class="chat-masthead">
          <span class="muted" id="chat-build"></span>
          <span class="spacer"></span>
          <span class="muted" id="chat-meter"></span>
          <button class="btn" id="chat-memory-btn"
            aria-expanded="false">⊚ memory</button>
          <div id="chat-memory-pop" class="skills-pop" hidden></div>
        </div>
        <div class="chat-thread" id="chat-thread"></div>
        <div class="chat-chiprow" id="chat-chiprow"></div>
        <div class="chat-composer">
          <textarea id="chat-input" rows="1"
            placeholder="Ask anything about your data…"></textarea>
          <div class="ask-actions">
            <span class="muted">Enter to send · Esc to stop</span>
            <span class="spacer"></span>
            <select id="chat-depth" class="chat-depth"
              title="How deeply Synapse thinks on this ask">
              <option value="quick">Quick</option>
              <option value="standard" selected>Standard</option>
              <option value="deep">Deep</option>
            </select>
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
      <details class="tool-activity" hidden>
        <summary><span class="tri">▸</span>
          <span class="tool-title">working…</span></summary>
        <div class="tool-steps"></div>
      </details>
      <div class="thinking-line" hidden>
        <span class="think-orb">✳</span>
        <span class="think-text">Thinking…</span></div>
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
             tickStart: 0 };
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
    run_sql: (e) => argOf(e, "mode") === "snapshot"
      ? "Running the query" : "Checking the query",
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

  function doneThinking(turn, elapsedMs) {
    turn.done = true;
    stopPulse(turn);
    turn.thinking.hidden = true;
    if (turn.steps) {
      const secs = elapsedMs ? `Worked for ${(elapsedMs / 1000)
        .toFixed(elapsedMs >= 10000 ? 0 : 1)}s` : "Worked";
      const verbs = [...new Set(turn.verbs)];
      turn.toolTitle.textContent = verbs.length
        ? `${secs} · ${verbs.join(", ")}` : secs;
    }
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
      <span class="step-text">${prose(friendly(event))}…</span>`;
    turn.toolSteps.appendChild(row);
    turn.rows.set(event.n, row);
    if (PAST[event.tool]) turn.verbs.push(PAST[event.tool]);
    turn.toolTitle.textContent = friendly(event).slice(0, 90);
    scroll();
  }

  function toolDone(turn, event) {
    if (QUIET.has(event.tool)) return;
    let row = turn.rows.get(event.n);
    if (!row) {
      toolStart(turn, event);
      row = turn.rows.get(event.n);
    }
    row.classList.remove("pending");
    const outcome = String(event.summary || "").split("\n")[0]
      .slice(0, 120);
    const failed = outcome.startsWith("ERROR");
    row.querySelector(".step-text").innerHTML =
      `${prose(friendly(event))}${outcome
        ? ` <span class="muted">— ${prose(failed
            ? outcome.replace(/^ERROR:\s*/, "did not work: ")
            : outcome)}</span>` : ""}`;
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
      : (suggestions || []).slice(0, 3).map((c) => ({ label: c, hint: "" }));
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
      case "turn_started":
        setRunning(true);
        pulse(turn, "Thinking…", event.ts);
        pingShelf();                       // the shelf marks it working
        break;
      case "model_prompt":
        // each model call restarts the clock: after a tool returns
        // the line says Thinking, never the tool's name
        if (event.kind === "call") pulse(turn, "Thinking…", event.ts);
        break;            // the transcript record lives in Operate
      case "thinking": {
        // the model's own summary of what it is thinking, live
        turn.thoughts += event.delta || "";
        const line = lastLine(turn.thoughts);
        if (line) { turn.tickLabel = line; showThinking(turn, line); }
        break;
      }
      case "tool_call":
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
        stopPulse(turn);
        turn.thinking.hidden = true;       // prose is the answer
        turn.buffer += event.delta || "";
        turn.prose.innerHTML = renderMarkdown(turn.buffer, "md");
        scroll();
        break;
      case "artifact":
        state.artifacts.set(event.artifact_id, event);
        artifactCard(turn.extras, event, true);
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
        doneThinking(turn, event.elapsed_ms);
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
      "tool_step", "tool_result", "say_token", "artifact", "chips",
      "budget_tick", "turn_done", "error"]) {
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
      thread.appendChild(div);
      // a card in the transcript reopens the artifact; the panel
      // itself stays shut until the model puts something new in it
      for (const id of message.payload?.artifacts || []) {
        const row = state.artifacts.get(id);
        if (row) artifactCard(div.querySelector(".chat-extras"),
                              row, false);
      }
      const chips = message.payload?.chips;
      if (chips?.length
          && message === boot.messages[boot.messages.length - 1]) {
        chipRow(chips);
      }
    }
  }
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
    userBubble(text);
    input.value = "";
    const accepted = await api.chatSend(state.session.id, text,
                                        el("chat-depth").value);
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
  return () => { if (state.source) state.source.close(); };
}
