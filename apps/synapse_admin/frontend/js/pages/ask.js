/** Ask (E18 Stage B): the chat surface, a pure consumer of the
 * turn's event stream.
 *
 * The UX contract, in code:
 *   - input is never blocked; the response streams from the first
 *     token, and deterministic steps land in the theater strip
 *     immediately, so structure visibly answers while composition
 *     thinks;
 *   - work is visible but tucked away: the strip is collapsed by
 *     default and expands to the step detail;
 *   - interactive elements ARE messages: chips render inline, block
 *     the run until answered, and the choice appears as the user's
 *     turn;
 *   - stop means stop (server-side abort), regenerate re-runs the
 *     last turn;
 *   - errors are cards with next actions, never dead ends.
 *
 * Nothing here calls a model or holds a key. Every value rendered
 * arrives on the stream from the real loop.
 */

import { api } from "../api.js";
import { renderMarkdown } from "../md.js";
import { esc, prose } from "../ui.js";
import { planPanel } from "../planpanel.js";

const SESSION_KEY = "synapse-ask-session";

const STEP_COPY = {
  classify_result: (e) => `read the turn: ${e.kind.replace("_", " ")}`
    + (e.model_used ? "" : " (deterministic)"),
  resolve_started: (e) => `searching the graph for ${
    (e.slots || []).join(", ") || "the plan"}`,
  resolve_result: (e) => (e.bound
    ? `bound ${e.bound.label || e.bound.id} on ${e.bound.table}`
    : (e.slots_resolved || []).length
      ? `re-resolved ${e.slots_resolved.join(", ")}`
      : "nothing needed re-resolving")
    + (e.elapsed_ms !== undefined ? ` · ${e.elapsed_ms}ms` : ""),
  plan_delta: (e) => (e.changes || []).length
    ? `plan v${e.version}: ${e.changes.map(
        (c) => `${c.slot} → ${c.to}`).join(", ")}`
    : `plan v${e.version} started`,
  contract_ready: (e) =>
    `${e.contract.will_verify.length} promises to verify, all unproven`,
  loop_started: (e) =>
    `navigating the graph (up to ${e.steps_max} looks)`,
  loop_step: (e) => `${e.tool}: ${
    String(e.summary || "").split("\n")[0]}`,
  loop_done: (e) => ({
    answered: `navigation done in ${e.steps} looks`,
    ask: `stopped to ask after ${e.steps} looks`,
    partial: `stopped honestly after ${e.steps} looks`,
  }[e.outcome] || `navigation ${e.outcome}`),
};

export async function renderAsk(outlet, wanted = "") {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <span class="muted">Ask · a governed answer with its receipts</span>
      <span class="spacer"></span>
      <span class="muted" id="ask-meter"></span>
      <button class="btn" id="ask-plan-toggle" aria-pressed="true"
        >plan</button>
      <button class="btn" id="ask-new">new session</button>
    </div>
    <div class="ask-cols">
    <div class="ask">
      <div class="ask-thread" id="ask-thread"></div>
      <div class="ask-composer">
        <div class="ask-skillbar">
          <button class="btn" id="ask-skills-btn" aria-expanded="false"
            title="Load a skill into this session's context">⊕ skills</button>
          <span id="ask-skill-chips"></span>
          <div id="ask-skills-pop" class="skills-pop" hidden></div>
        </div>
        <textarea id="ask-input" rows="1"
          placeholder="Ask about your data…"></textarea>
        <div class="ask-actions">
          <span class="muted" id="ask-hint">Enter to send ·
            Shift+Enter for a new line · Esc to stop</span>
          <span class="spacer"></span>
          <button class="btn" id="ask-regen" hidden>regenerate</button>
          <button class="btn" id="ask-stop" hidden>stop</button>
          <button class="btn primary" id="ask-send">Ask</button>
        </div>
      </div>
    </div>
    <aside class="plan-rail" id="ask-plan"></aside>
    </div>`;

  const thread = outlet.querySelector("#ask-thread");
  const input = outlet.querySelector("#ask-input");
  const sendBtn = outlet.querySelector("#ask-send");
  const stopBtn = outlet.querySelector("#ask-stop");
  const regenBtn = outlet.querySelector("#ask-regen");
  const meter = outlet.querySelector("#ask-meter");
  const skillsBtn = outlet.querySelector("#ask-skills-btn");
  const skillChips = outlet.querySelector("#ask-skill-chips");
  const skillsPop = outlet.querySelector("#ask-skills-pop");

  const state = { session: null, source: null, turns: new Map(),
                  lastText: "", running: false, seq: 0,
                  skillsAvailable: [], skillsLoaded: new Set() };

  // ── session ──────────────────────────────────────────────
  const say = (html, cls = "") => {
    const el = document.createElement("div");
    el.className = `ask-note ${cls}`;
    el.innerHTML = html;
    thread.appendChild(el);
    scroll();
    return el;
  };
  const scroll = () => { thread.scrollTop = thread.scrollHeight; };

  let boot = null;
  const stored = wanted || localStorage.getItem(SESSION_KEY);
  if (stored) {
    boot = await api.askSession(stored);
    if (!boot.available) boot = null;
  }
  if (!boot) {
    const created = await api.askNewSession("analyst");
    if (!created.available) {
      say(`<b>Ask is not available.</b> ${esc(created.reason ?? "")}`,
          "error");
      return () => {};
    }
    boot = await api.askSession(created.session.id);
    if (!boot.available) {
      say(`<b>Ask is not available.</b> ${esc(boot.reason ?? "")}`, "error");
      return () => {};
    }
  }
  state.session = boot.session;
  localStorage.setItem(SESSION_KEY, state.session.id);
  state.skillsLoaded = new Set(boot.session.skills || []);

  // ── skills: what the session carries into the loop's context ──
  // Claude-style: an explicit picker; chips show what is loaded; the
  // text itself only ever travels server-side into the system prompt.
  async function refreshSkills() {
    try {
      const got = await api.askSkills();
      state.skillsAvailable = got.available ? (got.skills || []) : [];
    } catch { state.skillsAvailable = []; }
    paintSkills();
  }
  function paintSkills() {
    const loaded = [...state.skillsLoaded];
    skillChips.innerHTML = loaded.map((name) =>
      `<button class="skill-chip" data-skill="${esc(name)}"
        title="click to unload">${esc(name)} ×</button>`).join(" ");
    skillsBtn.textContent = loaded.length
      ? `⊕ skills (${loaded.length})` : "⊕ skills";
    for (const chip of skillChips.querySelectorAll(".skill-chip")) {
      chip.addEventListener("click", () =>
        toggleSkill(chip.dataset.skill));
    }
  }
  function paintSkillsPop() {
    if (!state.skillsAvailable.length) {
      skillsPop.innerHTML = `<div class="muted" style="padding:6px">
        No skills yet. Drop a markdown file in
        <code>graph/skills/</code> — your own briefing on how you
        read the data — and it appears here.</div>`;
      return;
    }
    skillsPop.innerHTML = state.skillsAvailable.map((skill) => {
      const on = state.skillsLoaded.has(skill.name);
      return `<label class="skills-row">
        <input type="checkbox" data-skill="${esc(skill.name)}"
          ${on ? "checked" : ""}>
        <span><b>${esc(skill.title || skill.name)}</b>
          <span class="muted">${esc(skill.description || "")}</span>
        </span></label>`;
    }).join("") + `<div class="muted" style="padding:4px 6px">
      Loaded skills steer the agent's navigation; they apply from
      the next question.</div>`;
    for (const box of skillsPop.querySelectorAll("input")) {
      box.addEventListener("change", () =>
        toggleSkill(box.dataset.skill));
    }
  }
  async function toggleSkill(name) {
    const next = new Set(state.skillsLoaded);
    if (next.has(name)) next.delete(name); else next.add(name);
    const saved = await api.askSetSkills(state.session.id, [...next]);
    if (saved.available && saved.ok) {
      state.skillsLoaded = next;
    } else {
      say(`<b>skills:</b> ${esc(saved.reason || "could not save")}`,
          "error");
    }
    paintSkills();
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
  refreshSkills();
  window.dispatchEvent(new CustomEvent("synapse:sessions"));
  // History comes from the STORE (durable, survives a restart); the
  // stream then starts at the bus head, so nothing renders twice.
  state.seq = boot.head || 0;
  const panel = planPanel(outlet.querySelector("#ask-plan"), state);
  panel.load(boot.plan_versions || []);
  replay(boot);

  // ── the transcript, rebuilt from the record ──────────────
  function replay(payload) {
    const messages = payload.messages || [];
    messages.forEach((message, i) => {
      const turnId = message.turn_id || "replay";
      if (message.role === "user" && message.text) {
        userBubble(message.text);
      } else if (message.payload?.clarify) {
        // a question is still live only if nothing came after it
        chips(turnFor(turnId), message.payload.clarify,
              { answered: i < messages.length - 1 });
      } else if (message.payload?.schema === "a2ui.answer/1") {
        const turn = turnFor(turnId);
        turn.buffer = message.text || message.payload.prose || "";
        paint(turn);
        turn.title.textContent = "answered earlier in this session";
        answerCard(turn, message.payload);
      }
    });
    if (payload.budget) budget(payload.budget);
    if (payload.running) {
      say("a turn was already running here: reconnected to it, so the "
          + "written answer picks up from this point", "warn");
    }
  }

  function userBubble(text) {
    const el = document.createElement("div");
    el.className = "ask-user";
    el.textContent = text;
    thread.appendChild(el);
    scroll();
  }

  function turnFor(turnId) {
    let turn = state.turns.get(turnId);
    if (turn) return turn;
    const el = document.createElement("div");
    el.className = "ask-turn";
    el.innerHTML = `
      <details class="theater">
        <summary><span class="tri">▸</span>
          <span class="theater-title">working…</span></summary>
        <div class="theater-steps"></div>
      </details>
      <div class="ask-prose md"></div>
      <div class="ask-extras"></div>`;
    thread.appendChild(el);
    turn = {
      el, steps: el.querySelector(".theater-steps"),
      title: el.querySelector(".theater-title"),
      prose: el.querySelector(".ask-prose"),
      extras: el.querySelector(".ask-extras"),
      buffer: "", stepCount: 0, painting: false,
    };
    state.turns.set(turnId, turn);
    scroll();
    return turn;
  }

  function sawButton(turn) {
    if (turn.sawBtn || !turn.saw) return;
    const btn = document.createElement("button");
    btn.className = "btn saw-toggle";
    btn.textContent = "what the model saw";
    btn.addEventListener("click", () => {
      let panel = turn.el.querySelector(".saw-panel");
      if (panel) { panel.hidden = !panel.hidden; return; }
      panel = buildSaw(turn);
      turn.el.appendChild(panel);
      scroll();
    });
    turn.extras.appendChild(btn);
    turn.sawBtn = btn;
  }

  function buildSaw(turn) {
    // rebuilt purely from the event stream: the panel can never show
    // anything the events file does not hold, so replay == live
    const saw = turn.saw;
    const panel = document.createElement("div");
    panel.className = "saw-panel";
    const meta = saw.meta || {};
    const skills = (meta.skills || []).join(", ") || "none";
    let html = `<div class="muted saw-head">prompt ${
      esc(meta.prompt_version || "?")} · skills loaded: ${
      esc(skills)} · ${saw.steps.length} looks</div>`;
    html += `<details><summary>system prompt (${
      saw.system.length} chars)</summary><pre>${
      esc(saw.system)}</pre></details>`;
    for (const stepEv of saw.steps) {
      const promptEv = saw.prompts.find((q) => q.n === stepEv.n);
      const artifact = (turn.artifacts || {})[stepEv.ref];
      html += `<details><summary>look ${stepEv.n} · ${
        esc(stepEv.tool)} <span class="muted">${
        esc(stepEv.think || "")}</span></summary>${
        promptEv ? `<div class="saw-label">the model saw</div><pre>${
          esc(promptEv.content)}</pre>` : ""}
        <div class="saw-label">it called</div><pre>${
          esc(stepEv.tool)}(${esc(stepEv.args || "")})</pre>
        <div class="saw-label">the tool returned</div><pre>${
          esc(artifact ? artifact.content
                       : stepEv.summary || "")}</pre></details>`;
    }
    panel.innerHTML = html;
    return panel;
  }

  function step(turn, text, mark = "·") {
    const row = document.createElement("div");
    row.className = "theater-step";
    row.innerHTML = `<span class="mark">${esc(mark)}</span>
      <span>${prose(text)}</span>`;
    turn.steps.appendChild(row);
    turn.stepCount += 1;
    turn.title.textContent = text;      // the collapsed line is the
    scroll();                           // latest thing that happened
  }

  function paint(turn) {
    if (turn.painting) return;
    turn.painting = true;
    requestAnimationFrame(() => {
      turn.painting = false;
      turn.prose.innerHTML = renderMarkdown(turn.buffer, "md");
      scroll();
    });
  }

  function chips(turn, clarify, { answered = false } = {}) {
    const box = document.createElement("div");
    box.className = "card chips-card";
    box.innerHTML = `
      <div class="chips-question">${prose(clarify.question)}</div>
      <div class="chips-row">
        ${(clarify.options || []).map((option, i) => `
          <button class="chip-choice" data-i="${i}"${
            answered ? " disabled" : ""}>
            <b>${esc(option.label)}</b>
            ${option.why ? `<span class="muted">${prose(option.why)}</span>`
              : ""}
            ${option.evidence
              ? `<span class="muted mono">${prose(option.evidence)}</span>`
              : ""}
          </button>`).join("")}
      </div>
      <div class="muted">${answered ? "answered"
        : "remembered for this session: I will not ask twice"}</div>`;
    box.addEventListener("click", (e) => {
      const button = e.target.closest?.(".chip-choice");
      if (!button || button.disabled || state.running) return;
      const option = clarify.options[Number(button.dataset.i)];
      box.querySelectorAll(".chip-choice").forEach((b) => {
        b.disabled = true;
        b.classList.toggle("on", b === button);
      });
      // the choice becomes the user's turn, exactly like typing it
      userBubble(option.label);
      send(option.label, { slot: clarify.slot, value: option.value,
                           label: option.label });
    });
    turn.extras.appendChild(box);
    scroll();
  }

  function resultTable(payload) {
    const rows = payload.rows || [];
    if (!rows.length) return "";
    const cols = (payload.result_schema || []).map(
      (c) => (typeof c === "string" ? c : c.name)).filter(Boolean);
    const keys = cols.length ? cols : Object.keys(rows[0] || {});
    const shown = rows.slice(0, 12);
    return `
      <div class="result-wrap">
        <table class="result">
          <thead><tr>${keys.map(
            (k) => `<th>${esc(k)}</th>`).join("")}</tr></thead>
          <tbody>${shown.map((r) => `<tr>${keys.map(
            (k) => `<td>${esc(Array.isArray(r) ? r[keys.indexOf(k)]
              : r[k])}</td>`).join("")}</tr>`).join("")}</tbody>
        </table>
      </div>
      ${rows.length > shown.length ? `<div class="muted">${
        rows.length - shown.length} further rows in the result, not
        shown here</div>` : ""}`;
  }

  const PREVIEW_MARK = { safe: "✓", unproven: "?", unsafe: "✗",
                         unwitnessed: "✗" };

  function previewCard(turn, preview) {
    const card = document.createElement("div");
    card.className = `card preview-card v-${esc(preview.verdict)}`;
    card.innerHTML = `
      <div class="card-label">before running: join and grain</div>
      <div class="preview-head">
        <span class="preview-mark">${
          PREVIEW_MARK[preview.verdict] || "?"}</span>
        <b>${prose(preview.headline)}</b>
      </div>
      <div class="preview-grain muted">one row means
        <b>${esc(preview.grain)}</b>${preview.grain_source
          ? ` · ${prose(preview.grain_source)}` : ""}</div>
      ${preview.joins.map((j) => `
        <div class="preview-join">
          <div class="preview-join-head">
            <span class="preview-mark">${PREVIEW_MARK[j.verdict] || "?"}</span>
            <b>${esc(preview.base.short)} → ${esc(j.short)}</b>
            ${j.source ? `<span class="chip">${esc(j.source)}${
              j.support ? ` · ${esc(j.support)}` : ""}</span>` : ""}
            ${j.rows !== null && j.rows !== undefined
              ? `<span class="muted">${esc(
                  Number(j.rows).toLocaleString())} rows</span>` : ""}
          </div>
          ${j.on ? `<div class="mono preview-on">${esc(j.on)}</div>` : ""}
          <div class="muted">${prose(j.why)}</div>
          ${j.alternates ? `<div class="muted">${esc(j.alternates)} other
            witness${j.alternates > 1 ? "es" : ""} for this pair: open the
            table profile to compare them</div>` : ""}
        </div>`).join("")}
      ${preview.verdict !== "safe" ? `
        <div class="legend">
          <a class="linklike" href="#/table/${esc(preview.base.table)}"
            >inspect the join</a>
          <button class="linklike" data-steward>ask a steward</button>
        </div>` : ""}`;
    card.addEventListener("click", async (e) => {
      if (!e.target.closest?.("[data-steward]")) return;
      await api.askFeedback(state.session.id,
        { vote: "down", subject: "join_preview",
          note: `${preview.verdict}: ${preview.headline}` });
      e.target.replaceWith(Object.assign(document.createElement("span"),
        { className: "muted",
          textContent: "flagged: a steward will see this join" }));
    });
    turn.extras.appendChild(card);
    scroll();
  }

  function verdictChip(verdict) {
    const passed = verdict.will_verify.filter((c) => c.passed).length;
    const total = verdict.will_verify.length;
    const cls = verdict.verdict === "pass" ? "tier-gr" : "tier-in";
    return `<span class="chip ${cls}">verified ${passed}/${total}</span>`;
  }

  function answerCard(turn, payload) {
    const card = document.createElement("div");
    card.className = "card answer-card";
    card.innerHTML = `
      <div class="answer-head">
        <span class="chip acc">${esc(payload.metric.label
          || payload.metric.id)}</span>
        ${verdictChip(payload.verdict)}
        <span class="muted">one row = ${esc(payload.grain)}</span>
        <span class="spacer"></span>
        <span class="muted mono">${esc(payload.build_id)}</span>
      </div>
      <div class="meridian">${prose(payload.meridian_line)}</div>
      ${resultTable(payload)}
      <details class="answer-detail">
        <summary><span class="tri">▸</span>How was this calculated?</summary>
        <div class="answer-detail-body">
          <div class="verdict-list">
            ${payload.verdict.will_verify.map((c) => `
              <div class="verdict-row ${c.passed ? "ok" : "no"}">
                <span>${c.passed ? "✓" : "✗"}</span>
                <span>${prose(c.text)}</span>
                <span class="muted">${prose(c.evidence)}</span>
              </div>`).join("")}
          </div>
          <pre class="block sqlblock">${esc(payload.sql)}</pre>
          <button class="btn" data-copy>copy the SQL</button>
        </div>
      </details>
      ${payload.limits.length ? `
        <div class="limits">${payload.limits.map(
          (l) => `<div class="muted">· ${prose(l)}</div>`).join("")}</div>`
        : ""}
      <div class="legend">
        ${(payload.actions || []).filter((a) => a.enabled).map(
          (a) => `<a class="linklike" href="${esc(a.href)}">${
            esc(a.label)}</a>`).join("")}
        <span class="spacer"></span>
        <button class="linklike" data-vote="up"
          title="This reads right">👍</button>
        <button class="linklike" data-vote="down"
          title="Something is off">👎</button>
      </div>`;
    card.addEventListener("click", async (e) => {
      if (e.target.closest?.("[data-copy]")) {
        try { await navigator.clipboard.writeText(payload.sql); } catch {}
        e.target.closest("[data-copy]").textContent = "copied ✓";
      }
      const vote = e.target.closest?.("[data-vote]")?.dataset.vote;
      if (vote) {
        await api.askFeedback(state.session.id,
                              { vote, subject: "answer" });
        e.target.closest(".legend").insertAdjacentHTML(
          "beforeend", `<span class="muted">noted</span>`);
      }
    });
    turn.extras.appendChild(card);
    scroll();
  }

  function errorCard(turn, event) {
    const card = document.createElement("div");
    card.className = "card error-card";
    card.innerHTML = `
      <div class="card-label">${esc(event.code)}</div>
      <p>${prose(event.message)}</p>
      ${(event.next_actions || []).length ? `
        <div class="legend">${event.next_actions.map(
          (a) => `<span class="chip">${prose(a)}</span>`).join("")}</div>`
        : ""}`;
    turn.extras.appendChild(card);
    scroll();
  }

  function budget(tick) {
    const cost = tick.cost_usd === null || tick.cost_usd === undefined
      ? `${tick.tokens} tokens`
      : `$${tick.cost_usd} · ${tick.tokens} tokens`;
    meter.textContent = cost;
    meter.title = tick.cost_note || "";
  }

  // ── the stream: the single source of UI truth ────────────
  function connect() {
    if (state.source) state.source.close();
    const source = new EventSource(
      api.askStreamUrl(state.session.id, state.seq));
    state.source = source;
    source.onmessage = () => {};        // every event is named
    for (const name of [
      "turn_started", "classify_result", "plan_delta", "resolve_started",
      "resolve_result", "loop_started", "loop_prompt", "loop_step",
      "loop_artifact", "loop_done", "clarify_request", "contract_ready",
      "generate_token", "verify_progress", "verify_verdict",
      "answer_payload", "budget_tick", "budget_grace", "turn_done",
      "error"]) {
      source.addEventListener(name, (message) => {
        let event;
        try { event = JSON.parse(message.data); } catch { return; }
        state.seq = Math.max(state.seq, event.seq || 0);
        handle(event);
      });
    }
  }

  function handle(event) {
    const turn = turnFor(event.turn_id || "loose");
    switch (event.ev) {
      case "turn_started":
        setRunning(true);
        step(turn, "started");
        break;
      case "classify_result":
        if (event.chat_turn) {
          // E22: a conversation is not work. No theater strip, no
          // plan rail change: just the reply, like a colleague.
          turn.el.classList.add("chat-only");
          break;
        }
        step(turn, STEP_COPY[event.ev](event));
        break;
      case "resolve_started":
      case "resolve_result":
        step(turn, STEP_COPY[event.ev](event));
        break;
      case "loop_started":
        turn.saw = { meta: event, system: "", prompts: [], steps: [] };
        step(turn, STEP_COPY[event.ev](event));
        break;
      case "loop_prompt":
        // the panel's ground truth: exactly what the model saw
        if (!turn.saw) {
          turn.saw = { meta: {}, system: "", prompts: [], steps: [] };
        }
        if (event.kind === "system") turn.saw.system = event.content;
        else turn.saw.prompts.push(event);
        break;
      case "loop_step":
        if (turn.saw) turn.saw.steps.push(event);
        step(turn, STEP_COPY[event.ev](event));
        break;
      case "loop_done":
        step(turn, STEP_COPY[event.ev](event));
        sawButton(turn);
        break;
      case "loop_artifact":
        // the full tool result, held for the panel
        (turn.artifacts = turn.artifacts || {})[event.ref] = event;
        break;
      case "plan_delta":
        step(turn, STEP_COPY[event.ev](event));
        panel.delta(event);              // mark what this turn moved
        break;
      case "contract_ready":
        step(turn, STEP_COPY[event.ev](event));
        panel.plan(event.plan, event.plan?.version);
        if (event.preview) {
          if ((event.preview.joins || []).length) {
            previewCard(turn, event.preview);   // a join to judge
          } else {
            step(turn, event.preview.headline, "✓");
          }
        }
        break;
      case "clarify_request":
        chips(turn, event);
        break;
      case "generate_token":
        turn.buffer += event.delta || "";
        paint(turn);
        break;
      case "verify_progress":
        step(turn, `${event.criterion.text}: ${event.criterion.evidence}`,
             event.criterion.passed ? "✓" : "✗");
        break;
      case "verify_verdict":
        step(turn, `verdict: ${event.verdict}`,
             event.verdict === "pass" ? "✓" : "✗");
        break;
      case "answer_payload":
        answerCard(turn, event.payload);
        break;
      case "error":
        if (event.code !== "trace") errorCard(turn, event);
        break;
      case "budget_tick":
      case "budget_grace":
        budget(event);
        if (event.ev === "budget_grace") say(esc(event.message), "warn");
        break;
      case "turn_done":
        setRunning(false);
        turn.title.textContent =
          `${event.status} · ${turn.stepCount} steps · ${
            event.elapsed_ms}ms`;
        budget(event);
        // the session may have just earned its title
        window.dispatchEvent(new CustomEvent("synapse:sessions"));
        break;
      default:
        break;
    }
  }

  // ── sending ──────────────────────────────────────────────
  function setRunning(running) {
    state.running = running;
    sendBtn.disabled = running;
    stopBtn.hidden = !running;
    regenBtn.hidden = running || !state.lastText;
    // the input is NEVER blocked: you can type your next question
    // while this one is still composing
    input.placeholder = running
      ? "working… (Esc stops it)" : "Ask about your data…";
  }

  async function send(text, choice = null) {
    if (!text.trim()) return;
    state.lastText = text;
    setRunning(true);
    const accepted = await api.askSend(state.session.id, text, choice);
    if (!accepted.available) {
      const turn = turnFor("loose");
      errorCard(turn, {code: accepted.busy ? "busy" : "unavailable",
                       message: accepted.reason || "refused",
                       next_actions: accepted.busy ? ["stop the running turn"]
                         : ["compile a build", "check the .env"]});
      setRunning(false);
    }
  }

  const grow = () => {
    input.style.height = "auto";
    input.style.height = `${Math.min(input.scrollHeight, 200)}px`;
  };

  sendBtn.addEventListener("click", () => {
    const text = input.value.trim();
    if (!text) return;
    input.value = "";
    grow();
    userBubble(text);
    send(text);
  });
  input.addEventListener("input", grow);
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendBtn.click();
    }
  });
  stopBtn.addEventListener("click", async () => {
    await api.askStop(state.session.id);
  });
  regenBtn.addEventListener("click", () => {
    if (!state.lastText) return;
    userBubble(state.lastText);
    send(state.lastText);
  });
  const onKey = (e) => {
    if (e.key === "Escape" && state.running) api.askStop(state.session.id);
  };
  window.addEventListener("keydown", onKey);
  const railBtn = outlet.querySelector("#ask-plan-toggle");
  const cols = outlet.querySelector(".ask-cols");
  const applyRail = (on) => {
    cols.classList.toggle("no-rail", !on);
    railBtn.setAttribute("aria-pressed", String(on));
    railBtn.classList.toggle("primary", on);
  };
  let railOn = true;
  try { railOn = localStorage.getItem("synapse-ask-rail") !== "off"; }
  catch { /* private mode: the rail just stays on */ }
  applyRail(railOn);
  railBtn.addEventListener("click", () => {
    railOn = !railOn;
    try { localStorage.setItem("synapse-ask-rail", railOn ? "on" : "off"); }
    catch { /* fine */ }
    applyRail(railOn);
  });

  outlet.querySelector("#ask-new").addEventListener("click", () => {
    localStorage.removeItem(SESSION_KEY);
    if (location.hash.startsWith("#/ask/")) {
      location.hash = "#/ask";     // the router tears this page down
      return;                      // and renders the new one: once
    }
    if (state.source) state.source.close();
    window.removeEventListener("keydown", onKey);
    renderAsk(outlet);
  });

  connect();
  setRunning(Boolean(boot.running));
  input.focus();

  return () => {
    if (state.source) state.source.close();
    window.removeEventListener("keydown", onKey);
  };
}
