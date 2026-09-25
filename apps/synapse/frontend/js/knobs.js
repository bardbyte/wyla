/** The composer's two knobs, the way a chat assistant shows them: a pill
 * that opens a small panel above the composer.
 *
 *   Thinking effort   a slider with one stop per depth; the pill names
 *                     the stop that is on ("Thinking · Deep"), the
 *                     panel's title is the stop's name, a tap on it
 *                     shows what the stop means, and the stops under
 *                     the rail are marked — the one that is on is
 *                     checked
 *   Model             a list of every model on every plane, grouped by
 *                     plane, each with one plain line on when to pick
 *                     it; one is marked, the ones this machine cannot
 *                     ride are greyed with the reason; the pill names
 *                     the model and its plane ("Gemini 3.7 Flash ·
 *                     Gateway")
 *
 * Both knobs keep their value in a hidden <select> the rest of the page
 * already reads (chat-depth, chat-model), so the send path and the
 * switch handler stay exactly as they were: a knob only sets the select
 * and fires its change event. The stops and the models come from the
 * backend's dials catalog; until it answers, the select's own options
 * stand in. A panel never leaves the window: it opens upward when there
 * is no room below, downward when there is none above, and scrolls
 * inside when neither side has the height. */

const esc = (s) =>
  String(s ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));

/* one open panel at a time; a click outside or Escape closes it */
const open = new Set();
function register(pop, button) {
  open.add({ pop, button });
}
function closeAll(except = null) {
  for (const k of open) {
    if (k.pop === except) continue;
    k.pop.hidden = true;
    k.button.setAttribute("aria-expanded", "false");
  }
}
let listening = false;
function listen() {
  if (listening) return;
  listening = true;
  document.addEventListener("click", (e) => {
    for (const k of open) {
      if (k.pop.hidden) continue;
      if (k.pop.contains(e.target) || k.button.contains(e.target)) continue;
      k.pop.hidden = true;
      k.button.setAttribute("aria-expanded", "false");
    }
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeAll();
  });
}

/* the panel's side: the page says which way it prefers (below the
   composer while the chat is empty and the box sits mid-screen, above
   it once docked); the window has the last word, and the height is
   capped to the room on that side so nothing is ever cut off */
const EDGE = 12;
function place(pop, below) {
  const wantBelow = Boolean(below && below());
  pop.classList.toggle("below", wantBelow);
  pop.style.maxHeight = "";
  let box = pop.getBoundingClientRect();
  if (wantBelow && box.bottom > window.innerHeight - EDGE
      && box.top - EDGE >= box.height) {
    pop.classList.remove("below");           // no room below: open upward
  } else if (!wantBelow && box.top < EDGE
             && window.innerHeight - box.bottom - EDGE >= box.height) {
    pop.classList.add("below");              // no room above: open downward
  }
  box = pop.getBoundingClientRect();
  // the room on the side it opens to: from the anchor edge to the window's
  const room = pop.classList.contains("below")
    ? window.innerHeight - box.top - EDGE
    : box.bottom - EDGE;
  pop.style.maxHeight = `${Math.max(160, Math.floor(room))}px`;
}

function toggle(pop, button, below) {
  const show = pop.hidden;
  closeAll(pop);
  pop.hidden = !show;
  button.setAttribute("aria-expanded", String(show));
  if (show) place(pop, below);
  return show;
}

/** The thinking-effort knob. `depths` rows are {id, label, means}; the
 * slider's stops follow them, in order. */
export function mountDepthKnob({ button, pop, select, depths = [], below = null, onChange = null }) {
  listen();
  register(pop, button);
  const label = button.querySelector(".pill-label") || button;
  let rows = depths.length ? depths : [...select.options].map((o) => ({
    id: o.value, label: o.textContent, means: o.title || "",
  }));
  pop.innerHTML = `
    <button class="knob-title" type="button" aria-expanded="false"
      title="What this stop means">
      <b class="knob-level"></b><span class="chev">›</span>
    </button>
    <p class="knob-means" hidden></p>
    <div class="knob-track">
      <div class="knob-rail"><div class="knob-fill"></div><div class="knob-dots"></div></div>
      <input type="range" class="knob-range" min="0" step="1" value="0"
        aria-label="Thinking effort" />
    </div>
    <div class="knob-stops" role="radiogroup" aria-label="Thinking effort stops"></div>`;
  const title = pop.querySelector(".knob-title");
  const level = pop.querySelector(".knob-level");
  const means = pop.querySelector(".knob-means");
  const fill = pop.querySelector(".knob-fill");
  const dots = pop.querySelector(".knob-dots");
  const range = pop.querySelector(".knob-range");
  const stops = pop.querySelector(".knob-stops");

  const index = () => Math.max(0, rows.findIndex((r) => r.id === select.value));
  function paint() {
    const i = index();
    const n = Math.max(1, rows.length - 1);
    range.max = String(n);
    range.value = String(i);
    range.setAttribute("aria-valuetext", rows[i]?.label || "");
    fill.style.width = `${(i / n) * 100}%`;
    dots.innerHTML = rows.map((_, k) => `<i class="${k <= i ? "on" : ""}"></i>`).join("");
    // the stops by name under the rail; the one that is on is checked
    stops.innerHTML = rows.map((r, k) => `
      <button type="button" class="knob-stop${k === i ? " on" : ""}" role="radio"
        aria-checked="${k === i}" data-index="${k}" title="${esc(r.means)}">${
        esc(r.label)}</button>`).join("");
    level.textContent = rows[i]?.label || "";
    means.textContent = rows[i]?.means || "";
    // the pill says which stop is on, from the session's remembered
    // depth at load and the moment a stop is chosen
    label.textContent = `Thinking · ${rows[i]?.label || "Standard"}`;
    button.title = `Thinking effort: ${rows[i]?.label || ""} — ${rows[i]?.means || ""}`;
  }
  function pick(i) {
    const row = rows[Math.min(Math.max(0, i), rows.length - 1)];
    if (!row || select.value === row.id) { paint(); return; }
    select.value = row.id;
    select.dispatchEvent(new Event("change", { bubbles: true }));
    paint();
    if (onChange) onChange(row);
  }
  range.addEventListener("input", () => pick(Number(range.value)));
  stops.addEventListener("click", (e) => {
    const stop = e.target.closest(".knob-stop");
    if (stop) pick(Number(stop.dataset.index));
  });
  title.addEventListener("click", () => {
    means.hidden = !means.hidden;
    title.setAttribute("aria-expanded", String(!means.hidden));
    if (!pop.hidden) place(pop, below);
  });
  button.addEventListener("click", () => { paint(); toggle(pop, button, below); });
  select.addEventListener("change", paint);   // a change from elsewhere shows too
  paint();
  return {
    setDepths(next) {
      if (Array.isArray(next) && next.length) {
        rows = next.map((d) => ({ id: d.id, label: d.label, means: d.means || "" }));
        // the hidden select mirrors the catalog, so the send path reads the same ids
        const keep = select.value;
        select.innerHTML = rows.map((r) => `<option value="${esc(r.id)}"${
          r.id === keep ? " selected" : ""} title="${esc(r.means)}">${esc(r.label)}</option>`).join("");
        if (!rows.some((r) => r.id === keep)) select.value = rows[Math.floor(rows.length / 2)].id;
      }
      paint();
    },
    refresh: paint,
    close: () => closeAll(),
  };
}

/** The model knob. `models` rows are the dials catalog's:
 * {id, plane, plane_name, label, means, fit, facts, feel, available,
 * reason, default}; the small line under a name is its fit — one plain
 * sentence on when to pick it — and the engineer's facts (how it
 * thinks, its ceiling) ride the hover title with the plane's meaning. */
export function mountModelPicker({ button, pop, select, models = [], below = null, onPick = null }) {
  listen();
  register(pop, button);
  const label = button.querySelector(".pill-label") || button;
  let rows = models;

  function paintLabel() {
    const chosen = select.selectedOptions[0];
    const row = rows.find((m) => m.id === select.value);
    if (row) {
      label.textContent = `${row.label} · ${row.plane_name || row.plane}`;
    } else {
      label.textContent = chosen ? chosen.textContent.replace(/ · not configured$/, "") : "Model";
    }
    button.title = row ? `Model: ${row.label} on ${row.plane_name || row.plane}`
      : "Which model answers this chat";
  }
  const fitLine = (m) => {
    const pick = m.fit || m.feel || "";
    return m.default ? `${pick ? pick + " " : ""}Where a new chat starts.` : pick;
  };
  const facts = (m) => [m.means, m.facts].filter(Boolean).join(" ");
  function paintList() {
    if (!rows.length) {
      pop.innerHTML = `<p class="knob-note">Loading the models…</p>`;
      return;
    }
    const groups = [];
    for (const m of rows) {
      let g = groups.find((x) => x.plane === m.plane);
      if (!g) { g = { plane: m.plane, name: m.plane_name || m.plane, rows: [] }; groups.push(g); }
      g.rows.push(m);
    }
    pop.innerHTML = `<p class="knob-hint">Not sure? Keep the default.</p>` + groups.map((g) => `
      <div class="model-group" role="group" aria-label="${esc(g.name)}">
        <div class="model-group-head">${esc(g.name)}</div>
        ${g.rows.map((m) => `
          <button type="button" class="model-row${m.id === select.value ? " on" : ""}"
            role="option" aria-selected="${m.id === select.value}"
            data-id="${esc(m.id)}" ${m.available ? "" : "disabled"}
            title="${esc(m.available ? facts(m) : m.reason)}">
            <span class="model-mark" aria-hidden="true">${m.id === select.value ? "●" : "○"}</span>
            <span class="model-text"><b>${esc(m.label)}</b>
              <small>${esc(m.available ? fitLine(m) : `not configured here`)}</small></span>
          </button>`).join("")}
      </div>`).join("");
  }
  pop.addEventListener("click", (e) => {
    const row = e.target.closest(".model-row");
    if (!row || row.disabled) return;
    if (select.value !== row.dataset.id) {
      select.value = row.dataset.id;
      select.dispatchEvent(new Event("change", { bubbles: true }));
      if (onPick) onPick(row.dataset.id);
    }
    paintList();
    paintLabel();
    closeAll();
  });
  button.addEventListener("click", () => { paintList(); toggle(pop, button, below); });
  paintLabel();
  return {
    setModels(next) {
      if (Array.isArray(next)) rows = next;
      // the hidden select mirrors the catalog: the switch handler keeps reading it
      const keep = select.value;
      select.innerHTML = rows.map((m) => `<option value="${esc(m.id)}"${
        m.id === keep ? " selected" : ""}${m.available ? "" : " disabled"}>${esc(m.label)}${
        m.available ? "" : " · not configured"}</option>`).join("");
      if (keep && !rows.some((m) => m.id === keep)) select.value = keep;   // unknown: leave as the page had it
      paintList();
      paintLabel();
    },
    refresh() { paintList(); paintLabel(); },
    close: () => closeAll(),
  };
}
