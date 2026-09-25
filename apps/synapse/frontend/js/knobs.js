/** The composer's two knobs, the way a chat assistant shows them: a pill
 * that opens a small panel above the composer.
 *
 *   Thinking effort   a slider with one stop per depth; the panel's title
 *                     is the stop's name, and a tap on it shows what the
 *                     stop means
 *   Model             a list of every model on every plane, grouped by
 *                     plane; one is marked, the ones this machine cannot
 *                     ride are greyed with the reason
 *
 * Both knobs keep their value in a hidden <select> the rest of the page
 * already reads (chat-depth, chat-model), so the send path and the
 * switch handler stay exactly as they were: a knob only sets the select
 * and fires its change event. The stops and the models come from the
 * backend's dials catalog; until it answers, the select's own options
 * stand in. */

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

function toggle(pop, button, below) {
  const show = pop.hidden;
  closeAll(pop);
  pop.hidden = !show;
  pop.classList.toggle("below", Boolean(below && below()));
  button.setAttribute("aria-expanded", String(show));
  return show;
}

/** The thinking-effort knob. `depths` rows are {id, label, means}; the
 * slider's stops follow them, in order. */
export function mountDepthKnob({ button, pop, select, depths = [], below = null, onChange = null }) {
  listen();
  register(pop, button);
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
    </div>`;
  const title = pop.querySelector(".knob-title");
  const level = pop.querySelector(".knob-level");
  const means = pop.querySelector(".knob-means");
  const fill = pop.querySelector(".knob-fill");
  const dots = pop.querySelector(".knob-dots");
  const range = pop.querySelector(".knob-range");

  const index = () => Math.max(0, rows.findIndex((r) => r.id === select.value));
  function paint() {
    const i = index();
    const n = Math.max(1, rows.length - 1);
    range.max = String(n);
    range.value = String(i);
    range.setAttribute("aria-valuetext", rows[i]?.label || "");
    fill.style.width = `${(i / n) * 100}%`;
    dots.innerHTML = rows.map((_, k) => `<i class="${k <= i ? "on" : ""}"></i>`).join("");
    level.textContent = rows[i]?.label || "";
    means.textContent = rows[i]?.means || "";
    button.title = `Thinking effort: ${rows[i]?.label || ""}`;
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
  title.addEventListener("click", () => {
    means.hidden = !means.hidden;
    title.setAttribute("aria-expanded", String(!means.hidden));
  });
  button.addEventListener("click", () => { toggle(pop, button, below); paint(); });
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
 * {id, plane, plane_name, label, means, fit, feel, available, reason,
 * default}; the small line under a name is its fit (where the engine
 * belongs in the harness), else the plane's feel. */
export function mountModelPicker({ button, pop, select, models = [], below = null, onPick = null }) {
  listen();
  register(pop, button);
  const label = button.querySelector(".pill-label") || button;
  let rows = models;

  function paintLabel() {
    const chosen = select.selectedOptions[0];
    label.textContent = chosen ? chosen.textContent.replace(/ · not configured$/, "") : "Model";
  }
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
    pop.innerHTML = groups.map((g) => `
      <div class="model-group" role="group" aria-label="${esc(g.name)}">
        <div class="model-group-head">${esc(g.name)}</div>
        ${g.rows.map((m) => `
          <button type="button" class="model-row${m.id === select.value ? " on" : ""}"
            role="option" aria-selected="${m.id === select.value}"
            data-id="${esc(m.id)}" ${m.available ? "" : "disabled"}
            title="${esc(m.available ? m.means : m.reason)}">
            <span class="model-mark" aria-hidden="true">${m.id === select.value ? "●" : "○"}</span>
            <span class="model-text"><b>${esc(m.label)}</b>
              <small>${esc(m.available ? (m.default ? "where a new chat starts" : m.fit || m.feel || "")
                                       : `not configured here`)}</small></span>
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
  button.addEventListener("click", () => { toggle(pop, button, below); paintList(); });
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
