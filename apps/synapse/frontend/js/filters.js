/** The filter icon: one funnel button beside the search, a panel of
 * filter groups (each a list of options with counts, one pick per
 * group), the active picks as chips you can take off one by one. A
 * page declares its groups from the rows it has; the panel never
 * offers a value no row carries. */

import { esc } from "./ui.js";

// groups: [{ key, label, options: [{ value, label, n }] }]
// state: { [key]: value }   onChange(state) after every pick
export function filterBar(host, groups, state, onChange) {
  host.classList.add("filter-bar");
  host.innerHTML = `
    <button class="btn filter-btn" type="button" aria-expanded="false"
      title="filter">
      <span class="funnel" aria-hidden="true">⏷</span> Filter
      <span class="filter-count" hidden></span>
    </button>
    <span class="filter-chips"></span>
    <div class="filter-pop" hidden></div>`;
  const btn = host.querySelector(".filter-btn");
  const count = host.querySelector(".filter-count");
  const chips = host.querySelector(".filter-chips");
  const pop = host.querySelector(".filter-pop");

  const active = () => groups.filter((g) => state[g.key]);
  const labelOf = (g, value) =>
    (g.options.find((o) => o.value === value) || {}).label || value;

  function paint() {
    const on = active();
    count.hidden = on.length === 0;
    count.textContent = String(on.length);
    btn.classList.toggle("on", on.length > 0);
    chips.innerHTML = on.map((g) => `
      <span class="filter-chip" data-key="${esc(g.key)}">
        <span class="muted">${esc(g.label)}</span> ${esc(labelOf(g, state[g.key]))}
        <button type="button" class="chip-x" title="remove">×</button>
      </span>`).join("");
    pop.innerHTML = groups.map((g) => `
      <div class="filter-group">
        <div class="filter-head">${esc(g.label)}</div>
        <button type="button" class="filter-opt${state[g.key] ? "" : " on"}"
          data-key="${esc(g.key)}" data-value="">all</button>
        ${g.options.map((o) => `
          <button type="button" class="filter-opt${
            state[g.key] === o.value ? " on" : ""}"
            data-key="${esc(g.key)}" data-value="${esc(o.value)}">
            <span>${esc(o.label)}</span>
            ${o.n !== undefined ? `<span class="muted">${o.n}</span>` : ""}
          </button>`).join("")}
      </div>`).join("")
      || `<p class="muted">nothing to filter by</p>`;
  }
  btn.addEventListener("click", () => {
    pop.hidden = !pop.hidden;
    btn.setAttribute("aria-expanded", String(!pop.hidden));
  });
  pop.addEventListener("click", (e) => {
    const opt = e.target.closest(".filter-opt");
    if (!opt) return;
    if (opt.dataset.value) state[opt.dataset.key] = opt.dataset.value;
    else delete state[opt.dataset.key];
    paint();
    onChange(state);
  });
  chips.addEventListener("click", (e) => {
    const x = e.target.closest(".chip-x");
    if (!x) return;
    delete state[x.closest(".filter-chip").dataset.key];
    paint();
    onChange(state);
  });
  document.addEventListener("click", (e) => {
    if (!pop.hidden && !host.contains(e.target)) {
      pop.hidden = true;
      btn.setAttribute("aria-expanded", "false");
    }
  });
  paint();
  return { paint, setGroups(next) { groups = next; paint(); } };
}

// the options of a group, counted from rows: value → {label, n}
export function optionsFrom(rows, pick, labelOf = (v) => v) {
  const counts = new Map();
  for (const r of rows) {
    const v = pick(r);
    if (v === undefined || v === null || v === "") continue;
    counts.set(v, (counts.get(v) || 0) + 1);
  }
  return [...counts.entries()]
    .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
    .map(([value, n]) => ({ value, label: labelOf(value, rows), n }));
}
