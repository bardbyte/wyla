/** The plan panel (E18 Stage C): the versioned semantic plan, beside
 * the conversation rather than inside it.
 *
 * The plan IS the session's state — the transcript is not — so this
 * panel is the one place that answers "what will actually run". Three
 * things it must never do: invent a slot the plan does not carry,
 * show a value without saying where it came from, or let a version
 * disappear. Restore appends a new version; the chain is append-only,
 * so undo is scrubbing and never archaeology.
 */

import { api } from "./api.js";
import { esc, prose } from "./ui.js";

const SLOT_LABEL = {
  metric: "metric", table: "source", grain: "grain",
  time_window: "window", lob: "line of business",
  dimensions: "dimensions",
};

// how a slot got its value: shown always, because a plan you cannot
// attribute is a plan you cannot defend
const FROM = {
  user: "you chose this", resolver: "resolved from the graph",
  inherited: "carried from the last turn", default: "the default",
};

const rowsOf = (plan) => {
  const out = [];
  const push = (slot, value, extra = "") => {
    if (value === undefined || value === null || value === "") return;
    out.push({ slot, label: SLOT_LABEL[slot] || slot, value, extra });
  };
  push("metric", plan.metric_label || plan.metric_id);
  push("table", plan.table);
  push("grain", plan.grain);
  push("time_window", plan.time_window);
  push("lob", plan.lob);
  if ((plan.dimensions || []).length)
    push("dimensions", plan.dimensions.join(" × "));
  for (const [name, value] of Object.entries(plan.filters || {})) {
    const witness = (plan.filter_bindings || {})[name] || "";
    // the binding on record is the predicate this concept was MINED
    // from, literal included. Show the column it binds to, and label
    // the expression as the witness so it can never be misread as the
    // filter that will run.
    const column = (witness.match(/WHERE\s+([A-Za-z0-9_.]+)\s*(?:=|IN|>|<)/i)
      || [])[1] || "";
    out.push({
      slot: `filters.${name}`, label: name, value,
      extra: witness
        ? (column ? `binds to ${column.split(".").pop()} · witnessed by: `
                  : "witnessed by: ") + witness
        : "",
    });
  }
  return out;
};

export function planPanel(host, state) {
  let versions = [];       // [{version, plan, summary, parent}]
  let viewing = 0;         // the version on screen; 0 = the newest
  let changed = new Set(); // slots the last delta moved

  function current() {
    return versions.find((v) => v.version === viewing)
      || versions[versions.length - 1];
  }

  function render() {
    if (!versions.length) {
      host.innerHTML = `
        <div class="plan-head"><b>Semantic plan</b></div>
        <p class="muted">Nothing planned yet. Ask a question and the
          plan builds itself here, slot by slot, with where each value
          came from.</p>`;
      return;
    }
    const entry = current();
    const plan = entry.plan || {};
    const latest = versions[versions.length - 1];
    const ghost = entry.version !== latest.version;
    const at = versions.findIndex((v) => v.version === entry.version) + 1;

    host.innerHTML = `
      <div class="plan-head">
        <b>Semantic plan</b>
        <span class="spacer"></span>
        <div class="stepper">
          <button class="icon-btn" data-step="-1"
            ${at <= 1 ? "disabled" : ""} aria-label="Previous version"
            >‹</button>
          <span class="mono">v${esc(entry.version)}</span>
          <button class="icon-btn" data-step="1"
            ${at >= versions.length ? "disabled" : ""}
            aria-label="Next version">›</button>
        </div>
      </div>
      <div class="plan-sub muted">${ghost
        ? `previewing v${esc(entry.version)} of ${versions.length}`
        : `v${esc(entry.version)} of ${versions.length} · this is exactly
           what will run`}</div>
      <div class="plan-slots${ghost ? " ghost" : ""}">
        ${rowsOf(plan).map((row) => `
          <div class="plan-slot${
            !ghost && changed.has(row.slot) ? " changed" : ""}">
            <span class="plan-key">${esc(row.label)}</span>
            <span class="plan-val">${esc(row.value)}</span>
            ${!ghost && changed.has(row.slot)
              ? `<span class="chip acc">changed</span>` : ""}
            ${row.extra
              ? `<span class="plan-binding mono">${esc(row.extra)}</span>`
              : ""}
            ${(plan.provenance || {})[row.slot]
              ? `<span class="plan-from muted">${prose(
                  FROM[plan.provenance[row.slot]]
                  || plan.provenance[row.slot])}</span>`
              : ""}
          </div>`).join("")}
      </div>
      ${entry.summary ? `<div class="plan-sum muted">${
        prose(entry.summary)}</div>` : ""}
      ${ghost ? `
        <button class="btn primary" data-restore="${esc(entry.version)}"
          >Restore v${esc(entry.version)}</button>
        <p class="muted">Restoring appends this plan as the newest
          version. Nothing is erased: the chain still shows every step
          you took to get here.</p>` : ""}`;
  }

  host.addEventListener("click", async (e) => {
    const step = e.target.closest?.("[data-step]");
    if (step) {
      const at = versions.findIndex((v) => v.version === viewing);
      const next = versions[Math.min(versions.length - 1, Math.max(
        0, (at < 0 ? versions.length - 1 : at) + Number(step.dataset.step)))];
      if (next) { viewing = next.version; render(); }
      return;
    }
    const restore = e.target.closest?.("[data-restore]");
    if (!restore) return;
    restore.disabled = true;
    const done = await api.askRestorePlan(
      state.session.id, Number(restore.dataset.restore));
    if (!done.available) {
      restore.disabled = false;
      host.insertAdjacentHTML("beforeend",
        `<p class="warn">${prose(done.reason || "restore refused")}</p>`);
      return;
    }
    await refresh();
  });

  async function refresh() {
    const payload = await api.askSession(state.session.id);
    if (!payload.available) return;
    load(payload.plan_versions || []);
  }

  function load(rows) {
    versions = rows.map((r) => ({
      version: Number(r.version), plan: r.plan || {},
      summary: r.summary || "", parent: r.parent,
    })).sort((a, b) => a.version - b.version);
    viewing = versions.length ? versions[versions.length - 1].version : 0;
    render();
  }

  return {
    load,
    refresh,
    // a plan_delta names the slots this turn moved: the panel marks
    // them so a mutation reads as "one thing changed, the rest held"
    delta(event) {
      changed = new Set((event.changes || []).map((c) => c.slot));
    },
    // contract_ready carries the plan that will actually run. Paint it
    // at once so the rail keeps up with the turn, then reconcile with
    // the store: the append-only chain is the ONE authority for what
    // versions exist (a clarify turn stores one too, and a rail that
    // counted only its own events would undercount them).
    plan(planDict, version) {
      const at = versions.findIndex((v) => v.version === Number(version));
      const entry = { version: Number(version), plan: planDict,
                      summary: "", parent: planDict.parent };
      if (at >= 0) versions[at] = { ...versions[at], ...entry };
      else versions.push(entry);
      versions.sort((a, b) => a.version - b.version);
      viewing = versions[versions.length - 1].version;
      render();
      refresh();
    },
    render,
  };
}
