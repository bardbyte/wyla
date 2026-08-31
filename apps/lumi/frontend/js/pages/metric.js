/** Metric Profile: the artboard wired to the full metric row:
 * meaning with per-field provenance, witness bars + agreement, the
 * family with crimson conflict rows from real ReviewItems, binding,
 * who-uses, B2-labeled actions, the feedback affordance. */

import { api } from "../api.js";
import {
  card, esc, feedbackBar, loading, tierChip, unavailable,
} from "../ui.js";

export async function renderMetric(outlet, id) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <a class="linklike" href="#/semantics">Semantics</a>
      <span class="muted">› Metric Profile</span>
    </div>
    <div id="profile">${loading()}</div>`;
  const detail = await api.metric(id);
  const host = outlet.querySelector("#profile");
  if (!host) return;
  if (!detail.available) { host.innerHTML = unavailable(detail.reason); return; }
  if (!detail.found || !detail.metric) {
    host.innerHTML = card("", `<p>metric ${esc(id)} is not in the
      promoted build</p>
      <a class="btn" href="#/semantics">← back</a>`, "empty");
    return;
  }
  const m = detail.metric;
  const witnesses = m.support_by_witness ?? {};
  const maxW = Math.max(1, ...Object.values(witnesses));
  const conflicts = (detail.reviews ?? []).filter((r) =>
    r.kind === "metric_conflict" || r.kind === "witness_divergence");

  host.innerHTML = `
    <div class="card">
      <div class="profile-head">
        <span class="profile-title">${esc(m.label)
          || `${esc((m.fp ?? id).slice(0, 12))}… “?”`}</span>
        ${tierChip(detail.tier ?? "gu", m.status_served ?? m.status)}
        ${m.line_of_business
          ? `<span class="muted">${esc(m.line_of_business)}</span>` : ""}
      </div>
      ${m.canonical_sql
        ? `<code class="sqlchip">${esc(m.canonical_sql)}</code>` : ""}
      <div class="muted">${m.fp
        ? `fp <span class="mono">${esc(m.fp.slice(0, 8))}…</span> · ` : ""}
        evidence origin: ${esc(m.evidence_origin || "-")} · status
        transitions via the clerk (E7)</div>
    </div>

    <div class="grid2">
      ${card("MEANING", `
        ${m.question
          ? `<span>answers: <b>“${esc(m.question)}”</b>${
              m.question_source
                ? ` <span class="chip">${esc(m.question_source)}</span>` : ""}
             </span>`
          : `<span class="muted">no question on record: the
             enrichment loop drafts one, a steward confirms it</span>`}
        ${(m.grain || m.grain_observed)
          ? `<span>grain: <b>${esc(m.grain || m.grain_observed)}</b>${
              !m.grain && m.grain_observed
                ? ` <span class="chip">studio · observed</span>` : ""}
             </span>`
          : `<span class="muted">grain unknown</span>`}
        ${(m.common_filters ?? []).length
          ? `<span class="muted">filters (part of its identity):
             <span class="mono">${(m.common_filters ?? [])
               .map(esc).join(" · ")}</span></span>` : ""}`)}
      ${card(`WITNESSES: agreement ${m.witness_agreement ?? 0}`, `
        ${Object.keys(witnesses).length === 0
          ? `<span class="muted">no ranking witness yet: this is what
             “unverified” means</span>`
          : Object.entries(witnesses).map(([w, n]) => `
            <div class="witness"><span>${esc(w)}</span>
              <div class="bar"><i style="width:${
                Math.round((n / maxW) * 100)}%"></i></div>
              <span class="mono muted">×${n}</span></div>`).join("")}`)}
    </div>

    ${card("FAMILY: same expression class, competing registrations", `
      ${(detail.family ?? []).length === 0 && conflicts.length === 0
        ? `<span class="muted">no variants recorded: quiet accruals
           appear here</span>` : ""}
      ${(detail.family ?? []).map((f) => `
        <div class="family-row"><span>${esc(f.label || f.id)}</span>
          <span class="chip">${esc(f.status_served ?? f.status)}</span>
          <span class="mono muted">${esc(String(f.support ?? ""))}</span>
        </div>`).join("")}
      ${conflicts.map((r) => `
        <div class="conflict">⊘ ${esc(String(r.proposal ?? r.kind))} -
          <span class="muted">${esc(String(
            r.agent_recommendation ?? "steward decides"))}</span>
        </div>`).join("")}`)}

    <div class="grid2">
      ${card("BINDING", m.table
        ? `<a class="linklike mono" href="#/table/${
            encodeURIComponent(m.table)}">${esc(m.table)}</a>`
        : `<span class="muted">no table binding on record</span>`)}
      ${card("WHO USES", Object.keys(m.used_by ?? {}).length
        ? `<span>${Object.entries(m.used_by)
            .map(([u, n]) => `${esc(u)} ${n}`).join(" · ")}</span>`
        : `<span class="muted">no unit-level usage recorded in the
           30-day window</span>`)}
    </div>

    <div class="legend">
      <button class="btn" disabled
        title="Arrives with the steward loop (B2)">Rename…</button>
      <button class="btn" disabled
        title="Arrives with the steward loop (B2)">Deprecate…</button>
      <span class="spacer"></span>
      <span id="fb"></span>
    </div>`;
  host.querySelector("#fb").replaceWith(
    feedbackBar("metric_profile", id, api.feedback));
}
