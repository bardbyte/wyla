/** Operate — Builds & Diffs + Enrichment Runs, from the real reports.
 * Promote/rollback arrive with the steward loop (B2); this page is a
 * projection, never a second write path. */

import { api } from "../api.js";
import { card, esc, kv, loading, unavailable } from "../ui.js";

export async function renderOperate(outlet) {
  outlet.innerHTML = `
    <div class="masthead" style="padding:0">
      <span class="muted">Operate · Builds &amp; Enrichment</span>
    </div>
    <div id="op-body">${loading()}</div>`;
  const [builds, runsPayload] = await Promise.all(
    [api.builds(), api.enrichRuns()]);
  const body = outlet.querySelector("#op-body");
  if (!body) return;

  let buildsCard;
  if (!builds.available) {
    buildsCard = unavailable(builds.reason);
  } else {
    const counts = builds.manifest.counts ?? {};
    const recon = builds.manifest.table_reconciliation ?? {};
    const excluded = (recon.missing ?? [])
      .filter((m) => m.intentionally_excluded).length;
    buildsCard = card("CURRENT — the promoted build every surface reads", `
      <div class="profile-head">
        <span class="profile-title mono">${esc(builds.current)}</span>
        <span class="chip tier-gr">promoted ✓</span>
      </div>
      <div class="proof-counts">${kv(Object.entries(counts))}</div>
      ${recon.crosswalk_rows ? `<div class="muted">reconciliation:
        ${recon.built} of ${recon.crosswalk_rows} crosswalk tables
        built${excluded ? " · the rest excluded with reasons on record"
          : ""}</div>` : ""}
      <div class="muted">builds on this machine:
        ${builds.builds.map(esc).join(" · ")}</div>
      <button class="btn" id="toggle-diff">show DIFF_vs_prev</button>
      <pre class="block" id="diff" hidden>${esc(builds.diff)}</pre>`,
      "current-frame");
  }

  const runs = runsPayload.available ? runsPayload.runs : [];
  const runsCard = card(
    "ENRICHMENT RUNS — drafts, gated blind before any write",
    runs.length === 0
      ? `<p class="muted">no enrichment run recorded in this graph yet —
         run laptop.py enrich and this page fills itself</p>`
      : runs.slice().reverse().map((run) => {
          const blind = run.blind;
          const tierCls = blind
            ? (blind.tier === "batch" ? "tier-gr"
              : blind.tier === "item" ? "tier-in" : "tier-block")
            : "";
          return `<div class="runrow">
            <div class="head"><span class="mono">${esc(run.run)}</span>
              ${run.prompt_version
                ? `<span class="chip">prompt ${esc(run.prompt_version)}</span>`
                : ""}
              ${blind ? `<span class="chip ${tierCls}">blind ${
                blind.recovered}/${blind.n} (${
                Math.round(blind.rate * 100)}%) → ${esc(blind.tier)}</span>`
                : ""}
            </div>
            <div class="stats">
              ${blind && blind.leaky_contexts !== undefined
                ? `<span>leaky contexts ${blind.leaky_contexts}</span>` : ""}
              <span>metrics written ${run.metrics_enriched ?? 0}</span>
              <span>concepts ${run.concepts_enriched ?? 0}</span>
              <span>collisions→review ${run.collisions ?? 0}</span>
              <span>invalid_json ${run.invalid_json ?? 0}</span>
              ${run.usage ? `<span class="mono">${run.usage.calls ?? 0}
                calls · ${run.usage.thought_tokens ?? 0} thought
                tokens</span>` : ""}
            </div>
            ${blind?.grader ? `<div class="muted" style="font-size:11px">
              grader ${esc(blind.grader)} — a pass with leaky context
              measures leakage, not recovery</div>` : ""}
          </div>`;
        }).join(""));

  body.innerHTML = `${buildsCard}${runsCard}
    <div class="legend"><span class="muted">promote / rollback arrive
      with the steward loop (B2) — this page is a projection, never a
      second write path</span></div>`;
  const toggle = body.querySelector("#toggle-diff");
  if (toggle) {
    toggle.addEventListener("click", () => {
      const diff = body.querySelector("#diff");
      diff.hidden = !diff.hidden;
      toggle.textContent = diff.hidden
        ? "show DIFF_vs_prev" : "hide diff";
    });
  }
}
