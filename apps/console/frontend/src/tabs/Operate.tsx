/** Operate — Builds & Diffs + Enrichment Runs (their artboards wired
 * to the real reports). The CURRENT build card carries the promoted
 * pointer, the reconciliation (45 + 1 excluded — the exclusion is a
 * fact, not a rounding error), and the diff verbatim; enrichment runs
 * render enrich_report.json fields exactly — blind-gate tiers, the
 * leakage count, writes, collisions, token spend. */

import { useEffect, useState } from "react";
import type {
  EnrichRun, MeridianBuilds, Unavailable,
} from "../lib/meridian";
import { meridian } from "../lib/meridian";

export function OperateTab() {
  const [builds, setBuilds] =
    useState<MeridianBuilds | Unavailable | null>(null);
  const [runs, setRuns] = useState<EnrichRun[] | null>(null);
  const [showDiff, setShowDiff] = useState(false);

  useEffect(() => {
    meridian.builds().then(setBuilds).catch(
      () => setBuilds({ available: false, reason: "console unreachable" }));
    meridian.enrichRuns().then(
      (r) => setRuns(r.available ? r.runs : []),
    ).catch(() => setRuns([]));
  }, []);

  return (
    <div className="m-page">
      <div className="m-masthead">
        <span className="m-wordmark">LUMI</span>
        <span className="m-tagline">Operate · Builds &amp; Enrichment</span>
      </div>

      {builds && !builds.available && (
        <div className="m-card m-empty">
          <div className="m-card-label">NO COMPILED BUILD</div>
          <p>{builds.reason}</p>
        </div>
      )}

      {builds?.available && (
        <div className="m-card m-current">
          <div className="m-card-label">
            CURRENT — the promoted build every surface reads
          </div>
          <div className="m-current-head">
            <span className="m-mono m-current-id">{builds.current}</span>
            <span className="m-chip m-t-gr">promoted ✓</span>
          </div>
          <div className="m-proof-counts">
            {Object.entries(builds.manifest.counts ?? {}).map(
              ([key, n]) => (
                <span key={key}>
                  {key} <b className="m-mono">{String(n)}</b>
                </span>
              ),
            )}
          </div>
          {builds.manifest.table_reconciliation && (
            <div className="m-proof-line m-muted">
              reconciliation: {builds.manifest.table_reconciliation.built}
              {" of "}
              {builds.manifest.table_reconciliation.crosswalk_rows}
              {" crosswalk tables built"}
              {(builds.manifest.table_reconciliation.missing ?? [])
                .filter((m) => m.intentionally_excluded).length > 0 &&
                " · the rest excluded with reasons on record"}
            </div>
          )}
          <div className="m-proof-line m-muted">
            builds on this machine: {builds.builds.join(" · ")}
          </div>
          <button className="m-door" onClick={() => setShowDiff(!showDiff)}>
            {showDiff ? "hide diff" : "show DIFF_vs_prev"}
          </button>
          {showDiff && <pre className="m-diff">{builds.diff}</pre>}
        </div>
      )}

      <div className="m-card">
        <div className="m-card-label">
          ENRICHMENT RUNS — drafts, gated blind before any write
        </div>
        {runs === null && <p className="m-muted">loading…</p>}
        {runs !== null && runs.length === 0 && (
          <p className="m-muted">
            no enrichment run recorded in this graph yet — run
            `laptop.py enrich` and this page fills itself
          </p>
        )}
        {runs !== null && runs.slice().reverse().map((run) => (
          <div key={run.run} className="m-run">
            <div className="m-run-head">
              <span className="m-mono">{run.run}</span>
              {run.prompt_version && (
                <span className="m-chip">prompt {run.prompt_version}</span>
              )}
              {run.blind && (
                <span className={`m-chip ${
                  run.blind.tier === "batch" ? "m-t-gr"
                    : run.blind.tier === "item" ? "m-t-in" : "m-t-block"
                }`}>
                  blind {run.blind.recovered}/{run.blind.n}
                  {" "}({Math.round(run.blind.rate * 100)}%) → {run.blind.tier}
                </span>
              )}
            </div>
            <div className="m-run-stats m-muted">
              {run.blind?.leaky_contexts !== undefined &&
                <span>leaky contexts {run.blind.leaky_contexts}</span>}
              <span>metrics written {run.metrics_enriched ?? 0}</span>
              <span>concepts {run.concepts_enriched ?? 0}</span>
              <span>collisions→review {run.collisions ?? 0}</span>
              <span>invalid_json {run.invalid_json ?? 0}</span>
              {run.grain_divergences !== undefined &&
                <span>grain divergences {run.grain_divergences}</span>}
              {run.usage && (
                <span className="m-mono">
                  {run.usage.calls ?? 0} calls ·{" "}
                  {run.usage.thought_tokens ?? 0} thought tokens
                </span>
              )}
            </div>
            {run.blind?.grader && (
              <div className="m-muted m-run-grader">
                grader {run.blind.grader} — a pass with leaky context
                measures leakage, not recovery
              </div>
            )}
          </div>
        ))}
      </div>

      <div className="m-legend">
        <span className="m-muted">
          promote / rollback arrive with the steward loop (B2) — this
          page is a projection, never a second write path
        </span>
      </div>
    </div>
  );
}
