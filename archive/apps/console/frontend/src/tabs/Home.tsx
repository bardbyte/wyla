/** Home — the capabilities page (Lumi Home artboard, wired to the
 * real build). The hero states the promise; LIVE PROOF shows the
 * system's actual state; the Sources rail shows everything the
 * company handed Meridian and proof we read it (the utilization
 * ledger); SINCE LAST BUILD is the diff, verbatim. Reality law: no
 * build → the designed unavailable state with the server's reason. */

import { useEffect, useState } from "react";
import { useNav } from "../lib/nav";
import type {
  MeridianHome, MeridianSources, Unavailable,
} from "../lib/meridian";
import { meridian } from "../lib/meridian";

const PROMISES: [string, string][] = [
  ["continues your question", "“same for Canada” edits, never restarts"],
  ["asks, never guesses", "one crisp question when words are ambiguous"],
  ["never invents a metric", "unregistered gets a door, not a proxy"],
  ["explains every answer", "definition, filters, source, grain, SQL"],
  ["shows what exists", "lifecycle status with the owner of next action"],
  ["right access, automatically", "entitled users never file tickets"],
];

export function HomeTab() {
  const nav = useNav();
  const [home, setHome] =
    useState<MeridianHome | Unavailable | null>(null);
  const [sources, setSources] =
    useState<MeridianSources | Unavailable | null>(null);

  useEffect(() => {
    meridian.home().then(setHome).catch(
      () => setHome({ available: false, reason: "console unreachable" }));
    meridian.sources().then(setSources).catch(() => setSources(null));
  }, []);

  return (
    <div className="m-page">
      <div className="m-masthead">
        <span className="m-wordmark">LUMI</span>
        <span className="m-tagline">powered by Synapse over Meridian</span>
        <span className="m-spacer" />
        {home?.available && (
          <span className="m-chip m-mono">build {home.build_id.slice(0, 8)}…</span>
        )}
        <span className="m-chip">actor: admin ●</span>
      </div>

      <div className="m-hero">
        <div className="m-hero-title">One company. One number.</div>
        <div className="m-hero-sub">
          Ask in plain words. Get governed numbers with receipts — every
          answer carries its meridian line.
        </div>
        <div className="m-hero-doors">
          <button className="m-door m-door-primary"
            onClick={() => nav.go("ask")}>Ask Lumi</button>
          <button className="m-door"
            onClick={() => nav.go("semantics")}>Explore semantics</button>
          <button className="m-door"
            onClick={() => nav.go("operate")}>
            Operate{home?.available ? ` · ${home.open_reviews} open` : ""}
          </button>
        </div>
      </div>

      {home && !home.available && (
        <div className="m-card m-empty">
          <div className="m-card-label">NO COMPILED BUILD ON THIS MACHINE</div>
          <p>{home.reason}</p>
          <p className="m-muted">
            The console renders only the real promoted build — nothing is
            mocked. Compile on this machine and this page fills itself.
          </p>
        </div>
      )}

      <div className="m-grid2">
        <div className="m-card">
          <div className="m-card-label">WHAT IT DOES — six promises from user research</div>
          <div className="m-promises">
            {PROMISES.map(([lead, rest]) => (
              <span key={lead}>· <b>{lead}</b> — {rest}</span>
            ))}
          </div>
        </div>

        <div className="m-card">
          <div className="m-card-label">LIVE PROOF — the system status, in the open</div>
          {home?.available ? (
            <>
              <div className="m-proof-counts">
                <span><b>{home.counts.tables ?? "—"}</b> tables
                  {home.excluded_tables.length > 0 &&
                    <span className="m-muted"> (+{home.excluded_tables.length} excluded — on record)</span>}
                </span>
                <span><b>{home.counts.metrics ?? "—"}</b> metrics ·{" "}
                  <b>{home.counts.vocab ?? "—"}</b> vocab</span>
              </div>
              <div className="m-status-stack">
                {Object.entries(home.metrics_by_status)
                  .sort((a, b) => b[1] - a[1])
                  .map(([status, n]) => (
                    <span key={status} className="m-chip m-status">
                      {status} <b>{n}</b>
                    </span>
                  ))}
              </div>
              <div className="m-proof-line">
                joins <b>{home.joins.total}</b>
                {home.joins.scoped_only > 0 && (
                  <span className="m-warn"> · {home.joins.scoped_only} CTE-scoped
                    ◐ — evidence the relationship exists, not that raw
                    tables join safely</span>
                )}
              </div>
              {Object.entries(home.readiness).map(([lob, r]) => (
                <div key={lob} className="m-readiness">
                  <div className="m-readiness-head">
                    <span>readiness · {lob}</span>
                    <span><b>{r.pct}%</b> <span className="m-muted">
                      {r.witnessed}/{r.tables} tables witnessed</span></span>
                  </div>
                  <div className="m-bar">
                    <div className="m-bar-fill" style={{ width: `${r.pct}%` }} />
                  </div>
                </div>
              ))}
              <div className="m-proof-line m-muted">
                open reviews <b>{home.open_reviews}</b> · sources{" "}
                <b>{home.sources_count}</b>
              </div>
            </>
          ) : (
            <p className="m-muted">no build — nothing to prove yet</p>
          )}
        </div>
      </div>

      {home?.available && home.diff && (
        <div className="m-card">
          <div className="m-card-label">SINCE LAST BUILD — the diff, verbatim</div>
          <pre className="m-diff">{home.diff.split("\n").slice(0, 14).join("\n")}</pre>
        </div>
      )}

      <div className="m-card">
        <div className="m-card-label">
          SOURCES — everything the company handed Meridian, and proof we read it
        </div>
        {sources?.available ? (
          <div className="m-sources">
            {sources.sources.map((s) => (
              <div key={s.source + (s.sub ?? "")} className="m-source">
                <div className="m-source-head">
                  <span className="m-chip m-source-chip">{s.chip}</span>
                  {s.sub && <span className="m-muted">· {s.sub}</span>}
                </div>
                <div className="m-source-name">{s.display}</div>
                <div className="m-source-counts m-mono">
                  {Object.entries(s.contributes.nodes)
                    .map(([k, n]) => `${k} ${n}`).join(" · ") || "—"}
                </div>
                {Object.keys(s.ledger).length > 0 && (
                  <div className="m-source-ledger m-muted">
                    ledger:{" "}
                    {Object.entries(s.ledger)
                      .map(([k, n]) => `${n} ${k}`).join(" · ")}
                  </div>
                )}
              </div>
            ))}
          </div>
        ) : (
          <p className="m-muted">
            {sources && !sources.available
              ? sources.reason
              : "loading the shelf…"}
          </p>
        )}
      </div>

      {home?.available && home.excluded_tables.length > 0 && (
        <div className="m-card">
          <div className="m-card-label">EXCLUDED — with the reason on record</div>
          {home.excluded_tables.map((t) => (
            <div key={t.physical} className="m-excluded m-mono">
              {t.physical} — {t.intentionally_excluded || t.reason}
            </div>
          ))}
        </div>
      )}

      <div className="m-legend">
        legend:
        <span className="m-chip m-t-ha">● human</span>
        <span className="m-chip m-t-gr">◆ grounded</span>
        <span className="m-chip m-t-in">◐ inferred</span>
        <span className="m-chip m-t-gu">○ guessed</span>
        <span className="m-spacer" />
        <span className="m-muted">
          every write records an actor · every read is scoped by a
          principal
        </span>
      </div>
    </div>
  );
}
