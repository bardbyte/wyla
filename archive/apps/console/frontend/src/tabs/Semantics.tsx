/** Semantics — the Explorer + Metric/Table profiles (their artboards
 * wired to the real build). Progressive disclosure throughout: the
 * name first, the receipts one tap away, the SQL one more. Every
 * number renders from the build; gaps render as designed empty
 * states; writes are labeled with the milestone that brings them
 * (B2) rather than pretending. */

import { useEffect, useMemo, useState } from "react";
import type {
  MetricDetail, MetricRow, TableDetail, TableRow, Unavailable,
} from "../lib/meridian";
import { TIER_GLYPH, meridian } from "../lib/meridian";

type View =
  | { kind: "list" }
  | { kind: "metric"; id: string }
  | { kind: "table"; physical: string };

const STATUS_FILTERS = [
  "", "certified", "pending_certification", "unreviewed",
] as const;

export function SemanticsTab() {
  const [view, setView] = useState<View>({ kind: "list" });
  const [mode, setMode] = useState<"metrics" | "tables">("metrics");
  const [q, setQ] = useState("");
  const [status, setStatus] = useState("");
  const [metrics, setMetrics] = useState<
    { available: true; total: number; shown: number; rows: MetricRow[] }
    | Unavailable | null>(null);
  const [tables, setTables] = useState<
    { available: true; rows: TableRow[] } | Unavailable | null>(null);

  useEffect(() => {
    meridian.metrics({ q, status }).then(setMetrics);
  }, [q, status]);
  useEffect(() => {
    meridian.tables().then(setTables);
  }, []);

  if (view.kind === "metric")
    return <MetricProfile id={view.id}
      onBack={() => setView({ kind: "list" })}
      onTable={(t) => setView({ kind: "table", physical: t })} />;
  if (view.kind === "table")
    return <TableProfile physical={view.physical}
      onBack={() => setView({ kind: "list" })}
      onMetric={(id) => setView({ kind: "metric", id })} />;

  return (
    <div className="m-page">
      <div className="m-masthead">
        <span className="m-wordmark">LUMI</span>
        <span className="m-tagline">Understand · Semantics Explorer</span>
      </div>

      <div className="m-card m-toolbar">
        <div className="m-pills">
          {(["metrics", "tables"] as const).map((m) => (
            <button key={m}
              className={`m-pill ${mode === m ? "m-pill-on" : ""}`}
              onClick={() => setMode(m)}>{m}</button>
          ))}
        </div>
        {mode === "metrics" && (
          <>
            <input className="m-search" placeholder="search label, SQL, question…"
              value={q} onChange={(e) => setQ(e.target.value)} />
            <div className="m-pills">
              {STATUS_FILTERS.map((s) => (
                <button key={s || "all"}
                  className={`m-pill ${status === s ? "m-pill-on" : ""}`}
                  onClick={() => setStatus(s)}>
                  {s || "all"}
                </button>
              ))}
            </div>
          </>
        )}
        <span className="m-spacer" />
        <span className="m-muted">
          {mode === "metrics" && metrics?.available
            ? `${metrics.shown} of ${metrics.total}`
            : mode === "tables" && tables?.available
              ? `${tables.rows.length} tables`
              : ""}
        </span>
      </div>

      {mode === "metrics" && (
        <MetricList data={metrics}
          onPick={(id) => setView({ kind: "metric", id })}
          onTable={(t) => setView({ kind: "table", physical: t })} />
      )}
      {mode === "tables" && (
        <TableList data={tables}
          onPick={(p) => setView({ kind: "table", physical: p })} />
      )}

      <div className="m-legend">
        <span className="m-muted">
          promote / request-naming arrive with the steward loop (B2) —
          writes go through the clerk; this page is a projection, never
          a second write path
        </span>
      </div>
    </div>
  );
}

function MetricList({ data, onPick, onTable }: {
  data: { available: true; rows: MetricRow[] } | Unavailable | null;
  onPick: (id: string) => void;
  onTable: (t: string) => void;
}) {
  if (data && !data.available)
    return <div className="m-card m-empty"><p>{data.reason}</p></div>;
  if (!data) return <div className="m-card"><p className="m-muted">loading…</p></div>;
  if (!data.rows.length)
    return (
      <div className="m-card m-empty">
        <p className="m-muted">
          no metrics match — a narrower filter, or nothing here yet.
          Nothing here means the graph holds no witness for it.
        </p>
      </div>
    );
  return (
    <div className="m-card m-table-card">
      <div className="m-thead m-row-metrics">
        <span>METRIC</span><span>STATUS</span><span>WITNESSES</span>
        <span>USES</span><span>USED BY</span><span>TABLE</span>
      </div>
      {data.rows.map((r) => (
        <div key={r.id} className="m-trow m-row-metrics">
          <div className="m-cell-name">
            <button className="m-linklike" onClick={() => onPick(r.id)}>
              {r.label || `${(r.fp || r.id).slice(0, 12)}… “?”`}
            </button>
            <span className="m-mono m-expr">{r.expr}</span>
          </div>
          <span className={`m-chip m-t-${r.tier}`}>
            {r.status_served} {TIER_GLYPH[r.tier].glyph}
          </span>
          <span className="m-muted">
            {Object.entries(r.witnesses)
              .map(([w, n]) => `${w}×${n}`).join(" · ") || "—"}
          </span>
          <span className="m-mono">{r.support}</span>
          <span className="m-muted">
            {Object.entries(r.used_by)
              .map(([u, n]) => `${u} ${n}`).join(" · ") || "—"}
          </span>
          <span>
            {r.table ? (
              <button className="m-linklike m-mono"
                onClick={() => onTable(r.table)}>
                {r.table.split(".").pop()}
              </button>
            ) : <span className="m-muted">—</span>}
          </span>
        </div>
      ))}
    </div>
  );
}

function TableList({ data, onPick }: {
  data: { available: true; rows: TableRow[] } | Unavailable | null;
  onPick: (p: string) => void;
}) {
  if (data && !data.available)
    return <div className="m-card m-empty"><p>{data.reason}</p></div>;
  if (!data) return <div className="m-card"><p className="m-muted">loading…</p></div>;
  return (
    <div className="m-card m-table-card">
      <div className="m-thead m-row-tables">
        <span>TABLE</span><span>LOB</span><span>COLUMNS</span>
        <span>METRICS</span><span>JOINS</span><span>TICKETS</span>
      </div>
      {data.rows.map((r) => (
        <div key={r.physical} className="m-trow m-row-tables">
          <button className="m-linklike m-mono"
            onClick={() => onPick(r.physical)}>{r.physical}</button>
          <span>{r.lob || <span className="m-muted">unmapped</span>}</span>
          <span className="m-mono">{r.columns}</span>
          <span className="m-mono">{r.metrics_here}</span>
          <span className="m-mono">{r.joins}</span>
          <span className={r.tickets ? "m-warn" : "m-muted"}>
            {r.tickets || "—"}
          </span>
        </div>
      ))}
    </div>
  );
}

function Feedback({ screen, objectId }: {
  screen: string; objectId: string;
}) {
  const [sent, setSent] = useState<"up" | "down" | null>(null);
  const send = (vote: "up" | "down") => {
    meridian.feedback({ screen, object_id: objectId, vote });
    setSent(vote);
  };
  return (
    <span className="m-feedback">
      {sent ? (
        <span className="m-muted">noted — a steward will see it</span>
      ) : (
        <>
          <button className="m-linklike" onClick={() => send("up")}
            title="This reads right">👍</button>
          <button className="m-linklike" onClick={() => send("down")}
            title="Something is off — a steward will see it">👎</button>
        </>
      )}
    </span>
  );
}

function MetricProfile({ id, onBack, onTable }: {
  id: string; onBack: () => void; onTable: (t: string) => void;
}) {
  const [detail, setDetail] =
    useState<MetricDetail | Unavailable | null>(null);
  useEffect(() => { meridian.metricDetail(id).then(setDetail); }, [id]);

  if (!detail) return <div className="m-page"><p className="m-muted">loading…</p></div>;
  if (!detail.available)
    return <div className="m-page"><div className="m-card m-empty"><p>{detail.reason}</p></div></div>;
  if (!detail.found || !detail.metric)
    return (
      <div className="m-page">
        <div className="m-card m-empty">
          <p>metric {id} is not in the promoted build</p>
          <button className="m-door" onClick={onBack}>← back</button>
        </div>
      </div>
    );

  const m = detail.metric;
  const tier = detail.tier ?? "gu";
  const witnesses = m.support_by_witness ?? {};
  const maxW = Math.max(1, ...Object.values(witnesses));
  const conflicts = (detail.reviews ?? []).filter((r) =>
    r.kind === "metric_conflict" || r.kind === "witness_divergence");

  return (
    <div className="m-page">
      <div className="m-masthead">
        <span className="m-wordmark">LUMI</span>
        <span className="m-tagline">
          <button className="m-linklike" onClick={onBack}>Semantics</button>
          {" ›"} Metric Profile
        </span>
      </div>

      <div className="m-card">
        <div className="m-profile-head">
          <span className="m-profile-title">
            {m.label || `${(m.fp ?? id).slice(0, 12)}… “?”`}
          </span>
          <span className={`m-chip m-t-${tier}`}>
            {m.status_served ?? m.status} {TIER_GLYPH[tier].glyph}
          </span>
          {m.line_of_business && (
            <span className="m-muted">{m.line_of_business}</span>
          )}
        </div>
        {m.canonical_sql && (
          <div className="m-sqlrow">
            <code className="m-sqlchip m-mono">{m.canonical_sql}</code>
            {m.fp && <span className="m-muted m-mono">fp {m.fp.slice(0, 8)}…</span>}
          </div>
        )}
        <div className="m-muted">
          evidence origin: {m.evidence_origin || "—"} · status
          transitions via the clerk (E7)
        </div>
      </div>

      <div className="m-grid2">
        <div className="m-card">
          <div className="m-card-label">MEANING</div>
          {m.question ? (
            <span>answers: <b>“{m.question}”</b>{" "}
              {m.question_source && (
                <span className="m-chip">{m.question_source}</span>
              )}
            </span>
          ) : (
            <span className="m-muted">
              no question on record — the enrichment loop drafts one,
              a steward confirms it
            </span>
          )}
          {(m.grain || m.grain_observed) ? (
            <span>grain: <b>{m.grain || m.grain_observed}</b>{" "}
              {!m.grain && m.grain_observed && (
                <span className="m-chip">studio · observed</span>
              )}
            </span>
          ) : (
            <span className="m-muted">grain unknown</span>
          )}
          {(m.common_filters ?? []).length > 0 && (
            <span className="m-muted">
              filters (part of its identity):{" "}
              <span className="m-mono">
                {(m.common_filters ?? []).join(" · ")}
              </span>
            </span>
          )}
        </div>

        <div className="m-card">
          <div className="m-card-label">
            WITNESSES — agreement {m.witness_agreement ?? 0}
          </div>
          {Object.keys(witnesses).length === 0 && (
            <span className="m-muted">
              no ranking witness yet — this is what “unverified” means
            </span>
          )}
          {Object.entries(witnesses).map(([w, n]) => (
            <div key={w} className="m-witness">
              <span className="m-witness-name">{w}</span>
              <div className="m-bar">
                <div className="m-bar-fill"
                  style={{ width: `${Math.round((n / maxW) * 100)}%` }} />
              </div>
              <span className="m-mono m-muted">×{n}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="m-card">
        <div className="m-card-label">
          FAMILY — same expression class, competing registrations
        </div>
        {(detail.family ?? []).length === 0 && conflicts.length === 0 && (
          <span className="m-muted">
            no variants recorded — quiet accruals appear here
          </span>
        )}
        {(detail.family ?? []).map((f) => (
          <div key={f.id} className="m-family-row">
            <span>{f.label || f.id}</span>
            <span className="m-chip">{f.status_served ?? f.status}</span>
            <span className="m-mono m-muted">{String(f.support ?? "")}</span>
          </div>
        ))}
        {conflicts.map((r, i) => (
          <div key={i} className="m-conflict">
            ⊘ {String(r.proposal ?? r.kind)} —{" "}
            <span className="m-muted">
              {String(r.agent_recommendation ?? "steward decides")}
            </span>
          </div>
        ))}
      </div>

      <div className="m-grid2">
        <div className="m-card">
          <div className="m-card-label">BINDING</div>
          {m.table ? (
            <button className="m-linklike m-mono"
              onClick={() => onTable(m.table!)}>{m.table}</button>
          ) : (
            <span className="m-muted">no table binding on record</span>
          )}
        </div>
        <div className="m-card">
          <div className="m-card-label">WHO USES</div>
          {Object.keys(m.used_by ?? {}).length ? (
            <span>
              {Object.entries(m.used_by ?? {})
                .map(([u, n]) => `${u} ${n}`).join(" · ")}
            </span>
          ) : (
            <span className="m-muted">
              no unit-level usage recorded in the 30-day window
            </span>
          )}
        </div>
      </div>

      <div className="m-legend">
        <button className="m-door" disabled
          title="Arrives with the steward loop (B2)">Rename…</button>
        <button className="m-door" disabled
          title="Arrives with the steward loop (B2)">Deprecate…</button>
        <span className="m-spacer" />
        <Feedback screen="metric_profile" objectId={id} />
      </div>
    </div>
  );
}

function TableProfile({ physical, onBack, onMetric }: {
  physical: string; onBack: () => void; onMetric: (id: string) => void;
}) {
  const [detail, setDetail] =
    useState<TableDetail | Unavailable | null>(null);
  useEffect(() => {
    meridian.tableDetail(physical).then(setDetail);
  }, [physical]);
  const columns = useMemo(
    () => Object.entries(detail && "columns" in detail
      ? detail.columns ?? {} : {}),
    [detail]);

  if (!detail) return <div className="m-page"><p className="m-muted">loading…</p></div>;
  if (!detail.available)
    return <div className="m-page"><div className="m-card m-empty"><p>{detail.reason}</p></div></div>;
  if (!detail.found)
    return (
      <div className="m-page">
        <div className="m-card m-empty">
          <p>{physical} is not in the promoted build</p>
          <button className="m-door" onClick={onBack}>← back</button>
        </div>
      </div>
    );

  return (
    <div className="m-page">
      <div className="m-masthead">
        <span className="m-wordmark">LUMI</span>
        <span className="m-tagline">
          <button className="m-linklike" onClick={onBack}>Semantics</button>
          {" ›"} Table Profile
        </span>
      </div>

      <div className="m-card">
        <div className="m-profile-head">
          <span className="m-profile-title m-mono">{physical}</span>
          <span className="m-muted">{columns.length} columns servable</span>
        </div>
        {detail.cost_prior && (
          <div className="m-muted">
            usage prior: p50{" "}
            <span className="m-mono">{String(detail.cost_prior.p50 ?? "—")}</span>
            {" · p95 "}
            <span className="m-mono">{String(detail.cost_prior.p95 ?? "—")}</span>
            {" bytes/query (30-day activity)"}
          </div>
        )}
      </div>

      <div className="m-grid2">
        <div className="m-card">
          <div className="m-card-label">
            METRICS ON THIS TABLE — {detail.metrics_here?.length ?? 0}
          </div>
          {(detail.metrics_here ?? []).length === 0 && (
            <span className="m-muted">
              no witnessed metric yet — ask about this table and the
              mining will catch up
            </span>
          )}
          {(detail.metrics_here ?? []).map((mh) => (
            <div key={mh.id} className="m-family-row">
              <button className="m-linklike"
                onClick={() => onMetric(mh.id)}>
                {mh.label || mh.id}
              </button>
              <span className="m-chip">{mh.status_served}</span>
              <span className="m-mono m-muted">{mh.support}</span>
            </div>
          ))}
        </div>

        <div className="m-card">
          <div className="m-card-label">
            JOINS — how, with evidence, never by vibes
          </div>
          {(detail.joins ?? []).length === 0 && (
            <span className="m-muted">
              no join evidence on record — co-usage alone is not a join
            </span>
          )}
          {(detail.joins ?? []).map((j, i) => (
            <div key={i} className="m-joinrow">
              <span className="m-mono">
                {j.a === physical ? j.b : j.a}
              </span>
              <span className="m-chip">{j.source}</span>
              {j.scope === "scoped_only" && (
                <span className="m-warn">
                  ◐ CTE-scoped — NOT raw-safe
                </span>
              )}
              {j.on && (
                <span className="m-mono m-muted">
                  on {Array.isArray(j.on) ? j.on.join(" AND ") : j.on}
                </span>
              )}
            </div>
          ))}
        </div>
      </div>

      <div className="m-card">
        <div className="m-card-label">COLUMNS — as BigQuery can serve them</div>
        <div className="m-columns">
          {columns.map(([name, type]) => (
            <span key={name} className="m-chip m-mono">
              {name.split(".").pop()} <span className="m-muted">{type}</span>
            </span>
          ))}
        </div>
      </div>

      {detail.card && (
        <div className="m-card">
          <div className="m-card-label">THE SERVED CARD — what the agent reads</div>
          <pre className="m-diff">{detail.card}</pre>
        </div>
      )}

      <div className="m-legend">
        <span className="m-spacer" />
        <Feedback screen="table_profile" objectId={physical} />
      </div>
    </div>
  );
}
