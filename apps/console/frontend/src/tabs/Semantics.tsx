/** Semantics — the Explorer + Metric/Table/Business-unit profiles
 * (their artboards wired to the real build). Progressive disclosure
 * throughout: the name first, the receipts one tap away, the SQL one
 * more. Every number renders from the build; the Table Profile and
 * the served card are two renderings of ONE compiled facts row
 * (indexes/tables.jsonl), so the screen and the agent never disagree.
 * Gaps render as designed empty states; writes are labeled with the
 * milestone that brings them (B2) rather than pretending. */

import { useEffect, useMemo, useState } from "react";
import type {
  ColumnFact, LobDetail, LobFacts, MetricDetail, MetricRow, TableDetail,
  TableFacts, TableRow, Unavailable,
} from "../lib/meridian";
import { TIER_GLYPH, meridian } from "../lib/meridian";

type View =
  | { kind: "list" }
  | { kind: "metric"; id: string }
  | { kind: "table"; physical: string }
  | { kind: "lob"; code: string };

const STATUS_FILTERS = [
  "", "certified", "pending_certification", "unreviewed",
] as const;

type Mode = "metrics" | "tables" | "units";

function bytes(n: number | undefined | null): string {
  if (n === undefined || n === null) return "—";
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)} GB`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)} MB`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)} KB`;
  return `${n} B`;
}

export function SemanticsTab() {
  const [view, setView] = useState<View>({ kind: "list" });
  const [mode, setMode] = useState<Mode>("metrics");
  const [q, setQ] = useState("");
  const [status, setStatus] = useState("");
  const [metrics, setMetrics] = useState<
    { available: true; total: number; shown: number; rows: MetricRow[] }
    | Unavailable | null>(null);
  const [tables, setTables] = useState<
    { available: true; rows: TableRow[] } | Unavailable | null>(null);
  const [lobs, setLobs] = useState<
    { available: true; rows: LobFacts[] } | Unavailable | null>(null);

  useEffect(() => {
    meridian.metrics({ q, status }).then(setMetrics);
  }, [q, status]);
  useEffect(() => {
    meridian.tables().then(setTables);
    meridian.lobs().then(setLobs);
  }, []);

  if (view.kind === "metric")
    return <MetricProfile id={view.id}
      onBack={() => setView({ kind: "list" })}
      onTable={(t) => setView({ kind: "table", physical: t })} />;
  if (view.kind === "table")
    return <TableProfile physical={view.physical}
      onBack={() => setView({ kind: "list" })}
      onMetric={(id) => setView({ kind: "metric", id })}
      onTable={(t) => setView({ kind: "table", physical: t })}
      onLob={(c) => setView({ kind: "lob", code: c })} />;
  if (view.kind === "lob")
    return <LobProfile code={view.code}
      onBack={() => setView({ kind: "list" })}
      onTable={(t) => setView({ kind: "table", physical: t })} />;

  const count =
    mode === "metrics" && metrics?.available
      ? `${metrics.shown} of ${metrics.total}`
      : mode === "tables" && tables?.available
        ? `${tables.rows.length} tables`
        : mode === "units" && lobs?.available
          ? `${lobs.rows.length} units`
          : "";

  return (
    <div className="m-page">
      <div className="m-masthead">
        <span className="m-wordmark">LUMI</span>
        <span className="m-tagline">Understand · Semantics Explorer</span>
      </div>

      <div className="m-card m-toolbar">
        <div className="m-pills">
          {(["metrics", "tables", "units"] as const).map((m) => (
            <button key={m}
              className={`m-pill ${mode === m ? "m-pill-on" : ""}`}
              onClick={() => setMode(m)}>
              {m === "units" ? "business units" : m}
            </button>
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
        <span className="m-muted">{count}</span>
      </div>

      {mode === "metrics" && (
        <MetricList data={metrics}
          onPick={(id) => setView({ kind: "metric", id })}
          onTable={(t) => setView({ kind: "table", physical: t })} />
      )}
      {mode === "tables" && (
        <TableList data={tables}
          onPick={(p) => setView({ kind: "table", physical: p })}
          onLob={(c) => setView({ kind: "lob", code: c })} />
      )}
      {mode === "units" && (
        <LobList data={lobs}
          onPick={(c) => setView({ kind: "lob", code: c })} />
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

function TableList({ data, onPick, onLob }: {
  data: { available: true; rows: TableRow[] } | Unavailable | null;
  onPick: (p: string) => void;
  onLob: (c: string) => void;
}) {
  if (data && !data.available)
    return <div className="m-card m-empty"><p>{data.reason}</p></div>;
  if (!data) return <div className="m-card"><p className="m-muted">loading…</p></div>;
  return (
    <div className="m-card m-table-card">
      <div className="m-thead m-row-tables">
        <span>TABLE</span><span>UNIT · LOB</span><span>LIFECYCLE</span>
        <span>COLUMNS</span><span>METRICS</span><span>JOINS</span>
        <span>TICKETS</span>
      </div>
      {data.rows.map((r) => (
        <div key={r.physical} className="m-trow m-row-tables">
          <div className="m-cell-name">
            <button className="m-linklike m-mono"
              onClick={() => onPick(r.physical)}>{r.physical}</button>
            <span className="m-expr">
              {r.tier && (
                <span className={`m-chip m-t-${r.tier}`}
                  title={TIER_GLYPH[r.tier].word}>
                  {TIER_GLYPH[r.tier].glyph}
                </span>
              )}{" "}
              {r.business_name || <span className="m-muted">no business name on record</span>}
              {r.pii && <span className="m-chip m-chip-pii"> ⊘ PII</span>}
            </span>
          </div>
          <span>
            {r.business_unit
              ? <span title="MDM pipeline business unit">{r.business_unit}</span>
              : <span className="m-muted">—</span>}
            {(r.lobs ?? []).length > 0 && (
              <span className="m-muted">
                {" · "}
                {(r.lobs ?? []).map((c) => (
                  <button key={c} className="m-linklike"
                    onClick={() => onLob(c)}>{c}</button>
                ))}
              </span>
            )}
            {!r.business_unit && !(r.lobs ?? []).length && !r.lob && (
              <span className="m-muted">unmapped</span>
            )}
          </span>
          <span className={r.lifecycle?.startsWith("unknown") ? "m-warn" : ""}>
            {r.lifecycle || <span className="m-muted">—</span>}
          </span>
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

function LobList({ data, onPick }: {
  data: { available: true; rows: LobFacts[] } | Unavailable | null;
  onPick: (c: string) => void;
}) {
  if (data && !data.available)
    return <div className="m-card m-empty"><p>{data.reason}</p></div>;
  if (!data) return <div className="m-card"><p className="m-muted">loading…</p></div>;
  if (!data.rows.length)
    return (
      <div className="m-card m-empty">
        <p className="m-muted">
          no business units mapped — the steward's lob_map.jsonl is
          empty for this build. Nothing here means no unit has been
          declared, not that tables have no owner.
        </p>
      </div>
    );
  return (
    <div className="m-card m-table-card">
      <div className="m-thead m-row-lobs">
        <span>UNIT</span><span>KIND</span><span>TABLES</span>
        <span>WITNESSED</span><span>USAGE</span><span>VOCAB</span>
      </div>
      {data.rows.map((r) => (
        <div key={r.code} className="m-trow m-row-lobs">
          <div className="m-cell-name">
            <button className="m-linklike" onClick={() => onPick(r.code)}>
              {r.code}{r.name ? ` — ${r.name}` : ""}
            </button>
            {r.parent && (
              <span className="m-expr">org unit under {r.parent}</span>
            )}
          </div>
          <span className="m-muted">
            {r.kind === "org_unit" ? "org unit" : "line of business"}
          </span>
          <span className="m-mono">{r.tables.length}</span>
          <span>
            {r.readiness ? (
              <span>
                <span className="m-mono">{r.readiness.pct}%</span>
                <span className="m-muted">
                  {" "}({r.readiness.witnessed}/{r.readiness.tables})
                </span>
              </span>
            ) : <span className="m-muted">usage only</span>}
          </span>
          <span className="m-mono">
            {r.usage_support ? r.usage_support : <span className="m-muted">—</span>}
          </span>
          <span className="m-mono">
            {r.vocabulary_entries ? r.vocabulary_entries : <span className="m-muted">—</span>}
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

/* ── Table Profile: the facts row, rendered ───────────────── */

function KV({ rows }: { rows: [string, React.ReactNode][] }) {
  const shown = rows.filter(([, v]) => v !== undefined && v !== null
    && v !== "" && v !== false);
  if (!shown.length) return null;
  return (
    <dl className="m-kv">
      {shown.map(([k, v]) => (
        <div key={k} style={{ display: "contents" }}>
          <dt>{k}</dt><dd>{v}</dd>
        </div>
      ))}
    </dl>
  );
}

function ColumnRow({ c, physical }: { c: ColumnFact; physical: string }) {
  const chips: React.ReactNode[] = [];
  if (c.primary_key) chips.push(<span key="pk" className="m-chip m-chip-key">PK</span>);
  else if (c.primary_key_atlas)
    chips.push(<span key="pka" className="m-chip m-chip-key">PK · atlas</span>);
  if (c.partitioning) chips.push(<span key="pt" className="m-chip m-chip-key">PARTITION</span>);
  else if (c.partitioning_atlas)
    chips.push(<span key="pta" className="m-chip m-chip-key">PARTITION · atlas</span>);
  if (c.sensitive)
    chips.push(
      <span key="pii" className="m-chip m-chip-pii"
        title={`flagged by ${(c.sensitivity_sources ?? []).join(" + ") || "?"}`}>
        ⊘ {[c.pii_role, c.sde_group].filter(Boolean).join("/") || "SENSITIVE"}
      </span>);
  if (c.pii_role_table_declared)
    chips.push(
      <span key="piid" className="m-chip m-chip-pii">
        table declares {c.pii_role_table_declared}
      </span>);
  if (c.ungoverned)
    chips.push(<span key="ug" className="m-chip m-t-gu">○ ungoverned</span>);
  if (c.nullable_atlas === false)
    chips.push(<span key="nn" className="m-chip m-chip-soft">NOT NULL · atlas</span>);
  const texture: string[] = [];
  if (c.approx_distinct !== undefined) texture.push(`~${c.approx_distinct} distinct`);
  if (c.null_count !== undefined) texture.push(`${c.null_count} null`);
  if (c.column_length !== undefined) texture.push(`len ${c.column_length}`);
  if (c.ordinal !== undefined && c.ordinal_atlas !== undefined
      && c.ordinal !== c.ordinal_atlas)
    texture.push(`ordinal bq ${c.ordinal} vs atlas ${c.ordinal_atlas}`);
  return (
    <div className="m-colrow">
      <div className="m-col-name">
        <span className="m-mono">
          {c.name} <span className="m-muted">{(c.type ?? "").toLowerCase()}</span>
        </span>
        {c.business_name && <span className="m-col-bname">“{c.business_name}”</span>}
        {chips.length > 0 && <span className="m-chips">{chips}</span>}
      </div>
      <div className="m-col-meta">
        {c.description ? (
          <span style={{ color: "var(--ink-2)" }}>
            {c.description}
            {c.description_source && (
              <span className="m-muted"> · {c.description_source}</span>
            )}
          </span>
        ) : (
          <span className="m-muted">
            no business meaning on record — ask about {physical.split(".").pop()}.{c.name}
          </span>
        )}
        {c.description_supplementary && (
          <span>lumi: {c.description_supplementary}</span>
        )}
        {c.domain && (
          <span>
            {c.domain.n_values} known values
            {c.domain.top.length > 0 && (
              <span className="m-mono">
                {" "}({c.domain.top.map((v) =>
                  v.pct !== undefined ? `${String(v.value)} ${v.pct}%`
                    : String(v.value)).join(", ")})
              </span>
            )}
            {" "}· compiled snapshot, never live
          </span>
        )}
        {(c.terms ?? []).map((t, i) => (
          <span key={i}>
            term: <b>{t.name}</b>
            {t.status && <span className="m-chip m-chip-soft">{t.status}</span>}
            {t.description && ` — ${t.description}`}
            {t.matched_on && <span className="m-muted"> · matched on {t.matched_on}</span>}
          </span>
        ))}
        {(c.declared_terms ?? []).map((t, i) => (
          <span key={`d${i}`} className="m-muted">
            declared term (no glossary id): {t.name}
            {t.description && ` — ${t.description}`}
          </span>
        ))}
        {(c.fk_references ?? []).map((f, i) => (
          <span key={`fk${i}`}>
            FK → <span className="m-mono">{f.table}.{f.column}</span>
            <span className="m-muted"> · declared constraint</span>
          </span>
        ))}
        {c.derived_logic && (
          <span>computed: <code className="m-mono">{c.derived_logic}</code></span>
        )}
        {(c.derived_from ?? []).map((d, i) => (
          <span key={`df${i}`}>
            derived from <span className="m-mono">{d.source}</span>
            {d.logic && <span className="m-muted"> · {d.logic}</span>}
          </span>
        ))}
      </div>
      <div className="m-col-texture">
        {texture.length ? texture.join(" · ") : <span className="m-muted">—</span>}
        <div className="m-muted">
          {c.type_source ?? "bq"} · agree {c.agreement ?? 1}
          {(c.flags ?? []).length > 0 && ` · ${(c.flags ?? []).join(" ")}`}
        </div>
      </div>
    </div>
  );
}

function TableProfile({ physical, onBack, onMetric, onTable, onLob }: {
  physical: string; onBack: () => void; onMetric: (id: string) => void;
  onTable: (t: string) => void; onLob: (c: string) => void;
}) {
  const [detail, setDetail] =
    useState<TableDetail | Unavailable | null>(null);
  useEffect(() => {
    meridian.tableDetail(physical).then(setDetail);
  }, [physical]);
  const facts: TableFacts | undefined = useMemo(
    () => (detail && "facts" in detail ? detail.facts : undefined),
    [detail]);
  const columns = useMemo(() => {
    const rows = facts?.column_facts ?? [];
    return [...rows].sort((a, b) =>
      (a.ordinal ?? 1e6) - (b.ordinal ?? 1e6) || a.name.localeCompare(b.name));
  }, [facts]);

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

  const id = facts?.identity ?? {};
  const biz = facts?.business ?? {};
  const ops = facts?.operations ?? {};
  const trust = facts?.trust ?? {};
  const access = facts?.access ?? {};
  const joins = facts?.joins ?? {};
  const lineage = facts?.lineage ?? {};
  const tier = trust.tier ?? "gu";
  const livesAt = [id.project, id.dataset, physical.split(".").pop()]
    .filter(Boolean).join(".");
  const owners = biz.owners ?? [];

  return (
    <div className="m-page">
      <div className="m-masthead">
        <span className="m-wordmark">LUMI</span>
        <span className="m-tagline">
          <button className="m-linklike" onClick={onBack}>Semantics</button>
          {" ›"} Table Profile
        </span>
      </div>

      {/* ── head: what it is ── */}
      <div className="m-card">
        <div className="m-profile-head">
          <span className="m-profile-title m-mono">{physical}</span>
          {id.business_name && <span className="m-profile-title">{id.business_name}</span>}
          <span className={`m-chip m-t-${tier}`}>
            {TIER_GLYPH[tier].glyph} {TIER_GLYPH[tier].word}
          </span>
          {facts?.lifecycle && (
            <span className={`m-chip ${String(facts.lifecycle).startsWith("unknown") ? "m-warn" : ""}`}>
              {facts.lifecycle}
            </span>
          )}
          {access.pii_table && <span className="m-chip m-chip-pii">⊘ PII</span>}
          <span className="m-muted">{columns.length} columns servable</span>
        </div>
        {id.description ? (
          <div>
            {id.description}
            <span className="m-muted"> · {id.description_source || "atlas"}</span>
            {id.description_bq && (
              <span className="m-muted"> · bq: {id.description_bq}</span>
            )}
          </div>
        ) : (
          <div className="m-muted">
            no purpose on record — ask about this table and the
            enrichment loop drafts one for a steward to confirm
          </div>
        )}
        <div className="m-muted">
          lives at <span className="m-mono">{livesAt || physical}</span>
          {(id.technology || id.data_server) && (
            <> · {[id.technology, id.data_server].filter(Boolean).join(" via ")}</>
          )}
          {id.appl_id && <> · registry appl_id <span className="m-mono">{id.appl_id}</span></>}
          {id.target_system && <> · target {id.target_system}</>}
        </div>
        {!facts && (
          <div className="m-muted">
            build predates the facts row — recompile to see identity,
            grain, trust and ownership here
          </div>
        )}
      </div>

      <div className="m-grid2">
        <div className="m-card">
          <div className="m-card-label">IDENTITY & GRAIN — what it is, what one row is</div>
          <KV rows={[
            ["business unit", biz.business_unit
              ? <span>{biz.business_unit} <span className="m-muted">· MDM pipeline</span></span>
              : <span className="m-muted">not on record in MDM</span>],
            ["category", [id.data_category, id.data_sub_category]
              .filter(Boolean).join(" › ")],
            ["layer · type", [id.layer_type, id.table_type, id.object_type]
              .filter(Boolean).join(" · ")],
            ["primary key", (facts?.primary_key ?? []).length
              ? <span className="m-mono">{(facts?.primary_key ?? []).join(", ")}
                  <span className="m-muted"> · declared constraint</span></span>
              : (ops.primary_key_atlas ?? []).length
                ? <span className="m-mono">{(ops.primary_key_atlas ?? []).join(", ")}
                    <span className="m-muted"> · atlas only, not declared in BigQuery</span></span>
                : <span className="m-muted">none declared</span>],
            ["partitioned by", (ops.partition_columns ?? ops.partition_columns_atlas ?? []).length
              ? <span className="m-mono">{(ops.partition_columns ?? ops.partition_columns_atlas ?? []).join(", ")}
                  {ops.partition_latest && <span className="m-muted"> · latest {ops.partition_latest}</span>}
                  {ops.n_partitions !== undefined && <span className="m-muted"> · {ops.n_partitions} partitions</span>}</span>
              : id.is_partitioned_atlas === false ? "not partitioned"
                : <span className="m-muted">unknown</span>],
            ["load", id.load_type],
            ["size", ops.total_rows !== undefined || ops.size_bytes !== undefined
              ? `${ops.total_rows !== undefined ? `≈ ${ops.total_rows.toLocaleString()} rows` : "rows unknown"} · ${bytes(ops.size_bytes)}`
              : undefined],
            ["schema", id.schema_fingerprint
              ? <span className="m-mono">{id.schema_fingerprint}</span> : undefined],
          ]} />
        </div>

        <div className="m-card">
          <div className="m-card-label">TRUST & OPERATIONS — should you believe it, can you afford it</div>
          {trust.answerability && Object.keys(trust.answerability).length > 0 ? (
            <div className="m-chips">
              {Object.entries(trust.answerability).map(([k, v]) => (
                <span key={k}
                  className={`m-chip ${v === "strong" ? "m-t-gr" : v === "weak" ? "m-t-gu" : "m-t-in"}`}>
                  {k} {v}
                </span>
              ))}
              <span className="m-muted">answerability · MDM</span>
            </div>
          ) : (
            <span className="m-muted">no answerability profile from MDM</span>
          )}
          <KV rows={[
            ["last modified", ops.last_modified],
            ["created", ops.created],
            ["feed", [ops.feed_type, ops.pipeline_name && `(${ops.pipeline_name})`,
              ops.source_system && `from ${ops.source_system}`].filter(Boolean).join(" ")],
            ["environment", ops.environment],
            ["cost prior", ops.cost_prior
              ? <span>p50 <span className="m-mono">{bytes(ops.cost_prior.p50_bytes)}</span>
                  {" · p95 "}<span className="m-mono">{bytes(ops.cost_prior.p95_bytes)}</span>
                  <span className="m-muted"> per query over {ops.cost_prior.n_jobs ?? "?"} jobs · 30-day activity</span></span>
              : <span className="m-muted">no 30-day activity on record</span>],
            ["usage rhythm", (ops.usage_rhythm ?? []).join(" · ")],
            ["atlas flags", (["is_active_atlas", "is_latest_atlas", "is_lineage_exist_atlas"] as const)
              .filter((k) => trust[k] !== undefined)
              .map((k) => `${k.replace("is_", "").replace("_atlas", "").replace("_exist", " declared")}: ${trust[k] ? "yes" : "NO"}`)
              .join(" · ")],
          ]} />
        </div>
      </div>

      <div className="m-grid2">
        <div className="m-card">
          <div className="m-card-label">WHO — owners, units, and who actually runs queries</div>
          {owners.length ? owners.map((o) => (
            <div key={o.owner} className="m-joinrow">
              <span className="m-mono">{o.owner}</span>
              {o.roles.map((r) => <span key={r} className="m-chip m-chip-soft">{r}</span>)}
              <span className="m-muted">says {o.witnesses.join(" + ")}</span>
            </div>
          )) : <span className="m-muted">no owner on record in Atlas or MDM</span>}
          {(biz.lobs ?? []).map((l) => (
            <div key={l.code} className="m-joinrow">
              <button className="m-linklike" onClick={() => onLob(l.code)}>
                {l.code}{l.name ? ` — ${l.name}` : ""}
              </button>
              <span className="m-muted">
                line of business · {Object.entries(l.witnesses)
                  .map(([w, n]) => `${w}${n > 1 ? ` ×${n}` : ""}`).join(", ")}
              </span>
            </div>
          ))}
          {(biz.used_by ?? []).map((u) => (
            <div key={u.code} className="m-joinrow">
              <button className="m-linklike" onClick={() => onLob(u.code)}>
                {u.code}{u.name ? ` — ${u.name}` : ""}
              </button>
              <span className="m-muted">
                runs queries here · {u.support} mined patterns
                {u.parent && ` · under ${u.parent}`}
              </span>
            </div>
          ))}
          {(biz.top_users ?? []).length > 0 && (
            <div className="m-muted">
              top users (30d): {(biz.top_users ?? []).slice(0, 4)
                .map((u) => `${u.user} ×${u.queries}`).join(" · ")}
            </div>
          )}
        </div>

        <div className="m-card">
          <div className="m-card-label">ACCESS — whether you may</div>
          {access.restricted === "unknown_policy" ? (
            <div className="m-warn">
              ⊘ row-access policy UNKNOWN (listing denied) — live execution
              refuses this table until a steward resolves it
            </div>
          ) : access.restricted ? (
            <div className="m-warn">row-access policy: {access.restricted}</div>
          ) : (
            <div className="m-muted">no row-access policy on record</div>
          )}
          <div className="m-chips">
            {([["has_pii_atlas", "PII"], ["has_gdpr_atlas", "GDPR"],
              ["has_oncop_atlas", "ONCOP"]] as const).map(([k, label]) =>
              access[k] === undefined ? null : (
                <span key={k} className={`m-chip ${access[k] ? "m-chip-pii" : "m-chip-soft"}`}>
                  {label} {access[k] ? "yes" : "no"}
                </span>
              ))}
            {Object.entries(access.policies ?? {}).map(([p, ws]) => (
              <span key={p} className="m-chip m-chip-soft">
                policy {p} · {ws.join("+")}
              </span>
            ))}
            {access.has_pii_atlas === undefined && !Object.keys(access.policies ?? {}).length && (
              <span className="m-muted">no compliance flags from Atlas</span>
            )}
          </div>
          {(access.sensitive_columns ?? []).length > 0 ? (
            <div>
              sensitive columns:{" "}
              {(access.sensitive_columns ?? []).map((c) => (
                <span key={c.name} className="m-chip m-chip-pii m-mono">
                  {c.name}{(c.pii_role || c.sde_group) &&
                    ` ${[c.pii_role, c.sde_group].filter(Boolean).join("/")}`}
                </span>
              ))}
              <span className="m-muted"> · union-most-restrictive</span>
            </div>
          ) : (
            <span className="m-muted">no sensitive columns flagged</span>
          )}
        </div>
      </div>

      <div className="m-card">
        <div className="m-card-label">
          COLUMNS — {columns.length}, in schema order · as BigQuery can serve them
        </div>
        {columns.length === 0 && (
          <span className="m-muted">no servable columns on record</span>
        )}
        {columns.map((c) => <ColumnRow key={c.name} c={c} physical={physical} />)}
        {(facts?.omitted_catalog_only ?? []).length > 0 && (
          <div className="m-muted">
            omitted catalog-only columns (D1, not servable):{" "}
            <span className="m-mono">{(facts?.omitted_catalog_only ?? []).join(", ")}</span>
          </div>
        )}
      </div>

      <div className="m-grid2">
        <div className="m-card">
          <div className="m-card-label">
            JOINS & LINEAGE — how, with evidence, never by vibes
          </div>
          {(joins.declared ?? []).map((fk, i) => (
            <div key={`d${i}`} className="m-joinrow">
              <span className="m-mono">{fk.column}</span>
              <span>→</span>
              <button className="m-linklike m-mono" onClick={() => onTable(fk.ref_table)}>
                {fk.ref_table}
              </button>
              <span className="m-mono">.{fk.ref_column}</span>
              <span className="m-chip m-t-ha">● declared constraint</span>
            </div>
          ))}
          {(detail.joins ?? []).filter((j) => j.source !== "constraints").map((j, i) => (
            <div key={i} className="m-joinrow">
              <button className="m-linklike m-mono"
                onClick={() => onTable(j.a === physical ? j.b : j.a)}>
                {j.a === physical ? j.b : j.a}
              </button>
              <span className="m-chip">{j.source}</span>
              {j.scope === "scoped_only" && (
                <span className="m-warn">◐ CTE-scoped — NOT raw-safe</span>
              )}
              {j.on && (
                <span className="m-mono m-muted">
                  on {Array.isArray(j.on) ? j.on.join(" AND ") : j.on}
                </span>
              )}
            </div>
          ))}
          {!(joins.declared ?? []).length && !(detail.joins ?? []).length && (
            <span className="m-muted">
              no join evidence on record — co-usage alone is not a join
            </span>
          )}
          <KV rows={[
            ["upstream", (lineage.upstream ?? []).length
              ? <span className="m-mono">{(lineage.upstream ?? []).join(", ")}</span> : undefined],
            ["downstream", (lineage.downstream ?? []).length
              ? <span className="m-mono">{(lineage.downstream ?? []).join(", ")}</span> : undefined],
            ["computed columns", (lineage.derived_columns ?? []).join(", ")],
            ["view definition", lineage.view_sql ? "retained as a doc node" : undefined],
          ]} />
        </div>

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
      </div>

      <div className="m-card">
        <div className="m-card-label">
          VOCABULARY — the jargon in these column names, scoped to{" "}
          {(biz.business_units ?? []).join(", ") || "All"}
        </div>
        {(facts?.vocabulary ?? []).length === 0 ? (
          <span className="m-muted">
            no acronym or business term matches a column name here —
            search_semantics(kind=vocab) still serves the whole glossary
          </span>
        ) : (
          <div className="m-vocab">
            {(facts?.vocabulary ?? []).map((v, i) => (
              <div key={i} style={{ display: "contents" }}>
                <span className="m-mono">{v.symbol}</span>
                <span>
                  {v.definition || <span className="m-muted">no definition on record</span>}
                  {v.kind === "term" && (
                    <span className="m-chip m-chip-soft">
                      Atlas term{v.status ? ` · ${v.status}` : ""}
                    </span>
                  )}
                  <span className="m-muted"> → {v.columns.join(", ")}</span>
                </span>
                <span className="m-muted">{v.bu}{v.region && v.region.toLowerCase() !== "all" ? `/${v.region}` : ""}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {detail.card && (
        <div className="m-card">
          <div className="m-card-label">THE SERVED CARD — what the agent reads, rendered from the same facts</div>
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

/* ── Business-unit Profile: the shelf above the tables ─────── */

function LobProfile({ code, onBack, onTable }: {
  code: string; onBack: () => void; onTable: (t: string) => void;
}) {
  const [detail, setDetail] = useState<LobDetail | Unavailable | null>(null);
  useEffect(() => { meridian.lobDetail(code).then(setDetail); }, [code]);

  if (!detail) return <div className="m-page"><p className="m-muted">loading…</p></div>;
  if (!detail.available)
    return <div className="m-page"><div className="m-card m-empty"><p>{detail.reason}</p></div></div>;
  if (!detail.found || !detail.lob)
    return (
      <div className="m-page">
        <div className="m-card m-empty">
          <p>business unit {code} is not mapped in the promoted build</p>
          <button className="m-door" onClick={onBack}>← back</button>
        </div>
      </div>
    );
  const l = detail.lob;
  return (
    <div className="m-page">
      <div className="m-masthead">
        <span className="m-wordmark">LUMI</span>
        <span className="m-tagline">
          <button className="m-linklike" onClick={onBack}>Semantics</button>
          {" ›"} {l.kind === "org_unit" ? "Org Unit" : "Business Unit"}
        </span>
      </div>

      <div className="m-card">
        <div className="m-profile-head">
          <span className="m-profile-title">{l.code}</span>
          {l.name && <span className="m-profile-title">{l.name}</span>}
          <span className="m-chip m-t-ha">● steward-mapped</span>
          {l.parent && <span className="m-muted">org unit under {l.parent}</span>}
        </div>
        <KV rows={[
          ["readiness", l.readiness
            ? <span><span className="m-mono">{l.readiness.pct}%</span>
                <span className="m-muted"> · {l.readiness.witnessed} of {l.readiness.tables} tables carry a witnessed metric</span></span>
            : <span className="m-muted">no steward-mapped tables — a usage-only unit</span>],
          ["metric domains", (l.domains ?? []).join(", ")],
          ["usage", l.usage_support
            ? `${l.usage_support} mined patterns run by this unit across ${(l.used_tables ?? []).length} table(s)`
            : undefined],
          ["vocabulary", l.vocabulary_entries
            ? `${l.vocabulary_entries} Acropedia entries scoped to ${l.code}` : undefined],
        ]} />
      </div>

      <div className="m-card">
        <div className="m-card-label">TABLES — the shelf</div>
        {l.tables.length === 0 && (
          <span className="m-muted">none steward-mapped; see the tables it queries below</span>
        )}
        {l.tables.map((t) => (
          <div key={t.physical} className="m-colrow">
            <div className="m-col-name">
              <button className="m-linklike m-mono" onClick={() => onTable(t.physical)}>
                {t.physical}
              </button>
              {t.business_name && <span className="m-col-bname">{t.business_name}</span>}
              <span className="m-chips">
                {t.tier && (
                  <span className={`m-chip m-t-${t.tier}`}>
                    {TIER_GLYPH[t.tier as keyof typeof TIER_GLYPH]?.glyph ?? "○"}
                  </span>
                )}
                {t.lifecycle && <span className="m-chip m-chip-soft">{t.lifecycle}</span>}
                {t.pii && <span className="m-chip m-chip-pii">⊘ PII</span>}
                {t.business_unit && t.business_unit !== l.code && (
                  <span className="m-chip m-chip-soft">MDM unit {t.business_unit}</span>
                )}
              </span>
            </div>
            <div className="m-col-meta">
              {t.description
                ? <span style={{ color: "var(--ink-2)" }}>{t.description}</span>
                : <span className="m-muted">no purpose on record</span>}
            </div>
            <div className="m-col-texture">{t.metrics_here ?? 0} metrics</div>
          </div>
        ))}
      </div>

      <div className="m-grid2">
        <div className="m-card">
          <div className="m-card-label">QUERIES THESE TABLES — usage, not ownership</div>
          {(l.used_tables ?? []).length === 0 && (
            <span className="m-muted">no mined usage attributed to this unit</span>
          )}
          {(l.used_tables ?? []).map((p) => (
            <div key={p} className="m-joinrow">
              <button className="m-linklike m-mono" onClick={() => onTable(p)}>{p}</button>
            </div>
          ))}
        </div>
        <div className="m-card">
          <div className="m-card-label">OWNERS — across its tables</div>
          {(l.owners ?? []).length === 0 && (
            <span className="m-muted">no owner on record</span>
          )}
          {(l.owners ?? []).map((o) => (
            <div key={o.owner} className="m-joinrow">
              <span className="m-mono">{o.owner}</span>
              {o.roles.map((r) => <span key={r} className="m-chip m-chip-soft">{r}</span>)}
            </div>
          ))}
        </div>
      </div>

      {detail.card && (
        <div className="m-card">
          <div className="m-card-label">THE SERVED CARD — what the agent reads first for this unit</div>
          <pre className="m-diff">{detail.card}</pre>
        </div>
      )}

      <div className="m-legend">
        <span className="m-spacer" />
        <Feedback screen="lob_profile" objectId={`lob:${l.code.toLowerCase()}`} />
      </div>
    </div>
  );
}
