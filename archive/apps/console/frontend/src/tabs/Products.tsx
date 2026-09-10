/** Data products — "we know your business", grouped by the business
 * units MDM governs. Each unit leads with a governance + readiness
 * rollup (tables, columns, how much is described, how much is
 * corroborated, how many carry PII, how many are governed); its tables
 * follow as scorecard cards. Graph-only; honest empty when no snapshot. */

import { useEffect, useState } from "react";
import { WitnessDrawer } from "../components/WitnessDrawer";
import { SourceBadge, Spinner, TierChip } from "../components/ui";
import { api } from "../lib/api";
import { COMMON, ENTITY, PRODUCTS as C } from "../lib/copy";
import { useNav } from "../lib/nav";
import type { Product, Unit } from "../lib/types";

export function ProductsTab() {
  const nav = useNav();
  const [q, setQ] = useState("");
  const [units, setUnits] = useState<Unit[] | null>(null);
  const [live, setLive] = useState(false);
  const [inspect, setInspect] = useState<string | null>(null);

  useEffect(() => {
    let on = true;
    const t = window.setTimeout(() => {
      api.productsByUnit(q).then((d) => {
        if (!on) return;
        setUnits(d.units as Unit[]);
        setLive(d.live);
      }).catch(() => on && setUnits([]));
    }, q ? 200 : 0);
    return () => { on = false; window.clearTimeout(t); };
  }, [q]);

  const totalTables = (units ?? []).reduce((n, u) => n + u.table_count, 0);

  return (
    <div className="page">
      <div className="page-inner">
        <div className="page-head">
          <div className="grow">
            <h1 className="h-page">{C.title}</h1>
            <p className="h-sub">{C.sub}</p>
          </div>
          <SourceBadge live={live} />
        </div>

        <input
          className="input"
          style={{ maxWidth: 420, marginBottom: "var(--s-5)" }}
          placeholder={C.search}
          value={q}
          onChange={(e) => setQ(e.target.value)}
          aria-label={C.search}
        />

        {units === null && <Spinner />}
        {units !== null && !live && (
          <div className="empty">
            <div className="empty-title">{COMMON.noGraphTitle}</div>
            <p>{C.emptyNoGraph}</p>
          </div>
        )}
        {units !== null && live && totalTables === 0 && (
          <div className="empty">{C.empty}</div>
        )}

        <div className="unit-list">
          {(units ?? []).map((u) => (
            <section key={u.unit} className="unit">
              <div className="unit-head">
                <div className="unit-name">{u.unit || C.unassigned}</div>
                <div className="unit-rollups">
                  <Rollup n={u.table_count} l={C.uTables} />
                  <Rollup n={u.total_columns} l={C.uColumns} />
                  <Rollup n={`${u.mean_meaning_pct}%`} l={C.uDescribed} />
                  <Rollup n={u.grounded_tables} l={C.uGrounded} />
                  <Rollup n={u.pii_tables} l={C.uPii}
                    tone={u.pii_tables > 0 ? "warn" : undefined} />
                  <Rollup n={u.governed_tables} l={C.uGoverned} />
                </div>
              </div>
              <div className="grid-cards">
                {u.products.map((p) => (
                  <ProductCard key={p.ref} p={p} nav={nav}
                    onInspect={setInspect} />
                ))}
              </div>
            </section>
          ))}
        </div>
      </div>
      {inspect && (
        <WitnessDrawer refUri={inspect} onClose={() => setInspect(null)} />
      )}
    </div>
  );
}

function Rollup({ n, l, tone }: {
  n: number | string; l: string; tone?: "warn";
}) {
  return (
    <span className={`rollup ${tone ?? ""}`}>
      <b>{n}</b>
      <span>{l}</span>
    </span>
  );
}

function ProductCard({ p, nav, onInspect }: {
  p: Product;
  nav: ReturnType<typeof useNav>;
  onInspect: (ref: string) => void;
}) {
  return (
    <article className="card card-pad product-card">
      <div className="product-head">
        <div style={{ flex: 1, minWidth: 0 }}>
          <div className="product-name">{p.name}</div>
          <div style={{ display: "flex", gap: "var(--s-2)", marginTop: 6, flexWrap: "wrap" }}>
            {p.owner && <span className="tag">{p.owner}</span>}
            {p.lifecycle && <span className="tag">{p.lifecycle}</span>}
          </div>
        </div>
        <TierChip tier={p.tier} onClick={() => onInspect(p.ref)} />
      </div>
      {p.description && (
        <p style={{ color: "var(--ink-2)", fontSize: "var(--fs-13)" }}>
          {p.description}
        </p>
      )}
      <div style={{ display: "flex", gap: "var(--s-2)" }}>
        <button type="button" className="btn quiet"
          onClick={() => nav.askAbout(ENTITY.askPrefix + p.name)}>
          {ENTITY.ask}
        </button>
        <button type="button" className="btn quiet"
          onClick={() => nav.goToGraph(p.name)}>
          {ENTITY.explore}
        </button>
      </div>
      <div className="scorecard" aria-label={C.meaning}>
        <span className="score"><b>{p.readiness.columns}</b>{C.columns}</span>
        <span className="score"><b>{p.readiness.meaning_pct}%</b>{C.meaning}</span>
        <span className="score"><b>{p.readiness.related_tables}</b>{C.related}</span>
        <span className="score"><b>{p.readiness.metrics}</b>{C.metrics}</span>
        <span className="score">
          <b>{p.readiness.governance ? C.present : C.absent}</b>{C.governance}
        </span>
        <span className="score">
          <b>{p.readiness.lineage ? C.present : C.absent}</b>{C.lineage}
        </span>
      </div>
    </article>
  );
}
