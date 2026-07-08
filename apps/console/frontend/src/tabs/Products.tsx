import { useEffect, useState } from "react";
import { WitnessDrawer } from "../components/WitnessDrawer";
import { PreviewBadge, SourceBadge, Spinner, TierChip } from "../components/ui";
import { api } from "../lib/api";
import { ENTITY, PRODUCTS } from "../lib/copy";
import { useNav } from "../lib/nav";
import type { Product } from "../lib/types";

export function ProductsTab() {
  const nav = useNav();
  const [q, setQ] = useState("");
  const [rows, setRows] = useState<Product[] | null>(null);
  const [live, setLive] = useState(false);
  const [inspect, setInspect] = useState<string | null>(null);

  useEffect(() => {
    let on = true;
    const t = window.setTimeout(() => {
      api.products(q).then((d) => {
        if (!on) return;
        setRows(d.products as Product[]);
        setLive(d.live);
      }).catch(() => on && setRows([]));
    }, q ? 200 : 0);
    return () => { on = false; window.clearTimeout(t); };
  }, [q]);

  return (
    <div className="page">
      <div className="page-inner">
        <div className="page-head">
          <div className="grow">
            <h1 className="h-page">{PRODUCTS.title}</h1>
            <p className="h-sub">{PRODUCTS.sub}</p>
          </div>
          <div style={{ display: "flex", gap: "var(--s-3)", alignItems: "center" }}>
            <SourceBadge live={live} />
            <span title={PRODUCTS.addNote} style={{ display: "inline-flex", gap: "var(--s-2)", alignItems: "center" }}>
              <button type="button" className="btn" disabled>{PRODUCTS.add}</button>
              <PreviewBadge />
            </span>
          </div>
        </div>

        <input
          className="input"
          style={{ maxWidth: 420, marginBottom: "var(--s-5)" }}
          placeholder={PRODUCTS.search}
          value={q}
          onChange={(e) => setQ(e.target.value)}
          aria-label={PRODUCTS.search}
        />

        {rows === null && <Spinner />}
        {rows !== null && rows.length === 0 && (
          <div className="empty">{PRODUCTS.empty}</div>
        )}

        <div className="grid-cards">
          {(rows ?? []).map((p) => (
            <article key={p.name} className="card card-pad product-card">
              <div className="product-head">
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div className="product-name">{p.name}</div>
                  <div style={{ display: "flex", gap: "var(--s-2)", marginTop: 6, flexWrap: "wrap" }}>
                    {p.domain && <span className="tag">{p.domain}</span>}
                    {p.owner && <span className="tag">{p.owner}</span>}
                    {p.lifecycle && <span className="tag">{p.lifecycle}</span>}
                  </div>
                </div>
                <TierChip tier={p.tier} onClick={() => setInspect(p.ref)} />
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
              <div className="scorecard" aria-label={PRODUCTS.readiness}>
                <span className="score"><b>{p.readiness.columns}</b>{PRODUCTS.columns}</span>
                <span className="score"><b>{p.readiness.meaning_pct}%</b>{PRODUCTS.meaning}</span>
                <span className="score"><b>{p.readiness.related_tables}</b>{PRODUCTS.related}</span>
                <span className="score"><b>{p.readiness.metrics}</b>{PRODUCTS.metrics}</span>
                <span className="score">
                  <b>{p.readiness.governance ? PRODUCTS.present : PRODUCTS.absent}</b>
                  {PRODUCTS.governance}
                </span>
                <span className="score">
                  <b>{p.readiness.lineage ? PRODUCTS.present : PRODUCTS.absent}</b>
                  {PRODUCTS.lineage}
                </span>
              </div>
            </article>
          ))}
        </div>
      </div>
      {inspect && <WitnessDrawer refUri={inspect} onClose={() => setInspect(null)} />}
    </div>
  );
}
