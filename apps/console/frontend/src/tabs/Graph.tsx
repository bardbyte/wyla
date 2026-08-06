/** Context graph — the constellation, full viewport.
 *
 * The CALM surface: exploration, not the live feed. The agent's
 * traversal animates only in Ask's side panel; here the sky holds
 * still for reading, anticipation answers your own typing, and the
 * gold ceremony marks real tier changes. Selecting a node slides a
 * paper panel over the space:
 *   table  → Insights (description, witnessed relationships, questions)
 *   metric → where it is computed from, and its RELATED METRICS across
 *            those tables — the cross-table metric picture, explained
 *   other  → the essentials + Evidence / Ask
 * Everything deeper (the witness arithmetic) stays one click away in
 * the evidence dossier.
 */

import { useEffect, useMemo, useState } from "react";
import { SpaceCanvas } from "../components/SpaceCanvas";
import { SpaceAskBar } from "../components/SpaceBits";
import { WitnessDrawer } from "../components/WitnessDrawer";
import { SourceBadge, Spinner, TierChip } from "../components/ui";
import { computeListening } from "../lib/anticipation";
import { api } from "../lib/api";
import { COMMON, ENTITY, GRAPH as C } from "../lib/copy";
import { useNav } from "../lib/nav";
import type {
  GraphMap, GraphMapNode, TableInsights, Witness,
} from "../lib/types";

const HOP_ICON: Record<string, string> = {
  table: "▤", entity: "◆", column: "▦", join: "⋈", metric: "∑", skill: "❖",
};

export function GraphTab() {
  const nav = useNav();
  const [map, setMap] = useState<GraphMap | null>(null);
  const [live, setLive] = useState(false);
  const [inspect, setInspect] = useState<string | null>(null);
  const [sel, setSel] = useState<string | null>(null);
  const [spaceQ, setSpaceQ] = useState("");
  const [goldenLine, setGoldenLine] = useState(false);
  // first light (1a) plays once per session
  const [intro] = useState(() => {
    try {
      if (sessionStorage.getItem("synapse-firstlight")) return false;
      sessionStorage.setItem("synapse-firstlight", "1");
      return true;
    } catch { return false; }
  });

  useEffect(() => {
    api.graphMap().then((d) => { setMap(d.map); setLive(d.live); })
      .catch(() => setMap(null));
  }, []);

  // an anchor handed in from another tab selects its node on the map
  useEffect(() => {
    if (!nav.graphAnchor || !map) return;
    const want = nav.graphAnchor.toLowerCase();
    const node = map.nodes.find((n) =>
      n.label.toLowerCase() === want ||
      n.label.toLowerCase().endsWith("." + want));
    if (node) setSel(node.id);
    nav.clearGraphAnchor();
  }, [nav, nav.graphAnchor, map]);

  const anticipation = useMemo(
    () => computeListening(spaceQ, map), [spaceQ, map]);

  // the signature ceremony: gold ignition + the quiet line, then a map
  // refetch so the node's REAL new tier paints the sky
  useEffect(() => {
    if (!nav.ceremony) return;
    setGoldenLine(true);
    const t1 = setTimeout(() => setGoldenLine(false), 5200);
    const t2 = setTimeout(() => {
      api.graphMap().then((d) => setMap(d.map)).catch(() => undefined);
      nav.setCeremony(null);
    }, 5600);
    return () => { clearTimeout(t1); clearTimeout(t2); };
    /* eslint-disable-next-line react-hooks/exhaustive-deps */
  }, [nav.ceremony?.at]);

  const selNode = sel && map
    ? map.nodes.find((n) => n.id === sel) ?? null : null;

  return (
    <div className="space-page">
      {map === null && (
        <div className="space-page-center"><Spinner /></div>
      )}
      {map !== null && map.nodes.length === 0 && (
        <div className="space-page-center">
          <div className="empty">{live ? C.mapEmpty : COMMON.noGraphSub}</div>
        </div>
      )}
      {map !== null && map.nodes.length > 0 && (
        <SpaceCanvas map={map} fill
          selected={sel} onSelect={setSel}
          listening={anticipation.ids}
          intro={intro} introLine={C.firstLight}
          ceremonyRef={nav.ceremony?.ref ?? null}
          overlay={
            <>
              <header className="space-page-head">
                <span className="sph-title">{C.title}</span>
                <SourceBadge live={live} />
              </header>
              {goldenLine && (
                <div className="sp-goldenline" aria-hidden>
                  {C.goldenLine}
                </div>
              )}
              {!selNode && (
                <SpaceAskBar value={spaceQ} onChange={setSpaceQ}
                  onSubmit={(t) => {
                    nav.askAbout(t, { send: true });
                    setSpaceQ("");
                  }}
                  placeholder={C.askSpace} tally={anticipation} />
              )}
              {selNode && (
                <aside className="space-panel" role="dialog"
                  aria-label={selNode.label}>
                  <button type="button" className="space-panel-close"
                    onClick={() => setSel(null)}
                    aria-label={COMMON.close}>✕</button>
                  {selNode.kind === "table" && (
                    <InsightsPanel table={selNode.label}
                      onInspect={setInspect}
                      onSelectTable={(t) => {
                        const node = map.nodes.find((n) =>
                          n.label.toLowerCase() === t.toLowerCase());
                        if (node) setSel(node.id);
                      }} />
                  )}
                  {selNode.kind === "metric" && (
                    <MetricPanel metric={selNode} map={map}
                      onInspect={setInspect}
                      onSelect={(id) => setSel(id)} />
                  )}
                  {selNode.kind !== "table" && selNode.kind !== "metric" && (
                    <div className="panel-body">
                      <span className="panel-eyebrow">
                        {C.kinds[selNode.kind] ?? selNode.kind}
                      </span>
                      <h2 className="panel-title">{selNode.label}</h2>
                      <TierChip tier={selNode.tier}
                        onClick={() => setInspect(selNode.id)} />
                      <div className="panel-actions">
                        <button type="button" className="btn quiet"
                          onClick={() => setInspect(selNode.id)}>
                          {ENTITY.evidence}
                        </button>
                        <button type="button" className="btn quiet"
                          onClick={() => nav.askAbout(
                            ENTITY.askPrefix + selNode.label)}>
                          {ENTITY.ask}
                        </button>
                      </div>
                    </div>
                  )}
                </aside>
              )}
            </>
          } />
      )}
      {inspect && (
        <WitnessDrawer refUri={inspect} onClose={() => setInspect(null)} />
      )}
    </div>
  );
}

/* ── metric panel: where it comes from, and its siblings across
      tables — the cross-table metric picture, explained ───────── */

function MetricPanel({ metric, map, onInspect, onSelect }: {
  metric: GraphMapNode;
  map: GraphMap;
  onInspect: (ref: string) => void;
  onSelect: (id: string) => void;
}) {
  const nav = useNav();
  const [w, setW] = useState<Witness | null>(null);

  useEffect(() => {
    setW(null);
    api.witness(metric.id).then((d) => setW(d.witness))
      .catch(() => setW(null));
  }, [metric.id]);

  const nodesById = useMemo(
    () => new Map(map.nodes.map((n) => [n.id, n])), [map]);
  const sourceTables = useMemo(() =>
    map.edges
      .filter((e) => e.kind === "computed_from" && e.source === metric.id)
      .map((e) => nodesById.get(e.target))
      .filter((n): n is GraphMapNode => Boolean(n)),
    [map, metric.id, nodesById]);
  const related = useMemo(() => {
    const tableIds = new Set(sourceTables.map((t) => t.id));
    const byMetric = new Map<string, Set<string>>();
    for (const e of map.edges) {
      if (e.kind !== "computed_from" || e.source === metric.id) continue;
      if (!tableIds.has(e.target)) continue;
      const s = byMetric.get(e.source) ?? new Set<string>();
      s.add(e.target);
      byMetric.set(e.source, s);
    }
    return [...byMetric.entries()]
      .map(([id, tabs]) => ({
        node: nodesById.get(id),
        tables: [...tabs].map((t) => nodesById.get(t)?.label ?? t),
      }))
      .filter((r): r is { node: GraphMapNode; tables: string[] } =>
        Boolean(r.node));
  }, [map, metric.id, sourceTables, nodesById]);

  const formula = String(
    (w?.properties as Record<string, unknown> | undefined)?.formula_sql
    ?? "");
  const description = String(
    (w?.properties as Record<string, unknown> | undefined)?.description
    ?? "");

  return (
    <div className="panel-body">
      <span className="panel-eyebrow">{C.kinds.metric}</span>
      <h2 className="panel-title">{metric.label}</h2>
      <TierChip tier={metric.tier} onClick={() => onInspect(metric.id)} />

      {description && <p className="panel-desc">{description}</p>}
      {formula && <code className="panel-formula">{formula}</code>}

      <div className="h-section" style={{ marginTop: "var(--s-4)" }}>
        {C.metricFrom}
      </div>
      {sourceTables.length === 0 && (
        <p className="panel-muted">{C.metricNoSource}</p>
      )}
      <div className="panel-chips">
        {sourceTables.map((t) => (
          <button key={t.id} type="button" className="panel-chip"
            onClick={() => onSelect(t.id)}>
            {HOP_ICON.table} {t.label}
          </button>
        ))}
      </div>

      <div className="h-section" style={{ marginTop: "var(--s-4)" }}>
        {C.metricRelated}
      </div>
      {related.length === 0 && (
        <p className="panel-muted">{C.metricNoRelated}</p>
      )}
      {related.map((r) => (
        <button key={r.node.id} type="button" className="panel-row"
          onClick={() => onSelect(r.node.id)}>
          <span className="pr-name">{HOP_ICON.metric} {r.node.label}</span>
          <span className="pr-via">via {r.tables.join(", ")}</span>
          <TierChip tier={r.node.tier} />
        </button>
      ))}

      <div className="panel-actions">
        <button type="button" className="btn quiet"
          onClick={() => onInspect(metric.id)}>{ENTITY.evidence}</button>
        <button type="button" className="btn quiet"
          onClick={() => nav.askAbout(
            `What does ${metric.label} mean, exactly?`, { send: true })}>
          {ENTITY.ask}
        </button>
      </div>
    </div>
  );
}

/* ── the insights panel (per-table, catalog-style with receipts) ── */

function InsightsPanel({ table, onInspect, onSelectTable }: {
  table: string;
  onInspect: (ref: string) => void;
  onSelectTable: (table: string) => void;
}) {
  const nav = useNav();
  const [ins, setIns] = useState<TableInsights | null>(null);

  useEffect(() => {
    setIns(null);
    api.graphInsights(table)
      .then((d) => setIns(d.insights))
      .catch(() => setIns(null));
  }, [table]);

  return (
    <div className="panel-body insights">
      <span className="panel-eyebrow">{C.insightsTitle}</span>
      <h2 className="panel-title">{table}</h2>
      {ins === null && <Spinner />}
      {ins !== null && !ins.found && (
        <div className="empty">{C.pickerEmpty}</div>
      )}
      {ins !== null && ins.found && (
        <>
          <div className="ins-stats" style={{ marginBottom: "var(--s-3)" }}>
            {ins.description.tier && (
              <TierChip tier={ins.description.tier}
                onClick={() => ins.ref && onInspect(ins.ref)} />
            )}
            <span className="score"><b>{ins.columns.count ?? 0}</b>{C.insightsColumns}</span>
            <span className="score"><b>{ins.columns.described ?? 0}</b>{C.insightsDescribed}</span>
            <span className="score"><b>{ins.columns.pii ?? 0}</b>{C.insightsPii}</span>
          </div>
          {ins.description.curated && (
            <p className="ins-curated">{ins.description.curated}</p>
          )}
          {ins.description.ai && (
            <p className="ins-ai">
              <span className="tag">{C.insightsAiTag}</span>{" "}
              {ins.description.ai}
            </p>
          )}
          {!ins.description.curated && !ins.description.ai && (
            <p className="panel-muted">{C.insightsNoDesc}</p>
          )}

          <div className="h-section" style={{ margin: "var(--s-4) 0 var(--s-2)" }}>
            {C.insightsRels}
          </div>
          {ins.relationships.length === 0 && (
            <p className="panel-muted">{C.insightsNoRels}</p>
          )}
          {ins.relationships.map((r, i) => (
            <div key={i} className="panel-rel">
              <span className="pr-kind">
                {HOP_ICON[r.kind === "dq" ? "column"
                  : r.kind === "lineage" ? "join" : r.kind] ?? "•"}{" "}
                {r.kind}
              </span>
              <span className="pr-pred">{r.predicate}</span>
              <span className="pr-meta">
                <span className={`witness-chip w-${r.witness.replace(/[^a-z]+/gi, "-").toLowerCase()}`}>
                  {r.witness}
                </span>
                <TierChip tier={r.tier} onClick={
                  r.kind === "join"
                    ? () => onSelectTable(r.other)
                    : () => onInspect(r.other_ref)} />
              </span>
            </div>
          ))}

          {ins.recommendations.length > 0 && (
            <>
              <div className="h-section" style={{ margin: "var(--s-4) 0 var(--s-2)" }}>
                {C.insightsRecs}
              </div>
              <div className="suggest">
                {ins.recommendations.map((r) => (
                  <button key={r.question} type="button"
                    onClick={() => nav.askAbout(r.question)}>
                    {r.question}
                  </button>
                ))}
              </div>
            </>
          )}
        </>
      )}
    </div>
  );
}
