/** Knowledge graph — the whole connected picture.
 *
 * The map is the showcase: tables, the business entities they describe,
 * the metrics computed from them, and the playbooks that govern them,
 * laid out by a deterministic force simulation (no dependencies, no
 * randomness — same snapshot, same picture). Shape encodes kind so the
 * one reserved data hue never masquerades as brand or evidence.
 *
 * Two behaviors on top of the map:
 *  - LIVE TRAVERSAL: while the agent answers, the nodes and edges it
 *    touches light up and pulse (nav.activity, reported by Ask). Idle,
 *    the whole ecosystem renders calm — ask, and the synapse fires.
 *  - INSIGHTS: select a table and everything the graph knows about it
 *    renders below — description (curated vs AI), derived relationships
 *    with their WITNESSES (declared FK · analyst query log · LLM ·
 *    curated) and tier, and verified question recommendations.
 */

import { useEffect, useMemo, useState } from "react";
import type { MouseEvent as ReactMouseEvent } from "react";
import { WitnessDrawer } from "../components/WitnessDrawer";
import { SourceBadge, Spinner, TierChip } from "../components/ui";
import { api } from "../lib/api";
import { COMMON, ENTITY, GRAPH as C } from "../lib/copy";
import { useNav } from "../lib/nav";
import type {
  GraphMap, GraphMapNode, GraphSummary, TableInsights, ThreadHop, Tier,
} from "../lib/types";

const HOP_ICON: Record<string, string> = {
  table: "▤", entity: "◆", column: "▦", join: "⋈", metric: "∑", skill: "❖",
};

const shortName = (s: string) => {
  const tail = s.split(/[./]/).pop() ?? s;
  return tail.length > 22 ? tail.slice(0, 21) + "…" : tail;
};

/* Traversal highlights are PER-TURN, not time-decayed: a new question
 * clears the previous traversal (Ask calls nav.clearActivity), and the
 * touched subgraph stays lit after the answer for show-and-tell. No
 * wall-clock arithmetic — container/laptop clock steps can't kill it. */

export function GraphTab() {
  const nav = useNav();
  const [summary, setSummary] = useState<GraphSummary | null>(null);
  const [map, setMap] = useState<GraphMap | null>(null);
  const [hops, setHops] = useState<ThreadHop[] | null>(null);
  const [live, setLive] = useState(false);
  const [inspect, setInspect] = useState<string | null>(null);
  const [tables, setTables] = useState<string[]>([]);
  const [anchor, setAnchor] = useState("");
  const [sel, setSel] = useState<string | null>(null);

  useEffect(() => {
    if (nav.graphAnchor) {
      setAnchor(nav.graphAnchor);
      nav.clearGraphAnchor();
    }
  }, [nav, nav.graphAnchor]);

  useEffect(() => {
    api.graphSummary().then((d) => {
      setSummary(d.summary as GraphSummary);
      setLive(d.live);
    }).catch(() => setSummary(null));
    api.graphMap().then((d) => setMap(d.map)).catch(() => setMap(null));
    api.products().then((d) =>
      setTables((d.products as { name: string }[]).map((p) => p.name)),
    ).catch(() => undefined);
  }, []);

  useEffect(() => {
    setHops(null);
    api.graphThread(anchor)
      .then((d) => setHops(d.thread.hops))
      .catch(() => setHops([]));
  }, [anchor]);

  // an anchor handed in from another tab selects its node on the map
  useEffect(() => {
    if (!anchor || !map) return;
    const want = anchor.toLowerCase();
    const node = map.nodes.find((n) =>
      n.label.toLowerCase() === want ||
      n.label.toLowerCase().endsWith("." + want));
    if (node) setSel(node.id);
  }, [anchor, map]);

  const witnesses = summary ? Object.entries(summary.witnesses) : [];
  const maxW = witnesses.reduce((m, [, n]) => Math.max(m, n), 1);
  const selNode = sel && map
    ? map.nodes.find((n) => n.id === sel) ?? null : null;

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

        {/* ── the whole picture ── */}
        <section className="card card-pad" style={{ marginBottom: "var(--s-5)" }}>
          <div style={{ display: "flex", alignItems: "baseline", gap: "var(--s-3)", flexWrap: "wrap" }}>
            <div className="h-section">{C.mapTitle}</div>
            {nav.agentBusy && (
              <span className="activity-live">
                <span className="live-dot" aria-hidden /> {C.liveNow}
              </span>
            )}
          </div>
          <p style={{ color: "var(--ink-2)", margin: "var(--s-2) 0 var(--s-4)", maxWidth: "60ch" }}>
            {C.mapSub}
          </p>
          {map === null && <Spinner />}
          {map !== null && map.nodes.length === 0 && (
            <div className="empty">
              {live ? C.mapEmpty : COMMON.noGraphSub}
            </div>
          )}
          {map !== null && map.nodes.length > 0 && (
            <GraphCanvas map={map} activity={nav.activity}
              selected={sel} onSelect={setSel}
              onExplore={(t) => setAnchor(t)}
              onAsk={(t) => nav.askAbout(ENTITY.askPrefix + t)}
              onEvidence={(ref) => setInspect(ref)} />
          )}
          {map?.truncated && (
            <p style={{ color: "var(--ink-3)", fontSize: "var(--fs-12)", marginTop: "var(--s-2)" }}>
              {C.truncatedNote}
            </p>
          )}
        </section>

        {/* ── insights for the selected table ── */}
        {selNode && selNode.kind === "table" && (
          <InsightsPanel table={selNode.label}
            onInspect={setInspect}
            onSelectTable={(t) => {
              const node = map?.nodes.find((n) =>
                n.label.toLowerCase() === t.toLowerCase());
              if (node) setSel(node.id);
            }} />
        )}

        {/* ── what the graph knows · where facts come from ── */}
        <div className="two-col" style={{ marginBottom: "var(--s-5)" }}>
          <section className="card card-pad">
            <div className="h-section" style={{ marginBottom: "var(--s-3)" }}>
              {C.statsTitle}
            </div>
            {!summary && <Spinner />}
            {summary && (
              <>
                <div className="stat-tiles" style={{ marginBottom: "var(--s-4)" }}>
                  <div className="stat-tile card" style={{ boxShadow: "none" }}>
                    <div className="n">{summary.nodes.toLocaleString()}</div>
                    <div className="l">{C.nodes}</div>
                  </div>
                  <div className="stat-tile card" style={{ boxShadow: "none" }}>
                    <div className="n">{summary.edges.toLocaleString()}</div>
                    <div className="l">{C.edges}</div>
                  </div>
                </div>
                <div className="h-section" style={{ marginBottom: "var(--s-2)" }}>
                  {C.tierLegend}
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: "var(--s-2)" }}>
                  {(["human_asserted", "grounded", "inferred", "guessed"] as Tier[])
                    .filter((t) => summary.tiers[t])
                    .map((t) => (
                      <div key={t} style={{ display: "flex", gap: "var(--s-3)", alignItems: "center" }}>
                        <TierChip tier={t} />
                        <span style={{ color: "var(--ink-2)", fontSize: "var(--fs-12)" }}>
                          {summary.tiers[t].toLocaleString()} facts
                        </span>
                      </div>
                    ))}
                </div>
              </>
            )}
          </section>

          <section className="card card-pad">
            <div className="h-section">{C.witnessTitle}</div>
            <p style={{ color: "var(--ink-2)", fontSize: "var(--fs-13)", margin: "var(--s-2) 0 var(--s-4)" }}>
              {C.witnessSub}
            </p>
            <div style={{ display: "flex", flexDirection: "column", gap: "var(--s-2)" }}>
              {witnesses.map(([name, n]) => (
                <div key={name} className="bar-row">
                  <span style={{ color: "var(--ink-2)" }}>{name}</span>
                  <div className="bar-track">
                    <div className="bar-fill" style={{ width: `${(n / maxW) * 100}%` }} />
                  </div>
                  <span style={{ color: "var(--ink-3)", textAlign: "right" }}>
                    {n.toLocaleString()}
                  </span>
                </div>
              ))}
            </div>
          </section>
        </div>

        {/* ── one thread, end to end ── */}
        <section className="card card-pad">
          <div style={{ display: "flex", alignItems: "center", gap: "var(--s-4)", flexWrap: "wrap" }}>
            <div style={{ flex: 1, minWidth: 240 }}>
              <div className="h-section">{C.storyTitle}</div>
              <p style={{ color: "var(--ink-2)", margin: "var(--s-2) 0 var(--s-3)" }}>
                {C.storySub}
              </p>
            </div>
            <label style={{ display: "flex", alignItems: "center", gap: "var(--s-2)", fontSize: "var(--fs-12)", color: "var(--ink-2)" }}>
              {C.pickerLabel}
              <select
                className="input"
                style={{ width: "auto", fontSize: "var(--fs-13)" }}
                value={anchor}
                onChange={(e) => setAnchor(e.target.value)}
              >
                <option value="">{C.pickerDefault}</option>
                {tables.map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
            </label>
          </div>
          {hops === null && <Spinner />}
          {hops !== null && hops.length === 0 && (
            <div className="empty">{C.pickerEmpty}</div>
          )}
          <div className="thread-flow">
            {(hops ?? []).map((h, i) => (
              <span key={i} style={{ display: "contents" }}>
                {i > 0 && <span className="hop-link" aria-hidden>→</span>}
                <div className="hop card card-pad" style={{ boxShadow: "none" }}>
                  <div className="hop-kind">{HOP_ICON[h.kind] ?? "•"} {h.kind}</div>
                  <div className="hop-label">{h.label}</div>
                  {h.detail && <div className="hop-detail">{h.detail}</div>}
                  <div style={{ marginTop: "var(--s-2)", display: "flex", gap: "var(--s-2)", alignItems: "center", flexWrap: "wrap" }}>
                    <TierChip tier={h.tier} onClick={() => setInspect(h.ref)} />
                    {["table", "metric", "entity"].includes(h.kind) && (
                      <button type="button" className="btn quiet"
                        style={{ padding: "2px 8px", fontSize: "var(--fs-11)" }}
                        onClick={() => nav.askAbout(ENTITY.askPrefix + h.label)}>
                        {ENTITY.ask}
                      </button>
                    )}
                  </div>
                </div>
              </span>
            ))}
          </div>
          <p style={{ color: "var(--ink-3)", fontSize: "var(--fs-12)" }}>
            {C.openEvidence}
          </p>
        </section>
      </div>
      {inspect && <WitnessDrawer refUri={inspect} onClose={() => setInspect(null)} />}
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
    <section className="card card-pad insights" style={{ marginBottom: "var(--s-5)" }}>
      <div style={{ display: "flex", alignItems: "baseline", gap: "var(--s-3)", flexWrap: "wrap" }}>
        <div className="h-section">{C.insightsTitle}</div>
        <span className="ins-table">{table}</span>
      </div>
      {ins === null && <Spinner />}
      {ins !== null && !ins.found && (
        <div className="empty">{C.pickerEmpty}</div>
      )}
      {ins !== null && ins.found && (
        <>
          <div className="ins-cols">
            <div className="ins-desc">
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
                <p style={{ color: "var(--ink-3)" }}>{C.insightsNoDesc}</p>
              )}
            </div>
            <div className="ins-stats">
              {ins.description.tier && (
                <TierChip tier={ins.description.tier}
                  onClick={() => ins.ref && onInspect(ins.ref)} />
              )}
              <span className="score"><b>{ins.columns.count ?? 0}</b>{C.insightsColumns}</span>
              <span className="score"><b>{ins.columns.described ?? 0}</b>{C.insightsDescribed}</span>
              <span className="score"><b>{ins.columns.pii ?? 0}</b>{C.insightsPii}</span>
            </div>
          </div>

          <div className="h-section" style={{ margin: "var(--s-4) 0 var(--s-2)" }}>
            {C.insightsRels}
          </div>
          {ins.relationships.length === 0 && (
            <div className="empty">{C.insightsNoRels}</div>
          )}
          {ins.relationships.length > 0 && (
            <div className="results-wrap" style={{ margin: 0 }}>
              <table className="results-table ins-rel-table">
                <thead>
                  <tr>
                    <th>{C.insightsRelKind}</th>
                    <th>{C.insightsRelPredicate}</th>
                    <th>{C.insightsRelWitness}</th>
                    <th>{C.insightsRelTier}</th>
                  </tr>
                </thead>
                <tbody>
                  {ins.relationships.map((r, i) => (
                    <tr key={i}>
                      <td style={{ fontFamily: "var(--font-ui)" }}>
                        {HOP_ICON[r.kind === "dq" ? "column" : r.kind === "lineage" ? "join" : r.kind] ?? "•"}{" "}
                        {r.kind}
                      </td>
                      <td>{r.predicate}</td>
                      <td style={{ fontFamily: "var(--font-ui)" }}>
                        <span className={`witness-chip w-${r.witness.replace(/[^a-z]+/gi, "-").toLowerCase()}`}>
                          {r.witness}
                        </span>
                      </td>
                      <td style={{ fontFamily: "var(--font-ui)" }}>
                        <TierChip tier={r.tier} onClick={
                          r.kind === "join"
                            ? () => onSelectTable(r.other)
                            : () => onInspect(r.other_ref)} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

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
    </section>
  );
}

/* ── the whole-graph canvas ─────────────────────────────────── */

const W = 960;
const H = 560;
const PAD_X = 116;   // room for the centered labels under edge nodes
const PAD_Y = 54;

interface Pt { x: number; y: number }

/** Fruchterman–Reingold with mild centre gravity, clamped to a padded
 * frame. Deterministic: initial positions come from the node index (a
 * ring), never Math.random, so a snapshot always renders the same
 * graph. The connected core spreads by its edges; gravity keeps
 * disconnected nodes off the corners so they ring the cluster instead
 * of piling in it. ~24 spine nodes → the loop is trivially cheap. */
function layout(nodes: GraphMapNode[],
                edges: { source: string; target: string }[]): Pt[] {
  const n = nodes.length;
  if (!n) return [];
  const idx = new Map(nodes.map((nd, i) => [nd.id, i]));
  const pos: Pt[] = nodes.map((_, i) => {
    const a = (i / n) * 2 * Math.PI;
    return { x: W / 2 + Math.cos(a) * W * 0.22,
             y: H / 2 + Math.sin(a) * H * 0.22 };
  });
  const links = edges
    .map((e) => [idx.get(e.source), idx.get(e.target)] as [number?, number?])
    .filter((l): l is [number, number] => l[0] != null && l[1] != null);
  const k = Math.sqrt((W * H) / Math.max(1, n)) * 0.9;
  const iters = 400;
  for (let it = 0; it < iters; it++) {
    const disp: Pt[] = pos.map(() => ({ x: 0, y: 0 }));
    for (let a = 0; a < n; a++) {
      for (let b = a + 1; b < n; b++) {
        let dx = pos[a].x - pos[b].x;
        let dy = pos[a].y - pos[b].y;
        const d = Math.hypot(dx, dy) || 0.01;
        const f = (k * k) / d;
        dx /= d; dy /= d;
        disp[a].x += dx * f; disp[a].y += dy * f;
        disp[b].x -= dx * f; disp[b].y -= dy * f;
      }
    }
    for (const [a, b] of links) {
      let dx = pos[a].x - pos[b].x;
      let dy = pos[a].y - pos[b].y;
      const d = Math.hypot(dx, dy) || 0.01;
      const f = (d * d) / k;
      dx /= d; dy /= d;
      disp[a].x -= dx * f; disp[a].y -= dy * f;
      disp[b].x += dx * f; disp[b].y += dy * f;
    }
    const temp = (1 - it / iters) * (W * 0.045);
    for (let i = 0; i < n; i++) {
      // mild gravity toward centre keeps loose nodes off the corners
      disp[i].x += (W / 2 - pos[i].x) * 0.035;
      disp[i].y += (H / 2 - pos[i].y) * 0.035;
      const d = Math.hypot(disp[i].x, disp[i].y) || 0.01;
      pos[i].x += (disp[i].x / d) * Math.min(d, temp);
      pos[i].y += (disp[i].y / d) * Math.min(d, temp);
      pos[i].x = Math.max(PAD_X, Math.min(W - PAD_X, pos[i].x));
      pos[i].y = Math.max(PAD_Y, Math.min(H - PAD_Y, pos[i].y));
    }
  }
  return pos;
}

function Glyph({ kind, x, y, r, cls, onClick }: {
  kind: string; x: number; y: number; r: number; cls: string;
  onClick: (e: ReactMouseEvent) => void;
}) {
  const common = { className: cls, onClick,
    style: { cursor: "pointer" } as const };
  if (kind === "metric")
    return <rect x={x - r} y={y - r} width={2 * r} height={2 * r}
      rx={2} {...common} />;
  if (kind === "entity")
    return <polygon points={`${x},${y - r} ${x + r},${y} ${x},${y + r} ${x - r},${y}`}
      {...common} />;
  if (kind === "skill")
    return <polygon points={`${x},${y - r} ${x + r * 0.92},${y + r * 0.6} ${x - r * 0.92},${y + r * 0.6}`}
      {...common} />;
  return <circle cx={x} cy={y} r={r} {...common} />;
}

/** Is this node "touched" by the current turn's traversal? Matched by
 * full synapse:// id or by bare table/metric name (case-insensitive). */
function isActive(n: GraphMapNode,
                  activity: Record<string, number>): boolean {
  const label = n.label.toLowerCase();
  const bare = label.split(/[./]/).pop() ?? label;
  return Boolean(activity[n.id.toLowerCase()] ?? activity[label]
                 ?? activity[bare]);
}

export function GraphCanvas({
  map, activity = {}, selected = null, onSelect, mini = false,
  onExplore, onAsk, onEvidence,
}: {
  map: GraphMap;
  activity?: Record<string, number>;
  selected?: string | null;
  onSelect?: (id: string | null) => void;
  mini?: boolean;
  onExplore?: (table: string) => void;
  onAsk?: (label: string) => void;
  onEvidence?: (ref: string) => void;
}) {
  const [localSel, setLocalSel] = useState<string | null>(null);
  const sel = onSelect ? selected : localSel;
  const setSel = onSelect ?? setLocalSel;

  const sig = map.nodes.map((n) => n.id).join("|") + "#" + map.edges.length;
  const pos = useMemo(() => layout(map.nodes, map.edges), [sig]); // eslint-disable-line

  const idx = useMemo(
    () => new Map(map.nodes.map((n, i) => [n.id, i])), [sig]); // eslint-disable-line
  const degree = useMemo(() => {
    const d = new Map<string, number>();
    for (const e of map.edges) {
      d.set(e.source, (d.get(e.source) ?? 0) + 1);
      d.set(e.target, (d.get(e.target) ?? 0) + 1);
    }
    return d;
  }, [sig]); // eslint-disable-line

  const activeIds = useMemo(() => {
    const s = new Set<string>();
    for (const n of map.nodes) if (isActive(n, activity)) s.add(n.id);
    return s;
  }, [map, activity]);

  // neighborhood of the selected node (for dimming the rest)
  const neighbors = useMemo(() => {
    if (!sel) return null;
    const s = new Set<string>([sel]);
    for (const e of map.edges) {
      if (e.source === sel) s.add(e.target);
      if (e.target === sel) s.add(e.source);
    }
    return s;
  }, [sel, sig]); // eslint-disable-line

  const selNode = sel ? map.nodes.find((n) => n.id === sel) ?? null : null;
  const traversing = activeIds.size > 0;

  return (
    <div className={`graph-canvas-wrap ${mini ? "mini" : ""}`}>
      <svg className="graph-canvas" viewBox={`0 0 ${W} ${H}`}
        role="img" aria-label="Knowledge graph map"
        onClick={() => setSel(null)}>
        <g>
          {map.edges.map((e, i) => {
            const a = idx.get(e.source); const b = idx.get(e.target);
            if (a == null || b == null) return null;
            const hotSel = neighbors
              ? (e.source === sel || e.target === sel) : false;
            const hotAct = activeIds.has(e.source) && activeIds.has(e.target);
            const dim = (neighbors && !hotSel && !hotAct)
              || (traversing && !hotAct && !hotSel);
            return (
              <line key={i} x1={pos[a].x} y1={pos[a].y}
                x2={pos[b].x} y2={pos[b].y}
                className={`gedge ${hotSel || hotAct ? "hot" : ""} ${dim ? "dim" : ""}`} />
            );
          })}
        </g>
        <g>
          {map.nodes.map((n) => {
            const p = pos[idx.get(n.id)!];
            const r = 8 + Math.min(10, (degree.get(n.id) ?? 0) * 1.6);
            const isSel = n.id === sel;
            const act = activeIds.has(n.id);
            const dim = (neighbors && !neighbors.has(n.id) && !act)
              || (traversing && !act && n.id !== sel);
            const showLabel = !mini || act || isSel;
            return (
              <g key={n.id} className={`gnode-g ${dim ? "dim" : ""}`}>
                {act && (
                  <>
                    <circle cx={p.x} cy={p.y} r={r + 6}
                      className="gnode-ring" aria-hidden />
                    <circle cx={p.x} cy={p.y} r={r + 6}
                      className="gnode-halo" aria-hidden />
                  </>
                )}
                <Glyph kind={n.kind} x={p.x} y={p.y} r={r}
                  cls={`gnode k-${n.kind} ${isSel ? "sel" : ""} ${act ? "active" : ""}`}
                  onClick={(e) => {
                    e.stopPropagation();
                    setSel(isSel ? null : n.id);
                  }} />
                {showLabel && (
                  <text x={p.x} y={p.y + r + 12}
                    className={`glabel ${act ? "active" : ""} ${mini ? "mini" : ""}`}
                    textAnchor="middle">{shortName(n.label)}</text>
                )}
              </g>
            );
          })}
        </g>
      </svg>

      {!mini && (
        <div className="graph-legend" aria-hidden>
          <span><i className="lg-shape lg-table" /> {C.kinds.table}</span>
          <span><i className="lg-shape lg-entity" /> {C.kinds.entity}</span>
          <span><i className="lg-shape lg-metric" /> {C.kinds.metric}</span>
          <span><i className="lg-shape lg-skill" /> {C.kinds.skill}</span>
        </div>
      )}

      {!mini && selNode && (
        <div className="graph-inspect card">
          <div className="gi-head">
            <div>
              <div className="gi-kind">{C.kinds[selNode.kind] ?? selNode.kind}</div>
              <div className="gi-label">{shortName(selNode.label)}</div>
            </div>
            <TierChip tier={selNode.tier}
              onClick={() => onEvidence?.(selNode.id)} />
          </div>
          {selNode.kind === "table" && (
            <div className="gi-facts">
              {selNode.columns != null && <span>{selNode.columns} columns</span>}
              {selNode.business_unit && <span>{selNode.business_unit}</span>}
              {selNode.pii && <span className="gi-pii">PII</span>}
            </div>
          )}
          {selNode.subtitle && (
            <div className="gi-sub">{selNode.subtitle}</div>
          )}
          <div className="gi-actions">
            <button type="button" className="btn quiet"
              onClick={() => onEvidence?.(selNode.id)}>{ENTITY.evidence}</button>
            {selNode.kind === "table" && (
              <button type="button" className="btn quiet"
                onClick={() => onExplore?.(selNode.label)}>{ENTITY.explore}</button>
            )}
            <button type="button" className="btn quiet"
              onClick={() => onAsk?.(selNode.label)}>{ENTITY.ask}</button>
          </div>
        </div>
      )}
    </div>
  );
}
