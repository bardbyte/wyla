/** Knowledge graph — Synapse space, full size.
 *
 * The hero is the cosmos (components/SpaceCanvas): the whole estate as
 * a dark, living constellation — tables as suns orbited by their real
 * column counts, color = evidence tier, shape = kind. While the agent
 * answers, the traversal fires across it; idle, it breathes.
 *
 * Select a table and INSIGHTS renders below: description (curated vs
 * AI-labeled), column rollup, derived relationships with their
 * WITNESSES (declared FK · query log (analyst) · curated · LLM-inferred)
 * and tier, and ask-able question recommendations. Below that, the
 * evidence machinery: what the graph knows, where facts come from, and
 * the one-thread drill-down.
 */

import { useEffect, useState } from "react";
import { SpaceCanvas } from "../components/SpaceCanvas";
import { WitnessDrawer } from "../components/WitnessDrawer";
import { SourceBadge, Spinner, TierChip } from "../components/ui";
import { api } from "../lib/api";
import { COMMON, ENTITY, GRAPH as C } from "../lib/copy";
import { useNav } from "../lib/nav";
import type {
  GraphMap, GraphSummary, TableInsights, ThreadHop, Tier,
} from "../lib/types";

const HOP_ICON: Record<string, string> = {
  table: "▤", entity: "◆", column: "▦", join: "⋈", metric: "∑", skill: "❖",
};

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
  const [variant, setVariant] =
    useState<"constellation" | "orbits">("constellation");

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

        {/* ── Synapse space ── */}
        <section className="card space-hero" style={{ marginBottom: "var(--s-5)" }}>
          <div className="space-head">
            <div className="h-section">{C.mapTitle}</div>
            {nav.agentBusy && (
              <span className="activity-live">
                <span className="live-dot" aria-hidden /> {C.liveNow}
              </span>
            )}
            <div className="view-toggle" role="tablist"
              aria-label={C.viewLabel}>
              <button type="button" role="tab"
                aria-selected={variant === "constellation"}
                onClick={() => setVariant("constellation")}>
                {C.viewConstellation}
              </button>
              <button type="button" role="tab"
                aria-selected={variant === "orbits"}
                onClick={() => setVariant("orbits")}>
                {C.viewOrbits}
              </button>
            </div>
            <span style={{ marginLeft: "auto", color: "var(--ink-3)", fontSize: "var(--fs-12)" }}>
              {variant === "orbits" ? C.orbitsCaption : C.mapSub}
            </span>
          </div>
          {map === null && <div style={{ padding: "var(--s-5)" }}><Spinner /></div>}
          {map !== null && map.nodes.length === 0 && (
            <div className="empty">
              {live ? C.mapEmpty : COMMON.noGraphSub}
            </div>
          )}
          {map !== null && map.nodes.length > 0 && (
            <SpaceCanvas map={map} activity={nav.activity}
              selected={sel} onSelect={setSel} variant={variant}
              verb={nav.traversalVerb} />
          )}
          {map?.truncated && (
            <p style={{ color: "var(--ink-3)", fontSize: "var(--fs-12)", padding: "0 var(--s-5) var(--s-3)" }}>
              {C.truncatedNote}
            </p>
          )}
        </section>

        {/* ── selection: tables get Insights; the rest a slim bar ── */}
        {selNode && selNode.kind === "table" && (
          <InsightsPanel table={selNode.label}
            onInspect={setInspect}
            onSelectTable={(t) => {
              const node = map?.nodes.find((n) =>
                n.label.toLowerCase() === t.toLowerCase());
              if (node) setSel(node.id);
            }} />
        )}
        {selNode && selNode.kind !== "table" && (
          <section className="card card-pad sel-bar" style={{ marginBottom: "var(--s-5)" }}>
            <span className="h-section">{C.kinds[selNode.kind] ?? selNode.kind}</span>
            <span className="ins-table">{selNode.label}</span>
            <TierChip tier={selNode.tier} onClick={() => setInspect(selNode.id)} />
            <span style={{ flex: 1 }} />
            <button type="button" className="btn quiet"
              onClick={() => setInspect(selNode.id)}>{ENTITY.evidence}</button>
            <button type="button" className="btn quiet"
              onClick={() => nav.askAbout(ENTITY.askPrefix + selNode.label)}>
              {ENTITY.ask}
            </button>
          </section>
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
