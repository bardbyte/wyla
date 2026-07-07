import { useEffect, useState } from "react";
import { WitnessDrawer } from "../components/WitnessDrawer";
import { SourceBadge, Spinner, TierChip } from "../components/ui";
import { api } from "../lib/api";
import { GRAPH } from "../lib/copy";
import type { GraphSummary, ThreadHop, Tier } from "../lib/types";

const HOP_ICON: Record<string, string> = {
  table: "▤", entity: "◆", column: "▦", join: "⋈", metric: "∑", skill: "❖",
};

export function GraphTab() {
  const [summary, setSummary] = useState<GraphSummary | null>(null);
  const [hops, setHops] = useState<ThreadHop[] | null>(null);
  const [live, setLive] = useState(false);
  const [inspect, setInspect] = useState<string | null>(null);
  const [tables, setTables] = useState<string[]>([]);
  const [anchor, setAnchor] = useState("");

  useEffect(() => {
    api.graphSummary().then((d) => {
      setSummary(d.summary as GraphSummary);
      setLive(d.live);
    }).catch(() => setSummary(null));
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

  const witnesses = summary ? Object.entries(summary.witnesses) : [];
  const maxW = witnesses.reduce((m, [, n]) => Math.max(m, n), 1);

  return (
    <div className="page">
      <div className="page-inner">
        <div className="page-head">
          <div className="grow">
            <h1 className="h-page">{GRAPH.title}</h1>
            <p className="h-sub">{GRAPH.sub}</p>
          </div>
          <SourceBadge live={live} />
        </div>

        <section className="card card-pad" style={{ marginBottom: "var(--s-5)" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "var(--s-4)", flexWrap: "wrap" }}>
            <div style={{ flex: 1, minWidth: 240 }}>
              <div className="h-section">{GRAPH.storyTitle}</div>
              <p style={{ color: "var(--ink-2)", margin: "var(--s-2) 0 var(--s-3)" }}>
                {GRAPH.storySub}
              </p>
            </div>
            <label style={{ display: "flex", alignItems: "center", gap: "var(--s-2)", fontSize: "var(--fs-12)", color: "var(--ink-2)" }}>
              {GRAPH.pickerLabel}
              <select
                className="input"
                style={{ width: "auto", fontSize: "var(--fs-13)" }}
                value={anchor}
                onChange={(e) => setAnchor(e.target.value)}
              >
                <option value="">{GRAPH.pickerDefault}</option>
                {tables.map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
            </label>
          </div>
          {hops === null && <Spinner />}
          {hops !== null && hops.length === 0 && (
            <div className="empty">{GRAPH.pickerEmpty}</div>
          )}
          <div className="thread-flow">
            {(hops ?? []).map((h, i) => (
              <span key={i} style={{ display: "contents" }}>
                {i > 0 && <span className="hop-link" aria-hidden>→</span>}
                <div className="hop card card-pad" style={{ boxShadow: "none" }}>
                  <div className="hop-kind">{HOP_ICON[h.kind] ?? "•"} {h.kind}</div>
                  <div className="hop-label">{h.label}</div>
                  {h.detail && <div className="hop-detail">{h.detail}</div>}
                  <div style={{ marginTop: "var(--s-2)" }}>
                    <TierChip tier={h.tier} onClick={() => setInspect(h.ref)} />
                  </div>
                </div>
              </span>
            ))}
          </div>
          <p style={{ color: "var(--ink-3)", fontSize: "var(--fs-12)" }}>
            {GRAPH.openEvidence}
          </p>
        </section>

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "var(--s-4)", alignItems: "start" }}>
          <section className="card card-pad">
            <div className="h-section" style={{ marginBottom: "var(--s-3)" }}>
              {GRAPH.statsTitle}
            </div>
            {!summary && <Spinner />}
            {summary && (
              <>
                <div className="stat-tiles" style={{ marginBottom: "var(--s-4)" }}>
                  <div className="stat-tile card" style={{ boxShadow: "none" }}>
                    <div className="n">{summary.nodes.toLocaleString()}</div>
                    <div className="l">{GRAPH.nodes}</div>
                  </div>
                  <div className="stat-tile card" style={{ boxShadow: "none" }}>
                    <div className="n">{summary.edges.toLocaleString()}</div>
                    <div className="l">{GRAPH.edges}</div>
                  </div>
                </div>
                <div className="h-section" style={{ marginBottom: "var(--s-2)" }}>
                  {GRAPH.tierLegend}
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
            <div className="h-section">{GRAPH.witnessTitle}</div>
            <p style={{ color: "var(--ink-2)", fontSize: "var(--fs-13)", margin: "var(--s-2) 0 var(--s-4)" }}>
              {GRAPH.witnessSub}
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
      </div>
      {inspect && <WitnessDrawer refUri={inspect} onClose={() => setInspect(null)} />}
    </div>
  );
}
