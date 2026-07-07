import { useEffect, useState } from "react";
import { WitnessDrawer } from "../components/WitnessDrawer";
import { PreviewBadge, SourceBadge, Spinner, TierChip } from "../components/ui";
import { api } from "../lib/api";
import { METRICS } from "../lib/copy";
import type { Metric, Viability } from "../lib/types";

export function MetricsTab() {
  const [q, setQ] = useState("");
  const [rows, setRows] = useState<Metric[] | null>(null);
  const [live, setLive] = useState(false);
  const [inspect, setInspect] = useState<string | null>(null);

  // copilot state
  const [name, setName] = useState("");
  const [desc, setDesc] = useState("");
  const [checking, setChecking] = useState(false);
  const [verdict, setVerdict] = useState<Viability | null>(null);

  useEffect(() => {
    let on = true;
    const t = window.setTimeout(() => {
      api.metrics(q).then((d) => {
        if (!on) return;
        setRows(d.metrics as Metric[]);
        setLive(d.live);
      }).catch(() => on && setRows([]));
    }, q ? 200 : 0);
    return () => { on = false; window.clearTimeout(t); };
  }, [q]);

  const check = async () => {
    if (!name.trim() || checking) return;
    setChecking(true);
    setVerdict(null);
    try {
      const d = await api.viability(name.trim(), desc.trim());
      setVerdict(d.viability);
    } catch {
      setVerdict(null);
    } finally {
      setChecking(false);
    }
  };

  return (
    <div className="page">
      <div className="page-inner">
        <div className="page-head">
          <div className="grow">
            <h1 className="h-page">{METRICS.title}</h1>
            <p className="h-sub">{METRICS.sub}</p>
          </div>
          <SourceBadge live={live} />
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "minmax(320px, 1fr) minmax(360px, 1fr)", gap: "var(--s-5)", alignItems: "start" }}>
          {/* the canon */}
          <section>
            <input
              className="input"
              style={{ marginBottom: "var(--s-3)" }}
              placeholder={METRICS.search}
              value={q}
              onChange={(e) => setQ(e.target.value)}
              aria-label={METRICS.search}
            />
            {rows === null && <Spinner />}
            {rows !== null && rows.length === 0 && (
              <div className="empty">{METRICS.empty}</div>
            )}
            <div style={{ display: "flex", flexDirection: "column", gap: "var(--s-3)" }}>
              {(rows ?? []).slice(0, 30).map((m) => (
                <article key={m.ref} className="card card-pad" style={{ display: "flex", flexDirection: "column", gap: "var(--s-2)" }}>
                  <div style={{ display: "flex", alignItems: "center", gap: "var(--s-2)" }}>
                    <strong style={{ flex: 1 }}>{m.name}</strong>
                    <TierChip tier={m.tier} onClick={() => setInspect(m.ref)} />
                  </div>
                  {m.description && (
                    <p style={{ color: "var(--ink-2)", fontSize: "var(--fs-13)" }}>{m.description}</p>
                  )}
                  {m.formula && (
                    <code style={{ background: "var(--surface-2)", padding: "var(--s-2) var(--s-3)", borderRadius: "var(--r-sm)", overflowX: "auto" }}>
                      {m.formula}
                    </code>
                  )}
                  {m.sources.length > 0 && (
                    <div style={{ display: "flex", gap: "var(--s-1)", flexWrap: "wrap" }}>
                      {m.sources.map((s) => <span key={s} className="tag">{s}</span>)}
                    </div>
                  )}
                </article>
              ))}
            </div>
          </section>

          {/* the copilot */}
          <section className="card card-pad" style={{ position: "sticky", top: 0 }}>
            <div className="h-section">{METRICS.copilotTitle}</div>
            <p style={{ color: "var(--ink-2)", fontSize: "var(--fs-13)", margin: "var(--s-2) 0 var(--s-4)" }}>
              {METRICS.copilotSub}
            </p>
            <label style={{ display: "block", marginBottom: "var(--s-3)" }}>
              <div className="h-section" style={{ marginBottom: 4 }}>{METRICS.nameLabel}</div>
              <input className="input" value={name} placeholder={METRICS.namePlaceholder}
                onChange={(e) => { setName(e.target.value); setVerdict(null); }} />
            </label>
            <label style={{ display: "block", marginBottom: "var(--s-3)" }}>
              <div className="h-section" style={{ marginBottom: 4 }}>{METRICS.descLabel}</div>
              <textarea className="textarea" rows={3} value={desc}
                placeholder={METRICS.descPlaceholder}
                onChange={(e) => { setDesc(e.target.value); setVerdict(null); }} />
            </label>
            <button type="button" className="btn primary" onClick={check}
              disabled={!name.trim() || checking}>
              {checking ? METRICS.checking : METRICS.check}
            </button>

            {verdict && (
              <div style={{ marginTop: "var(--s-4)", display: "flex", flexDirection: "column", gap: "var(--s-3)" }}>
                {verdict.verdict === "exact" && (
                  <div className="copilot-verdict">
                    <strong>{METRICS.verdictExact}</strong>
                    <p style={{ color: "var(--ink-2)", fontSize: "var(--fs-13)", margin: "4px 0 var(--s-2)" }}>
                      {METRICS.verdictExactSub}
                    </p>
                    {verdict.exact.map((m) => (
                      <MetricHit key={m.ref} m={m} onInspect={setInspect} />
                    ))}
                  </div>
                )}
                {verdict.verdict === "near_duplicate" && (
                  <div className="copilot-verdict near">
                    <strong>{METRICS.verdictNear}</strong>
                    <p style={{ color: "var(--ink-2)", fontSize: "var(--fs-13)", margin: "4px 0 var(--s-2)" }}>
                      {METRICS.verdictNearSub}
                    </p>
                    {verdict.near.map((m) => (
                      <MetricHit key={m.ref} m={m} shared={m.shared_terms} onInspect={setInspect} />
                    ))}
                  </div>
                )}
                {verdict.verdict === "clear" && (
                  <div className="copilot-verdict">
                    <strong>{METRICS.verdictClear}</strong>
                    <p style={{ color: "var(--ink-2)", fontSize: "var(--fs-13)", margin: "4px 0 var(--s-2)" }}>
                      {METRICS.verdictClearSub}
                    </p>
                    <div className="card card-pad" style={{ boxShadow: "none", display: "flex", flexDirection: "column", gap: "var(--s-2)" }}>
                      <div style={{ display: "flex", alignItems: "center", gap: "var(--s-2)" }}>
                        <strong style={{ flex: 1 }}>{name.trim()}</strong>
                        <TierChip tier="inferred" />
                      </div>
                      {desc.trim() && (
                        <p style={{ color: "var(--ink-2)", fontSize: "var(--fs-13)" }}>{desc.trim()}</p>
                      )}
                      <p style={{ color: "var(--ink-3)", fontSize: "var(--fs-12)" }}>
                        {METRICS.draftTierNote}
                      </p>
                      <div style={{ display: "flex", gap: "var(--s-2)", alignItems: "center" }}>
                        <button type="button" className="btn" disabled>{METRICS.submit}</button>
                        <PreviewBadge />
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )}
          </section>
        </div>
      </div>
      {inspect && <WitnessDrawer refUri={inspect} onClose={() => setInspect(null)} />}
    </div>
  );
}

function MetricHit({
  m, shared, onInspect,
}: {
  m: Metric; shared?: string[]; onInspect: (ref: string) => void;
}) {
  return (
    <div className="card card-pad" style={{ boxShadow: "none", marginBottom: "var(--s-2)" }}>
      <div style={{ display: "flex", alignItems: "center", gap: "var(--s-2)" }}>
        <strong style={{ flex: 1 }}>{m.name}</strong>
        <TierChip tier={m.tier} onClick={() => onInspect(m.ref)} />
      </div>
      {m.formula && (
        <code style={{ display: "block", marginTop: "var(--s-2)", background: "var(--surface-2)", padding: "var(--s-1) var(--s-2)", borderRadius: "var(--r-sm)", overflowX: "auto" }}>
          {m.formula}
        </code>
      )}
      {shared && shared.length > 0 && (
        <div style={{ display: "flex", gap: "var(--s-1)", flexWrap: "wrap", marginTop: "var(--s-2)" }}>
          <span style={{ color: "var(--ink-3)", fontSize: "var(--fs-11)" }}>{METRICS.sharedTerms}:</span>
          {shared.map((t) => <span key={t} className="tag">{t}</span>)}
        </div>
      )}
    </div>
  );
}
