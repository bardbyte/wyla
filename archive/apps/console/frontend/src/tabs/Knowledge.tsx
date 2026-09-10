/** Bring your knowledge — the partnership close (P5).
 *
 * The graph gets its meaning from the team. This surface explains the
 * one move that outranks everything AI can infer: a signed assertion.
 * Stating a definition or correction in Ask records it through the
 * agent's capture tool at the highest evidence level, credited to the
 * person and durable across runs. The example assertions hand off to
 * Ask, prefilled; connectors are the planned way to bring in the places
 * a team already writes things down.
 */

import { PreviewBadge, TierChip } from "../components/ui";
import { KNOWLEDGE as C } from "../lib/copy";
import { useNav } from "../lib/nav";

export function KnowledgeTab() {
  const nav = useNav();
  return (
    <div className="page">
      <div className="page-inner">
        <div className="page-head">
          <div className="grow">
            <h1 className="h-page">{C.title}</h1>
            <p className="h-sub">{C.sub}</p>
          </div>
        </div>

        {/* ── the capture close ── */}
        <section className="card card-pad capture-card" style={{ marginBottom: "var(--s-5)" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "var(--s-3)", flexWrap: "wrap" }}>
            <div className="grow" style={{ minWidth: 240 }}>
              <div className="h-section">{C.captureTitle}</div>
              <p style={{ color: "var(--ink-2)", margin: "var(--s-2) 0 0", maxWidth: "62ch" }}>
                {C.captureSub}
              </p>
            </div>
            <TierChip tier="human_asserted" />
          </div>
          <div className="capture-examples">
            {C.captureExamples.map((ex) => (
              <button key={ex} type="button" className="capture-ex"
                onClick={() => nav.askAbout(ex)}>
                <span className="capture-quote" aria-hidden>“</span>
                <span>{ex}</span>
                <span className="capture-cta">{C.captureCta} →</span>
              </button>
            ))}
          </div>
          <div className="notice" style={{ marginTop: "var(--s-4)" }}>
            <span aria-hidden>⚖</span>
            <span>{C.how}</span>
          </div>
        </section>

        {/* ── the evidence ladder ── */}
        <section className="card card-pad" style={{ maxWidth: 720, marginBottom: "var(--s-6)" }}>
          <div className="h-section" style={{ marginBottom: "var(--s-4)" }}>
            {C.ladderTitle}
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: "var(--s-3)" }}>
            {C.ladder.map((step, i) => (
              <div key={i} style={{ display: "flex", gap: "var(--s-3)", alignItems: "baseline" }}>
                <span style={{ flex: "none", width: 148 }}>
                  <TierChip tier={step.tier} />
                </span>
                <span style={{ color: "var(--ink-2)" }}>{step.text}</span>
              </div>
            ))}
          </div>
          <p style={{ color: "var(--ink-3)", fontSize: "var(--fs-12)", marginTop: "var(--s-4)", borderTop: "1px solid var(--line)", paddingTop: "var(--s-3)" }}>
            {C.ladderFoot}
          </p>
        </section>

        {/* ── connectors (planned) ── */}
        <div className="h-section" style={{ marginBottom: "var(--s-3)" }}>
          {C.connectTitle}
        </div>
        <div className="grid-cards">
          {C.connectors.map((c) => (
            <article key={c.name} className="card card-pad connector-tile">
              <div className="icon" aria-hidden>{c.icon}</div>
              <strong>{c.name}</strong>
              <p style={{ color: "var(--ink-2)", fontSize: "var(--fs-13)", flex: 1 }}>
                {c.desc}
              </p>
              <div style={{ display: "flex", gap: "var(--s-2)", alignItems: "center" }}>
                <button type="button" className="btn" disabled>
                  {C.connect}
                </button>
                <PreviewBadge />
              </div>
            </article>
          ))}
        </div>
      </div>
    </div>
  );
}
