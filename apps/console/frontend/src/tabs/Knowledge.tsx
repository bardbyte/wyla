import { PreviewBadge } from "../components/ui";
import { KNOWLEDGE } from "../lib/copy";

export function KnowledgeTab() {
  return (
    <div className="page">
      <div className="page-inner">
        <div className="page-head">
          <div className="grow">
            <h1 className="h-page">{KNOWLEDGE.title}</h1>
            <p className="h-sub">{KNOWLEDGE.sub}</p>
          </div>
        </div>

        <div className="notice" style={{ marginBottom: "var(--s-5)" }}>
          <span aria-hidden>⚖</span>
          <span>{KNOWLEDGE.how}</span>
        </div>

        <div className="grid-cards" style={{ marginBottom: "var(--s-6)" }}>
          {KNOWLEDGE.connectors.map((c) => (
            <article key={c.name} className="card card-pad connector-tile">
              <div className="icon" aria-hidden>{c.icon}</div>
              <strong>{c.name}</strong>
              <p style={{ color: "var(--ink-2)", fontSize: "var(--fs-13)", flex: 1 }}>
                {c.desc}
              </p>
              <div style={{ display: "flex", gap: "var(--s-2)", alignItems: "center" }}>
                <button type="button" className="btn" disabled>
                  {KNOWLEDGE.connect}
                </button>
                <PreviewBadge />
              </div>
            </article>
          ))}
        </div>

        <section className="card card-pad" style={{ maxWidth: 640 }}>
          <div className="h-section" style={{ marginBottom: "var(--s-3)" }}>
            {KNOWLEDGE.ladderTitle}
          </div>
          <ol style={{ paddingLeft: "1.2em", display: "flex", flexDirection: "column", gap: "var(--s-2)", color: "var(--ink-2)" }}>
            {KNOWLEDGE.ladder.map((step, i) => (
              <li key={i}>{step}</li>
            ))}
          </ol>
        </section>
      </div>
    </div>
  );
}
