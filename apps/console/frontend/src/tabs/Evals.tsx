/** Evals — is what we just did actually right?
 *
 * The industry-standard split, live in the product: deterministic
 * checks run on 100% of turns the moment they complete (the same event
 * stream the UI rendered), each with a plain-language explanation.
 * Self-corrections (a validator caught the draft, the agent revised
 * before the seal) are surfaced as a feature, not hidden.
 */

import { useEffect, useState } from "react";
import { api } from "../lib/api";
import { EVALS as C } from "../lib/copy";
import { useNav } from "../lib/nav";
import type { EvalCheck, EvalTurn, EvalsRecent } from "../lib/types";

const STATUS_GLYPH: Record<EvalCheck["status"], string> = {
  pass: "✓", warn: "△", fail: "✕", skip: "—",
};

function VerdictPill({ turn }: { turn: EvalTurn }) {
  return (
    <span className={`verdict-pill v-${turn.verdict}`}>
      {C.verdicts[turn.verdict] ?? turn.verdict}
      <em>{Math.round(turn.score * 100)}%</em>
    </span>
  );
}

function CheckRow({ c }: { c: EvalCheck }) {
  return (
    <div className={`eval-check s-${c.status}`}>
      <span className="ec-glyph" aria-hidden>{STATUS_GLYPH[c.status]}</span>
      <span className="ec-label">{c.label}</span>
      <span className="ec-expl">{c.explanation}</span>
    </div>
  );
}

function TurnCard({ turn }: { turn: EvalTurn }) {
  const [open, setOpen] = useState(true);
  return (
    <section className="card eval-turn">
      <button type="button" className="eval-turn-head"
        onClick={() => setOpen((v) => !v)} aria-expanded={open}>
        <span className="eval-q">{turn.question}</span>
        <VerdictPill turn={turn} />
      </button>
      <p className="eval-verdict-text">{turn.verdict_text}</p>
      {open && (
        <>
          {turn.corrections.length > 0 && (
            <div className="eval-corrections">
              <div className="h-section">{C.correctionsTitle}</div>
              {turn.corrections.map((c, i) => (
                <p key={i} className="eval-correction">
                  <span aria-hidden>↺</span> {c}
                </p>
              ))}
            </div>
          )}
          <div className="h-section">{C.checksTitle}</div>
          <div className="eval-checks">
            {turn.checks.map((c) => <CheckRow key={c.id} c={c} />)}
          </div>
        </>
      )}
    </section>
  );
}

export function EvalsTab() {
  const nav = useNav();
  const [feed, setFeed] = useState<EvalsRecent | null>(null);

  useEffect(() => {
    let alive = true;
    const load = () =>
      api.evalsRecent().then((f) => { if (alive) setFeed(f); })
        .catch(() => { /* server owns the truth; retry next tick */ });
    load();
    const t = setInterval(load, 2500);
    return () => { alive = false; clearInterval(t); };
  }, []);

  const s = feed?.summary;
  return (
    <div className="page">
      <div className="page-inner">
        <div className="page-head">
          <div className="grow">
            <h1 className="h-page">{C.title}</h1>
            <p className="h-sub">{C.sub}</p>
          </div>
          {nav.agentBusy && (
            <span className="activity-live">
              <span className="live-dot" aria-hidden /> {C.liveNow}
            </span>
          )}
        </div>

        {s && s.n_turns > 0 && (
          <div className="eval-stats">
            <div className="eval-stat"><b>{s.n_turns}</b>
              <span>{C.statTurns}</span></div>
            <div className="eval-stat">
              <b>{s.grounded_rate == null
                ? "—" : `${Math.round(s.grounded_rate * 100)}%`}</b>
              <span>{C.statGrounded}</span></div>
            <div className="eval-stat">
              <b>{s.avg_score == null
                ? "—" : `${Math.round(s.avg_score * 100)}%`}</b>
              <span>{C.statScore}</span></div>
            <div className="eval-stat"><b>{s.self_corrections}</b>
              <span>{C.statCorrections}</span></div>
          </div>
        )}

        {(!feed || feed.turns.length === 0) && (
          <section className="card eval-empty">
            <h2 className="h-card">{C.empty}</h2>
            <p>{C.emptySub}</p>
            <button type="button" className="btn primary"
              onClick={() => nav.go("ask")}>Go to Ask</button>
          </section>
        )}

        {feed?.turns.map((t) => <TurnCard key={t.turn_id} turn={t} />)}

        <p className="eval-rubric-note">{C.rubricNote}</p>
      </div>
    </div>
  );
}
