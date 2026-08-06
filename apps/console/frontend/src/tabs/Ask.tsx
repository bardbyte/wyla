/** Ask — the agentic centerpiece.
 *
 * One column, one conversation. Each turn renders the question, the
 * live work log (the agent thinking; tool calls with provenance chips;
 * the SQL gate; query results as a table), then the answer — sources,
 * confidence, and the trail that produced them. Known graph objects in
 * an answer are live tokens (Evidence · Explore · Ask about this), so a
 * conversation is a door into every other surface.
 *
 * This is where P1–P4 close in one place: find the data, trust it, get
 * the number safely (nothing runs without a held approval), and defend
 * it later (citations, ledger ids, tier chips). Keeping what was learned
 * — P5 — is a signed assertion, made through the agent and surfaced in
 * Bring your knowledge.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import { RefText } from "../components/EntityRef";
import { WitnessDrawer } from "../components/WitnessDrawer";
import {
  ResultsTable, extractRows, formatBytes, HoldButton, RichText,
  Spinner, TierChip,
} from "../components/ui";
import { api } from "../lib/api";
import { ASK as C, EVALS, GRAPH, TIERS } from "../lib/copy";
import { useNav } from "../lib/nav";
import { computeListening } from "../lib/anticipation";
import { streamChat } from "../lib/sse";
import { SpaceCanvas } from "../components/SpaceCanvas";
import { SpaceAskBar, SpaceSeal } from "../components/SpaceBits";
import type {
  AgentSelftest, AnswerSections, ConsoleEvent, EvalTurn, GraphMap,
  Starter,
} from "../lib/types";

/** Pull graph-object mentions out of a streamed event so the map can
 * light up what the agent touches: synapse:// refs anywhere, plus known
 * table names appearing in args, SQL, or result summaries. */
function extractActivity(ev: ConsoleEvent,
                         known: ReadonlySet<string>): string[] {
  const found = new Set<string>();
  const scan = (v: unknown, depth = 0) => {
    if (depth > 6 || v == null) return;
    if (typeof v === "string") {
      for (const m of v.matchAll(/synapse:\/\/[\w./-]+/g)) found.add(m[0]);
      const low = v.toLowerCase();
      for (const t of known) if (low.includes(t)) found.add(t);
      return;
    }
    if (Array.isArray(v)) { v.forEach((x) => scan(x, depth + 1)); return; }
    if (typeof v === "object") {
      Object.values(v as Record<string, unknown>)
        .forEach((x) => scan(x, depth + 1));
    }
  };
  switch (ev.type) {
    case "tool_call": scan(ev.args); scan(ev.args_summary); scan(ev.verb); break;
    case "tool_result": scan(ev.summary); scan(ev.payload); break;
    case "sql_gate": scan(ev.sql); break;
    case "answer": scan(ev.sections.citations); break;
    default: break;
  }
  return [...found];
}

interface Turn {
  question: string;
  log: ConsoleEvent[];
  liveText: string;
  answer: AnswerSections | null;
  gate: Extract<ConsoleEvent, { type: "sql_gate" }> | null;
  heldNote: string | null;
  done: boolean;
  evalv: EvalTurn | null;
}

const newTurn = (question: string): Turn => ({
  question, log: [], liveText: "", answer: null,
  gate: null, heldNote: null, done: false, evalv: null,
});

export function AskTab() {
  const nav = useNav();
  const conversationId = useRef<string>(
    globalThis.crypto?.randomUUID?.() ?? `c${Date.now()}`);
  const pending = useRef<ConsoleEvent[]>([]);
  const gated = useRef(false);

  const [turns, setTurns] = useState<Turn[]>([]);
  const turnsRef = useRef<Turn[]>(turns);
  useEffect(() => { turnsRef.current = turns; }, [turns]);

  const [busy, setBusy] = useState(false);
  const [question, setQuestion] = useState("");
  const [starters, setStarters] = useState<Starter[]>([]);
  const [spaceQ, setSpaceQ] = useState("");
  const [inspect, setInspect] = useState<string | null>(null);
  const [demoMode, setDemoMode] = useState(false);
  const [map, setMap] = useState<GraphMap | null>(null);
  const [selftest, setSelftest] = useState<AgentSelftest | null>(null);
  // first light (1a) plays once per session, on whichever surface opens
  const [intro] = useState(() => {
    try {
      if (sessionStorage.getItem("synapse-firstlight")) return false;
      sessionStorage.setItem("synapse-firstlight", "1");
      return true;
    } catch { return false; }
  });
  const knownTables = useRef<Set<string>>(new Set());
  const scrollRef = useRef<HTMLDivElement>(null);
  const approveGateRef = useRef<(id: string) => void>(() => undefined);
  const holdGateRef = useRef<(id: string) => void>(() => undefined);
  const captureRefs = useRef<Map<string, string>>(new Map());

  useEffect(() => {
    api.starters().then((d) => setStarters(d.starters))
      .catch(() => undefined);
    api.config()
      .then((c) => setDemoMode(c.runner === "ScriptedRunner"))
      .catch(() => undefined);
    api.graphMap().then((d) => {
      setMap(d.map);
      const names = new Set<string>();
      for (const n of d.map.nodes) {
        if (n.kind !== "table") continue;
        const bare = n.label.toLowerCase().split(/[./]/).pop();
        if (bare && bare.length > 3) names.add(bare);
      }
      knownTables.current = names;
    }).catch(() => setMap(null));
    // the live agent proves it can START (imports, versions, snapshot)
    // without spending a token — failures render with their exact fix
    api.agentSelftest().then(setSelftest).catch(() => undefined);
  }, []);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight });
  }, [turns, busy]);

  /* the sky listens while you type; the traversal owns it once you ask */
  const anticipation = useMemo(
    () => computeListening(question, map), [question, map]);
  const spaceListening = useMemo(
    () => computeListening(spaceQ, map), [spaceQ, map]);
  useEffect(() => {
    nav.setListening(busy ? [] : anticipation.ids);
    /* eslint-disable-next-line react-hooks/exhaustive-deps */
  }, [anticipation, busy]);

  const patchLast = (fn: (t: Turn) => Turn) =>
    setTurns((ts) =>
      ts.length ? [...ts.slice(0, -1), fn(ts[ts.length - 1])] : ts);

  const handleEvent = (ev: ConsoleEvent) => {
    nav.reportActivity(extractActivity(ev, knownTables.current));
    if (ev.type === "tool_call" && ev.verb) nav.setTraversalVerb(ev.verb);
    if (ev.type === "tool_call" && ev.tool === "capture_knowledge") {
      const a = (ev.args ?? {}) as Record<string, unknown>;
      const subject = [String(a.subject_type ?? ""), String(a.subject_ref ?? "")]
        .filter(Boolean).join("/");
      if (subject) captureRefs.current.set(ev.call_id, subject);
    }
    switch (ev.type) {
      case "turn_start":
        break;
      case "tool_result":
        // a successful capture is a SIGNATURE — the sky turns gold (1e)
        if (ev.ok !== false) {
          const cap = captureRefs.current.get(ev.call_id);
          if (cap) nav.setCeremony({ ref: cap, at: Date.now() });
        }
        patchLast((t) => ({ ...t, log: [...t.log, ev] }));
        break;
      case "tool_call":
      case "gate_resolved":
      case "thinking":
      case "sandbox":
      case "artifact":
      case "error":
        patchLast((t) => ({ ...t, log: [...t.log, ev] }));
        break;
      case "text":
        patchLast((t) => ({ ...t, liveText: t.liveText + ev.delta }));
        break;
      case "sql_gate":
        gated.current = true;
        patchLast((t) => ({ ...t, gate: ev }));
        nav.setPendingGate({
          gateId: ev.gate_id, sql: ev.sql,
          bytes: ev.bytes_estimate ?? 0,
          checks: ev.guardrail_checks,
          resolve: (approved: boolean) => (approved
            ? approveGateRef.current(ev.gate_id)
            : holdGateRef.current(ev.gate_id)),
        });
        break;
      case "answer":
        // the structured card supersedes streamed text (scripted mode)
        patchLast((t) => ({ ...t, liveText: "", answer: ev.sections }));
        // condensation: the graph raises the paper plate + threads (1d)
        nav.setLastFinding({
          text: ev.sections.answer.replace(/\*\*/g, ""),
          citations: ev.sections.citations.map((c) => ({
            label: c.label, ref: c.ref })),
        });
        break;
      case "turn_end":
        patchLast((t) => ({ ...t, done: true }));
        setBusy(false);
        nav.setAgentBusy(false);
        // the turn was scored server-side the moment it finished —
        // attach the verdict to the turn it belongs to
        api.evalsRecent().then((f) => {
          const scored = f.turns[0];
          if (!scored) return;
          setTurns((ts) => ts.map((t) =>
            t.question === scored.question && t.done && !t.evalv
              ? { ...t, evalv: scored } : t));
        }).catch(() => undefined);
        break;
    }
  };

  const ask = async (text: string) => {
    const q = text.trim();
    if (!q || busy) return;
    setQuestion("");
    gated.current = false;
    pending.current = [];
    setTurns((ts) => [...ts, newTurn(q)]);
    setBusy(true);
    nav.clearActivity();          // a fresh traversal per question
    nav.setLastFinding(null);     // the last plate lowers
    nav.setAgentBusy(true);
    try {
      for await (const ev of streamChat(q, conversationId.current)) {
        if (gated.current) pending.current.push(ev);
        else handleEvent(ev);
      }
      if (!gated.current && pending.current.length === 0) {
        setBusy(false);
        nav.setAgentBusy(false);
      }
    } catch (e) {
      handleEvent({
        type: "error",
        message: e instanceof Error ? e.message : String(e),
        recoverable: true,
      });
      patchLast((t) => ({ ...t, done: true }));
      setBusy(false);
      nav.setAgentBusy(false);
    }
  };

  // the interconnection spine hands questions in from other tabs
  const askRef = useRef(ask);
  askRef.current = ask;
  useEffect(() => {
    if (!nav.handoff || nav.tab !== "ask") return;
    const { text, send } = nav.handoff;
    nav.clearHandoff();
    if (send) void askRef.current(text);
    else setQuestion(text);
  }, [nav, nav.handoff, nav.tab]);

  const approveGate = async (gateId: string) => {
    await api.approve(gateId, true).catch(() => undefined);
    gated.current = false;
    nav.setPendingGate(null);
    patchLast((t) => ({ ...t, gate: null }));
    const queued = pending.current;
    pending.current = [];
    queued.forEach(handleEvent);
  };

  const holdGate = async (gateId: string) => {
    await api.approve(gateId, false).catch(() => undefined);
    gated.current = false;
    pending.current = [];
    nav.setPendingGate(null);
    patchLast((t) => ({
      ...t, gate: null, heldNote: C.heldNote, done: true,
    }));
    setBusy(false);
  };
  approveGateRef.current = approveGate;
  holdGateRef.current = holdGate;

  return (
    <div className="ask-wrap">
    <div className="chat">
      {demoMode && (
        <div className="demo-banner" role="alert">
          <span aria-hidden>▲</span> {C.demoBanner}
        </div>
      )}
      {selftest && !selftest.ok && (
        <div className="notice agent-issue" role="alert">
          <span aria-hidden>⚠</span>
          <span><strong>{C.agentIssue}.</strong> {selftest.error}</span>
        </div>
      )}
      {map && map.nodes.length > 0 && (
        <section
          className={`ask-space ${turns.length === 0 ? "tall" : "slim"}`}>
          <SpaceCanvas map={map} activity={nav.activity} backdrop
            listening={spaceQ ? spaceListening.ids : nav.listening}
            verb={nav.traversalVerb}
            intro={intro} introLine={GRAPH.firstLight}
            finding={nav.lastFinding}
            ceremonyRef={nav.ceremony?.ref ?? null}
            overlay={
              <>
                {nav.pendingGate && <SpaceSeal gate={nav.pendingGate} />}
                {turns.length === 0 && !busy && (
                  <SpaceAskBar value={spaceQ} onChange={setSpaceQ}
                    onSubmit={(t) => { setSpaceQ(""); void ask(t); }}
                    placeholder={GRAPH.askSpace} tally={spaceListening}
                    raised={!!nav.lastFinding} />
                )}
              </>
            } />
        </section>
      )}
      <div className="chat-scroll" ref={scrollRef}>
        {turns.length === 0 && (!map || map.nodes.length === 0) && (
          <div className="ask-hero">
            <h1 className="h-page">{C.emptyTitle}</h1>
            <p className="h-sub">{C.emptySub}</p>
          </div>
        )}
        {turns.length === 0 && starters.length > 0 && (
          <div className="starters">
            <div className="h-section">{C.startersTitle}</div>
            <p className="starters-sub">{C.startersSub}</p>
            <div className="starters-grid">
              {starters.map((st) => (
                <button key={st.question} type="button"
                  className="starter-card"
                  onClick={() => st.prefill
                    ? setSpaceQ(st.question)
                    : void ask(st.question)}>
                  <span className="starter-cat">{st.category}</span>
                  <span className="starter-q">
                    {st.question}{st.prefill ? "…" : ""}
                  </span>
                  <span className="starter-why">{st.why}</span>
                </button>
              ))}
            </div>
          </div>
        )}

        {turns.map((t, i) => (
          <section key={i} className="turn">
            <div className="msg-user">{t.question}</div>
            {t.log.length > 0 && (
              <WorkLog log={t.log} onInspect={setInspect} />
            )}
            {t.gate && (
              <GateCard
                gate={t.gate}
                onApprove={() => approveGate(t.gate!.gate_id)}
                onHold={() => holdGate(t.gate!.gate_id)}
              />
            )}
            {t.heldNote && (
              <div className="notice">
                <span aria-hidden>⏸</span>
                <span>{t.heldNote}</span>
              </div>
            )}
            {t.answer && (
              <AnswerCard sections={t.answer} onInspect={setInspect} />
            )}
            {!t.answer && t.liveText && (
              <div className="answer-card card card-pad">
                <Markdown text={t.liveText} />
              </div>
            )}
            {t.evalv && (
              <button type="button"
                className={`eval-chip v-${t.evalv.verdict}`}
                onClick={() => nav.go("evals")}
                title={t.evalv.verdict_text}>
                <span aria-hidden>
                  {t.evalv.verdict === "needs_review" ? "✕" : "✓"}
                </span>
                {EVALS.verdicts[t.evalv.verdict]} ·{" "}
                {Math.round(t.evalv.score * 100)}%
                {t.evalv.corrections.length > 0 &&
                  ` · ${t.evalv.corrections.length} self-correction${
                    t.evalv.corrections.length > 1 ? "s" : ""}`}
              </button>
            )}
          </section>
        ))}

        {busy && (
          <div className="status-line">
            <Spinner /> {C.working}
          </div>
        )}
      </div>

      {turns.length > 0 && (
      <div className="composer">
        <div className="composer-row">
          <textarea
            className="textarea"
            rows={2}
            placeholder={C.placeholder}
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                void ask(question);
              }
            }}
            aria-label={C.placeholder}
          />
          <button type="button" className="btn primary"
            onClick={() => void ask(question)}
            disabled={!question.trim() || busy}>
            {C.send}
          </button>
        </div>
        {!busy && anticipation.ids.length > 0 && (
          <div className="listen-tally" aria-live="polite">
            <span>
              {anticipation.ids.length} nodes listening
              {anticipation.nMetrics > 0 &&
                ` · ${anticipation.nMetrics} metric${
                  anticipation.nMetrics > 1 ? "s" : ""}`}
              {anticipation.nTables > 0 &&
                ` · ${anticipation.nTables} table${
                  anticipation.nTables > 1 ? "s" : ""}`}
            </span>
            {anticipation.best && (
              <button type="button" className="listen-best"
                onClick={() => nav.openEvidence(anticipation.best!.ref)}>
                {anticipation.best.ref}
                <em className={`lt-${anticipation.best.tier}`}>
                  {TIERS[anticipation.best.tier as keyof typeof TIERS]
                    ?.word ?? anticipation.best.tier}
                </em>
              </button>
            )}
          </div>
        )}
      </div>
      )}

      {inspect && (
        <WitnessDrawer refUri={inspect} onClose={() => setInspect(null)} />
      )}
    </div>

    </div>
  );
}

/* ── pieces ── */

function WorkLog({
  log, onInspect,
}: {
  log: ConsoleEvent[]; onInspect: (ref: string) => void;
}) {
  return (
    <div className="worklog">
      {log.map((ev, i) => {
        switch (ev.type) {
          case "thinking":
            return (
              <div key={i} className="wl-row">
                <span className="g" aria-hidden>≋</span>
                <span className="think">{ev.delta}</span>
              </div>
            );
          case "tool_call":
            return (
              <div key={i} className="wl-row">
                <span className="g" aria-hidden>▸</span>
                <span className="verb">
                  {ev.verb}
                  {ev.args_summary ? ` — ${ev.args_summary}` : ""}
                </span>
              </div>
            );
          case "tool_result": {
            const rows = extractRows(ev.payload);
            return (
              <div key={i}>
                <div className="wl-row">
                  <span className="g" aria-hidden>{ev.ok ? "✓" : "✕"}</span>
                  <span className="sum">{ev.summary}</span>
                  {ev.provenance && <TierChip tier={ev.provenance.tier} />}
                </div>
                {rows && <ResultsTable rows={rows} />}
              </div>
            );
          }
          case "sandbox":
            return (
              <div key={i}>
                <div className="wl-row">
                  <span className="g" aria-hidden>⌗</span>
                  <span className="sum">Computed</span>
                </div>
                <pre className="wl-code">
                  {ev.code}
                  {ev.result !== null && ev.result !== undefined
                    ? `\n# → ${JSON.stringify(ev.result)}`
                    : ""}
                </pre>
              </div>
            );
          case "gate_resolved":
            return (
              <div key={i} className="wl-row audit">
                <span className="g" aria-hidden>✓</span>
                <span>
                  {ev.decision === "approved"
                    ? `Approved by ${ev.actor}` +
                      (ev.ledger_id ? ` · ledger #${ev.ledger_id}` : "") +
                      (ev.rows_returned !== null
                        ? ` · ${ev.rows_returned} rows`
                        : "")
                    : `Held by ${ev.actor} — nothing ran`}
                </span>
                {ev.ledger_id && (
                  <button type="button" className="toggle-btn"
                    onClick={() => onInspect(`ledger:#${ev.ledger_id}`)}>
                    ledger
                  </button>
                )}
              </div>
            );
          case "artifact":
            return (
              <iframe key={i} className="artifact-frame" title={ev.title}
                sandbox="" srcDoc={ev.html} />
            );
          case "error":
            return (
              <div key={i} className="wl-row error">
                <span className="g" aria-hidden>⚠</span>
                <span>
                  <strong>{C.errorTitle}.</strong> {ev.message}
                </span>
              </div>
            );
          default:
            return null;
        }
      })}
    </div>
  );
}

function AnswerCard({
  sections, onInspect,
}: {
  sections: AnswerSections; onInspect: (ref: string) => void;
}) {
  return (
    <div className="answer-card card card-pad">
      <span className="finding-eyebrow">Finding</span>
      <p className="brief-answer">
        <RefText text={sections.answer} />
      </p>
      {sections.citations.length > 0 && (
        <div className="ac-block">
          <div className="h-section">{C.citations}</div>
          <div className="cite-list">
            {sections.citations.map((c) => (
              <button key={c.ref + c.label} type="button" className="cite"
                onClick={() => onInspect(c.ref)}>
                <span aria-hidden>⌕</span> {c.label}
                <span className="ref">{c.ref}</span>
              </button>
            ))}
          </div>
        </div>
      )}
      {sections.how_i_got_there && (
        <details className="ac-block">
          <summary className="h-section" style={{ cursor: "pointer" }}>
            {C.howTitle}
          </summary>
          <p style={{ color: "var(--ink-2)", marginTop: "var(--s-2)" }}>
            {sections.how_i_got_there}
          </p>
        </details>
      )}
      {(sections.governance || sections.status) && (
        <div className="ac-foot">
          {sections.governance && <span>{sections.governance}</span>}
          {sections.status && <span className="tag">{sections.status}</span>}
        </div>
      )}
    </div>
  );
}

function GateCard({
  gate, onApprove, onHold,
}: {
  gate: Extract<ConsoleEvent, { type: "sql_gate" }>;
  onApprove: () => void;
  onHold: () => void;
}) {
  return (
    <div className="gate" role="group" aria-label={C.gateTitle}>
      <div className="gate-head">
        <span aria-hidden>⏸</span> {C.gateTitle}
      </div>
      <div className="gate-body">
        <p style={{ color: "var(--ink-2)", fontSize: "var(--fs-13)" }}>
          {C.gateSub}
        </p>
        <pre>{gate.sql}</pre>
        <div style={{ display: "flex", gap: "var(--s-4)", alignItems: "center", flexWrap: "wrap" }}>
          <span style={{ fontSize: "var(--fs-12)", color: "var(--ink-2)" }}>
            {C.gateScan}: <strong>{formatBytes(gate.bytes_estimate)}</strong>
          </span>
          <div className="gate-checks">
            {gate.guardrail_checks.map((c) => (
              <span key={c} className="tag">{c}</span>
            ))}
          </div>
        </div>
        <div className="gate-actions">
          <HoldButton label={C.gateHold} onConfirm={onApprove} />
          <button type="button" className="btn quiet" onClick={onHold}>
            {C.gateLater}
          </button>
          <span style={{ color: "var(--ink-3)", fontSize: "var(--fs-11)" }}>
            {C.gateHoldHint}
          </span>
        </div>
      </div>
    </div>
  );
}

/** Minimal markdown for the live agent's five-section answers:
 * ## headings, **bold**, `code`, - lists, | tables. No HTML input.
 * Paragraphs and list items linkify known graph objects. */
function Markdown({ text }: { text: string }) {
  const blocks: JSX.Element[] = [];
  const lines = text.split("\n");
  let i = 0;
  let k = 0;
  while (i < lines.length) {
    const line = lines[i];
    if (/^#{1,3}\s/.test(line)) {
      blocks.push(
        <div key={k++} className="h-section md-h">
          {line.replace(/^#{1,3}\s*/, "")}
        </div>,
      );
      i += 1;
    } else if (line.trimStart().startsWith("|")) {
      const tbl: string[] = [];
      while (i < lines.length && lines[i].trimStart().startsWith("|")) {
        tbl.push(lines[i]);
        i += 1;
      }
      const parse = (l: string) =>
        l.trim().replace(/^\||\|$/g, "").split("|").map((c) => c.trim());
      const body = tbl.filter((l) => !/^[\s|:-]+$/.test(l));
      const [head, ...rest] = body.map(parse);
      blocks.push(
        <div key={k++} className="results-wrap">
          <table className="results-table">
            <thead>
              <tr>{(head ?? []).map((h, j) => <th key={j}>{h}</th>)}</tr>
            </thead>
            <tbody>
              {rest.map((r, ri) => (
                <tr key={ri}>
                  {r.map((c, ci) => (
                    <td key={ci}><RichText text={c} /></td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>,
      );
    } else if (/^\s*[-*]\s+/.test(line)) {
      const items: string[] = [];
      while (i < lines.length && /^\s*[-*]\s+/.test(lines[i])) {
        items.push(lines[i].replace(/^\s*[-*]\s+/, ""));
        i += 1;
      }
      blocks.push(
        <ul key={k++} className="md-list">
          {items.map((it, j) => (
            <li key={j}><RefText text={it} /></li>
          ))}
        </ul>,
      );
    } else if (line.trim() === "") {
      i += 1;
    } else {
      const para: string[] = [line];
      i += 1;
      while (i < lines.length && lines[i].trim() !== "" &&
             !/^#{1,3}\s/.test(lines[i]) &&
             !lines[i].trimStart().startsWith("|") &&
             !/^\s*[-*]\s+/.test(lines[i])) {
        para.push(lines[i]);
        i += 1;
      }
      blocks.push(
        <p key={k++} className="md-p">
          <RefText text={para.join(" ")} />
        </p>,
      );
    }
  }
  return <>{blocks}</>;
}
