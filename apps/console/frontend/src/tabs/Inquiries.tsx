/** The chat — one column, no document workspace.
 *
 * Each turn renders: the question, the live work log (thinking, tool
 * calls with provenance chips, the SQL gate, query results as a
 * table), then the answer. Scripted turns end with a structured
 * `answer` card; live agent turns end with the model's markdown
 * (the five-section contract), rendered by the small renderer below.
 * The gate still buffers the stream and requires hold-to-run.
 */

import { useEffect, useRef, useState } from "react";
import { WitnessDrawer } from "../components/WitnessDrawer";
import {
  formatBytes, HoldButton, RichText, Spinner, TierChip,
} from "../components/ui";
import { api } from "../lib/api";
import { INQUIRIES as C } from "../lib/copy";
import { streamChat } from "../lib/sse";
import type { AnswerSections, ConsoleEvent } from "../lib/types";

interface Turn {
  question: string;
  log: ConsoleEvent[];
  liveText: string;
  answer: AnswerSections | null;
  gate: Extract<ConsoleEvent, { type: "sql_gate" }> | null;
  heldNote: string | null;
  done: boolean;
}

const newTurn = (question: string): Turn => ({
  question, log: [], liveText: "", answer: null,
  gate: null, heldNote: null, done: false,
});

export function InquiriesTab() {
  const conversationId = useRef<string>(
    globalThis.crypto?.randomUUID?.() ?? `c${Date.now()}`);
  const pending = useRef<ConsoleEvent[]>([]);
  const gated = useRef(false);

  const [turns, setTurns] = useState<Turn[]>([]);
  const [busy, setBusy] = useState(false);
  const [question, setQuestion] = useState("");
  const [suggestions, setSuggestions] = useState<
    { question: string; archetype: string }[]
  >([]);
  const [inspect, setInspect] = useState<string | null>(null);
  const [demoMode, setDemoMode] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    api.questions().then((d) => setSuggestions(d.questions))
      .catch(() => undefined);
    api.config()
      .then((c) => setDemoMode(c.runner === "ScriptedRunner"))
      .catch(() => undefined);
  }, []);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight });
  }, [turns, busy]);

  const patchLast = (fn: (t: Turn) => Turn) =>
    setTurns((ts) =>
      ts.length ? [...ts.slice(0, -1), fn(ts[ts.length - 1])] : ts);

  const handleEvent = (ev: ConsoleEvent) => {
    switch (ev.type) {
      case "turn_start":
        break;
      case "thinking":
      case "tool_call":
      case "tool_result":
      case "sandbox":
      case "gate_resolved":
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
        break;
      case "answer":
        // the structured card supersedes streamed text (scripted mode)
        patchLast((t) => ({ ...t, liveText: "", answer: ev.sections }));
        break;
      case "turn_end":
        patchLast((t) => ({ ...t, done: true }));
        setBusy(false);
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
    try {
      for await (const ev of streamChat(q, conversationId.current)) {
        if (gated.current) pending.current.push(ev);
        else handleEvent(ev);
      }
      // stream closed while gated (live mode): keep the gate visible
      if (!gated.current && pending.current.length === 0) setBusy(false);
    } catch (e) {
      handleEvent({
        type: "error",
        message: e instanceof Error ? e.message : String(e),
        recoverable: true,
      });
      patchLast((t) => ({ ...t, done: true }));
      setBusy(false);
    }
  };

  const approveGate = async (gateId: string) => {
    await api.approve(gateId, true).catch(() => undefined);
    gated.current = false;
    patchLast((t) => ({ ...t, gate: null }));
    const queued = pending.current;
    pending.current = [];
    queued.forEach(handleEvent);
  };

  const holdGate = async (gateId: string) => {
    await api.approve(gateId, false).catch(() => undefined);
    gated.current = false;
    pending.current = [];
    patchLast((t) => ({
      ...t, gate: null, heldNote: C.heldNote, done: true,
    }));
    setBusy(false);
  };

  return (
    <div className="chat">
      {demoMode && (
        <div className="demo-banner" role="alert">
          <span aria-hidden>▲</span> {C.demoBanner}
        </div>
      )}
      <div className="chat-scroll" ref={scrollRef}>
        {turns.length === 0 && (
          <div className="empty" style={{ margin: "auto", maxWidth: 480 }}>
            <h1 className="h-page" style={{ color: "var(--ink)" }}>
              {C.emptyTitle}
            </h1>
            <p className="h-sub" style={{ margin: "var(--s-3) auto 0" }}>
              {C.emptySub}
            </p>
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
          </section>
        ))}

        {busy && (
          <div className="status-line">
            <Spinner /> {C.working}
          </div>
        )}
      </div>

      <div className="composer">
        {turns.length === 0 && suggestions.length > 0 && (
          <div className="suggest" aria-label={C.suggestTitle}>
            {suggestions.slice(0, 4).map((s) => (
              <button key={s.question} type="button"
                onClick={() => ask(s.question)}>
                {s.question}
              </button>
            ))}
          </div>
        )}
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
                ask(question);
              }
            }}
            aria-label={C.placeholder}
          />
          <button type="button" className="btn primary"
            onClick={() => ask(question)}
            disabled={!question.trim() || busy}>
            {C.send}
          </button>
        </div>
      </div>

      {inspect && (
        <WitnessDrawer refUri={inspect} onClose={() => setInspect(null)} />
      )}
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
      <p className="brief-answer">
        <RichText text={sections.answer} />
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

/** Query rows out of a tool payload, wherever the runner put them. */
function extractRows(
  payload: Record<string, unknown> | null,
): Record<string, unknown>[] | null {
  if (!payload) return null;
  const data = (payload.data ?? payload) as Record<string, unknown>;
  const rows = data?.rows;
  if (Array.isArray(rows) && rows.length &&
      typeof rows[0] === "object" && rows[0] !== null) {
    return rows as Record<string, unknown>[];
  }
  return null;
}

function ResultsTable({ rows }: { rows: Record<string, unknown>[] }) {
  const cols = Object.keys(rows[0]).slice(0, 8);
  const shown = rows.slice(0, 20);
  return (
    <div className="results-wrap">
      <table className="results-table">
        <thead>
          <tr>{cols.map((c) => <th key={c}>{c}</th>)}</tr>
        </thead>
        <tbody>
          {shown.map((r, i) => (
            <tr key={i}>
              {cols.map((c) => <td key={c}>{String(r[c] ?? "")}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > shown.length && (
        <div className="results-more">
          …and {rows.length - shown.length} more row(s)
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
 * ## headings, **bold**, `code`, - lists, | tables. No HTML input. */
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
            <li key={j}><RichText text={it} /></li>
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
          <RichText text={para.join(" ")} />
        </p>,
      );
    }
  }
  return <>{blocks}</>;
}
