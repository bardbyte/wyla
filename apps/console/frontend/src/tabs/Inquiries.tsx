/** The inquiry workspace — three zones:
 *  rail (finished briefs) · conversation (composer + work log) ·
 *  brief document (the artifact the turn produces).
 *
 * Contract with the event stream (events.py):
 *  - streamed `text` is the in-flight rendering; `answer` REPLACES it.
 *  - `sql_gate` pauses rendering: later events buffer until the user
 *    approves (hold-to-run) or holds. A gate never just vanishes.
 *  - the work log renders thinking/tools/sandbox — the work, visibly,
 *    while the user waits. The answer is never streamed as tokens.
 */

import { useEffect, useRef, useState } from "react";
import { WitnessDrawer } from "../components/WitnessDrawer";
import {
  formatBytes, formatWhen, HoldButton, RichText, Spinner, TierChip,
} from "../components/ui";
import { api } from "../lib/api";
import { COMMON, INQUIRIES as C } from "../lib/copy";
import { streamChat } from "../lib/sse";
import type {
  AnswerSections, Brief, BriefCard, ConsoleEvent, Tier,
} from "../lib/types";

type RunState = "idle" | "streaming" | "gated" | "done" | "error";

interface LiveTurn {
  question: string;
  log: ConsoleEvent[];
  liveText: string;
  answer: AnswerSections | null;
  artifacts: { title: string; html: string }[];
  gate: Extract<ConsoleEvent, { type: "sql_gate" }> | null;
  heldNote: string | null;
}

const EMPTY_TURN: LiveTurn = {
  question: "", log: [], liveText: "", answer: null,
  artifacts: [], gate: null, heldNote: null,
};

export function InquiriesTab() {
  const conversationId = useRef<string>(
    globalThis.crypto?.randomUUID?.() ?? `c${Date.now()}`);
  const pending = useRef<ConsoleEvent[]>([]);
  const gated = useRef(false);

  const [briefs, setBriefs] = useState<BriefCard[]>([]);
  const [selected, setSelected] = useState<Brief | null>(null);
  const [run, setRun] = useState<RunState>("idle");
  const [turn, setTurn] = useState<LiveTurn>(EMPTY_TURN);
  const [question, setQuestion] = useState("");
  const [suggestions, setSuggestions] = useState<
    { question: string; archetype: string }[]
  >([]);
  const [mode, setMode] = useState<"brief" | "analysis">("brief");
  const [panel, setPanel] = useState<"none" | "thread" | "ledger">("none");
  const [inspect, setInspect] = useState<string | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  const refreshBriefs = () =>
    api.briefs().then((d) => setBriefs(d.briefs)).catch(() => undefined);

  useEffect(() => {
    refreshBriefs();
    api.questions().then((d) => setSuggestions(d.questions)).catch(() => undefined);
  }, []);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight });
  }, [turn, run]);

  const handleEvent = (ev: ConsoleEvent) => {
    switch (ev.type) {
      case "turn_start":
        break;
      case "thinking":
      case "tool_call":
      case "tool_result":
      case "sandbox":
      case "gate_resolved":
        setTurn((t) => ({ ...t, log: [...t.log, ev] }));
        break;
      case "text":
        setTurn((t) => ({ ...t, liveText: t.liveText + ev.delta }));
        break;
      case "sql_gate":
        gated.current = true;
        setTurn((t) => ({ ...t, gate: ev }));
        setRun("gated");
        break;
      case "artifact":
        setTurn((t) => ({
          ...t,
          artifacts: [...t.artifacts, { title: ev.title, html: ev.html }],
        }));
        break;
      case "answer":
        // the answer SUPERSEDES streamed text — replace, never both
        setTurn((t) => ({ ...t, liveText: "", answer: ev.sections }));
        break;
      case "error":
        setTurn((t) => ({ ...t, log: [...t.log, ev] }));
        setRun("error");
        break;
      case "turn_end":
        setRun((s) => (s === "error" ? s : "done"));
        refreshBriefs();
        break;
    }
  };

  const ask = async (text: string) => {
    const q = text.trim();
    if (!q || run === "streaming" || run === "gated") return;
    setSelected(null);
    setQuestion("");
    setMode("brief");
    setPanel("none");
    gated.current = false;
    pending.current = [];
    setTurn({ ...EMPTY_TURN, question: q });
    setRun("streaming");
    try {
      for await (const ev of streamChat(q, conversationId.current)) {
        if (gated.current && ev.type !== "turn_end") {
          pending.current.push(ev);      // hold rendering at the gate
        } else if (gated.current && ev.type === "turn_end") {
          pending.current.push(ev);      // flushed on approval
        } else {
          handleEvent(ev);
        }
      }
    } catch (e) {
      handleEvent({
        type: "error",
        message: e instanceof Error ? e.message : String(e),
        recoverable: true,
      });
      setRun("error");
    }
  };

  const approveGate = async (gateId: string) => {
    await api.approve(gateId, true).catch(() => undefined);
    gated.current = false;
    setRun("streaming");
    const queued = pending.current;
    pending.current = [];
    queued.forEach(handleEvent);
  };

  const holdGate = async (gateId: string) => {
    await api.approve(gateId, false).catch(() => undefined);
    gated.current = false;
    pending.current = [];
    setTurn((t) => ({ ...t, heldNote: C.heldNote }));
    setRun("done");
    refreshBriefs();
  };

  const openBrief = (id: string) => {
    api.brief(id).then((b) => {
      if (b && (b as { found?: boolean }).found !== false) {
        setSelected(b as Brief);
        setMode("brief");
        setPanel("none");
      }
    }).catch(() => undefined);
  };

  const newInquiry = () => {
    setSelected(null);
    setTurn(EMPTY_TURN);
    setRun("idle");
  };

  // what the document panel shows: the selected brief, or the live turn
  const doc = selected
    ? {
        title: selected.title,
        status: selected.status,
        tier: selected.tier as Tier | "blocked",
        sections: selected.sections,
        thread: selected.thread,
        ledger: selected.ledger,
        artifacts: [] as { title: string; html: string }[],
        log: [] as ConsoleEvent[],
      }
    : turn.answer
      ? {
          title: turn.question,
          status: turn.answer.status,
          tier: tierFromStatus(turn.answer.status),
          sections: turn.answer,
          thread: [
            { role: "user", text: turn.question },
            { role: "agent", text: turn.answer.answer },
          ],
          ledger: ledgerFromLog(turn.log),
          artifacts: turn.artifacts,
          log: turn.log,
        }
      : null;

  const busy = run === "streaming" || run === "gated";

  return (
    <div className="inquiries">
      {/* ── rail: finished briefs ── */}
      <aside className="rail">
        <div className="rail-head">
          <span className="h-section">{C.railTitle}</span>
          <button type="button" className="btn quiet" onClick={newInquiry}>
            {C.newInquiry}
          </button>
        </div>
        <div className="rail-list">
          {briefs.map((b) => (
            <button
              key={b.id}
              type="button"
              className="brief-item"
              aria-current={selected?.id === b.id}
              onClick={() => openBrief(b.id)}
            >
              <div className="t">{b.title}</div>
              <div className="m">
                <TierChip tier={b.tier as Tier | "blocked"} />
                <span>{formatWhen(b.created_at)}</span>
              </div>
            </button>
          ))}
        </div>
      </aside>

      {/* ── conversation column ── */}
      <section className="convo">
        <div className="convo-scroll" ref={scrollRef}>
          {run === "idle" && !selected && (
            <div className="empty" style={{ margin: "auto", maxWidth: 480 }}>
              <h1 className="h-page" style={{ color: "var(--ink)" }}>
                {C.emptyTitle}
              </h1>
              <p className="h-sub" style={{ margin: "var(--s-3) auto 0" }}>
                {C.emptySub}
              </p>
            </div>
          )}

          {selected && (
            <ThreadList thread={selected.thread} />
          )}

          {!selected && turn.question && (
            <div className="msg-user">{turn.question}</div>
          )}

          {!selected && turn.log.length > 0 && (
            <WorkLog log={turn.log} onInspect={setInspect} />
          )}

          {!selected && turn.liveText && (
            <p style={{ color: "var(--ink-2)" }}>{turn.liveText}</p>
          )}

          {!selected && run === "streaming" && (
            <div className="status-line">
              <Spinner /> {C.working}
            </div>
          )}

          {!selected && run === "gated" && turn.gate && (
            <GateCard
              gate={turn.gate}
              onApprove={() => approveGate(turn.gate!.gate_id)}
              onHold={() => holdGate(turn.gate!.gate_id)}
            />
          )}

          {!selected && turn.heldNote && (
            <div className="notice">
              <span aria-hidden>⏸</span>
              <span>{turn.heldNote}</span>
            </div>
          )}
        </div>

        {/* composer */}
        <div className="composer">
          {run === "idle" && !selected && suggestions.length > 0 && (
            <div className="suggest" aria-label={C.suggestTitle}>
              {suggestions.slice(0, 4).map((s) => (
                <button
                  key={s.question}
                  type="button"
                  onClick={() => ask(s.question)}
                >
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
            <button
              type="button"
              className="btn primary"
              onClick={() => ask(question)}
              disabled={!question.trim() || busy}
            >
              {C.send}
            </button>
          </div>
        </div>
      </section>

      {/* ── brief document panel ── */}
      <section className="briefdoc">
        <div className="briefdoc-bar">
          <div className="mode-switch" role="tablist" aria-label="Document mode">
            <button role="tab" aria-selected={mode === "brief"}
              onClick={() => setMode("brief")}>{C.briefMode}</button>
            <button role="tab" aria-selected={mode === "analysis"}
              onClick={() => setMode("analysis")}>{C.analysisMode}</button>
          </div>
          <span style={{ flex: 1 }} />
          <button type="button" className="toggle-btn"
            aria-pressed={panel === "thread"}
            onClick={() => setPanel(panel === "thread" ? "none" : "thread")}>
            {C.threadToggle}
          </button>
          <button type="button" className="toggle-btn"
            aria-pressed={panel === "ledger"}
            onClick={() => setPanel(panel === "ledger" ? "none" : "ledger")}>
            {C.ledgerToggle}
          </button>
        </div>

        <div className="briefdoc-scroll">
          {!doc && (
            <div className="empty">
              {run === "gated" ? C.needsSignature : C.noBriefYet}
            </div>
          )}

          {doc && panel === "thread" && (
            <>
              <div className="h-section" style={{ marginBottom: "var(--s-3)" }}>
                {C.threadTitle}
              </div>
              <ThreadList thread={doc.thread} />
            </>
          )}

          {doc && panel === "ledger" && (
            <>
              <div className="h-section" style={{ marginBottom: "var(--s-3)" }}>
                {C.ledgerTitle}
              </div>
              {doc.ledger.length === 0 ? (
                <p style={{ color: "var(--ink-3)" }}>{C.ledgerEmpty}</p>
              ) : (
                <div className="cite-list">
                  {doc.ledger.map((l, i) => (
                    <button key={i} type="button" className="cite"
                      onClick={() => setInspect(l.ref)}>
                      <span aria-hidden>▤</span> Executed query
                      <span className="ref">{l.ref}</span>
                    </button>
                  ))}
                </div>
              )}
            </>
          )}

          {doc && panel === "none" && (
            <>
              <h2 className="brief-title">{doc.title}</h2>
              <div className="brief-meta">
                <TierChip tier={doc.tier} />
                {doc.status && (
                  <span style={{ color: "var(--ink-3)", fontSize: "var(--fs-12)" }}>
                    {doc.status}
                  </span>
                )}
              </div>

              {mode === "brief" && (
                <>
                  <p className="brief-answer">
                    <RichText text={doc.sections.answer} />
                  </p>
                  {doc.sections.citations.length > 0 && (
                    <div className="brief-block">
                      <div className="h-section">{C.citations}</div>
                      <div className="cite-list">
                        {doc.sections.citations.map((c) => (
                          <button key={c.ref + c.label} type="button"
                            className="cite" onClick={() => setInspect(c.ref)}>
                            <span aria-hidden>⌕</span> {c.label}
                            <span className="ref">{c.ref}</span>
                          </button>
                        ))}
                      </div>
                    </div>
                  )}
                  {doc.sections.governance && (
                    <div className="brief-block">
                      <div className="h-section">{C.governance}</div>
                      <p style={{ color: "var(--ink-2)" }}>
                        {doc.sections.governance}
                      </p>
                    </div>
                  )}
                </>
              )}

              {mode === "analysis" && (
                <>
                  {doc.sections.how_i_got_there && (
                    <div className="brief-block" style={{ marginTop: 0 }}>
                      <div className="h-section">{C.howTitle}</div>
                      <p style={{ color: "var(--ink-2)" }}>
                        {doc.sections.how_i_got_there}
                      </p>
                    </div>
                  )}
                  {doc.artifacts.map((a, i) => (
                    <div className="brief-block" key={i}>
                      <div className="h-section">{a.title}</div>
                      <iframe
                        className="artifact-frame"
                        title={a.title || `artifact ${i + 1}`}
                        sandbox=""
                        srcDoc={a.html}
                      />
                    </div>
                  ))}
                  {doc.log.length > 0 && (
                    <div className="brief-block">
                      <div className="h-section">Work log</div>
                      <WorkLog log={doc.log} onInspect={setInspect} />
                    </div>
                  )}
                </>
              )}
            </>
          )}
        </div>
        <div className="govfoot">{COMMON.evidenceFooter}</div>
      </section>

      {inspect && (
        <WitnessDrawer refUri={inspect} onClose={() => setInspect(null)} />
      )}
    </div>
  );
}

/* ── pieces ── */

function ThreadList({ thread }: { thread: { role: string; text: string }[] }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--s-3)" }}>
      {thread.map((m, i) =>
        m.role === "user" ? (
          <div key={i} className="msg-user">{m.text}</div>
        ) : (
          <p key={i} style={{ color: "var(--ink-2)" }}>
            <RichText text={m.text} />
          </p>
        ),
      )}
    </div>
  );
}

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
          case "tool_result":
            return (
              <div key={i} className="wl-row">
                <span className="g" aria-hidden>{ev.ok ? "✓" : "✕"}</span>
                <span className="sum">{ev.summary}</span>
                {ev.provenance && <TierChip tier={ev.provenance.tier} />}
              </div>
            );
          case "sandbox":
            return (
              <div key={i}>
                <div className="wl-row">
                  <span className="g" aria-hidden>⌗</span>
                  <span className="sum">Computed in the sandbox</span>
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
                  <button
                    type="button"
                    className="toggle-btn"
                    onClick={() => onInspect(`ledger:#${ev.ledger_id}`)}
                  >
                    ledger
                  </button>
                )}
              </div>
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

function tierFromStatus(status: string): Tier | "blocked" {
  const s = status.toLowerCase();
  if (s.includes("guardrail") || s.includes("refused")) return "blocked";
  if (s.includes("grounded")) return "grounded";
  return "inferred";
}

function ledgerFromLog(log: ConsoleEvent[]): { ref: string }[] {
  return log
    .filter(
      (e): e is Extract<ConsoleEvent, { type: "gate_resolved" }> =>
        e.type === "gate_resolved" && !!e.ledger_id,
    )
    .map((e) => ({ ref: `ledger:#${e.ledger_id}` }));
}
