/** The briefing — the product answers before being asked.
 *
 * One tile per pin: the headline number, the delta vs the previous
 * run, the worst-of-citations evidence chip, and the staleness cue.
 * Re-run replays the pin's exact SQL through every gate server-side;
 * a refusal renders honestly (offline `no_client` is a feature, not an
 * error). Deltas use glyphs in neutral ink — tier colors stay reserved
 * for evidence.
 */

import { useCallback, useEffect, useState } from "react";
import {
  HoldButton, PreviewBadge, ResultsTable, RichText, SourceBadge,
  Spinner, TierChip, formatRelative,
} from "../components/ui";
import { api } from "../lib/api";
import { BRIEFING as B } from "../lib/copy";
import { useNav } from "../lib/nav";
import type { Pin, PinRun } from "../lib/types";

export function BriefingTab() {
  const nav = useNav();
  const [pins, setPins] = useState<Pin[] | null>(null);
  const [seeded, setSeeded] = useState(false);
  const [live, setLive] = useState(false);
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [notes, setNotes] = useState<Record<string, string>>({});
  const [openTables, setOpenTables] = useState<Record<string, boolean>>({});

  const refresh = useCallback(() => {
    api.pins().then((d) => {
      setPins(d.pins);
      setSeeded(d.seeded);
      setLive(d.live);
    }).catch(() => setPins([]));
  }, []);

  useEffect(() => {
    refresh();
    api.questions()
      .then((d) => setSuggestions(d.questions.map((q) => q.question)))
      .catch(() => undefined);
  }, [refresh]);

  const rerun = async (pin: Pin) => {
    setBusyId(pin.id);
    setNotes((n) => ({ ...n, [pin.id]: "" }));
    try {
      const out = await api.rerunPin(pin.id);
      if (out.run.status === "refused") {
        setNotes((n) => ({
          ...n,
          [pin.id]: noteForRefusal(out.run),
        }));
      } else if (out.run.locator_missed) {
        setNotes((n) => ({ ...n, [pin.id]: B.shapeChanged }));
      }
      refresh();
    } catch (e) {
      setNotes((n) => ({
        ...n,
        [pin.id]: e instanceof Error && e.message === "seed_pin"
          ? B.rerunSeed : B.unreachable,
      }));
    } finally {
      setBusyId(null);
    }
  };

  const pinnedQuestions = new Set(
    (pins ?? []).map((p) => p.question.trim().toLowerCase()));
  const open = (pins ?? []).length > 0;

  return (
    <div className="page">
      <div className="page-inner">
        <div className="page-head">
          <div className="grow">
            <h1 className="h-page">{B.title}</h1>
            <p className="h-sub">{B.sub}</p>
          </div>
          <SourceBadge live={live} />
        </div>

        {pins === null && <Spinner />}

        {pins !== null && pins.length === 0 && (
          <div className="card card-pad empty" style={{ maxWidth: 520, margin: "var(--s-6) auto" }}>
            <p style={{ marginBottom: "var(--s-4)" }}>{B.empty}</p>
            <button type="button" className="btn primary"
              onClick={() => nav.go("inquiries")}>
              {B.emptyCta}
            </button>
          </div>
        )}

        <div className="brief-grid">
          {(pins ?? []).map((pin) => (
            <PinTile key={pin.id} pin={pin}
              busy={busyId === pin.id}
              note={notes[pin.id] ?? ""}
              tableOpen={!!openTables[pin.id]}
              onToggleTable={() => setOpenTables((o) => ({
                ...o, [pin.id]: !o[pin.id]}))}
              onRerun={() => rerun(pin)}
              onEvidence={() => {
                const ref = pin.citations.find(
                  (c) => !c.ref.startsWith("ledger:"))?.ref;
                if (ref) nav.openEvidence(ref);
              }}
              onFollowUp={() => nav.askAbout(pin.question)}
              onVerify={async () => {
                await api.verifyPin(pin.id, !pin.verified)
                  .catch(() => undefined);
                refresh();
              }}
              onUnpin={async () => {
                await api.deletePin(pin.id).catch(() => undefined);
                refresh();
              }}
            />
          ))}
        </div>

        {open && suggestions.length > 0 && (
          <section style={{ marginTop: "var(--s-6)" }}>
            <div className="h-section" style={{ marginBottom: "var(--s-3)" }}>
              {B.suggestTitle}
            </div>
            <div className="suggest">
              {suggestions
                .filter((q) => !pinnedQuestions.has(q.trim().toLowerCase()))
                .slice(0, 4)
                .map((q) => (
                  <button key={q} type="button"
                    onClick={() => nav.askAbout(
                      q, { send: true, autoPin: true })}>
                    {q} — {B.answerAndPin}
                  </button>
                ))}
            </div>
          </section>
        )}

        {seeded && open && (
          <p style={{ color: "var(--ink-3)", fontSize: "var(--fs-12)", marginTop: "var(--s-5)" }}>
            <PreviewBadge /> These are sample tiles — pin a real answer
            and they retire.
          </p>
        )}
      </div>
    </div>
  );
}

function noteForRefusal(run: PinRun): string {
  if (run.code === "no_client" || run.code === "no_graph") {
    return `${B.unreachable} (${run.code}).`;
  }
  return `${B.refusedPrefix}: ${run.code}${
    run.reason ? ` — ${run.reason}` : ""}`;
}

function delta(pin: Pin): { text: string } | null {
  const ok = pin.history.filter(
    (h) => h.status === "ok" && typeof h.value === "number");
  if (ok.length < 2) return null;
  const prev = ok[ok.length - 2].value as number;
  const curr = ok[ok.length - 1].value as number;
  if (prev === 0) return null;
  const diff = curr - prev;
  const pct = (diff / Math.abs(prev)) * 100;
  const glyph = diff > 0 ? "▲" : diff < 0 ? "▼" : "◆";
  const num = Math.abs(diff) >= 1
    ? Math.abs(diff).toLocaleString(undefined, { maximumFractionDigits: 1 })
    : Math.abs(diff).toFixed(3);
  return {
    text: `${glyph} ${diff >= 0 ? "+" : "−"}${num} `
      + `(${pct >= 0 ? "+" : ""}${pct.toFixed(1)}%) ${B.vsPrevious}`,
  };
}

function headlineText(pin: Pin): string | null {
  const h = pin.headline;
  if (h.kind === "scalar" || h.kind === "series_last") {
    const v = h.value as number;
    return Math.abs(v) < 1 && v !== 0
      ? v.toLocaleString(undefined, { maximumFractionDigits: 3 })
      : v.toLocaleString();
  }
  return null;
}

function PinTile({
  pin, busy, note, tableOpen, onToggleTable, onRerun, onEvidence,
  onFollowUp, onVerify, onUnpin,
}: {
  pin: Pin; busy: boolean; note: string; tableOpen: boolean;
  onToggleTable: () => void; onRerun: () => void; onEvidence: () => void;
  onFollowUp: () => void; onVerify: () => void; onUnpin: () => void;
}) {
  const num = headlineText(pin);
  const d = delta(pin);
  const lastOk = [...pin.history].reverse().find((h) => h.status === "ok");
  const asOf = lastOk ? formatRelative(lastOk.ts) : "";
  const isSeed = pin.source === "seed";
  const canRerun = !!pin.sql && !isSeed;

  return (
    <article className="card card-pad brief-tile">
      <div className="brief-q">{pin.question}</div>

      {num !== null ? (
        <div className="brief-headline">
          <span className="brief-num">{num}</span>
          {pin.headline.column && (
            <span className="brief-col">{pin.headline.column}</span>
          )}
        </div>
      ) : pin.headline.kind === "rows" ? (
        <div className="brief-headline">
          <span className="brief-num">{pin.headline.n_rows}</span>
          <span className="brief-col">{B.rowsWord}</span>
        </div>
      ) : (
        <div className="brief-fact">
          <RichText text={pin.answer} />
        </div>
      )}

      {d && <div className="brief-delta">{d.text}</div>}
      {pin.rows.length > 0 && (
        <button type="button" className="btn quiet"
          style={{ alignSelf: "flex-start", padding: "2px 8px" }}
          onClick={onToggleTable}>
          {tableOpen ? B.hideTable : B.viewTable}
        </button>
      )}
      {tableOpen && pin.rows.length > 0 && (
        <ResultsTable rows={pin.rows} />
      )}

      <div className="brief-meta">
        <TierChip tier={pin.tier} onClick={onEvidence} />
        {pin.verified && (
          <span className="tag"
            title={`${pin.verified.by} · ${pin.verified.at}`}>
            {B.verifiedBadge}
          </span>
        )}
        {isSeed && <span className="tag">{B.seedBadge}</span>}
        {asOf && (
          <span className="brief-asof">{B.asOf} {asOf}</span>
        )}
      </div>

      {note && (
        <div className="notice" style={{ fontSize: "var(--fs-12)" }}>
          <span aria-hidden>⏸</span>
          <span>{note}</span>
        </div>
      )}

      <div className="brief-actions">
        <button type="button" className="btn"
          disabled={!canRerun || busy}
          title={isSeed ? B.rerunSeed : !pin.sql ? B.rerunNoSql : ""}
          onClick={onRerun}>
          {busy ? B.rerunning : B.rerun}
        </button>
        {busy && <Spinner />}
        <button type="button" className="btn quiet" onClick={onFollowUp}>
          {B.followUp}
        </button>
        {!isSeed && (
          <span style={{ marginLeft: "auto", display: "flex", gap: "var(--s-2)" }}>
            {!pin.verified && (
              <HoldButton label={B.verifyHold} onConfirm={onVerify} />
            )}
            <HoldButton label={B.unpinHold} onConfirm={onUnpin} />
          </span>
        )}
      </div>
    </article>
  );
}
