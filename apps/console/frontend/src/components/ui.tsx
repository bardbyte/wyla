/** Shared primitives. Each one enforces a design rule so the tabs
 * can't drift: tiers render through TierChip (word + fill, never a
 * percentage), sample data through SourceBadge (truth of state), and
 * consequential actions through HoldButton (a click is not a
 * signature). */

import { useEffect, useRef, useState } from "react";
import { COMMON, TIERS } from "../lib/copy";
import type { Tier } from "../lib/types";

export function TierChip({
  tier,
  onClick,
}: {
  tier: Tier | "blocked";
  onClick?: () => void;
}) {
  const t = TIERS[tier] ?? TIERS.guessed;
  const cls = `chip ${t.cls} ${onClick ? "" : "static"}`;
  const body = (
    <>
      <span className="fill" aria-hidden>
        {t.fill}
      </span>
      {t.word}
    </>
  );
  if (!onClick)
    return <span className={cls}>{body}</span>;
  return (
    <button
      type="button"
      className={cls}
      onClick={onClick}
      title="Inspect the evidence"
    >
      {body}
    </button>
  );
}

export function SourceBadge({ live }: { live: boolean }) {
  return (
    <span className="env-badge" title={live
      ? "Reading from the compiled knowledge graph"
      : "Illustrative data — connect a graph snapshot to go live"}>
      <span className={`live-dot ${live ? "" : "sample"}`} aria-hidden />
      {live ? COMMON.liveGraph : COMMON.sampleData}
    </span>
  );
}

export function PreviewBadge() {
  return <span className="badge-preview">{COMMON.preview}</span>;
}

export function Spinner() {
  return <span className="spinner" role="status" aria-label="Working" />;
}

/** Press-and-hold confirmation for consequential actions. Fires only
 * after an unbroken 900 ms hold; releasing early cancels. */
export function HoldButton({
  label,
  onConfirm,
  disabled,
}: {
  label: string;
  onConfirm: () => void;
  disabled?: boolean;
}) {
  const [holding, setHolding] = useState(false);
  const timer = useRef<number | null>(null);
  const clear = () => {
    if (timer.current !== null) window.clearTimeout(timer.current);
    timer.current = null;
    setHolding(false);
  };
  useEffect(() => clear, []);
  const start = () => {
    if (disabled) return;
    setHolding(true);
    timer.current = window.setTimeout(() => {
      clear();
      onConfirm();
    }, 900);
  };
  return (
    <button
      type="button"
      className={`hold-btn ${holding ? "holding" : ""}`}
      disabled={disabled}
      onPointerDown={start}
      onPointerUp={clear}
      onPointerLeave={clear}
      onKeyDown={(e) => {
        // keyboard path: hold Space/Enter down for the same duration
        if ((e.key === " " || e.key === "Enter") && !e.repeat) start();
      }}
      onKeyUp={clear}
    >
      <span className="fillbar" aria-hidden />
      {label}
    </button>
  );
}

/** Minimal, safe markdown-ish renderer: **bold** and `code` only. */
export function RichText({ text }: { text: string }) {
  const parts: (string | JSX.Element)[] = [];
  let rest = text;
  let k = 0;
  const rx = /(\*\*[^*]+\*\*|`[^`]+`)/;
  for (;;) {
    const m = rest.match(rx);
    if (!m || m.index === undefined) {
      parts.push(rest);
      break;
    }
    if (m.index > 0) parts.push(rest.slice(0, m.index));
    const tok = m[0];
    if (tok.startsWith("**"))
      parts.push(<strong key={k++}>{tok.slice(2, -2)}</strong>);
    else parts.push(<code key={k++}>{tok.slice(1, -1)}</code>);
    rest = rest.slice(m.index + tok.length);
  }
  return <>{parts}</>;
}

export function formatBytes(n: number | null): string {
  if (n === null || !Number.isFinite(n)) return "unknown";
  if (n >= 1e12) return `${(n / 1e12).toFixed(2)} TB`;
  if (n >= 1e9) return `${(n / 1e9).toFixed(2)} GB`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)} MB`;
  return `${Math.round(n / 1e3)} KB`;
}

export function formatRelative(iso: string): string {
  if (!iso) return "";
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "";
  const mins = Math.max(0, Math.round((Date.now() - then) / 60000));
  if (mins < 2) return "just now";
  if (mins < 60) return `${mins} min ago`;
  const hours = Math.round(mins / 60);
  if (hours < 48) return `${hours} h ago`;
  return `${Math.round(hours / 24)} d ago`;
}

/** Query rows out of a tool payload, wherever the runner put them. */
export function extractRows(
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

export function ResultsTable({ rows }: {
  rows: Record<string, unknown>[];
}) {
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

export function formatWhen(iso: string): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}
