/** The in-space paper chrome shared by Ask and the Knowledge Graph:
 * the question bar that floats in the cosmos (mockup 1b) and the SQL
 * seal — governance as a physical act (mockup 1c). */

import { HoldButton, formatBytes } from "./ui";
import type { Listening } from "../lib/anticipation";
import { ASK, TIERS } from "../lib/copy";
import type { PendingGate } from "../lib/nav";

export function SpaceAskBar({
  value, onChange, onSubmit, placeholder, tally, raised = false,
}: {
  value: string;
  onChange(v: string): void;
  onSubmit(text: string): void;
  placeholder: string;
  tally: Listening;
  raised?: boolean;
}) {
  return (
    <div className={`space-ask ${raised ? "raised" : ""}`}>
      <span className="space-ask-dot" aria-hidden />
      <input value={value}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter" && value.trim()) onSubmit(value.trim());
        }}
        placeholder={placeholder} aria-label={placeholder} />
      {tally.ids.length > 0 && (
        <span className="space-ask-tally">
          {tally.ids.length} listening
          {tally.best &&
            ` · ${tally.best.label} (${
              TIERS[tally.best.tier as keyof typeof TIERS]?.word
              ?? tally.best.tier})`}
        </span>
      )}
    </div>
  );
}

export function SpaceSeal({ gate }: { gate: PendingGate }) {
  return (
    <div className="space-seal" role="group" aria-label={ASK.gateTitle}>
      <div className="seal-eyebrow">{ASK.gateTitle}</div>
      <code className="seal-sql">
        {gate.sql.length > 90 ? gate.sql.slice(0, 89) + "…" : gate.sql}
      </code>
      <div className="seal-row">
        <span className="seal-scan">
          {ASK.gateScan}: <strong>{formatBytes(gate.bytes)}</strong>
        </span>
        <HoldButton label={ASK.gateHold}
          onConfirm={() => gate.resolve(true)} />
        <button type="button" className="btn quiet"
          onClick={() => gate.resolve(false)}>
          {ASK.gateLater}
        </button>
      </div>
    </div>
  );
}
