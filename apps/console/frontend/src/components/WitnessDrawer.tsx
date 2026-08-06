/** The evidence panel behind every chip and citation: what the graph
 * holds at a reference, which sources said it, and what it connects
 * to. Chips are doors, not decorations — this is the room. */

import { useEffect, useState } from "react";
import { api } from "../lib/api";
import { COMMON } from "../lib/copy";
import type { Tier, Witness } from "../lib/types";
import { SourceBadge, Spinner, TierChip } from "./ui";

export function WitnessDrawer({
  refUri,
  onClose,
}: {
  refUri: string;
  onClose: () => void;
}) {
  const [data, setData] = useState<{ live: boolean; witness: Witness } | null>(
    null,
  );
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    let on = true;
    setData(null);
    setFailed(false);
    if (refUri.startsWith("ledger:")) return; // rendered inline below
    api
      .witness(refUri)
      .then((d) => on && setData(d))
      .catch(() => on && setFailed(true));
    return () => {
      on = false;
    };
  }, [refUri]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const w = data?.witness;
  const isLedger = refUri.startsWith("ledger:");

  return (
    <>
      <div className="drawer-veil" onClick={onClose} aria-hidden />
      <aside
        className="drawer"
        role="dialog"
        aria-label="Evidence"
        aria-modal="true"
      >
        <div className="drawer-head">
          <strong>Evidence</strong>
          {data && <SourceBadge live={data.live} />}
          <button
            type="button"
            className="icon-btn"
            style={{ marginLeft: "auto" }}
            onClick={onClose}
            aria-label={COMMON.close}
          >
            ✕
          </button>
        </div>
        <div className="drawer-body">
          <code style={{ color: "var(--ink-3)", overflowWrap: "anywhere" }}>
            {refUri}
          </code>

          {isLedger && (
            <p style={{ color: "var(--ink-2)" }}>
              An audit-ledger entry: the executed query, its scan size, row
              count, approver, and timestamp are retained on the warehouse
              ledger under this reference.
            </p>
          )}

          {!isLedger && !data && !failed && <Spinner />}
          {failed && (
            <p style={{ color: "var(--tier-block)" }}>
              The evidence service is unreachable right now.
            </p>
          )}

          {w && !w.found && (
            <p style={{ color: "var(--ink-2)" }}>
              Nothing is recorded at this reference in the current snapshot.
            </p>
          )}

          {w && w.found && (
            <>
              <div style={{ display: "flex", gap: "var(--s-2)", alignItems: "center" }}>
                <TierChip tier={(w.provenance?.tier ?? "guessed") as Tier} />
                <span className="tag">{w.kind}</span>
              </div>

              {w.ledger && w.ledger.rows.length > 0 ? (
                <section className="dossier">
                  <div className="dossier-eyebrow">Witness ledger</div>
                  {w.ledger.rows.map((r) => (
                    <div key={r.source} className="dossier-row">
                      <span className="d-src">{r.source}</span>
                      <span className="d-note">
                        weight {r.weight} × {r.capped}
                        {r.count > r.capped
                          ? ` (of ${r.count}, capped)` : ""}
                      </span>
                      <span className="d-val">
                        +{r.contribution.toFixed(2)}
                      </span>
                    </div>
                  ))}
                  <div className="dossier-eyebrow"
                    style={{ marginTop: "var(--s-4)" }}>Arithmetic</div>
                  <div className="dossier-math">
                    <div>{w.ledger.rows.map((r) =>
                      `${r.weight}×${r.capped}`).join(" + ")}
                      {" = "}{w.ledger.weighted}
                      {" / "}{w.ledger.denominator}</div>
                    <div>= <strong>{w.ledger.score.toFixed(2)}</strong>
                      {" · "}{w.ledger.distinct} distinct
                      witness{w.ledger.distinct === 1 ? "" : "es"}</div>
                    <div className="d-rule">{w.ledger.rule}</div>
                  </div>
                </section>
              ) : w.provenance && (
                <section>
                  <div className="h-section">Sources</div>
                  <div style={{ display: "flex", gap: "var(--s-2)", flexWrap: "wrap", marginTop: "var(--s-2)" }}>
                    {w.provenance.sources.length ? (
                      w.provenance.sources.map((s) => (
                        <span key={s} className="tag">
                          {s}
                        </span>
                      ))
                    ) : (
                      <span style={{ color: "var(--ink-3)" }}>none recorded</span>
                    )}
                  </div>
                </section>
              )}

              {w.properties && Object.keys(w.properties).length > 0 && (
                <section>
                  <div className="h-section">Recorded facts</div>
                  <dl className="kv" style={{ marginTop: "var(--s-2)" }}>
                    {Object.entries(w.properties)
                      .slice(0, 14)
                      .map(([k, v]) => (
                        <span key={k} style={{ display: "contents" }}>
                          <dt>{k}</dt>
                          <dd>{String(v).slice(0, 160)}</dd>
                        </span>
                      ))}
                  </dl>
                </section>
              )}

              {w.edges && w.edges.length > 0 && (
                <section>
                  <div className="h-section">Connections</div>
                  <dl className="kv" style={{ marginTop: "var(--s-2)" }}>
                    {w.edges.slice(0, 12).map((e, i) => (
                      <span key={i} style={{ display: "contents" }}>
                        <dt>
                          {e.direction === "out" ? "→" : "←"} {e.type}
                        </dt>
                        <dd>{e.other.split("/").slice(-2).join("/")}</dd>
                      </span>
                    ))}
                  </dl>
                </section>
              )}
            </>
          )}
        </div>
      </aside>
    </>
  );
}
