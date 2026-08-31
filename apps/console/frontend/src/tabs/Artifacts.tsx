/** Artifacts — the Knowledge Files shelf. What Meridian already
 * knows from curated domain files (travel, CFR, …), what is staged
 * and waiting for the loader, and the door to add one for another
 * business unit. Staging is a SOURCE drop — the file lands in
 * sources/artifacts/, honestly labeled as staged until the Knowledge
 * Files loader lands; the clerk stays the only graph writer. */

import { useEffect, useState } from "react";
import type { SourceCard, Unavailable } from "../lib/meridian";

interface ArtifactsPayload {
  available: boolean;
  reason?: string;
  known: SourceCard[];
  staged: string[];
  staging_dir: string;
}

export function ArtifactsTab() {
  const [payload, setPayload] =
    useState<ArtifactsPayload | Unavailable | null>(null);
  const [bu, setBu] = useState("");
  const [name, setName] = useState("");
  const [content, setContent] = useState("");
  const [result, setResult] = useState<string | null>(null);

  const load = () =>
    fetch("/api/meridian/artifacts").then((r) => r.json())
      .then(setPayload)
      .catch(() => setPayload(
        { available: false, reason: "console unreachable" }));
  useEffect(() => { load(); }, []);

  const stage = async () => {
    setResult(null);
    const response = await fetch("/api/meridian/artifacts", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(
        { business_unit: bu, name, content }),
    });
    const body = await response.json();
    if (response.ok && body.staged) {
      setResult(`staged as ${body.file} — ${body.note}`);
      setBu(""); setName(""); setContent("");
      load();
    } else {
      setResult(body.reason ??
        (body.detail ? JSON.stringify(body.detail) : "refused"));
    }
  };

  const canStage = /^[A-Za-z0-9_-]{1,40}$/.test(bu)
    && name.trim().length > 0 && content.trim().length > 0;

  return (
    <div className="m-page">
      <div className="m-masthead">
        <span className="m-wordmark">LUMI</span>
        <span className="m-tagline">Understand · Artifacts (Knowledge Files)</span>
      </div>

      <div className="m-card">
        <div className="m-card-label">
          WHAT MERIDIAN ALREADY KNOWS — curated knowledge files
        </div>
        {payload === null && <p className="m-muted">loading…</p>}
        {payload && "known" in payload && payload.known.length === 0 && (
          <p className="m-muted">
            no knowledge file has reached the graph yet
            {payload.reason ? ` — ${payload.reason}` : ""}
          </p>
        )}
        {payload && "known" in payload && payload.known.map((s) => (
          <div key={s.source} className="m-source">
            <div className="m-source-head">
              <span className="m-chip m-source-chip">{s.chip}</span>
            </div>
            <div className="m-source-name">{s.display}</div>
            <div className="m-source-counts m-mono">
              {Object.entries(s.contributes.nodes)
                .map(([k, n]) => `${k} ${n}`).join(" · ") || "—"}
            </div>
            {Object.keys(s.ledger).length > 0 && (
              <div className="m-source-ledger m-muted">
                ledger: {Object.entries(s.ledger)
                  .map(([k, n]) => `${n} ${k}`).join(" · ")}
              </div>
            )}
          </div>
        ))}
      </div>

      <div className="m-card">
        <div className="m-card-label">STAGED — waiting for the loader</div>
        {payload && "staged" in payload && payload.staged.length === 0 && (
          <p className="m-muted">
            nothing staged — add a knowledge file below and it appears
            here, honestly labeled, until the Knowledge Files loader
            ingests it
          </p>
        )}
        {payload && "staged" in payload && payload.staged.map((f) => (
          <div key={f} className="m-mono m-muted">
            {f} <span className="m-chip">staged</span>
          </div>
        ))}
      </div>

      <div className="m-card">
        <div className="m-card-label">
          ADD A KNOWLEDGE FILE — a source drop, named from the domain
        </div>
        <div className="m-artifact-form">
          <input className="m-search" placeholder="business unit (e.g. USCS)"
            value={bu} onChange={(e) => setBu(e.target.value)} />
          <input className="m-search" placeholder="artifact name (e.g. lending vocabulary)"
            value={name} onChange={(e) => setName(e.target.value)} />
        </div>
        <textarea className="m-artifact-content"
          placeholder={"the knowledge itself — definitions, joins, "
            + "vocabulary, guardrails for this business unit"}
          value={content} onChange={(e) => setContent(e.target.value)} />
        <div className="m-legend">
          <button className="m-door m-door-primary" disabled={!canStage}
            onClick={stage}>Stage artifact</button>
          <span className="m-muted">
            lands in sources/artifacts/ · ingested by the next build
            once the loader ships · recorded as you
          </span>
        </div>
        {result && <p className="m-muted">{result}</p>}
      </div>
    </div>
  );
}
