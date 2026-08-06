/** Mirrors apps/console/backend/events.py + the /api payloads.
 * The frontend is a pure function of these shapes. */

export type Tier =
  | "deprecated" | "guessed" | "inferred" | "grounded" | "human_asserted";

export interface Provenance {
  tier: Tier;
  score: number;
  sources: string[];
  evidence_count: number;
}

export interface Citation { label: string; ref: string }

export interface AnswerSections {
  answer: string;
  how_i_got_there: string;
  citations: Citation[];
  governance: string;
  status: string;
}

export type ConsoleEvent =
  | { type: "turn_start"; turn_id: string; ts?: string }
  | { type: "thinking"; delta: string; ts?: string }
  | { type: "text"; delta: string; ts?: string }
  | { type: "tool_call"; call_id: string; tool: string; verb: string;
      args_summary: string; args: Record<string, unknown>; ts?: string }
  | { type: "tool_result"; call_id: string; ok: boolean; summary: string;
      provenance: Provenance | null;
      payload: Record<string, unknown> | null; ts?: string }
  | { type: "sql_gate"; gate_id: string; sql: string;
      bytes_estimate: number | null; guardrail_checks: string[];
      requires_approval: boolean; ts?: string }
  | { type: "gate_resolved"; gate_id: string;
      decision: "approved" | "held"; actor: string;
      ledger_id: string | null; rows_returned: number | null; ts?: string }
  | { type: "sandbox"; code: string; stdout: string; result: unknown;
      ok: boolean; ts?: string }
  | { type: "artifact"; artifact_id: string; kind: "chart" | "dashboard";
      title: string; html: string; ts?: string }
  | { type: "answer"; sections: AnswerSections; ts?: string }
  | { type: "error"; message: string; recoverable: boolean; ts?: string }
  | { type: "turn_end"; turn_id: string;
      usage: Record<string, unknown>; ts?: string };

/* ── /api payloads ── */

export interface Readiness {
  columns: number; meaning_pct: number; related_tables: number;
  metrics: number; governance: boolean; lineage: boolean;
}

export interface Product {
  name: string; ref: string; description: string; owner: string;
  domain: string; lifecycle: string; tier: Tier; readiness: Readiness;
}

export interface Unit {
  unit: string; table_count: number; total_columns: number;
  mean_meaning_pct: number; grounded_tables: number;
  pii_tables: number; governed_tables: number; products: Product[];
}

export interface GraphMapNode {
  id: string; label: string;
  kind: "table" | "entity" | "metric" | "skill";
  tier: Tier; columns?: number; business_unit?: string;
  pii?: boolean; subtitle?: string;
}

export interface GraphMapEdge {
  source: string; target: string;
  kind: "equivalent_to" | "identifies" | "computed_from" | "applies_to";
  tier: Tier;
}

export interface InsightRelationship {
  kind: "join" | "identifies" | "metric" | "lineage" | "dq";
  predicate: string; other: string; other_ref: string;
  sources: string[]; witness: string; tier: Tier;
}

export interface TableInsights {
  table: string; found: boolean; ref?: string;
  description: { curated?: string; ai?: string; tier?: Tier;
    sources?: string[] };
  columns: { count?: number; described?: number; pii?: number };
  relationships: InsightRelationship[];
  recommendations: { question: string; source: string }[];
}

export interface AgentSelftest {
  ok: boolean; runner: string; model?: string; note?: string;
  error?: string;
}

export interface GraphMap {
  nodes: GraphMapNode[]; edges: GraphMapEdge[]; truncated: boolean;
}

export interface Metric {
  name: string; ref: string; formula: string; description: string;
  tier: Tier; sources: string[];
}

export interface Viability {
  verdict: "exact" | "near_duplicate" | "clear";
  exact: Metric[];
  near: (Metric & { shared_terms?: string[] })[];
  canon_size: number;
}

export interface GraphSummary {
  nodes: number; edges: number;
  nodes_by_type: Record<string, number>;
  edges_by_type: Record<string, number>;
  tiers: Record<string, number>;
  witnesses: Record<string, number>;
  snapshot_version: string | null;
}

export interface ThreadHop {
  kind: string; label: string; ref: string; tier: Tier; detail: string;
}

export interface WitnessLedgerRow {
  source: string; weight: number; count: number; capped: number;
  contribution: number;
}

export interface WitnessLedger {
  rows: WitnessLedgerRow[]; weighted: number; denominator: number;
  score: number; distinct: number; rule: string;
}

export interface Witness {
  ref: string; found: boolean; kind?: string;
  properties?: Record<string, unknown>;
  provenance?: { tier: Tier; score: number; sources: string[] };
  ledger?: WitnessLedger;
  edges?: { type: string; other: string; direction: string; tier: Tier }[];
}

export interface LexiconEntry {
  name: string; kind: "table" | "metric" | "entity"; ref: string;
}

export interface PinCitation { label: string; ref: string; tier: Tier | null }

export interface PinRun {
  kind: "capture" | "rerun"; ts: string; actor: string;
  status: "ok" | "refused"; code: string | null;
  value?: number | null; n_rows?: number;
  ledger_id?: string | null; reason?: string; locator_missed?: boolean;
}

export interface Headline {
  kind: "scalar" | "series_last" | "rows" | "none";
  column?: string; value?: number; n_rows?: number;
  locator_missed?: boolean;
}

export interface Pin {
  id: string; question: string; answer: string;
  sql: string | null; sql_sha256: string | null;
  citations: PinCitation[]; tier: Tier;
  locator: { row: string; column: string } | null;
  headline: Headline;
  rows: Record<string, unknown>[];
  created_at: string; actor: string;
  source: "live" | "scripted";
  verified: { by: string; at: string } | null;
  history: PinRun[];
}

export interface AppConfig {
  runner: string; model: string; thinking_budget: string;
  vertexai: boolean; project_set: boolean; credentials_set: boolean;
  tls: Record<string, unknown>;
  graph: { path: string; live: boolean };
}

export interface EvalCheck {
  id: string; label: string;
  status: "pass" | "warn" | "fail" | "skip";
  explanation: string;
}

export interface EvalTurn {
  turn_id: string; question: string;
  verdict: "grounded" | "grounded_caveats" | "needs_review";
  verdict_text: string; score: number;
  checks: EvalCheck[]; corrections: string[];
  n_tool_calls: number; ts: number;
}

export interface EvalsRecent {
  turns: EvalTurn[];
  summary: {
    n_turns: number; grounded_rate: number | null;
    avg_score: number | null; self_corrections: number;
  };
}

export interface Starter {
  category: string; question: string; why: string; prefill: boolean;
}
