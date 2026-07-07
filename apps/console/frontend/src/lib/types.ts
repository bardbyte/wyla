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

export interface BriefCard {
  id: string; title: string; created_at: string; status: string;
  tier: string; live: boolean; citation_count: number;
}

export interface Brief extends BriefCard {
  question: string;
  sections: AnswerSections;
  thread: { role: string; text: string }[];
  ledger: { ref: string }[];
}

export interface Witness {
  ref: string; found: boolean; kind?: string;
  properties?: Record<string, unknown>;
  provenance?: { tier: Tier; score: number; sources: string[] };
  edges?: { type: string; other: string; direction: string; tier: Tier }[];
}

export interface AppConfig {
  runner: string; model: string; thinking_budget: string;
  vertexai: boolean; project_set: boolean; credentials_set: boolean;
  tls: Record<string, unknown>;
  graph: { path: string; live: boolean };
}
