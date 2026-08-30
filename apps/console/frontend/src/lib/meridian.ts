/** Typed client for the Meridian read plane (/api/meridian/*).
 *
 * Every payload carries `available`: false means no compiled build on
 * this machine — the UI renders its designed empty state with the
 * server's own reason. Nothing is mocked, ever. */

export interface Unavailable {
  available: false;
  reason: string;
}

export type MeridianTier = "ha" | "gr" | "in" | "gu";

export interface MeridianHome {
  available: true;
  build_id: string;
  counts: Record<string, number>;
  metrics_by_status: Record<string, number>;
  joins: { total: number; scoped_only: number };
  readiness: Record<
    string, { tables: number; witnessed: number; pct: number }>;
  sources_count: number;
  open_reviews: number;
  excluded_tables: {
    physical: string; reason: string;
    intentionally_excluded?: string;
  }[];
  census: Record<string, unknown>;
  diff: string;
}

export interface SourceCard {
  source: string;
  family: string;
  display: string;
  chip: string;
  sub?: string;
  blurb: string;
  contributes: {
    nodes: Record<string, number>;
    edges: Record<string, number>;
  };
  ledger: Record<string, number>;
}

export interface MeridianSources {
  available: true;
  build_id: string;
  sources: SourceCard[];
  readiness: Record<
    string, { tables: number; witnessed: number; pct: number }>;
  meta: Record<string, string>;
}

export interface MetricRow {
  id: string;
  fp: string;
  label: string;
  expr: string;
  status_served: string;
  evidence_origin: string;
  tier: MeridianTier;
  agreement: number;
  support: number;
  witnesses: Record<string, number>;
  used_by: Record<string, number>;
  table: string;
  lob: string;
}

export interface EnrichRun {
  run: string;
  prompt_version?: string;
  blind?: {
    n: number; recovered: number; rate: number; tier: string;
    leaky_contexts?: number; grader?: string;
  } | null;
  metrics_enriched?: number;
  concepts_enriched?: number;
  collisions?: number;
  invalid_json?: number;
  grain_divergences?: number;
  usage?: Record<string, number> | null;
}

export interface MeridianBuilds {
  available: true;
  current: string;
  builds: string[];
  manifest: Record<string, unknown> & {
    counts?: Record<string, number>;
    table_reconciliation?: {
      crosswalk_rows?: number; built?: number;
      missing?: { physical: string; reason: string;
                  intentionally_excluded?: string }[];
    };
  };
  diff: string;
}

async function get<T>(url: string): Promise<T | Unavailable> {
  const r = await fetch(url);
  if (!r.ok) return { available: false, reason: `${url} → ${r.status}` };
  return r.json() as Promise<T | Unavailable>;
}

export const meridian = {
  home: () => get<MeridianHome>("/api/meridian/home"),
  sources: () => get<MeridianSources>("/api/meridian/sources"),
  metrics: (params: { q?: string; status?: string; lob?: string } = {}) => {
    const search = new URLSearchParams();
    if (params.q) search.set("q", params.q);
    if (params.status) search.set("status", params.status);
    if (params.lob) search.set("lob", params.lob);
    return get<{
      available: true; total: number; shown: number; rows: MetricRow[];
    }>(`/api/meridian/explorer/metrics?${search.toString()}`);
  },
  builds: () => get<MeridianBuilds>("/api/meridian/builds"),
  enrichRuns: () =>
    get<{ available: true; runs: EnrichRun[] }>(
      "/api/meridian/enrich_runs"),
  feedback: (payload: {
    screen: string; object_id?: string; vote: "up" | "down";
    note?: string; session_kind?: string;
  }) =>
    fetch("/api/meridian/feedback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }),
};

/** Tier symbol vocabulary — shape + word, never color alone. */
export const TIER_GLYPH: Record<MeridianTier, { glyph: string; word: string }> = {
  ha: { glyph: "●", word: "human-asserted" },
  gr: { glyph: "◆", word: "grounded" },
  in: { glyph: "◐", word: "inferred" },
  gu: { glyph: "○", word: "unverified" },
};
