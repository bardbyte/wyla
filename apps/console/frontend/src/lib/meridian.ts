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

export interface TableRow {
  physical: string;
  short: string;
  columns: number;
  lob: string;
  metrics_here: number;
  joins: number;
  tickets: number;
  cost_prior: { p50?: number; p95?: number } | null;
  business_name?: string;
  business_unit?: string;
  lobs?: string[];
  lifecycle?: string;
  tier?: MeridianTier;
  pii?: boolean;
  description?: string;
}

/** The compiled table facts row (meridian.table_facts/1) — the card
 * is a rendering of this, and so is the profile. Fluid by design:
 * the keys the UI renders are typed, the rest stay open. */
export interface ColumnFact extends Record<string, unknown> {
  name: string;
  type?: string;
  type_source?: string;
  agreement?: number;
  business_name?: string;
  description?: string;
  description_source?: string;
  description_supplementary?: string;
  sensitive?: boolean;
  sensitivity_sources?: string[];
  pii_role?: string;
  pii_role_table_declared?: string;
  sde_group?: string;
  primary_key?: boolean;
  primary_key_atlas?: boolean;
  partitioning?: boolean;
  partitioning_atlas?: boolean;
  nullable_atlas?: boolean;
  ordinal?: number;
  ordinal_atlas?: number;
  column_length?: number;
  approx_distinct?: number;
  null_count?: number;
  domain?: { n_values: number; top: { value: unknown; pct?: number }[] };
  terms?: { id?: string; name?: string; status?: string;
            description?: string; matched_on?: string }[];
  declared_terms?: { name?: string; description?: string }[];
  derived_logic?: string;
  derived_from?: { source: string; logic?: string }[];
  fk_references?: { table: string; column: string }[];
  flags?: string[];
  ungoverned?: boolean;
}

export interface OwnerFact {
  owner: string; roles: string[]; witnesses: string[];
}

export interface VocabFact {
  symbol: string; definition: string; kind: string; status?: string;
  bu: string; region: string; ref?: string; columns: string[];
}

export interface TableFacts extends Record<string, unknown> {
  schema: string;
  physical: string;
  short: string;
  columns: number;
  primary_key: string[];
  total_rows?: number | null;
  lifecycle?: string | null;
  identity: Record<string, string | boolean | undefined> & {
    business_name?: string; description?: string;
    description_source?: string; description_bq?: string;
    project?: string; dataset?: string; object_type?: string;
    table_type?: string; layer_type?: string; load_type?: string;
    data_category?: string; data_sub_category?: string;
    technology?: string; data_server?: string; appl_id?: string;
    target_system?: string; schema_fingerprint?: string;
    is_partitioned_atlas?: boolean;
  };
  business: {
    business_unit?: string; business_units?: string[];
    lobs?: { code: string; name?: string;
             witnesses: Record<string, number> }[];
    used_by?: { code: string; name?: string; parent?: string;
                support: number }[];
    owners?: OwnerFact[];
    ownership_ids?: Record<string, string>;
    top_users?: { user: string; queries: number }[];
  };
  operations: {
    lifecycle?: string; environment?: string; feed_type?: string;
    pipeline_name?: string; source_system?: string; created?: string;
    last_modified?: string; total_rows?: number; size_bytes?: number;
    n_partitions?: number; partition_latest?: string;
    partition_columns?: string[]; partition_columns_atlas?: string[];
    primary_key_atlas?: string[]; usage_rhythm?: string[];
    cost_prior?: { p50_bytes?: number; p95_bytes?: number;
                   n_jobs?: number; daily_days?: number };
  };
  trust: {
    answerability?: Record<string, string>;
    is_active_atlas?: boolean; is_latest_atlas?: boolean;
    is_lineage_exist_atlas?: boolean;
    tier?: MeridianTier; metrics_here?: number; filters_here?: number;
    structural?: Record<string, number>;
  };
  access: {
    restricted?: string | null; pii_table?: boolean;
    has_pii_atlas?: boolean; has_gdpr_atlas?: boolean;
    has_oncop_atlas?: boolean;
    policies?: Record<string, string[]>;
    sensitive_columns?: { name: string; pii_role?: string;
                          sde_group?: string; sources?: string[] }[];
  };
  column_facts: ColumnFact[];
  joins: {
    declared?: { column: string; ref_table: string; ref_column: string }[];
    observed?: { other: string; support: number }[];
    scoped?: { other: string; on?: string[]; scope?: string;
               join_type?: string; witness?: string;
               preconditions?: string[] }[];
  };
  lineage: {
    upstream?: string[]; downstream?: string[];
    derived_columns?: string[]; view_sql?: boolean; docs?: string[];
  };
  vocabulary: VocabFact[];
  omitted_catalog_only?: string[];
}

export interface LobTableFact {
  physical: string; business_name?: string; description?: string;
  tier?: MeridianTier | ""; metrics_here?: number;
  lifecycle?: string | null; pii?: boolean; business_unit?: string;
}

export interface LobFacts {
  code: string; name?: string; kind: string; parent?: string;
  domains?: string[]; tables: LobTableFact[]; used_tables?: string[];
  usage_support?: number;
  readiness?: { tables: number; witnessed: number; pct: number };
  owners?: { owner: string; roles: string[] }[];
  vocabulary_entries?: number;
}

export interface LobDetail {
  available: true;
  found: boolean;
  code?: string;
  lob?: LobFacts;
  card?: string;
}

/** The full metric row is fluid by design (props ride through) —
 * the keys the UI renders are typed, the rest stay open. */
export interface MetricFull extends Record<string, unknown> {
  id: string;
  fp?: string;
  label?: string;
  canonical_sql?: string;
  status?: string;
  status_served?: string;
  evidence_origin?: string;
  question?: string;
  question_source?: string;
  grain?: string;
  grain_observed?: string;
  common_filters?: string[];
  support?: number;
  support_by_witness?: Record<string, number>;
  witness_agreement?: number;
  used_by?: Record<string, number>;
  table?: string;
  line_of_business?: string;
}

export interface MetricDetail {
  available: true;
  found: boolean;
  id?: string;
  metric?: MetricFull;
  tier?: MeridianTier;
  family?: MetricFull[];
  reviews?: Record<string, unknown>[];
}

export interface TableDetail {
  available: true;
  found: boolean;
  physical: string;
  columns?: Record<string, string>;
  facts?: TableFacts;
  card?: string;
  joins?: {
    a: string; b: string; source: string; support?: number;
    on?: string[] | string; scope?: string;
  }[];
  metrics_here?: {
    id: string; label: string; status_served: string; support: number;
  }[];
  cost_prior?: { p50?: number; p95?: number } | null;
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
  tables: () =>
    get<{ available: true; rows: TableRow[] }>(
      "/api/meridian/explorer/tables"),
  metricDetail: (id: string) =>
    get<MetricDetail>(
      `/api/meridian/metric/${encodeURIComponent(id)}`),
  tableDetail: (physical: string) =>
    get<TableDetail>(
      `/api/meridian/table/${encodeURIComponent(physical)}`),
  lobs: () =>
    get<{ available: true; rows: LobFacts[] }>(
      "/api/meridian/explorer/lobs"),
  lobDetail: (code: string) =>
    get<LobDetail>(`/api/meridian/lob/${encodeURIComponent(code)}`),
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
