/** Thin typed client for the console server. Every read returns the
 * payload plus its `live` flag so callers can label sample data. */

import type {
  AgentSelftest, AppConfig, EvalsRecent, GraphMap, GraphSummary,
  LexiconEntry, Metric, Pin, PinRun, Product, Starter, TableInsights,
  ThreadHop, Unit, Viability, Witness,
} from "./types";

async function post<T>(url: string, body: unknown,
                        method = "POST"): Promise<T> {
  const r = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!r.ok) {
    let code = String(r.status);
    try { code = (await r.json()).code ?? code; } catch { /* keep */ }
    throw new Error(code);
  }
  return r.json() as Promise<T>;
}

async function get<T>(url: string): Promise<T> {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url} → ${r.status}`);
  return r.json() as Promise<T>;
}

type Live<T> = T & { live: boolean; source: "graph" | "empty" };

export const api = {
  config: () => get<AppConfig>("/api/config"),

  products: (q = "") =>
    get<Live<{ products: Product[] }>>(
      `/api/products?q=${encodeURIComponent(q)}`),

  productsByUnit: (q = "") =>
    get<Live<{ units: Unit[] }>>(
      `/api/products/by-unit?q=${encodeURIComponent(q)}`),

  graphMap: () =>
    get<Live<{ map: GraphMap }>>("/api/graph/map"),

  graphInsights: (table: string) =>
    get<Live<{ insights: TableInsights }>>(
      `/api/graph/insights?table=${encodeURIComponent(table)}`),

  agentSelftest: () => get<AgentSelftest>("/api/agent/selftest"),

  metrics: (q = "") =>
    get<Live<{ metrics: Metric[] }>>(
      `/api/metrics?q=${encodeURIComponent(q)}`),

  viability: async (name: string, description: string) => {
    const r = await fetch("/api/metrics/viability", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, description }),
    });
    if (!r.ok) throw new Error(`viability → ${r.status}`);
    return r.json() as Promise<Live<{ viability: Viability }>>;
  },

  graphSummary: () =>
    get<Live<{ summary: GraphSummary }>>("/api/graph/summary"),

  graphThread: (table = "") =>
    get<Live<{ thread: { hops: ThreadHop[] } }>>(
      `/api/graph/thread?table=${encodeURIComponent(table)}`),

  lexicon: () =>
    get<Live<{ lexicon: LexiconEntry[] }>>("/api/lexicon"),

  pins: () =>
    get<Live<{ seeded: boolean; pins: Pin[] }>>("/api/pins"),

  createPin: (body: {
    question: string; answer?: string;
    citations?: { label: string; ref: string }[];
    sql?: string | null; rows?: Record<string, unknown>[] | null;
    ledger_id?: string | null; source?: string;
  }) => post<{ pin: Pin }>("/api/pins", body),

  rerunPin: (id: string) =>
    post<{ pin: Pin; run: PinRun }>(
      `/api/pins/${encodeURIComponent(id)}/rerun`, {}),

  verifyPin: (id: string, verified = true) =>
    post<{ pin: Pin }>(
      `/api/pins/${encodeURIComponent(id)}/verify`, { verified }),

  deletePin: (id: string) =>
    post<{ deleted: string }>(
      `/api/pins/${encodeURIComponent(id)}`, undefined, "DELETE"),

  questions: () =>
    get<Live<{ questions: { question: string; archetype: string }[] }>>(
      "/api/questions"),

  witness: (ref: string) =>
    get<Live<{ witness: Witness }>>(
      `/api/witness?ref=${encodeURIComponent(ref)}`),

  approve: (gateId: string, approved: boolean) =>
    fetch("/approve", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ gate_id: gateId, approved }),
    }),

  evalsRecent: () => get<EvalsRecent>("/api/evals/recent"),

  starters: () =>
    get<Live<{ starters: Starter[] }>>("/api/questions/starters"),
};
