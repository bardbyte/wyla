/** Thin typed client for the console server. Every read returns the
 * payload plus its `live` flag so callers can label sample data. */

import type {
  AppConfig, Brief, BriefCard, GraphSummary, Metric, Product, ThreadHop,
  Viability, Witness,
} from "./types";

async function get<T>(url: string): Promise<T> {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url} → ${r.status}`);
  return r.json() as Promise<T>;
}

type Live<T> = T & { live: boolean; source: "graph" | "sample" };

export const api = {
  config: () => get<AppConfig>("/api/config"),

  products: (q = "") =>
    get<Live<{ products: Product[] }>>(
      `/api/products?q=${encodeURIComponent(q)}`),

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

  briefs: () => get<{ briefs: BriefCard[] }>("/api/briefs"),
  brief: (id: string) => get<Brief & { found?: boolean }>(`/api/briefs/${id}`),

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
};
