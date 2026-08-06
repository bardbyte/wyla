/** Anticipation (mockups 1b + 1h): while the user types, nodes
 * matching the draft glimmer in the sky, a mono tally counts them,
 * and the strongest match whispers its name. Shared by the Ask
 * composer and the in-space question bar on the Knowledge Graph. */

import type { GraphMap } from "./types";

export interface Listening {
  ids: string[];
  nMetrics: number;
  nTables: number;
  best: { ref: string; label: string; tier: string } | null;
}

export const NO_LISTENING: Listening =
  { ids: [], nMetrics: 0, nTables: 0, best: null };

export function computeListening(q: string,
                                 map: GraphMap | null): Listening {
  if (!map || q.trim().length < 4) return NO_LISTENING;
  const tokens = q.toLowerCase().split(/[^a-z0-9_]+/)
    .filter((t) => t.length >= 4);
  if (!tokens.length) return NO_LISTENING;
  const hits: { id: string; label: string; kind: string; tier: string;
                score: number }[] = [];
  for (const n of map.nodes) {
    const label = n.label.toLowerCase();
    const bare = label.split(/[./]/).pop() ?? label;
    let score = 0;
    for (const t of tokens)
      if (label.includes(t) || bare.includes(t)) score += t.length;
    if (score > 0) hits.push({ id: n.id, label: n.label, kind: n.kind,
                               tier: n.tier, score });
  }
  hits.sort((a, b) => b.score - a.score);
  const top = hits.slice(0, 6);
  const best = top[0] && top[0].score >= 8 ? top[0] : null;
  return {
    ids: top.map((h) => h.id),
    nMetrics: top.filter((h) => h.kind === "metric").length,
    nTables: top.filter((h) => h.kind === "table").length,
    best: best ? { ref: best.id, label: best.label, tier: best.tier }
               : null,
  };
}
