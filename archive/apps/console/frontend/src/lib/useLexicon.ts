/** Known graph-object names for answer linkification. Fetched once per
 * page load; conservative by construction — exact word-boundary
 * matches, ≥4 chars, a stoplist of generic analytics words, longest
 * name wins. */

import { useEffect, useState } from "react";
import { api } from "./api";
import type { LexiconEntry } from "./types";

export interface Lexicon {
  regex: RegExp | null;
  byName: Map<string, LexiconEntry>;
}

const STOP = new Set([
  "rate", "count", "total", "month", "year", "date", "table", "column",
  "value", "data", "account", "accounts", "daily", "summary", "status",
  "type", "name", "time", "amount", "number", "level", "group", "index",
  "score", "flag", "code", "history", "detail", "record", "field",
]);

let cached: Lexicon | null = null;
let inflight: Promise<Lexicon> | null = null;

function escape(name: string): string {
  return name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

async function load(): Promise<Lexicon> {
  try {
    const d = await api.lexicon();
    const entries = d.lexicon.filter(
      (e) => e.name.length >= 4 && !STOP.has(e.name.toLowerCase()));
    const byName = new Map(entries.map(
      (e) => [e.name.toLowerCase(), e] as const));
    const names = entries.map((e) => e.name)
      .sort((a, b) => b.length - a.length).map(escape);
    const regex = names.length
      ? new RegExp(`\\b(${names.join("|")})\\b`, "gi")
      : null;
    return { regex, byName };
  } catch {
    return { regex: null, byName: new Map() };
  }
}

export function useLexicon(): Lexicon | null {
  const [lex, setLex] = useState<Lexicon | null>(cached);
  useEffect(() => {
    if (cached) return;
    inflight = inflight ?? load();
    let on = true;
    inflight.then((l) => {
      cached = l;
      if (on) setLex(l);
    });
    return () => { on = false; };
  }, []);
  return lex;
}
