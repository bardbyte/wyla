/** The Synapse copy deck — every static string in the product, in one
 * reviewable place.
 *
 * Editorial rules (applied everywhere):
 *  - Sentence case. No exclamation marks. No filler ("simply", "just").
 *  - Enterprise-neutral: the product serves any team; no role or
 *    industry personas in the interface. (The data's own business
 *    units come from MDM and are shown as-is.)
 *  - Verbs lead on actions. States are honest ("No graph loaded",
 *    never an unlabeled mock). Claims are specific, not superlative.
 *  - Evidence levels use words, never percentages, at the surface.
 *
 * The information architecture is one loop, four surfaces:
 *   Ask               — ask a question, watch it get answered, defend it
 *   Data products     — what we know about your business, by unit
 *   Knowledge graph   — the whole connected picture, provenance-typed
 *   Bring your        — your team's knowledge, at the highest tier
 *     knowledge
 */

import type { Tier } from "./types";

export const BRAND = {
  name: "Synapse",
};

export const TABS = [
  { id: "home", label: "Home", preview: false },
  { id: "semantics", label: "Semantics", preview: false },
  { id: "ask", label: "Ask", preview: false },
  { id: "products", label: "Data products", preview: false },
  { id: "graph", label: "Context graph", preview: false },
  { id: "knowledge", label: "Bring your knowledge", preview: false },
  { id: "operate", label: "Operate", preview: false },
] as const;

export type TabId = (typeof TABS)[number]["id"];

/** The evidence vocabulary — words and fill glyphs, never numbers. */
export const TIERS: Record<
  Tier | "blocked",
  { word: string; fill: string; cls: string }
> = {
  human_asserted: { word: "Signed", fill: "●●●●", cls: "t-high" },
  grounded: { word: "Corroborated", fill: "●●●○", cls: "t-high" },
  inferred: { word: "Inferred", fill: "●●○○", cls: "t-mid" },
  guessed: { word: "Unverified", fill: "●○○○", cls: "" },
  deprecated: { word: "Retired", fill: "○○○○", cls: "" },
  blocked: { word: "Declined", fill: "◼", cls: "t-block" },
};

export const COMMON = {
  liveGraph: "Live graph",
  noGraph: "No graph loaded",
  preview: "Preview — planned",
  loading: "Loading…",
  close: "Close",
  noGraphTitle: "No graph is loaded",
  noGraphSub:
    "Point the server at a compiled snapshot (SYNAPSE_GRAPH_PATH) to " +
    "bring every surface to life. Until then Synapse shows nothing it " +
    "can't stand behind.",
};

export const ENTITY = {
  evidence: "Evidence",
  explore: "Explore in graph",
  ask: "Ask about this",
  askPrefix: "Tell me about ",
};

/* ── Ask — the agentic centerpiece ────────────────────────── */

export const ASK = {
  emptyTitle: "Ask a question. Get an answer you can defend.",
  emptySub:
    "Synapse reads your knowledge graph, checks the meaning of every " +
    "field, and — when a query is needed — drafts it, prices it, and " +
    "waits for your approval before anything runs. Answers arrive with " +
    "their sources, their confidence, and the trail that produced them.",
  placeholder:
    "Ask about ownership, definitions, lineage, trust, or the numbers " +
    "themselves…",
  send: "Ask",
  working: "Working…",
  gateTitle: "Approval needed to run this query",
  gateSub: "Nothing runs without you. Review the query and its cost.",
  gateScan: "Estimated scan",
  gateHold: "Hold to run",
  gateHoldHint: "Press and hold — a click is not a signature.",
  gateLater: "Not now",
  heldNote:
    "Held. The query is drafted and validated — run it whenever " +
    "you're ready.",
  citations: "Citations",
  governance: "Governance",
  howTitle: "How this was produced",
  errorTitle: "That didn't go through",
  suggestTitle: "Verified questions you can ask now",
  demoBanner:
    "Demo mode — scripted transcripts, not the live agent. Unset " +
    "SYNAPSE_CONSOLE_RUNNER (or set it to adk) and restart the server " +
    "to chat with Gemini.",
  activityTitle: "Synapse activity",
  activityHint:
    "The part of the graph the agent touches lights up as it works. " +
    "Idle, you see the whole ecosystem.",
  activityOpen: "Show activity",
  activityClose: "Hide activity",
  activityFull: "Open the full graph",
  agentIssue: "The live agent can't start on this server",
};

/* ── Data products — "we know your business", by unit ─────── */

export const PRODUCTS = {
  title: "Data products",
  sub:
    "What we know about your business, grouped by the units MDM " +
    "governs. Each table carries a readiness scorecard — how completely " +
    "it is described in the knowledge graph — and its governance signals.",
  search: "Search data products…",
  empty: "No data products match that search.",
  emptyNoGraph: COMMON.noGraphSub,
  // per-unit rollup labels
  uTables: "tables",
  uColumns: "columns",
  uDescribed: "described",
  uGrounded: "corroborated",
  uPii: "with PII",
  uGoverned: "governed",
  // per-product scorecard labels
  columns: "Columns",
  meaning: "Described",
  related: "Related tables",
  metrics: "Metrics",
  governance: "Governance",
  lineage: "Lineage",
  present: "Yes",
  absent: "—",
  unassigned: "Unassigned",
};

/* ── Knowledge graph — the whole connected picture ────────── */

export const GRAPH = {
  title: "Context graph",
  sub:
    "One connected picture of your data. Every fact is typed by where " +
    "it came from and how strongly it is supported — no single source, " +
    "including AI enrichment, can reach the highest level alone.",
  mapTitle: "The whole picture",
  mapSub:
    "Tables, the business entities they describe, the metrics computed " +
    "from them, and the playbooks that govern them. Select any node to " +
    "inspect its evidence or ask about it.",
  mapEmpty: "Nothing is recorded in the current snapshot.",
  truncatedNote: "Showing the most connected nodes.",
  legendKinds: "Node types",
  kinds: {
    table: "Table",
    entity: "Entity",
    metric: "Metric",
    skill: "Playbook",
  } as Record<string, string>,
  storyTitle: "One thread, end to end",
  storySub:
    "Anchor on a table to trace how it connects to a business entity, " +
    "a proven join, a canonical metric, and the playbook that governs " +
    "it.",
  pickerLabel: "Trace a data product",
  pickerDefault: "The canonical thread",
  pickerEmpty:
    "Nothing is recorded for this table in the current snapshot.",
  statsTitle: "What the graph knows",
  witnessTitle: "Where the facts come from",
  witnessSub:
    "Independent sources contribute facts; agreement raises " +
    "confidence. No single source can reach the highest level alone.",
  tierLegend: "Evidence levels",
  nodes: "Facts",
  edges: "Connections",
  openEvidence: "Select any step to inspect its evidence.",
  liveNow: "agent working — watching the traversal",
  insightsTitle: "Insights",
  insightsAiTag: "AI-generated",
  insightsNoDesc:
    "No description yet. Ask the agent about this table, or assert one " +
    "in Bring your knowledge.",
  insightsColumns: "columns",
  insightsDescribed: "described",
  insightsPii: "PII",
  insightsRels: "Derived relationships — each with its witnesses",
  insightsNoRels:
    "No relationships recorded for this table in the current snapshot.",
  insightsRelKind: "Kind",
  insightsRelPredicate: "Relationship",
  insightsRelWitness: "Witness",
  insightsRelTier: "Evidence",
  insightsRecs: "Questions you can ask about this table",
};

/* ── Bring your knowledge — the partnership close (P5) ─────── */

export const KNOWLEDGE = {
  title: "Bring your knowledge",
  sub:
    "The graph gets its meaning from your team. What you know — the " +
    "definitions, the caveats, the way your unit actually uses a field — " +
    "becomes the highest tier of evidence, attributed to you.",
  captureTitle: "Tell Synapse what your team knows",
  captureSub:
    "State a definition or a correction in your own words. The agent " +
    "records it as a signed assertion — the top evidence level, above " +
    "anything AI can infer — credited to you and durable across runs.",
  captureExamples: [
    "In our unit, an account with status 'A' is actively managed, not " +
      "merely open.",
    "Roll rate is measured on the closing balance, not the average.",
    "custins_cardmember is the system of record for tenure, not " +
      "risk_pers_acct.",
  ],
  captureCta: "Assert this in Ask",
  how:
    "A signed assertion outranks every inferred or connected fact. " +
    "Corroboration across sources raises confidence; your signature " +
    "settles it.",
  ladderTitle: "How knowledge earns its evidence level",
  ladder: [
    {
      tier: "inferred" as const,
      text:
        "A connected source or AI enrichment contributes a fact with " +
        "its own weight — labeled evidence, never pasted context.",
    },
    {
      tier: "grounded" as const,
      text:
        "Independent agreement across sources corroborates an " +
        "assertion and raises its level.",
    },
    {
      tier: "human_asserted" as const,
      text:
        "Your signature settles it — the highest level, and the only " +
        "way to reach it.",
    },
  ],
  ladderFoot:
    "Everything stays attributed: every fact can name the person, " +
    "document, or catalog entry it came from.",
  connectTitle: "Connect the places you already write things down",
  connectors: [
    {
      icon: "▤",
      name: "Google Knowledge Catalog",
      desc: "Sync curated glossary terms and dataset documentation.",
    },
    {
      icon: "◫",
      name: "Confluence",
      desc: "Runbooks and specifications become citable evidence.",
    },
    {
      icon: "▢",
      name: "SharePoint & Word",
      desc: "Working documents contribute definitions and context.",
    },
    {
      icon: "▣",
      name: "PowerPoint",
      desc: "Business reviews carry metric context worth keeping.",
    },
  ],
  connect: "Connect",
};

export const CONFIG = {
  scripted: "Demo transcript",
  live: "Live agent",
};
