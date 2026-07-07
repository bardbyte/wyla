/** The Radix copy deck — every static string in the product, in one
 * reviewable place.
 *
 * Editorial rules (applied everywhere):
 *  - Sentence case. No exclamation marks. No filler ("simply", "just").
 *  - Enterprise-neutral: the product serves any team; no role or
 *    industry personas in the interface.
 *  - Verbs lead on actions. States are honest ("Sample data", never an
 *    unlabeled mock). Claims are specific, not superlative.
 *  - Evidence levels use words, never percentages, at the surface.
 */

import type { Tier } from "./types";

export const BRAND = {
  name: "Radix",
};

export const TABS = [
  { id: "inquiries", label: "Inquiries", preview: false },
  { id: "products", label: "Data products", preview: false },
  { id: "metrics", label: "Metrics", preview: false },
  { id: "graph", label: "Knowledge graph", preview: false },
  { id: "knowledge", label: "Bring your knowledge", preview: true },
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
  sampleData: "Sample data",
  preview: "Preview — planned",
  loading: "Loading…",
  evidenceFooter: "Every number ships with its evidence.",
  close: "Close",
};

export const INQUIRIES = {
  emptyTitle: "Ask about your data.",
  emptySub:
    "Answers arrive with their sources, their confidence, and the " +
    "trail that produced them.",
  placeholder:
    "Ask about ownership, definitions, lineage, or the numbers themselves…",
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
};

export const PRODUCTS = {
  title: "Data products",
  sub:
    "Governed tables with their owners, lifecycle, and a readiness " +
    "scorecard — how completely each one is described in the knowledge " +
    "graph.",
  search: "Search data products…",
  add: "Add a data product",
  addNote:
    "Adding a product syncs its schema, ownership, and usage into the " +
    "graph.",
  readiness: "Readiness",
  columns: "Columns",
  meaning: "Described",
  related: "Related tables",
  metrics: "Metrics",
  governance: "Governance",
  lineage: "Lineage",
  present: "Yes",
  absent: "—",
  empty: "No data products match that search.",
};

export const METRICS = {
  title: "Metrics",
  sub:
    "The metric canon: definitions with their formulas, sources, and " +
    "evidence level. New definitions are checked against the canon " +
    "before anything is drafted.",
  search: "Search metrics…",
  copilotTitle: "Define a metric",
  copilotSub:
    "Describe the measure in plain language. Radix checks the canon " +
    "first — duplicates are surfaced, not minted.",
  nameLabel: "Metric name",
  namePlaceholder: "e.g. Average settlement lag",
  descLabel: "What it measures",
  descPlaceholder:
    "The average days between transaction and settlement, over settled " +
    "transactions…",
  check: "Check the canon",
  checking: "Checking…",
  verdictExact: "Already canonical",
  verdictExactSub:
    "This metric exists. Use the canonical definition below — a second " +
    "definition of the same measure is how numbers stop agreeing.",
  verdictNear: "Close to existing definitions",
  verdictNearSub:
    "Review these before drafting. If yours is genuinely different, " +
    "say how it differs in the description and check again.",
  verdictClear: "No conflicts found",
  verdictClearSub:
    "The canon has no matching or near definition. A draft can be " +
    "grounded in existing columns and submitted for review.",
  draftTitle: "Draft definition",
  draftTierNote:
    "Drafts carry the Inferred mark until a steward signs them. " +
    "Signature is what makes canon — generation never does.",
  submit: "Submit for steward review",
  sharedTerms: "Shared terms",
  formula: "Formula",
  sources: "Sources",
  empty: "No metrics match that search.",
};

export const GRAPH = {
  title: "Knowledge graph",
  sub:
    "One connected picture of your data. Every fact is typed by where " +
    "it came from and how strongly it is supported.",
  storyTitle: "One thread, end to end",
  storySub:
    "How a business entity connects to physical columns, a proven " +
    "join, a canonical metric, and the playbook that governs it.",
  pickerLabel: "Explore a data product",
  pickerDefault: "The canonical thread",
  pickerEmpty:
    "Nothing is recorded for this table in the current snapshot.",
  statsTitle: "What the graph knows",
  witnessTitle: "Where the facts come from",
  witnessSub:
    "Independent sources contribute facts; agreement raises " +
    "confidence. No single source — including AI enrichment — can " +
    "reach the highest levels alone.",
  tierLegend: "Evidence levels",
  nodes: "Facts",
  edges: "Connections",
  openEvidence: "Select any step to inspect its evidence.",
};

export const KNOWLEDGE = {
  title: "Bring your knowledge",
  sub:
    "Connect the places your organization already writes things down. " +
    "Each source becomes weighted evidence in the graph — reviewed, " +
    "attributed, and never pasted in as unaccountable context.",
  how:
    "Connected sources contribute facts with a defined weight. " +
    "Corroboration across sources raises confidence; a steward's " +
    "signature settles it.",
  connectors: [
    {
      icon: "▤",
      name: "Knowledge catalog",
      desc: "Sync curated glossary terms and dataset documentation.",
    },
    {
      icon: "◫",
      name: "Confluence",
      desc: "Runbooks and specifications become citable evidence.",
    },
    {
      icon: "▢",
      name: "Google Docs",
      desc: "Working documents contribute definitions and context.",
    },
    {
      icon: "▣",
      name: "Slides",
      desc: "Business reviews carry metric context worth keeping.",
    },
  ],
  connect: "Connect",
  ladderTitle: "How outside knowledge earns trust",
  ladder: [
    "A connected source contributes facts with its own weight.",
    "Independent agreement raises an assertion's confidence.",
    "A steward's signature promotes it to the highest level.",
    "Everything stays attributed — every fact can name its origin.",
  ],
};

export const CONFIG = {
  scripted: "Demo transcript",
  live: "Live agent",
};
