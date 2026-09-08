"""Streamlit UI for Synapse — the X-ray for a table.

Run locally:

    streamlit run synapse/scripts/synapse_ui.py

Information architecture: synapse/docs/UX_INFORMATION_ARCHITECTURE.md
Visual system + tokens:    synapse/docs/UX_VISUAL_SYSTEM.md

The CSS block below is the paste-ready section §9 of the visual-system spec.
Component primitives (`.synapse-badge`, `.synapse-pill`, `.synapse-hero`, …)
match the spec exactly.
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNAPSE_ROOT = REPO_ROOT / "synapse"
sys.path.insert(0, str(SYNAPSE_ROOT))

import streamlit.components.v1 as components  # noqa: E402, F401

from synapse.graph import build_graph_from_sources, inspect_table  # noqa: E402
from synapse.graph.store import canonical_uri  # noqa: E402, F401
from synapse.graph.viz import (  # noqa: E402, F401
    consumption_flow_html,
    ego_html,
    lineage_dag_html,
    neighborhood_html,
)
from synapse.synthetic import generate_all_sources  # noqa: E402

DEFAULT_DEMO_DIR = SYNAPSE_ROOT / "data" / "demo"


# ─── Design tokens (visual-system spec §9, paste-ready) ─────


SYNAPSE_CSS = """
<style>
@import url('https://rsms.me/inter/inter.css');
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;460;540;620&display=swap');

:root {
  --font-sans:    'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  --font-mono:    'JetBrains Mono', 'SF Mono', Menlo, Consolas, monospace;

  --surface-0:        #FFFFFF;
  --surface-1:        #FAFAFA;
  --surface-2:        #F4F4F5;
  --surface-3:        #EEEEF0;
  --border-subtle:    #E8E8EB;
  --border-default:   #D4D4D8;
  --border-strong:    #A1A1AA;
  --fg-muted:         #71717A;
  --fg-default:       #3F3F46;
  --fg-strong:        #18181B;

  --accent-bg:        #EEF0FF;
  --accent-border:    #A5AEFF;
  --accent-fg:        #4F46E5;
  --accent-solid:     #4338CA;

  --conf-deprecated-fg:  #7A1F1F;  --conf-deprecated-bg:  #FEECEC;  --conf-deprecated-dot: #DC2626;
  --conf-guessed-fg:     #71717A;  --conf-guessed-bg:     #F4F4F5;  --conf-guessed-dot:    #A1A1AA;
  --conf-inferred-fg:    #92400E;  --conf-inferred-bg:    #FEF3C7;  --conf-inferred-dot:   #F59E0B;
  --conf-grounded-fg:    #155E75;  --conf-grounded-bg:    #CFFAFE;  --conf-grounded-dot:   #06B6D4;
  --conf-human-fg:       #14532D;  --conf-human-bg:       #DCFCE7;  --conf-human-dot:      #22C55E;

  --dq-pass-fg:    #15803D;  --dq-pass-bg:    #DCFCE7;
  --dq-warn-fg:    #B45309;  --dq-warn-bg:    #FEF3C7;
  --dq-fail-fg:    #B91C1C;  --dq-fail-bg:    #FEE2E2;
  --dq-unknown-fg: #71717A;  --dq-unknown-bg: #F4F4F5;

  --src-catalog: #7C3AED;
  --src-runtime: #0891B2;
  --src-corpus:  #0D9488;
  --src-ai:      #DB2777;

  --space-1: 2px;  --space-2: 4px;  --space-3: 8px;  --space-4: 12px;
  --space-5: 16px; --space-6: 24px; --space-7: 32px; --space-8: 48px;

  --radius-xs: 4px; --radius-sm: 6px; --radius-md: 8px; --radius-lg: 12px;

  --shadow-rest:  0 0 0 1px var(--border-subtle);
  --shadow-hover: 0 0 0 1px var(--border-default), 0 1px 2px rgba(0,0,0,0.04), 0 4px 8px rgba(0,0,0,0.02);

  --motion-fast:    80ms;
  --motion-default: 120ms;
  --motion-ease:    cubic-bezier(0.2, 0.0, 0.0, 1.0);
}

@media (prefers-color-scheme: dark) {
  :root {
    --surface-0: #0A0A0B;  --surface-1: #101113;  --surface-2: #161719;  --surface-3: #1C1D20;
    --border-subtle: #202125; --border-default: #2A2B2F; --border-strong: #3F4046;
    --fg-muted: #71727A; --fg-default: #C9CACE; --fg-strong: #F4F4F6;
    --accent-bg: #1E1F4A; --accent-border: #4F56CF; --accent-fg: #A5B0FF; --accent-solid: #6366F1;
    --conf-deprecated-fg: #FCA5A5; --conf-deprecated-bg: #3A1414;
    --conf-guessed-fg:    #A1A1AA; --conf-guessed-bg:    #1C1D20;
    --conf-inferred-fg:   #FCD34D; --conf-inferred-bg:   #3A2810;
    --conf-grounded-fg:   #67E8F9; --conf-grounded-bg:   #0E3A45;
    --conf-human-fg:      #86EFAC; --conf-human-bg:      #0E2E1A;
    --src-catalog: #A78BFA; --src-runtime: #22D3EE; --src-corpus: #5EEAD4; --src-ai: #F472B6;
  }
}

html, body, [class*="css"], .stApp {
  font-family: var(--font-sans);
  font-feature-settings: 'cv11', 'ss01', 'ss03';
  font-variant-ligatures: none;
  color: var(--fg-default);
  background: var(--surface-0);
  font-size: 13px;
  line-height: 20px;
}
code, pre, .mono {
  font-family: var(--font-mono);
  font-feature-settings: 'calt' 0;
  font-variant-ligatures: none;
  font-size: 12px;
  line-height: 18px;
}
.tabular { font-variant-numeric: tabular-nums; }

.stApp > header { background: transparent; }
section[data-testid="stSidebar"] {
  background: var(--surface-1);
  border-right: 1px solid var(--border-subtle);
}
.stMarkdown h1 { font-size: 20px; line-height: 28px; font-weight: 600; letter-spacing: -0.2px; color: var(--fg-strong); margin: 0; }
.stMarkdown h2 { font-size: 15px; line-height: 22px; font-weight: 600; color: var(--fg-strong); margin-top: var(--space-6); margin-bottom: var(--space-3); }
.stMarkdown h3, .stMarkdown h4 { font-size: 13px; line-height: 20px; font-weight: 540; color: var(--fg-strong); margin-top: var(--space-5); margin-bottom: var(--space-3); }
.stMarkdown p { font-size: 13px; line-height: 20px; margin-bottom: var(--space-3); }
.stMarkdown code { font-family: var(--font-mono); font-size: 12px; background: var(--surface-3); color: var(--fg-strong); padding: 1px 4px; border-radius: 2px; }
.stCodeBlock, .stCodeBlock pre { background: var(--surface-3) !important; border-radius: var(--radius-sm) !important; border: 1px solid var(--border-subtle) !important; }
hr, .stDivider { border-color: var(--border-subtle) !important; opacity: 1; }
[data-testid="stMetricValue"] { font-family: var(--font-mono); font-variant-numeric: tabular-nums; color: var(--fg-strong); }
[data-testid="stMetricLabel"] { font-family: var(--font-mono); font-size: 10px; letter-spacing: 0.4px; text-transform: uppercase; color: var(--fg-muted); }

.stTabs [data-baseweb="tab-list"] { gap: 4px; border-bottom: 1px solid var(--border-subtle); }
.stTabs [data-baseweb="tab"] {
  font-family: var(--font-mono); font-size: 12px; font-weight: 540;
  letter-spacing: 0.2px; color: var(--fg-muted);
  border-radius: var(--radius-sm) var(--radius-sm) 0 0;
  padding: 8px 12px;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] { color: var(--accent-fg); border-bottom: 2px solid var(--accent-solid); }

.synapse-badge {
  display: inline-flex; align-items: center; gap: 6px;
  height: 22px; padding: 0 8px;
  font-family: var(--font-mono); font-size: 10px; line-height: 14px;
  font-weight: 540; letter-spacing: 0.4px;
  border-radius: var(--radius-xs);
  border: 1px solid;
  white-space: nowrap;
}
.synapse-badge .dot { width: 8px; height: 8px; border-radius: 50%; }
.synapse-badge .score { font-weight: 460; opacity: 0.85; }
.synapse-badge[data-tier="deprecated"]    { background: var(--conf-deprecated-bg); color: var(--conf-deprecated-fg); border-color: color-mix(in srgb, var(--conf-deprecated-fg) 18%, transparent); }
.synapse-badge[data-tier="deprecated"] .dot { background: var(--conf-deprecated-dot); }
.synapse-badge[data-tier="guessed"]       { background: var(--conf-guessed-bg);    color: var(--conf-guessed-fg);    border-color: color-mix(in srgb, var(--conf-guessed-fg)    18%, transparent); }
.synapse-badge[data-tier="guessed"] .dot    { background: var(--conf-guessed-dot); }
.synapse-badge[data-tier="inferred"]      { background: var(--conf-inferred-bg);   color: var(--conf-inferred-fg);   border-color: color-mix(in srgb, var(--conf-inferred-fg)   18%, transparent); }
.synapse-badge[data-tier="inferred"] .dot   { background: var(--conf-inferred-dot); }
.synapse-badge[data-tier="grounded"]      { background: var(--conf-grounded-bg);   color: var(--conf-grounded-fg);   border-color: color-mix(in srgb, var(--conf-grounded-fg)   18%, transparent); }
.synapse-badge[data-tier="grounded"] .dot   { background: var(--conf-grounded-dot); }
.synapse-badge[data-tier="human_asserted"]{ background: var(--conf-human-bg);      color: var(--conf-human-fg);      border-color: color-mix(in srgb, var(--conf-human-fg)      18%, transparent); }
.synapse-badge[data-tier="human_asserted"] .dot { background: var(--conf-human-dot); }
.synapse-badge.large { height: 26px; font-size: 12px; padding: 0 10px; }

.synapse-pill {
  display: inline-flex; align-items: center; gap: 4px;
  height: 20px; padding: 0 6px;
  background: var(--surface-1); border: 1px solid var(--border-subtle);
  border-radius: var(--radius-xs);
  font-family: var(--font-mono); font-size: 11px; line-height: 14px;
  color: var(--fg-default);
  transition: background var(--motion-fast) var(--motion-ease),
              border-color var(--motion-fast) var(--motion-ease);
}
.synapse-pill:hover { background: var(--surface-2); border-color: var(--border-default); }
.synapse-pill .glyph { display: inline-block; width: 6px; height: 6px; border-radius: 50%; }
.synapse-pill[data-family="catalog"] .glyph { background: var(--src-catalog); }
.synapse-pill[data-family="runtime"] .glyph { background: var(--src-runtime); }
.synapse-pill[data-family="corpus"]  .glyph { background: var(--src-corpus); }
.synapse-pill[data-family="ai"]      .glyph { background: var(--src-ai); }
.synapse-pill.muted { opacity: 0.35; border-color: transparent; background: transparent; }
.synapse-pill.muted .glyph { background: var(--fg-muted); }

.synapse-tag {
  display: inline-flex; align-items: center; height: 18px; padding: 0 6px;
  font-family: var(--font-mono); font-size: 11px; color: var(--fg-muted);
  background: var(--surface-2); border-radius: var(--radius-xs);
  margin-right: 4px;
}
.synapse-syn { font-style: italic; color: var(--accent-fg); font-size: 12px; margin-right: 6px; }

.synapse-hero {
  background: var(--surface-1); box-shadow: var(--shadow-rest);
  border-radius: var(--radius-md); padding: var(--space-6);
  margin-bottom: var(--space-6);
}
.synapse-hero .eyebrow {
  font-family: var(--font-mono); font-size: 11px; font-weight: 540;
  letter-spacing: 0.6px; text-transform: uppercase;
  color: var(--fg-muted); margin-bottom: var(--space-4);
}
.synapse-hero .title-row {
  display: flex; align-items: baseline; justify-content: space-between;
  gap: var(--space-5); margin-bottom: var(--space-3);
}
.synapse-hero .title {
  font-family: var(--font-mono); font-size: 24px; line-height: 32px;
  font-weight: 620; letter-spacing: -0.4px; color: var(--fg-strong);
  word-break: break-all;
}
.synapse-hero .breadcrumb {
  font-family: var(--font-mono); font-size: 11px; color: var(--fg-muted);
  margin-bottom: var(--space-3);
}
.synapse-hero .desc {
  font-size: 13px; line-height: 20px; color: var(--fg-default);
  max-width: 760px; margin-bottom: var(--space-4);
}
.synapse-hero .sources-label {
  font-family: var(--font-mono); font-size: 10px; letter-spacing: 0.4px;
  text-transform: uppercase; color: var(--fg-muted); margin-bottom: 4px;
}
.synapse-hero .sources-row {
  display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: var(--space-5);
}
.synapse-hero .stat-grid {
  display: grid; grid-template-columns: repeat(5, 1fr);
  border-top: 1px solid var(--border-subtle); padding-top: var(--space-4);
}
.synapse-hero .stat { padding: 0 var(--space-4); border-left: 1px solid var(--border-subtle); }
.synapse-hero .stat:first-child { padding-left: 0; border-left: none; }
.synapse-hero .stat-label {
  font-family: var(--font-mono); font-size: 10px; font-weight: 540;
  letter-spacing: 0.4px; text-transform: uppercase; color: var(--fg-muted);
}
.synapse-hero .stat-value {
  font-family: var(--font-mono); font-size: 16px; font-weight: 540;
  color: var(--fg-strong); font-variant-numeric: tabular-nums; margin-top: 2px;
}
.synapse-hero .stat-sub {
  font-family: var(--font-mono); font-size: 11px; color: var(--fg-muted); margin-top: 2px;
}
.synapse-hero .stat-value.pass    { color: var(--dq-pass-fg); }
.synapse-hero .stat-value.warn    { color: var(--dq-warn-fg); }
.synapse-hero .stat-value.fail    { color: var(--dq-fail-fg); }
.synapse-hero .stat-value.unknown { color: var(--dq-unknown-fg); }

.synapse-keycol {
  background: var(--surface-1); box-shadow: var(--shadow-rest);
  border-radius: var(--radius-sm); padding: 10px 12px;
}
.synapse-keycol .name {
  font-family: var(--font-mono); font-size: 13px; font-weight: 540; color: var(--fg-strong);
}
.synapse-keycol .type {
  font-family: var(--font-mono); font-size: 11px; color: var(--fg-muted); margin-top: 2px;
}
.synapse-keycol .meta {
  font-family: var(--font-mono); font-size: 11px; color: var(--fg-muted); margin-top: 6px;
}

.synapse-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.synapse-table thead tr {
  text-align: left; border-bottom: 1px solid var(--border-subtle);
  color: var(--fg-muted);
}
.synapse-table thead th {
  padding: 6px 8px; font-weight: 540; font-family: var(--font-mono);
  font-size: 10px; letter-spacing: 0.4px; text-transform: uppercase;
}
.synapse-table tbody td { padding: 8px; border-bottom: 0.5px solid var(--border-subtle); vertical-align: middle; }
.synapse-table tbody tr:hover { background: var(--surface-2); }
.synapse-table tbody td.mono { font-family: var(--font-mono); }
.synapse-table tbody td .col-name { font-family: var(--font-mono); font-weight: 540; color: var(--fg-strong); }
.synapse-table tbody td .type-chip {
  display: inline-block; padding: 1px 4px; background: var(--surface-3);
  color: var(--fg-muted); font-family: var(--font-mono);
  font-size: 11px; border-radius: 2px;
}

.synapse-lineage {
  display: grid; grid-template-columns: 1fr auto 1fr;
  gap: var(--space-5); align-items: start;
}
.synapse-lineage .col-head {
  font-family: var(--font-mono); font-size: 10px; font-weight: 540;
  letter-spacing: 0.4px; text-transform: uppercase; color: var(--fg-muted);
  margin-bottom: var(--space-3);
}
.synapse-lineage .item {
  background: var(--surface-1); padding: 8px 12px;
  border-radius: var(--radius-sm); box-shadow: var(--shadow-rest);
  margin-bottom: 6px;
  font-family: var(--font-mono); font-size: 12px; color: var(--fg-strong);
}
.synapse-lineage .center {
  align-self: center; padding: 12px 16px;
  background: var(--accent-bg); border: 1px solid var(--accent-border);
  border-radius: var(--radius-sm);
  font-family: var(--font-mono); font-size: 13px; font-weight: 620;
  color: var(--accent-fg); white-space: nowrap;
}

.synapse-dq { display: block; padding: 10px var(--space-5);
  border-bottom: 0.5px solid var(--border-subtle); background: var(--surface-1); }
.synapse-dq[data-status="fail"]    { border-left: 3px solid var(--dq-fail-fg); }
.synapse-dq[data-status="warning"] { border-left: 3px solid var(--dq-warn-fg); }
.synapse-dq[data-status="pass"]    { border-left: 3px solid var(--dq-pass-fg); }
.synapse-dq[data-status="unknown"] { border-left: 3px solid var(--dq-unknown-fg); }

.synapse-source-card {
  background: var(--surface-1); box-shadow: var(--shadow-rest);
  border-radius: var(--radius-md); padding: var(--space-4);
  margin-bottom: var(--space-3);
}
.synapse-source-card[data-contributed="false"] {
  background: transparent; box-shadow: 0 0 0 1px var(--border-subtle);
  opacity: 0.55;
}
.synapse-source-card .head {
  display: flex; align-items: center; gap: var(--space-3);
  margin-bottom: var(--space-3);
}
.synapse-source-card .head .name {
  font-family: var(--font-mono); font-weight: 620; font-size: 13px; color: var(--fg-strong);
}
.synapse-source-card .head .ev {
  font-family: var(--font-mono); font-size: 11px; color: var(--fg-muted);
}
.synapse-source-card .fact-row {
  display: grid; grid-template-columns: 140px 1fr; gap: 8px; padding: 3px 0;
  font-family: var(--font-mono); font-size: 12px;
}
.synapse-source-card .fact-row .k { color: var(--fg-muted); }
.synapse-source-card .fact-row .v { color: var(--fg-default); word-break: break-word; }

.synapse-ai {
  border-left: 2px solid var(--src-ai); padding: 6px 10px;
  background: color-mix(in srgb, var(--src-ai) 6%, transparent);
  border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
  font-style: italic; font-size: 12px; color: var(--fg-default); opacity: 0.9;
  margin-top: 6px;
}

@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    animation-duration: 0.01ms !important;
    transition-duration: 0.01ms !important;
  }
}

#MainMenu, footer, [data-testid="stToolbar"] { visibility: hidden; }
</style>
"""


# ─── Source-to-family mapping (visual-system spec §5) ───────


SOURCE_FAMILY = {
    "mdm":             "catalog",
    "corpus":          "corpus",
    "bq":              "runtime",
    "baseline_lookml": "catalog",
    "glossary":        "catalog",
    "metric_catalog":  "catalog",
    "table_catalog":   "catalog",
    "usage":           "corpus",
    "dq_engine":       "runtime",
    "llm_generated":   "ai",
    "human_approval":  "catalog",
}

# Canonical 10-source order — used by the hero source row
ALL_SOURCES_ORDERED = [
    "mdm", "table_catalog", "metric_catalog", "glossary", "baseline_lookml",
    "bq", "dq_engine",
    "corpus", "usage",
    "llm_generated",
]


# ─── Cached graph build ─────────────────────────────────────


@st.cache_resource(show_spinner="Generating sources + building graph…")
def _load_graph(demo_dir_str: str, regenerate: bool):
    demo_dir = Path(demo_dir_str)
    if regenerate or not demo_dir.exists():
        generate_all_sources(demo_dir)
    store = build_graph_from_sources(demo_dir)
    return store, store.stats()


# ─── Visual primitives (spec'd components) ──────────────────


def _badge(tier: str, score: float | None = None, *, large: bool = False) -> str:
    """ConfidenceBadge per visual-system §2.

    Renders a labeled dot + tier code + optional score.
    Color comes from the spec's confidence-tier palette."""
    classes = "synapse-badge" + (" large" if large else "")
    short = {
        "human_asserted": "HUMAN",
        "grounded":       "GROUNDED",
        "inferred":       "INFERRED",
        "guessed":        "GUESSED",
        "deprecated":     "DEPRECATED",
    }.get(tier, tier.upper())
    score_html = (
        f"<span class='score'>{score:.2f}</span>" if score is not None else ""
    )
    return (
        f"<span class='{classes}' data-tier='{tier}'>"
        f"<span class='dot'></span>"
        f"<span>{short}</span>"
        f"{score_html}"
        f"</span>"
    )


def _pill(source: str, *, muted: bool = False) -> str:
    """SourcePill per visual-system §2. Glyph color = family tint."""
    family = SOURCE_FAMILY.get(source, "catalog")
    classes = "synapse-pill" + (" muted" if muted else "")
    return (
        f"<span class='{classes}' data-family='{family}' title='{source}'>"
        f"<span class='glyph'></span>{source}"
        f"</span>"
    )


def _tag(tag: str) -> str:
    return f"<span class='synapse-tag'>{tag}</span>"


def _synonym(s: str) -> str:
    return f"<span class='synapse-syn'>“{s}”</span>"


# ─── Hero (Trust Header + identity + sources row) ───────────


def _freshness_class(hours: float | None) -> tuple[str, str]:
    if hours is None: return "unknown", "unknown"
    if hours < 6:  return "pass", "fresh"
    if hours < 24: return "pass", "ok"
    if hours < 72: return "warn", "stale"
    return "fail", "old"


def _dq_class(passing: int, total: int) -> str:
    if total == 0: return "unknown"
    pct = passing / total
    if pct >= 0.95: return "pass"
    if pct >= 0.75: return "warn"
    return "fail"


def _pii_class(n: int) -> str:
    if n == 0: return "pass"
    if n <= 2: return "warn"
    return "fail"


def _render_hero(inspection: dict) -> None:
    """Visual-system §2.HeroHeader. Inverts current UI's:
    - 5-tile metrics → glyph-row + stat grid
    - Numeric n-of-10 → literal row of 10 source pills with non-contribs
      muted (the signature visual move)"""
    identity   = inspection["identity"]
    fused      = inspection["fused_view"]
    dq         = inspection["data_quality"]
    governance = inspection["governance"]
    usage      = inspection["usage"]

    fqn = identity.get("fqn") or identity["table"]
    parts = fqn.split(".")
    breadcrumb = " <span style='opacity:0.5'>›</span> ".join(parts)

    business = identity.get("business_name") or identity["table"]
    desc = identity.get("description") or ""

    # The signature move: row of 10 source pills, muted if not contributing.
    contributing = set(fused.get("sources_contributed", []))
    sources_html = "".join(
        _pill(s, muted=(s not in contributing))
        for s in ALL_SOURCES_ORDERED
    )

    # Trust stat
    tier = fused["confidence_tier"]
    trust_html = _badge(tier, fused.get("confidence_score"), large=True)
    n_agree = len(contributing)

    # Freshness
    fresh_hours = dq.get("freshness_hours")
    fresh_kls, fresh_word = _freshness_class(fresh_hours)
    fresh_val = f"{fresh_hours:.1f}h" if fresh_hours is not None else "—"

    # DQ
    rules = dq.get("rules", []) or []
    passing = sum(1 for r in rules if r.get("last_run_status") == "pass")
    warning = sum(1 for r in rules if r.get("last_run_status") == "warning")
    failing = sum(1 for r in rules if r.get("last_run_status") == "fail")
    dq_kls = _dq_class(passing, len(rules))
    dq_val = f"{passing}/{len(rules)}" if rules else "—"
    parts_dq = []
    if warning: parts_dq.append(f"{warning} warn")
    if failing: parts_dq.append(f"{failing} fail")
    dq_sub = " · ".join(parts_dq) if parts_dq else "all passing"

    # PII
    n_pii = len(governance.get("pii_columns", []))
    pii_kls = _pii_class(n_pii)
    pii_val = f"{n_pii}"
    pii_sub = "PII columns" if n_pii != 1 else "PII column"

    # Usage
    q = usage.get("total_queries_observed", 0)
    top_team = ""
    if usage.get("top_users"):
        top_team = (usage["top_users"][0].get("team") or "").strip()
    usage_sub = f"top: {top_team}" if top_team else "queries observed"

    tags_html = "".join(_tag(t) for t in (identity.get("tags") or [])[:8])

    eyebrow = (
        f"Synapse · {identity.get('asset_kind', 'Table')}"
        + (f" · owner {governance.get('owner_team', '')}" if governance.get("owner_team") else "")
    )

    desc_short = desc if len(desc) <= 280 else desc[:277] + "…"

    st.markdown(
        f"""
<div class='synapse-hero'>
  <div class='eyebrow'>{eyebrow}</div>
  <div class='title-row'>
    <div class='title'>{business}</div>
    <div>{trust_html}</div>
  </div>
  <div class='breadcrumb'>{breadcrumb}</div>
  <div class='desc'>{desc_short}</div>
  <div>{tags_html}</div>
  <div class='sources-label' style='margin-top:14px'>
    {n_agree} of {len(ALL_SOURCES_ORDERED)} sources contributed
  </div>
  <div class='sources-row'>{sources_html}</div>
  <div class='stat-grid'>
    <div class='stat'>
      <div class='stat-label'>Trust</div>
      <div class='stat-value'>{tier}</div>
      <div class='stat-sub'>{fused.get('confidence_score', 0):.2f} · {n_agree} sources</div>
    </div>
    <div class='stat'>
      <div class='stat-label'>Fresh</div>
      <div class='stat-value {fresh_kls}'>{fresh_val}</div>
      <div class='stat-sub'>{fresh_word}</div>
    </div>
    <div class='stat'>
      <div class='stat-label'>DQ</div>
      <div class='stat-value {dq_kls}'>{dq_val}</div>
      <div class='stat-sub'>{dq_sub}</div>
    </div>
    <div class='stat'>
      <div class='stat-label'>PII</div>
      <div class='stat-value {pii_kls}'>{pii_val}</div>
      <div class='stat-sub'>{pii_sub}</div>
    </div>
    <div class='stat'>
      <div class='stat-label'>Usage</div>
      <div class='stat-value'>{q:,}</div>
      <div class='stat-sub'>{usage_sub}</div>
    </div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )


# ─── L1: Key Columns strip ──────────────────────────────────


def _render_key_columns(columns: list[dict]) -> None:
    if not columns:
        return
    pk = [c for c in columns if c.get("is_primary")][:1]
    partition = [c for c in columns if c.get("is_partitioning") and c not in pk][:1]
    by_use = sorted(
        [c for c in columns if c not in pk and c not in partition],
        key=lambda c: -(c.get("reference_count") or 0),
    )
    top_used = [c for c in by_use if (c.get("reference_count") or 0) > 0][:3]
    key_cols = pk + partition + top_used
    if not key_cols:
        key_cols = columns[:4]

    st.markdown(
        "<div style='font-family:var(--font-mono);font-size:10px;"
        "letter-spacing:0.4px;text-transform:uppercase;font-weight:540;"
        "color:var(--fg-muted);margin-bottom:6px;'>Key columns</div>",
        unsafe_allow_html=True,
    )
    cols = st.columns(len(key_cols))
    for col_widget, c in zip(cols, key_cols):
        with col_widget:
            flags = []
            if c["is_primary"]:      flags.append("PK")
            if c["is_partitioning"]: flags.append("PART")
            if c["is_pii"]:          flags.append("PII")
            flag_html = (
                f"<span class='synapse-tag' style='margin-left:6px;font-size:10px'>"
                f"{'·'.join(flags)}</span>" if flags else ""
            )
            ref_html = ""
            if c.get("reference_count"):
                ref_html = f"used {c['reference_count']:,}× in corpus · "
            st.markdown(
                f"""
<div class='synapse-keycol'>
  <div class='name'>{c['name']}{flag_html}</div>
  <div class='type'>{c.get('data_type', '?')}</div>
  <div class='meta'>{ref_html}{_badge(c['confidence_tier'], c['confidence_score'])}</div>
</div>
""",
                unsafe_allow_html=True,
            )


# ─── L2: Columns dense table ────────────────────────────────


def _render_columns_table(columns: list[dict]) -> None:
    if not columns:
        st.caption("(no columns)")
        return

    f1, f2, f3, f4 = st.columns([3, 1, 1, 1])
    with f1:
        q = st.text_input(
            "Filter by name",
            placeholder="cm11, *_id, *_dt …",
            label_visibility="collapsed",
        )
    with f2:
        only_pii = st.checkbox("PII only")
    with f3:
        only_joins = st.checkbox("Join keys")
    with f4:
        min_tier = st.selectbox(
            "Min tier",
            ["any", "inferred", "grounded", "human_asserted"],
            label_visibility="collapsed",
        )

    tier_rank = {"deprecated": 0, "guessed": 1, "inferred": 2, "grounded": 3, "human_asserted": 4}
    min_rank = tier_rank.get(min_tier, 0) if min_tier != "any" else 0

    filtered = []
    qlow = (q or "").strip().lower().replace("*", "")
    for c in columns:
        if qlow and qlow not in c["name"].lower():
            continue
        if only_pii and not c.get("is_pii"):
            continue
        if only_joins and not c.get("is_join_key"):
            continue
        if tier_rank.get(c["confidence_tier"], 0) < min_rank:
            continue
        filtered.append(c)

    st.caption(f"{len(filtered)} of {len(columns)} columns")

    rows_html = []
    for c in filtered:
        flags = []
        if c["is_primary"]:      flags.append("🔑")
        if c["is_partitioning"]: flags.append("📅")
        if c.get("is_join_key"): flags.append("🔗")
        if c["is_pii"]:          flags.append("🔒")
        if c.get("is_coded"):    flags.append("🆔")
        flag_str = "".join(flags)
        sources_html = "".join(
            _pill(s) for s in c["sources_contributed"]
        )
        card = c.get("approx_distinct")
        card_str = f"{card:,}" if card is not None else "—"
        desc = c.get("description") or c.get("ai_generated_description") or ""
        if len(desc) > 110:
            desc = desc[:107] + "…"
        rows_html.append(
            "<tr>"
            f"<td><span class='col-name'>{c['name']}</span> "
            f"<span style='color:var(--fg-muted)'>{flag_str}</span></td>"
            f"<td><span class='type-chip'>{c.get('data_type', '?')}</span></td>"
            f"<td>{_badge(c['confidence_tier'], c['confidence_score'])}</td>"
            f"<td class='mono' style='color:var(--fg-muted)'>{card_str}</td>"
            f"<td>{sources_html}</td>"
            f"<td style='font-size:12px;color:var(--fg-default)'>{desc}</td>"
            "</tr>"
        )
    st.markdown(
        "<table class='synapse-table'>"
        "<thead><tr>"
        "<th>Column</th><th>Type</th><th>Confidence</th>"
        "<th>Distinct</th><th>Sources</th><th>Description</th>"
        f"</tr></thead><tbody>{''.join(rows_html)}</tbody></table>",
        unsafe_allow_html=True,
    )


# ─── L2: Metrics ────────────────────────────────────────────


def _render_metrics(metrics: list[dict]) -> None:
    if not metrics:
        st.caption("(no metrics sourced from this table)")
        return
    for m in metrics:
        with st.container(border=True):
            cols = st.columns([4, 1])
            with cols[0]:
                st.markdown(
                    f"<div style='font-family:var(--font-mono);font-weight:620;"
                    f"font-size:13px;color:var(--fg-strong)'>{m['technical_name']}"
                    f"</div>"
                    f"<div style='color:var(--fg-muted);font-size:12px'>"
                    f"{m.get('business_name') or ''}</div>",
                    unsafe_allow_html=True,
                )
                st.code(m.get("formula") or "", language="sql")
                meta = []
                if m.get("domain"): meta.append(f"domain: <code>{m['domain']}</code>")
                if m.get("grain"):  meta.append(f"grain: <code>{m['grain']}</code>")
                if meta:
                    st.markdown(
                        "<div style='font-size:12px;color:var(--fg-muted)'>"
                        + " · ".join(meta) + "</div>",
                        unsafe_allow_html=True,
                    )
                if m.get("synonyms"):
                    st.markdown(
                        "".join(_synonym(s) for s in m["synonyms"]),
                        unsafe_allow_html=True,
                    )
            with cols[1]:
                st.markdown(
                    _badge(m["confidence_tier"], m["confidence_score"])
                    + "<div style='margin-top:6px'>"
                    + "".join(_pill(s) for s in m["sources_contributed"])
                    + "</div>",
                    unsafe_allow_html=True,
                )


# ─── L2: Lineage rail ───────────────────────────────────────


def _render_lineage(lineage: dict, table_name: str) -> None:
    up = lineage.get("upstream", [])
    down = lineage.get("downstream", [])
    if not (up or down):
        st.caption("(no observed lineage)")
        return
    up_html = "".join(
        f"<div class='item'>← {u['table']}</div>" for u in up
    ) or "<div style='color:var(--fg-muted);font-size:12px'>(root table)</div>"
    down_html = "".join(
        f"<div class='item'>{d['table']} →</div>" for d in down
    ) or "<div style='color:var(--fg-muted);font-size:12px'>(leaf)</div>"
    st.markdown(
        f"""
<div class='synapse-lineage'>
  <div>
    <div class='col-head'>↑ Upstream ({len(up)})</div>
    {up_html}
  </div>
  <div class='center'>{table_name}</div>
  <div>
    <div class='col-head'>↓ Downstream ({len(down)})</div>
    {down_html}
  </div>
</div>
""",
        unsafe_allow_html=True,
    )


# ─── L2: Related, Usage, Governance, DQ, Codes ──────────────


def _render_related(related: list[dict]) -> None:
    if not related:
        st.caption("(no observed joins)")
        return
    for r in related:
        with st.expander(
            f"{r['table']} — {r['n_join_observations']} JOIN observation(s)"
        ):
            for lc in r["linking_columns"][:5]:
                st.markdown(
                    f"<code>{lc['from']}</code> ↔ <code>{lc['to']}</code>",
                    unsafe_allow_html=True,
                )


def _render_usage(usage: dict) -> None:
    cols = st.columns([1, 3])
    with cols[0]:
        st.metric("Total queries", f"{usage['total_queries_observed']:,}")
        if usage.get("peak_query_hours"):
            st.caption(
                "Peak hours UTC: "
                + ", ".join(str(h) for h in usage["peak_query_hours"])
            )
    with cols[1]:
        st.markdown(
            "<div style='font-family:var(--font-mono);font-size:10px;"
            "letter-spacing:0.4px;text-transform:uppercase;color:var(--fg-muted);"
            "margin-bottom:8px'>Top users</div>",
            unsafe_allow_html=True,
        )
        for u in (usage.get("top_users") or [])[:10]:
            st.markdown(
                f"<div style='padding:3px 0;font-size:12px'>"
                f"<code>{u.get('email', '?')}</code> · "
                f"<span style='color:var(--fg-muted)'>{u.get('team', '?')}</span> · "
                f"<b style='font-family:var(--font-mono)'>{u.get('query_count', 0):,}</b> queries"
                f"</div>",
                unsafe_allow_html=True,
            )


def _render_governance(g: dict) -> None:
    cols = st.columns([1, 2])
    with cols[0]:
        st.markdown(
            f"<div style='font-size:12px;color:var(--fg-muted)'>Owner</div>"
            f"<div style='font-family:var(--font-mono);font-weight:540;"
            f"margin-top:2px'>{g.get('owner_team') or '—'}</div>",
            unsafe_allow_html=True,
        )
        if g.get("has_pii"):
            st.markdown(
                f"<div style='margin-top:12px;color:var(--dq-fail-fg);font-weight:620'>"
                f"PII present</div>"
                f"<div style='font-size:11px;color:var(--fg-muted)'>"
                f"{len(g.get('pii_columns', []))} columns</div>",
                unsafe_allow_html=True,
            )
        else:
            st.caption("No PII columns")
    with cols[1]:
        if g.get("pii_columns"):
            for p in g["pii_columns"][:30]:
                st.markdown(
                    f"<div style='padding:3px 0;font-size:12px'>"
                    f"<code>{p['name']}</code> · "
                    f"<span style='color:var(--fg-muted);font-family:var(--font-mono);"
                    f"font-size:11px'>{p['pii_taxonomy']}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )


def _render_dq_rules(dq: dict) -> None:
    rules = dq.get("rules", [])
    top = st.columns(3)
    top[0].metric("Completeness", f"{dq['completeness_score']:.2f}")
    top[1].metric("Consistency",  f"{dq['consistency_score']:.2f}")
    if dq.get("freshness_hours") is not None:
        top[2].metric("Freshness (h)", f"{dq['freshness_hours']:.1f}")
    if not rules:
        st.caption("(no DQ rules)")
        return
    rows = []
    for r in rules:
        status = r.get("last_run_status", "unknown")
        dot_color = {
            "pass": "var(--dq-pass-fg)", "warning": "var(--dq-warn-fg)",
            "fail": "var(--dq-fail-fg)", "unknown": "var(--dq-unknown-fg)",
        }.get(status, "var(--dq-unknown-fg)")
        target = r.get("target_column") or "(table)"
        auto = " ✨" if r.get("auto_suggested") else ""
        rows.append(
            "<tr>"
            f"<td><span style='display:inline-block;width:8px;height:8px;"
            f"background:{dot_color};border-radius:50%'></span> "
            f"<span class='mono' style='font-size:11px;color:var(--fg-default)'>{status}</span></td>"
            f"<td class='mono' style='font-size:12px'>{r.get('rule_kind', '')}</td>"
            f"<td class='mono' style='font-size:12px'>{target}{auto}</td>"
            f"<td class='mono' style='font-size:11px;color:var(--fg-muted)'>"
            f"{r.get('threshold', '')}</td>"
            f"<td class='mono' style='font-size:11px;color:var(--fg-muted)'>"
            f"observed: {r.get('last_run_value', '')}</td>"
            "</tr>"
        )
    st.markdown(
        "<table class='synapse-table' style='margin-top:12px'>"
        "<thead><tr>"
        "<th>Status</th><th>Kind</th><th>Target</th>"
        "<th>Threshold</th><th>Observed</th>"
        f"</tr></thead><tbody>{''.join(rows)}</tbody></table>",
        unsafe_allow_html=True,
    )


def _render_code_resolutions(crs: list[dict]) -> None:
    if not crs:
        st.caption("(none)")
        return
    rows = []
    for cm in crs[:80]:
        rows.append(
            "<tr>"
            f"<td class='mono' style='font-size:12px'>{cm['column']}</td>"
            f"<td class='mono' style='font-size:12px;font-weight:620'>{cm['raw_value']}</td>"
            f"<td style='font-size:12px;color:var(--fg-default)'>{cm['human_meaning']}</td>"
            f"<td class='mono' style='font-size:11px;color:var(--fg-muted)'>{cm['source']}</td>"
            "</tr>"
        )
    st.markdown(
        "<table class='synapse-table'>"
        "<thead><tr><th>Column</th><th>Raw</th><th>Meaning</th><th>Source</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>",
        unsafe_allow_html=True,
    )


def _render_per_source_audit(per_source: dict) -> None:
    """L3 audit. The PRIMARY entrypoint to provenance is per-fact pills
    on each fact; this is the full-table-level breakdown for auditors."""
    items = list(per_source.items())
    grid = st.columns(2)
    half = (len(items) + 1) // 2
    for col_widget, chunk in zip(grid, [items[:half], items[half:]]):
        with col_widget:
            for source_name, view in chunk:
                contributed = bool(view.get("contributed"))
                n = view.get("evidence_count", 0)
                family = SOURCE_FAMILY.get(source_name, "catalog")
                facts_html_parts = []
                for k, v in view.items():
                    if k in {"contributed", "evidence_count"}:
                        continue
                    if k == "note":
                        continue
                    if v in (None, "", [], 0):
                        continue
                    val = str(v)
                    if len(val) > 200:
                        val = val[:197] + "…"
                    facts_html_parts.append(
                        f"<div class='fact-row'><div class='k'>{k}</div>"
                        f"<div class='v'>{val}</div></div>"
                    )
                    if len(facts_html_parts) >= 8:
                        break
                facts_html = "".join(facts_html_parts) or (
                    "<div style='color:var(--fg-muted);font-size:12px;"
                    "font-style:italic'>(no facts contributed)</div>"
                )
                note_html = ""
                if view.get("note"):
                    note_html = (
                        f"<div style='font-size:11px;color:var(--fg-muted);"
                        f"margin-top:8px;font-style:italic'>{view['note']}</div>"
                    )
                marker = "●" if contributed else "○"
                marker_color = (
                    f"var(--src-{family})" if contributed
                    else "var(--fg-muted)"
                )
                st.markdown(
                    f"""
<div class='synapse-source-card' data-contributed='{'true' if contributed else 'false'}'>
  <div class='head'>
    <span style='color:{marker_color};font-size:14px'>{marker}</span>
    <span class='name'>{source_name}</span>
    <span class='ev'>{n} event{'s' if n != 1 else ''}</span>
  </div>
  {facts_html}
  {note_html}
</div>
""",
                    unsafe_allow_html=True,
                )


# ─── Graph tab (neighborhood + lineage DAG + consumption flow) ──


_NODE_TYPE_HINTS = {
    "Table":           "Tables — facts and dims",
    "Column":          "Columns — atomic schema",
    "Metric":          "Metrics — computed measures",
    "Entity":          "Entities — business concepts",
    "Synonym":         "Synonyms — surface forms / acronyms",
    "User":            "Users — query operators",
    "CodeMapping":     "Code mappings — value decodes",
    "FilterValue":     "Filter values — observed literals",
    "DataQualityRule": "DQ rules — Auto-DQ checks",
}


def _render_graph_legend() -> None:
    """Compact key for node shape + edge style + opacity = confidence."""
    st.markdown(
        """
<div style='display:flex;gap:18px;flex-wrap:wrap;
            font-family:var(--font-mono);font-size:11px;
            color:var(--fg-muted);padding:8px 0'>
  <div><b style='color:var(--fg-default)'>Nodes:</b>
    ▪ Table &nbsp; ● Column &nbsp; ◆ Metric &nbsp; ⬢ Entity
    &nbsp; ⬭ Synonym &nbsp; ▲ CodeMapping &nbsp; ★ DQ rule</div>
  <div><b style='color:var(--fg-default)'>Edges:</b>
    — solid: structural &nbsp; - - dashed: observed &nbsp;
    → arrows: derived / lineage</div>
  <div><b style='color:var(--fg-default)'>Opacity:</b>
    higher fill = higher confidence</div>
</div>
""",
        unsafe_allow_html=True,
    )


def _render_graph_tab(store, default_table: str) -> None:
    """The Graph tab — three lenses on the same underlying graph.

    Designer note (from UX_VISUAL_SYSTEM §5 + IA §7.5):
    A 391-node graph is a hairball if rendered whole. Always show a
    *focused* subgraph: neighborhood around one node, or the lineage
    spine, or a single ego. The picker lets the user re-center."""
    st.markdown(
        "<div style='font-family:var(--font-mono);font-size:11px;"
        "color:var(--fg-muted);margin-bottom:8px'>"
        "Three lenses on the same graph. Click any node in a view to inspect "
        "it; drag to reposition; scroll to zoom."
        "</div>",
        unsafe_allow_html=True,
    )

    sub = st.tabs([
        "Neighborhood",
        "Lineage DAG",
        "How it's consumed",
    ])

    # ── Neighborhood ──────────────────────────────────────
    with sub[0]:
        ctrl = st.columns([1, 2, 1, 1])
        with ctrl[0]:
            node_type = st.selectbox(
                "Re-center on",
                ["Table", "Column", "Metric", "Entity", "Synonym",
                 "CodeMapping", "FilterValue", "DataQualityRule", "User"],
                index=0,
                help="Pick the kind of node to center the graph on.",
            )
        with ctrl[1]:
            # All node URIs of that type — labels shortened to last segment
            nodes_of_type = [
                n for n in store.nodes_by_type(node_type)
            ]
            options = sorted(
                [
                    (n.canonical_uri.rsplit("/", 1)[-1], n.canonical_uri)
                    for n in nodes_of_type
                ],
                key=lambda kv: kv[0],
            )
            labels = [label for label, _ in options]
            # Default: the currently-selected table when applicable
            default_label = (
                default_table if node_type == "Table" and default_table in labels
                else (labels[0] if labels else "")
            )
            default_idx = labels.index(default_label) if default_label in labels else 0
            chosen_label = st.selectbox(
                "Center node",
                labels,
                index=default_idx if labels else 0,
            )
            center_uri = next(uri for label, uri in options if label == chosen_label)
        with ctrl[2]:
            hops = st.slider("Hops", 1, 3, 2)
        with ctrl[3]:
            max_nodes = st.slider("Max nodes", 20, 200, 80, step=10)

        st.caption(_NODE_TYPE_HINTS.get(node_type, ""))
        _render_graph_legend()

        html = neighborhood_html(
            store, center_uri, hops=hops, max_nodes=max_nodes, height=620,
        )
        components.html(html, height=660, scrolling=False)

        # Inline relationship breakdown — what edges connect this node
        node = store.get(center_uri)
        if node:
            outs = store.outgoing(center_uri)
            ins = store.incoming(center_uri)
            counts: dict[str, int] = {}
            for e in outs + ins:
                counts[e.edge_type] = counts.get(e.edge_type, 0) + 1
            if counts:
                pills = "".join(
                    f"<span style='display:inline-flex;align-items:center;gap:4px;"
                    f"padding:3px 8px;margin-right:6px;margin-bottom:4px;"
                    f"background:var(--surface-2);border:1px solid var(--border-subtle);"
                    f"border-radius:4px;font-family:var(--font-mono);font-size:11px;"
                    f"color:var(--fg-default)'>{etype} "
                    f"<b style='color:var(--accent-fg)'>{n}</b></span>"
                    for etype, n in sorted(counts.items(), key=lambda kv: -kv[1])
                )
                st.markdown(
                    "<div style='margin-top:12px'>"
                    f"<div style='font-family:var(--font-mono);font-size:10px;"
                    f"letter-spacing:0.4px;text-transform:uppercase;"
                    f"color:var(--fg-muted);margin-bottom:6px'>"
                    f"Relationships from <code>{chosen_label}</code> "
                    f"({len(outs) + len(ins)} edges)</div>"
                    f"{pills}</div>",
                    unsafe_allow_html=True,
                )

    # ── Lineage DAG ────────────────────────────────────────
    with sub[1]:
        st.caption(
            "Hierarchical left-to-right DAG along UPSTREAM_OF edges. "
            "Upstream tables left → this table → downstream tables right."
        )
        table_uri = canonical_uri("table", default_table)
        lin_html = lineage_dag_html(store, table_uri, depth=3, height=520)
        components.html(lin_html, height=560, scrolling=False)

    # ── Consumption flow ───────────────────────────────────
    with sub[2]:
        st.caption(
            "The graph is one shared substrate fused from 10 ingest sources "
            "and consumed by ~8 downstream products. The graph is the "
            "product; LookML / agent / governance are renderings."
        )
        components.html(consumption_flow_html(height=420), height=520, scrolling=True)


# ─── Page ───────────────────────────────────────────────────


def main() -> None:
    st.set_page_config(
        page_title="Synapse",
        page_icon="🧠",
        layout="wide",
    )
    st.markdown(SYNAPSE_CSS, unsafe_allow_html=True)

    qp = st.query_params
    debug = qp.get("debug") in ("1", "true")

    with st.sidebar:
        st.markdown(
            "<div style='font-family:var(--font-mono);font-weight:620;"
            "font-size:16px;color:var(--fg-strong)'>Synapse</div>"
            "<div style='font-size:11px;color:var(--fg-muted);margin-top:2px'>"
            "the X-ray for a table</div>",
            unsafe_allow_html=True,
        )
        st.divider()
        demo_dir = (
            st.text_input("Demo source dir", value=str(DEFAULT_DEMO_DIR))
            if debug else str(DEFAULT_DEMO_DIR)
        )
        regenerate = (
            st.checkbox("Regenerate sources") if debug else False
        )
        if debug and st.button("Rebuild graph"):
            st.cache_resource.clear()

    store, stats = _load_graph(demo_dir, regenerate)

    # Table selector + nav bar
    all_tables = sorted(
        n.properties.get("table_name", "")
        for n in store.nodes_by_type("Table")
        if n.properties.get("table_name")
    )
    default = "custins_customer_insights_cardmember"
    default_idx = all_tables.index(default) if default in all_tables else 0

    top = st.columns([4, 2, 1])
    with top[0]:
        selected = st.selectbox(
            "Inspect table", all_tables, index=default_idx,
            label_visibility="collapsed",
        )
    with top[1]:
        st.markdown(
            "<div style='color:var(--fg-muted);font-size:11px;padding-top:8px;"
            "font-family:var(--font-mono)'>"
            f"{stats['n_nodes']:,} nodes · {stats['n_edges']:,} edges"
            "</div>",
            unsafe_allow_html=True,
        )
    with top[2]:
        href = "?" if debug else "?debug=1"
        word = "exit debug" if debug else "debug"
        st.markdown(
            f"<div style='text-align:right;padding-top:8px'>"
            f"<a href='{href}' style='color:var(--fg-muted);font-size:11px;"
            f"font-family:var(--font-mono);text-decoration:none'>{word}</a>"
            f"</div>",
            unsafe_allow_html=True,
        )

    inspection = inspect_table(store, selected)
    if "error" in inspection:
        st.error(f"{inspection['error']}: {inspection.get('table')}")
        return

    # ── L1: hero ───────────────────────
    _render_hero(inspection)

    # ── L1: key columns ────────────────
    _render_key_columns(inspection["columns"])

    # ── L2: tabs ───────────────────────
    n_cols = len(inspection["columns"])
    n_metrics = len(inspection["metrics"])
    n_dq = len(inspection["data_quality"].get("rules", []))
    n_rel = len(inspection["related_tables"])
    n_code = len(inspection["code_resolutions"])
    n_pii = len(inspection["governance"].get("pii_columns", []))

    st.markdown("<div style='margin-top:24px'></div>", unsafe_allow_html=True)

    tabs = st.tabs([
        "Graph",
        f"Columns ({n_cols})",
        f"Metrics ({n_metrics})",
        "Lineage",
        f"Related ({n_rel})",
        "Usage",
        f"Governance ({n_pii} PII)",
        f"DQ ({n_dq})",
        f"Codes ({n_code})",
        "Per-source audit",
    ])

    with tabs[0]: _render_graph_tab(store, selected)
    with tabs[1]: _render_columns_table(inspection["columns"])
    with tabs[2]: _render_metrics(inspection["metrics"])
    with tabs[3]: _render_lineage(inspection["lineage"], selected)
    with tabs[4]: _render_related(inspection["related_tables"])
    with tabs[5]: _render_usage(inspection["usage"])
    with tabs[6]: _render_governance(inspection["governance"])
    with tabs[7]: _render_dq_rules(inspection["data_quality"])
    with tabs[8]: _render_code_resolutions(inspection["code_resolutions"])
    with tabs[9]:
        st.caption(
            "Table-level provenance breakdown across the 10 sources. "
            "The primary entrypoint to provenance is per-fact pills on each "
            "section (right-rail pane is v1.5 — see UX_VISUAL_SYSTEM.md §2)."
        )
        _render_per_source_audit(inspection["per_source_view"])

    if debug:
        st.markdown("---")
        with st.expander("Raw inspection JSON"):
            st.json(inspection)
        with st.expander("Graph stats"):
            st.write(stats)


if __name__ == "__main__":
    main()
