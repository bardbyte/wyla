"""Obsidian-like graph viz for the Synapse semantic graph.

Reads the saved graph snapshot, renders via force-graph (Vasco Asturiano,
WebGL/Canvas) embedded in Streamlit as a full-screen HTML component.

Features:
  • WebGL-accelerated force-directed layout (handles 5-10k nodes smoothly)
  • Confidence-tier node fill colors (grounded=cyan, inferred=amber, guessed=gray,
    deprecated=red, human_asserted=green)
  • Edge stroke color by edge type; edge opacity = confidence_score
  • Hover any node → tooltip card with type, tier, sources
  • Click any node → focus mode (fade non-neighbors, side panel with full provenance)
  • Click background → reset
  • Filter chips (node-type checkboxes) along the bottom
  • Dark background, glow effects, Obsidian aesthetic

Run:
    streamlit run scripts/graph_obsidian.py --server.port 8502

Prerequisite: the graph snapshot must exist at data/cache/graph_snapshot.json.
Run `python scripts/build_graph.py` first if it doesn't.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))

from semantic_graph.config import load_config  # noqa: E402
from semantic_graph.graph import load_cached_graph  # noqa: E402


# ─── Visual encoding ──────────────────────────────────────────


TIER_COLOR = {
    "human_asserted": "#22C55E",   # green
    "grounded":       "#06B6D4",   # cyan
    "inferred":       "#F59E0B",   # amber
    "guessed":        "#A1A1AA",   # gray
    "deprecated":     "#DC2626",   # red
}

NODE_RADIUS = {
    "Table":           14,
    "Entity":          11,
    "Metric":           8,
    "Synonym":          6,
    "User":             6,
    "Column":           4,
    "CodeMapping":      5,
    "FilterValue":      3,
    "DataQualityRule":  4,
}

EDGE_COLOR = {
    "CONTAINS":      "#3F3F46",   # subtle gray
    "EQUIVALENT_TO": "#0D9488",   # teal
    "COMPUTED_FROM": "#0891B2",   # cyan
    "SLICEABLE_BY":  "#5EEAD4",   # light teal
    "HAS_SYNONYM":   "#A78BFA",   # violet
    "QUERIED_BY":    "#71717A",   # mid gray
    "RESOLVED_BY":   "#7C3AED",   # purple
    "UPSTREAM_OF":   "#4338CA",   # indigo
    "VALIDATED_BY":  "#06B6D4",   # cyan
    "RELATES_TO":    "#A78BFA",   # violet
    "IDENTIFIES":    "#22C55E",   # green
    "HAS_ACCESS":    "#F59E0B",   # amber
    "ALWAYS_FILTER": "#7C3AED",   # purple
}

# Edges with directional arrowheads
ARROW_EDGES = {
    "UPSTREAM_OF", "COMPUTED_FROM", "VALIDATED_BY",
    "RESOLVED_BY", "QUERIED_BY", "IDENTIFIES",
}


def main() -> None:
    st.set_page_config(
        page_title="Synapse · Graph",
        page_icon="🧠",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    # Full-bleed dark theme for Obsidian aesthetic
    st.markdown(
        """
        <style>
        .stApp { background: #0a0a0b; }
        .block-container { padding: 0 !important; max-width: 100% !important; }
        header[data-testid="stHeader"] { background: transparent; height: 0; }
        [data-testid="stToolbar"] { display: none; }
        #MainMenu, footer { visibility: hidden; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    cfg = load_config()
    try:
        store = load_cached_graph(cfg)
    except RuntimeError as e:
        st.error(f"Graph not built yet: {e}")
        st.info("Run `python scripts/build_graph.py` first.")
        return

    nodes, links = _serialize(store)
    html = _build_html({"nodes": nodes, "links": links})
    components.html(html, height=920, scrolling=False)


# ─── Serialization ────────────────────────────────────────────


def _serialize(store) -> tuple[list[dict], list[dict]]:
    nodes: list[dict] = []
    for uri, node in store.nodes.items():
        sources = sorted(set(node.provenance.sources))
        # Description: prefer human description, fall back to ai-generated
        desc = (
            node.properties.get("description")
            or node.properties.get("ai_generated_description")
            or node.properties.get("business_name")
            or ""
        )
        nodes.append({
            "id": uri,
            "name": uri.rsplit("/", 1)[-1],
            "type": node.node_type,
            "tier": node.provenance.confidence_tier,
            "score": round(node.provenance.confidence_score, 3),
            "sources": sources,
            "color": TIER_COLOR.get(node.provenance.confidence_tier, "#71717A"),
            "size": NODE_RADIUS.get(node.node_type, 5),
            "table_name": node.properties.get("table_name", ""),
            "data_type": node.properties.get("data_type", ""),
            "is_pii": bool(node.properties.get("is_pii")),
            "pii_taxonomy": node.properties.get("pii_taxonomy") or "",
            "formula": node.properties.get("formula", ""),
            "description": desc[:240],
            "evidence_count": sum(node.provenance.evidence_count_by_source.values()),
            "conflicts": node.provenance.conflicts,
        })

    links: list[dict] = []
    for edge in store.edges.values():
        color = EDGE_COLOR.get(edge.edge_type, "#52525B")
        links.append({
            "source": edge.from_uri,
            "target": edge.to_uri,
            "type": edge.edge_type,
            "color": color,
            "opacity": max(0.2, round(edge.provenance.confidence_score, 2)),
            "arrow": edge.edge_type in ARROW_EDGES,
        })
    return nodes, links


# ─── HTML / force-graph ───────────────────────────────────────


def _build_html(data: dict) -> str:
    payload = json.dumps(data, default=str)
    return f"""
<!DOCTYPE html>
<html>
<head>
<style>
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 0; overflow: hidden;
    background: #0a0a0b;
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    color: #c9cace;
  }}
  #graph {{ width: 100vw; height: 100vh; }}

  /* Legend (top-right) */
  .legend {{
    position: fixed; top: 16px; right: 16px;
    background: rgba(20,21,23,0.92);
    border: 1px solid rgba(255,255,255,0.06);
    padding: 14px 16px;
    border-radius: 8px;
    font-size: 11px;
    backdrop-filter: blur(8px);
    z-index: 10;
  }}
  .legend .head {{
    font-family: 'JetBrains Mono', 'SF Mono', monospace;
    font-size: 9px; letter-spacing: 0.6px; text-transform: uppercase;
    color: #71717a; margin-bottom: 8px;
  }}
  .legend-row {{ display: flex; align-items: center; gap: 8px; margin: 5px 0; }}
  .legend-dot {{ width: 10px; height: 10px; border-radius: 50%; }}
  .legend-line {{ width: 16px; height: 2px; }}

  /* Side panel (left) */
  .panel {{
    position: fixed; top: 16px; left: 16px;
    background: rgba(16,17,19,0.96);
    border: 1px solid rgba(255,255,255,0.08);
    padding: 16px 18px;
    border-radius: 10px;
    max-width: 380px; min-width: 320px;
    max-height: calc(100vh - 100px);
    overflow-y: auto;
    font-size: 12px; line-height: 18px;
    display: none;
    backdrop-filter: blur(12px);
    z-index: 10;
  }}
  .panel.show {{ display: block; }}
  .panel h3 {{
    margin: 0 0 6px 0;
    font-family: 'JetBrains Mono', 'SF Mono', monospace;
    color: #f4f4f6; font-size: 14px; word-break: break-word;
  }}
  .panel .type-row {{
    display: flex; gap: 8px; flex-wrap: wrap;
    font-family: 'JetBrains Mono', 'SF Mono', monospace;
    font-size: 10px; color: #a1a1aa; margin-bottom: 12px;
  }}
  .panel .tier-chip {{
    padding: 2px 8px; border-radius: 4px;
    font-family: 'JetBrains Mono', 'SF Mono', monospace;
    font-size: 10px; font-weight: 540;
  }}
  .panel .meta {{ color: #a1a1aa; margin: 4px 0; }}
  .panel .meta code {{
    background: rgba(255,255,255,0.06);
    padding: 1px 5px; border-radius: 3px;
    font-family: 'JetBrains Mono', 'SF Mono', monospace;
    font-size: 11px; color: #d4d4d8;
  }}
  .panel .section {{
    margin-top: 14px;
    font-family: 'JetBrains Mono', 'SF Mono', monospace;
    font-size: 9px; letter-spacing: 0.6px; text-transform: uppercase;
    color: #71717a;
  }}
  .panel .desc {{
    margin-top: 6px; color: #e4e4e7;
    font-style: italic;
    border-left: 2px solid rgba(255,255,255,0.1);
    padding: 4px 8px;
  }}
  .panel .source-pill {{
    display: inline-block;
    padding: 2px 6px; margin: 2px 4px 2px 0;
    background: rgba(99, 102, 241, 0.18);
    border: 1px solid rgba(99,102,241,0.35);
    border-radius: 3px;
    font-family: 'JetBrains Mono', 'SF Mono', monospace;
    font-size: 10px;
    color: #c7d2fe;
  }}
  .panel .pii-flag {{
    display: inline-block;
    padding: 2px 6px;
    background: rgba(220, 38, 38, 0.2);
    border: 1px solid rgba(220, 38, 38, 0.4);
    color: #fca5a5;
    border-radius: 3px;
    font-family: 'JetBrains Mono', 'SF Mono', monospace;
    font-size: 10px;
  }}

  /* Filter chips (bottom) */
  .filter-bar {{
    position: fixed; bottom: 16px; left: 50%; transform: translateX(-50%);
    background: rgba(20,21,23,0.92);
    border: 1px solid rgba(255,255,255,0.06);
    padding: 8px 12px;
    border-radius: 8px;
    display: flex; gap: 6px; flex-wrap: wrap;
    max-width: 90vw;
    backdrop-filter: blur(8px);
    z-index: 10;
  }}
  .filter-chip {{
    font-family: 'JetBrains Mono', 'SF Mono', monospace;
    font-size: 10px;
    padding: 4px 10px;
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 4px;
    cursor: pointer;
    color: #71717a;
    user-select: none;
    transition: all 120ms ease;
  }}
  .filter-chip:hover {{
    border-color: rgba(255,255,255,0.25);
    color: #c9cace;
  }}
  .filter-chip.active {{
    background: rgba(99,102,241,0.15);
    border-color: rgba(99,102,241,0.5);
    color: #c7d2fe;
  }}

  /* Stats (bottom-right) */
  .stats {{
    position: fixed; bottom: 16px; right: 16px;
    font-family: 'JetBrains Mono', 'SF Mono', monospace;
    font-size: 10px;
    color: #71717a;
    background: rgba(20,21,23,0.92);
    border: 1px solid rgba(255,255,255,0.06);
    padding: 6px 12px;
    border-radius: 6px;
    backdrop-filter: blur(8px);
    z-index: 10;
  }}

  /* Brand (top-left, fixed) */
  .brand {{
    position: fixed; top: 16px; left: 16px;
    font-family: 'JetBrains Mono', 'SF Mono', monospace;
    font-weight: 620;
    font-size: 14px;
    color: #f4f4f6;
    background: rgba(20,21,23,0.92);
    border: 1px solid rgba(255,255,255,0.06);
    padding: 8px 14px;
    border-radius: 8px;
    backdrop-filter: blur(8px);
    z-index: 10;
  }}
  .brand .sub {{
    color: #71717a; font-weight: 400; font-size: 10px;
    margin-left: 6px; letter-spacing: 0.4px; text-transform: uppercase;
  }}
</style>
<script src="https://unpkg.com/force-graph@1.43.4"></script>
</head>
<body>

<div class="brand" id="brand">🧠 Synapse<span class="sub">graph</span></div>

<div id="graph"></div>

<div class="legend">
  <div class="head">Confidence tier</div>
  <div class="legend-row"><div class="legend-dot" style="background:#22C55E"></div>human_asserted</div>
  <div class="legend-row"><div class="legend-dot" style="background:#06B6D4"></div>grounded</div>
  <div class="legend-row"><div class="legend-dot" style="background:#F59E0B"></div>inferred</div>
  <div class="legend-row"><div class="legend-dot" style="background:#A1A1AA"></div>guessed</div>
  <div class="legend-row"><div class="legend-dot" style="background:#DC2626"></div>deprecated</div>
  <div class="head" style="margin-top:14px">Edge type</div>
  <div class="legend-row"><div class="legend-line" style="background:#3F3F46"></div>CONTAINS</div>
  <div class="legend-row"><div class="legend-line" style="background:#0D9488"></div>EQUIVALENT_TO</div>
  <div class="legend-row"><div class="legend-line" style="background:#0891B2"></div>COMPUTED_FROM</div>
  <div class="legend-row"><div class="legend-line" style="background:#A78BFA"></div>HAS_SYNONYM</div>
  <div class="legend-row"><div class="legend-line" style="background:#4338CA"></div>UPSTREAM_OF</div>
  <div class="legend-row"><div class="legend-line" style="background:#06B6D4"></div>VALIDATED_BY</div>
</div>

<div id="panel" class="panel"></div>

<div class="filter-bar" id="filterbar"></div>

<div class="stats" id="stats">loading…</div>

<script>
const data = {payload};
const allNodes = data.nodes;
const allLinks = data.links;
let activeTypes = new Set(allNodes.map(n => n.type));
let selectedNode = null;

function applyFilter() {{
  const visible = allNodes.filter(n => activeTypes.has(n.type));
  const visibleIds = new Set(visible.map(n => n.id));
  const links = allLinks.filter(l => {{
    const s = typeof l.source === 'object' ? l.source.id : l.source;
    const t = typeof l.target === 'object' ? l.target.id : l.target;
    return visibleIds.has(s) && visibleIds.has(t);
  }});
  graph.graphData({{ nodes: visible, links: links }});
  document.getElementById('stats').textContent =
    visible.length + ' / ' + allNodes.length + ' nodes  ·  ' +
    links.length + ' edges';
}}

function buildFilterBar() {{
  const counts = {{}};
  allNodes.forEach(n => {{ counts[n.type] = (counts[n.type]||0)+1; }});
  const types = Object.keys(counts).sort((a,b)=>counts[b]-counts[a]);
  const bar = document.getElementById('filterbar');
  types.forEach(t => {{
    const chip = document.createElement('div');
    chip.className = 'filter-chip active';
    chip.textContent = t + ' (' + counts[t] + ')';
    chip.onclick = () => {{
      if (activeTypes.has(t)) {{
        activeTypes.delete(t); chip.classList.remove('active');
      }} else {{
        activeTypes.add(t); chip.classList.add('active');
      }}
      applyFilter();
    }};
    bar.appendChild(chip);
  }});
}}

function showPanel(node) {{
  const panel = document.getElementById('panel');
  if (!node) {{ panel.classList.remove('show'); return; }}
  panel.classList.add('show');
  let html = '<h3>' + node.name + '</h3>';
  html += '<div class="type-row">';
  html += '<span>' + node.type + '</span>';
  html += '<span class="tier-chip" style="background:' + node.color + '22;border:1px solid ' + node.color + ';color:' + node.color + '">' + node.tier + ' · ' + node.score.toFixed(2) + '</span>';
  if (node.is_pii) html += '<span class="pii-flag">PII · ' + node.pii_taxonomy + '</span>';
  html += '</div>';
  if (node.table_name && node.type === 'Column') {{
    html += '<div class="meta">in table: <code>' + node.table_name + '</code></div>';
  }}
  if (node.data_type) {{
    html += '<div class="meta">type: <code>' + node.data_type + '</code></div>';
  }}
  if (node.formula) {{
    html += '<div class="meta">formula: <code>' + node.formula + '</code></div>';
  }}
  if (node.description) {{
    html += '<div class="section">Description</div>';
    html += '<div class="desc">' + node.description + '</div>';
  }}
  html += '<div class="section">Sources contributed (' + node.sources.length + ')</div>';
  html += '<div style="margin-top:6px">';
  node.sources.forEach(s => {{ html += '<span class="source-pill">' + s + '</span>'; }});
  html += '</div>';
  html += '<div class="meta" style="margin-top:10px">evidence events: <code>' + node.evidence_count + '</code></div>';
  if (node.conflicts && node.conflicts.length > 0) {{
    html += '<div class="section">⚠ Conflicts</div>';
    node.conflicts.forEach(c => {{ html += '<div class="meta" style="color:#fca5a5">' + c + '</div>'; }});
  }}
  panel.innerHTML = html;
}}

function focusOnNode(node) {{
  if (!node) {{
    selectedNode = null;
    graph
      .nodeColor(n => n.color)
      .linkColor(l => l.color)
      .linkWidth(l => 0.5 + (l.opacity || 0.5) * 1.5);
    return;
  }}
  selectedNode = node;
  const neighbors = new Set([node.id]);
  allLinks.forEach(l => {{
    const s = typeof l.source === 'object' ? l.source.id : l.source;
    const t = typeof l.target === 'object' ? l.target.id : l.target;
    if (s === node.id) neighbors.add(t);
    if (t === node.id) neighbors.add(s);
  }});
  graph
    .nodeColor(n => neighbors.has(n.id) ? n.color : 'rgba(70,70,75,0.3)')
    .linkColor(l => {{
      const s = typeof l.source === 'object' ? l.source.id : l.source;
      const t = typeof l.target === 'object' ? l.target.id : l.target;
      return (s === node.id || t === node.id) ? l.color : 'rgba(70,70,75,0.15)';
    }})
    .linkWidth(l => {{
      const s = typeof l.source === 'object' ? l.source.id : l.source;
      const t = typeof l.target === 'object' ? l.target.id : l.target;
      return (s === node.id || t === node.id) ? 2.5 : 0.3;
    }});
}}

const graph = ForceGraph()(document.getElementById('graph'))
  .graphData({{ nodes: allNodes, links: allLinks }})
  .backgroundColor('#0a0a0b')
  .nodeVal(n => n.size)
  .nodeColor(n => n.color)
  .nodeLabel(n =>
    '<div style="background:rgba(20,21,23,0.95);padding:6px 10px;border-radius:4px;' +
    'font-family:JetBrains Mono,monospace;font-size:11px;color:#e4e4e7;' +
    'border:1px solid ' + n.color + '">' + n.name +
    '<br/><span style="color:#71717a;font-size:9px">' + n.type + ' · ' + n.tier + '</span></div>'
  )
  .nodeCanvasObjectMode(() => 'after')
  .nodeCanvasObject((node, ctx, globalScale) => {{
    // Glow effect
    const r = Math.sqrt(node.size) * 2;
    ctx.shadowColor = node.color;
    ctx.shadowBlur = 8;
    ctx.beginPath();
    ctx.arc(node.x, node.y, r, 0, 2 * Math.PI, false);
    ctx.fillStyle = node.color;
    ctx.fill();
    ctx.shadowBlur = 0;
    // Label for large nodes (Tables, Entities) at zoom > 1.5
    if (globalScale > 1.5 && (node.type === 'Table' || node.type === 'Entity')) {{
      ctx.font = '10px JetBrains Mono';
      ctx.fillStyle = '#e4e4e7';
      ctx.textAlign = 'center';
      ctx.fillText(node.name, node.x, node.y + r + 12);
    }}
  }})
  .linkColor(l => l.color)
  .linkWidth(l => 0.5 + (l.opacity || 0.5) * 1.5)
  .linkDirectionalArrowLength(l => l.arrow ? 4 : 0)
  .linkDirectionalArrowRelPos(0.95)
  .linkDirectionalArrowColor(l => l.color)
  .onNodeHover(n => {{
    document.body.style.cursor = n ? 'pointer' : 'default';
    if (!selectedNode) showPanel(n);
  }})
  .onNodeClick(n => {{
    showPanel(n);
    focusOnNode(n);
  }})
  .onBackgroundClick(() => {{
    selectedNode = null;
    showPanel(null);
    focusOnNode(null);
  }})
  .cooldownTicks(200)
  .d3AlphaDecay(0.015)
  .d3VelocityDecay(0.4)
  .warmupTicks(20);

// Tune the force simulation for cleaner layouts
graph.d3Force('charge').strength(-180).distanceMax(300);
graph.d3Force('link').distance(40);

buildFilterBar();
applyFilter();

const resize = () => {{ graph.width(window.innerWidth).height(window.innerHeight); }};
window.addEventListener('resize', resize);
resize();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
