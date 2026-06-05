"""Graph visualization — pyvis renderers for the Synapse UI's Graph tab.

Three lenses (per UX_VISUAL_SYSTEM design):

    neighborhood_html(store, center_uri, hops=2)
        Force-directed view: center node at middle, all 1–2 hop
        neighbors fanning out. Color by node type, edge style by
        edge family, opacity by confidence.

    lineage_dag_html(store, center_uri, depth=3)
        Hierarchical left-to-right DAG: upstream tables on the left,
        center table in the middle, downstream tables on the right.

    ego_html(store, center_uri)
        Single-hop star around any node — works for Column / Metric /
        Synonym / etc., not just Tables.

    consumption_flow_html()
        Static sankey-like diagram: 10 sources → graph → consumers
        (Streamlit UI, MCP tool, agentic SQL gen, governance, lineage).

Output: ready-to-embed HTML string. The UI hands it to
`st.components.v1.html(..., height=...)`.
"""

from __future__ import annotations

from collections import deque

from pyvis.network import Network

from synapse.graph.store import GraphStore


# ─── Visual encoding (matches UX_VISUAL_SYSTEM tokens) ──────


# Node-type → (shape, color, size)
# pyvis shapes: dot, square, triangle, triangleDown, star, diamond,
# hexagon, ellipse, box, database, image
_NODE_STYLE: dict[str, tuple[str, str, int]] = {
    "Table":           ("box",      "#4338CA", 32),  # accent indigo
    "Column":          ("dot",      "#71717A", 14),  # neutral
    "Metric":          ("diamond",  "#0891B2", 20),  # runtime teal
    "Entity":          ("hexagon",  "#7C3AED", 22),  # catalog violet
    "Synonym":         ("ellipse",  "#A78BFA", 16),  # lighter violet
    "User":            ("dot",      "#A1A1AA", 10),  # muted
    "CodeMapping":     ("triangle", "#7C3AED", 14),  # catalog
    "FilterValue":     ("dot",      "#0D9488", 10),  # corpus
    "DataQualityRule": ("star",     "#06B6D4", 14),  # runtime cyan
}

# Edge-type → (color, dashes, width, arrows)
_EDGE_STYLE: dict[str, dict] = {
    "CONTAINS":      {"color": "#D4D4D8", "dashes": False, "width": 1,    "arrows": ""},
    "IDENTIFIES":    {"color": "#7C3AED", "dashes": False, "width": 1.5,  "arrows": "to"},
    "RELATES_TO":    {"color": "#7C3AED", "dashes": True,  "width": 1.5,  "arrows": "to"},
    "EQUIVALENT_TO": {"color": "#0D9488", "dashes": True,  "width": 1.5,  "arrows": ""},
    "COMPUTED_FROM": {"color": "#0891B2", "dashes": False, "width": 1.5,  "arrows": "to"},
    "SLICEABLE_BY":  {"color": "#0D9488", "dashes": True,  "width": 1,    "arrows": ""},
    "QUERIED_BY":    {"color": "#A1A1AA", "dashes": False, "width": 1,    "arrows": "to"},
    "RESOLVED_BY":   {"color": "#7C3AED", "dashes": True,  "width": 1,    "arrows": "to"},
    "UPSTREAM_OF":   {"color": "#4338CA", "dashes": False, "width": 2.5,  "arrows": "to"},
    "VALIDATED_BY":  {"color": "#06B6D4", "dashes": True,  "width": 1,    "arrows": "to"},
    "HAS_SYNONYM":   {"color": "#A78BFA", "dashes": False, "width": 1,    "arrows": ""},
    "ALWAYS_FILTER": {"color": "#71717A", "dashes": True,  "width": 1,    "arrows": ""},
    "LOOKUP_TO":     {"color": "#7C3AED", "dashes": True,  "width": 1,    "arrows": "to"},
}

# Confidence-tier → node opacity (0–1)
_TIER_OPACITY = {
    "deprecated":     0.30,
    "guessed":        0.55,
    "inferred":       0.75,
    "grounded":       0.95,
    "human_asserted": 1.00,
}


def _short_label(uri: str) -> str:
    """Last segment of the URI, suitable for a node label."""
    return uri.rsplit("/", 1)[-1]


def _node_tooltip(node) -> str:
    """Rich HTML tooltip — pyvis renders this on hover."""
    p = node.properties
    prov = node.provenance
    title_lines = [
        f"<b>{node.node_type}</b>: {_short_label(node.canonical_uri)}",
        f"confidence: {prov.confidence_tier} ({prov.confidence_score:.2f})",
        f"sources: {', '.join(prov.sources)}",
    ]
    if node.node_type == "Column":
        if p.get("data_type"):    title_lines.append(f"type: {p['data_type']}")
        if p.get("is_pii"):       title_lines.append("PII: yes")
        if p.get("description"):
            d = p["description"][:80]
            title_lines.append(f"desc: {d}")
    if node.node_type == "Table":
        if p.get("business_name"): title_lines.append(f"name: {p['business_name']}")
        if p.get("row_count"):     title_lines.append(f"rows: {p['row_count']:,}")
    if node.node_type == "Metric":
        if p.get("formula"):       title_lines.append(f"formula: {p['formula']}")
    return "<br/>".join(title_lines)


def _add_node(net: Network, node) -> None:
    shape, color, size = _NODE_STYLE.get(node.node_type, ("dot", "#71717A", 12))
    opacity = _TIER_OPACITY.get(node.provenance.confidence_tier, 0.7)
    # Encode opacity by mixing color with white (pyvis doesn't expose alpha)
    fill = _mix_white(color, opacity)
    net.add_node(
        node.canonical_uri,
        label=_short_label(node.canonical_uri),
        title=_node_tooltip(node),
        shape=shape,
        color={"background": fill, "border": color},
        size=size,
        font={
            "face": "JetBrains Mono, Menlo, monospace",
            "size": 12,
            "color": "#18181B",
        },
    )


def _add_edge(net: Network, edge) -> None:
    style = _EDGE_STYLE.get(
        edge.edge_type,
        {"color": "#D4D4D8", "dashes": False, "width": 1, "arrows": ""},
    )
    net.add_edge(
        edge.from_uri, edge.to_uri,
        color=style["color"],
        width=style["width"],
        dashes=style["dashes"],
        arrows=style["arrows"],
        title=edge.edge_type,
    )


def _mix_white(hex_color: str, alpha: float) -> str:
    """Mix a hex color with white to fake opacity. alpha in [0,1]."""
    hex_color = hex_color.lstrip("#")
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    r2 = int(r * alpha + 255 * (1 - alpha))
    g2 = int(g * alpha + 255 * (1 - alpha))
    b2 = int(b * alpha + 255 * (1 - alpha))
    return f"#{r2:02X}{g2:02X}{b2:02X}"


def _new_network(*, height: int = 560, physics: bool = True,
                 hierarchical: bool = False) -> Network:
    net = Network(
        height=f"{height}px",
        width="100%",
        bgcolor="#FFFFFF",
        font_color="#18181B",
        notebook=False,
        directed=True,
        cdn_resources="remote",
    )
    options: dict = {
        "interaction": {
            "hover": True,
            "tooltipDelay": 80,
            "navigationButtons": True,
            "keyboard": True,
        },
        "edges": {
            "smooth": {"type": "cubicBezier", "roundness": 0.4},
        },
        "nodes": {
            "borderWidth": 1.5,
            "borderWidthSelected": 3,
            "shadow": False,
        },
    }
    if hierarchical:
        options["layout"] = {
            "hierarchical": {
                "enabled": True,
                "direction": "LR",
                "sortMethod": "directed",
                "levelSeparation": 220,
                "nodeSpacing": 140,
                "treeSpacing": 200,
            },
        }
        options["physics"] = {"enabled": False}
    elif not physics:
        options["physics"] = {"enabled": False}
    else:
        options["physics"] = {
            "enabled": True,
            "solver": "forceAtlas2Based",
            "forceAtlas2Based": {
                "gravitationalConstant": -50,
                "centralGravity": 0.01,
                "springLength": 110,
                "springConstant": 0.08,
                "damping": 0.5,
            },
            "stabilization": {"iterations": 200},
        }
    import json as _json
    net.set_options(_json.dumps(options))
    return net


# ─── Public renderers ───────────────────────────────────────


def neighborhood_html(
    store: GraphStore, center_uri: str,
    *, hops: int = 2, max_nodes: int = 80, height: int = 560,
) -> str:
    """BFS `hops` levels around `center_uri`. Capped at `max_nodes`."""
    visited: set[str] = set()
    queue: deque[tuple[str, int]] = deque([(center_uri, 0)])
    while queue and len(visited) < max_nodes:
        uri, depth = queue.popleft()
        if uri in visited:
            continue
        visited.add(uri)
        if depth >= hops:
            continue
        for e in store.outgoing(uri):
            queue.append((e.to_uri, depth + 1))
        for e in store.incoming(uri):
            queue.append((e.from_uri, depth + 1))

    net = _new_network(height=height, physics=True)
    # Add nodes first
    for uri in visited:
        node = store.get(uri)
        if node is None:
            continue
        _add_node(net, node)
    # Highlight the center
    if center_uri in visited:
        net.get_node(center_uri)["borderWidth"] = 4
        net.get_node(center_uri)["size"] = (
            _NODE_STYLE.get(store.get(center_uri).node_type, (None, None, 30))[2] + 12
        )
    # Edges that lie inside the visited subgraph
    for e in store.edges.values():
        if e.from_uri in visited and e.to_uri in visited:
            _add_edge(net, e)
    return net.generate_html(notebook=False)


def lineage_dag_html(
    store: GraphStore, center_uri: str,
    *, depth: int = 3, height: int = 480,
) -> str:
    """Upstream + downstream chains along UPSTREAM_OF edges, hierarchical L→R."""
    visited: set[str] = {center_uri}
    # Walk upstream
    frontier = {center_uri}
    for _ in range(depth):
        nxt = set()
        for uri in frontier:
            for e in store.incoming(uri, "UPSTREAM_OF"):
                if e.from_uri not in visited:
                    nxt.add(e.from_uri)
                    visited.add(e.from_uri)
        frontier = nxt
    # Walk downstream
    frontier = {center_uri}
    for _ in range(depth):
        nxt = set()
        for uri in frontier:
            for e in store.outgoing(uri, "UPSTREAM_OF"):
                if e.to_uri not in visited:
                    nxt.add(e.to_uri)
                    visited.add(e.to_uri)
        frontier = nxt

    net = _new_network(height=height, hierarchical=True)
    for uri in visited:
        node = store.get(uri)
        if node:
            _add_node(net, node)
    if center_uri in visited:
        net.get_node(center_uri)["borderWidth"] = 4
        net.get_node(center_uri)["color"] = {
            "background": "#EEF0FF", "border": "#4338CA",
        }
    for e in store.edges.values():
        if (e.edge_type == "UPSTREAM_OF"
                and e.from_uri in visited and e.to_uri in visited):
            _add_edge(net, e)
    return net.generate_html(notebook=False)


def ego_html(
    store: GraphStore, center_uri: str,
    *, height: int = 480,
) -> str:
    """1-hop star around any node."""
    return neighborhood_html(
        store, center_uri, hops=1, max_nodes=60, height=height,
    )


# ─── How this is consumed (static flow) ─────────────────────


def consumption_flow_html(*, height: int = 380) -> str:
    """Static SVG-as-HTML showing 10 sources → graph → consumers.

    Sources (left), Synapse graph (center, the product), consumers (right).
    Pure HTML/CSS — no JS — so it renders instantly under
    `st.components.v1.html`."""
    return f"""
<style>
  .syn-flow {{
    font-family: 'JetBrains Mono', Menlo, monospace;
    background: #FAFAFA; padding: 24px; border-radius: 8px;
    border: 1px solid #E8E8EB; min-height: {height}px;
  }}
  .syn-flow .row {{ display: grid; grid-template-columns: 1fr auto 1fr; gap: 24px; align-items: center; }}
  .syn-flow .col {{ display: flex; flex-direction: column; gap: 6px; }}
  .syn-flow .pill {{
    padding: 6px 10px; border-radius: 4px; font-size: 11px; line-height: 14px;
    background: #FFFFFF; box-shadow: 0 0 0 1px #E8E8EB;
    display: flex; align-items: center; gap: 6px;
  }}
  .syn-flow .pill .dot {{ width: 6px; height: 6px; border-radius: 50%; }}
  .syn-flow .pill[data-fam="catalog"] .dot {{ background: #7C3AED; }}
  .syn-flow .pill[data-fam="runtime"] .dot {{ background: #0891B2; }}
  .syn-flow .pill[data-fam="corpus"]  .dot {{ background: #0D9488; }}
  .syn-flow .pill[data-fam="ai"]      .dot {{ background: #DB2777; }}
  .syn-flow .hub {{
    background: #4338CA; color: white; padding: 20px 18px;
    border-radius: 8px; text-align: center; min-width: 180px;
    box-shadow: 0 4px 16px rgba(67,56,202,0.18);
  }}
  .syn-flow .hub .name {{ font-weight: 620; font-size: 14px; letter-spacing: 0.4px; }}
  .syn-flow .hub .sub {{ font-size: 10px; opacity: 0.9; margin-top: 4px; line-height: 14px; }}
  .syn-flow .hub .meta {{ font-size: 10px; opacity: 0.75; margin-top: 8px; padding-top: 8px; border-top: 1px solid rgba(255,255,255,0.2); }}
  .syn-flow .arrow {{ text-align: center; color: #71717A; font-size: 12px; padding: 4px 0; }}
  .syn-flow h4 {{ font-size: 10px; letter-spacing: 0.6px; text-transform: uppercase; color: #71717A; margin: 0 0 8px 0; font-weight: 540; }}
</style>
<div class='syn-flow'>
  <div class='row'>
    <div>
      <h4>10 sources (ingest)</h4>
      <div class='col'>
        <div class='pill' data-fam='catalog'><span class='dot'></span>mdm — canonical metadata</div>
        <div class='pill' data-fam='catalog'><span class='dot'></span>table_catalog — directory</div>
        <div class='pill' data-fam='catalog'><span class='dot'></span>metric_catalog — definitions</div>
        <div class='pill' data-fam='catalog'><span class='dot'></span>glossary — vocabulary</div>
        <div class='pill' data-fam='catalog'><span class='dot'></span>baseline_lookml — dim model</div>
        <div class='pill' data-fam='runtime'><span class='dot'></span>bq — INFORMATION_SCHEMA + profile</div>
        <div class='pill' data-fam='runtime'><span class='dot'></span>dq_engine — Auto-DQ checks</div>
        <div class='pill' data-fam='corpus'><span class='dot'></span>corpus — gold SQL extraction</div>
        <div class='pill' data-fam='corpus'><span class='dot'></span>usage — query telemetry</div>
        <div class='pill' data-fam='ai'><span class='dot'></span>llm_generated — AI descriptions</div>
      </div>
    </div>
    <div>
      <div class='arrow'>→</div>
      <div class='hub'>
        <div class='name'>SYNAPSE GRAPH</div>
        <div class='sub'>typed nodes · per-fact provenance · calibrated confidence</div>
        <div class='meta'>Table · Column · Metric · Entity · Synonym · CodeMapping · FilterValue · User · DataQualityRule</div>
      </div>
      <div class='arrow'>→</div>
    </div>
    <div>
      <h4>Consumers</h4>
      <div class='col'>
        <div class='pill'><b>Streamlit UI</b> — the X-ray you're using</div>
        <div class='pill'><b>MCP tool surface</b> — agents call inspect_table()</div>
        <div class='pill'><b>NL→BQ SQL agent</b> — graph context + Gemini</div>
        <div class='pill'><b>LookML renderer</b> — emits .view + .model.lkml</div>
        <div class='pill'><b>Governance</b> — PII roles, owners, lineage</div>
        <div class='pill'><b>DQ monitoring</b> — rule status feed</div>
        <div class='pill'><b>Validation harness</b> — promotes/demotes claims</div>
        <div class='pill'><b>Radix (optional)</b> — secondary retrieval signal</div>
      </div>
    </div>
  </div>
</div>
"""
