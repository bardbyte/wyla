"""Visualization layer — typed chart specs rendered to themed, dependency-
free HTML the agent can hand to any surface (chat panel, artifact, email).

The spec is the contract: the agent *describes* the visualization
(kind, series, format, audience framing); rendering rules — validated
palette, one axis, ≤4 series, direct labels, thin marks — are enforced in
code so the model cannot produce an off-system chart even if it tries.
"""

from synapse.viz.chartspec import (
    BarChart,
    Dashboard,
    DashboardItem,
    DataTable,
    LineChart,
    Series,
    StatTile,
    parse_spec,
)
from synapse.viz.render import render_page, render_spec

__all__ = [
    "BarChart", "Dashboard", "DashboardItem", "DataTable", "LineChart",
    "Series", "StatTile", "parse_spec", "render_page", "render_spec",
]
