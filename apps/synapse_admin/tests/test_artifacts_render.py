"""The artifact rendering module on both surfaces: one file, identical,
imported by each chat page; its number-format twin answers the shared
cases exactly as the Python side does; it draws every kind the
validator knows, with axes, gridlines, legends and tooltips; the table's
Show all toggle keeps its state on the wrapper and the strip under a
number wraps instead of clipping — pinned in the CSS block both
stylesheets carry."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SILO = REPO_ROOT / "synapse-agentic-harness-system"
SURFACES = {"admin": REPO_ROOT / "apps" / "synapse_admin" / "frontend",
            "synapse": REPO_ROOT / "apps" / "synapse" / "frontend"}
MODULES = {name: root / "js" / "artifacts-render.js"
           for name, root in SURFACES.items()}
CHECK = Path(__file__).resolve().parent / "artifacts_render_check.mjs"
CASES = SILO / "tests" / "fixtures" / "number_format_cases.json"
BLOCK_START = "/* ══ artifacts-render:"
BLOCK_END = "/* ══ end artifacts-render ══ */"
sys.path.insert(0, str(SILO))


def _block(css: str) -> str:
    assert css.count(BLOCK_START) == 1 and css.count(BLOCK_END) == 1
    return css.split(BLOCK_START)[1].split(BLOCK_END)[0]


def test_the_render_module_exists_on_both_surfaces_and_is_identical():
    texts = {name: path.read_text(encoding="utf-8")
             for name, path in MODULES.items()}
    assert texts["admin"] == texts["synapse"]
    for name, root in SURFACES.items():
        chat = (root / "js" / "pages" / "chat.js").read_text(encoding="utf-8")
        assert 'import { createArtifactRenderer } from "../artifacts-render.js";' \
            in chat, name
        assert "createArtifactRenderer({" in chat
        # the words under a number stay the surface's own, as a hook
        assert "meridian: (prov) =>" in chat
        # the page keeps none of the drawing: the module owns it
        for gone in ("function chartSVG", "function tableReport",
                     "function kpiTile", "function diagramSVG",
                     "const PALETTE"):
            assert gone not in chat, (name, gone)
    # the module's kind list is the validator's
    from sahs.assistant.artifacts import CHART_KINDS
    listed = re.search(r"export const CHART_KINDS = \[(.*?)\];",
                       texts["admin"], re.S).group(1)
    assert re.findall(r'"(\w+)"', listed) == list(CHART_KINDS)
    # the CSS block rides both stylesheets, identical, and both chat
    # pages parse
    blocks = {name: _block((root / "styles" / "app.css")
                           .read_text(encoding="utf-8"))
              for name, root in SURFACES.items()}
    assert blocks["admin"] == blocks["synapse"]


@pytest.mark.skipif(shutil.which("node") is None, reason="node missing")
def test_every_changed_js_file_parses():
    for name, root in SURFACES.items():
        for rel in ("js/artifacts-render.js", "js/pages/chat.js"):
            done = subprocess.run(["node", "--check", str(root / rel)],
                                  capture_output=True, text=True)
            assert done.returncode == 0, (name, rel, done.stderr)


@pytest.fixture(scope="module")
def rendered(tmp_path_factory):
    if shutil.which("node") is None:
        pytest.skip("node missing")
    from sahs.assistant.artifacts import EXAMPLES, validate_artifact
    prov = {"status": "exploratory", "meridian_line": "a look, not a claim"}
    specs = {"charts": {}, "bodies": {}}
    for kind, example in EXAMPLES.items():
        spec, problems = validate_artifact("chart", example, build_id="b1")
        assert problems == [], (kind, problems)
        specs["charts"][kind] = spec
    table, problems = validate_artifact("table", {
        "columns": [{"key": "day", "label": "Day"},
                    {"key": "spend", "label": "Spend", "unit": "USD"},
                    {"key": "conversion_rate", "label": "Conversion"},
                    {"key": "orders", "label": "Orders"},
                    {"key": "merchant_id", "label": "Merchant"}],
        "rows": [{"day": f"2026-01-{i + 1:02d}", "spend": 1234.5 * (i + 1),
                  "conversion_rate": 0.125 + i / 100,
                  "orders": 100 * (i + 1), "merchant_id": 1000 + i}
                 for i in range(60)],
        "provenance": prov}, build_id="b1")
    assert problems == []
    specs["bodies"]["table"] = table
    kpi, problems = validate_artifact("kpi", {
        "value": 1234567.8, "unit": "USD", "label": "Net spend",
        "delta": -0.042, "delta_format": "percent",
        "compare_label": "vs last month", "provenance": prov},
        build_id="b1")
    assert problems == []
    specs["bodies"]["kpi"] = kpi
    dash, problems = validate_artifact("dashboard", {
        "grid": 3, "panels": [
            {"type": "kpi", "title": "Spend", "spec": {
                "value": 812000, "unit": "USD", "provenance": prov}},
            {"type": "chart", "title": "By country", "span": 2,
             "spec": EXAMPLES["bar"]},
            {"type": "chart", "title": "Heat", "spec": EXAMPLES["heatmap"]},
            {"type": "table", "title": "Rows", "spec": {
                "columns": [{"key": "a", "label": "A"}],
                "rows": [{"a": i} for i in range(30)],
                "provenance": prov}}],
        "filters": [{"slot": "country", "options": ["US", "CA"]}],
        "notes": "one line"}, build_id="b1")
    assert problems == []
    specs["bodies"]["dashboard"] = dash
    tmp = tmp_path_factory.mktemp("render")
    (tmp / "specs.json").write_text(json.dumps(specs), encoding="utf-8")
    out = {}
    for name, module in MODULES.items():
        done = subprocess.run(
            ["node", str(CHECK), str(module), str(CASES),
             str(tmp / "specs.json")], capture_output=True, text=True)
        assert done.returncode == 0, (name, done.stderr[-2000:])
        out[name] = json.loads(done.stdout)
    return out


def test_the_js_twin_answers_the_shared_cases_like_python(rendered):
    cases = json.loads(CASES.read_text(encoding="utf-8"))
    for name, got in rendered.items():
        assert len(got["cases"]) == len(cases)
        for case, answer in zip(cases, got["cases"]):
            assert answer["format"] == case["expect"], (name, case["name"])
            assert answer["formatted"] == case["formatted"], \
                (name, case["name"])


def test_every_kind_draws_with_axes_grid_legend_and_tooltips(rendered):
    from sahs.assistant.artifacts import CHART_KINDS
    for name, got in rendered.items():
        assert got["kinds"] == list(CHART_KINDS)
        assert set(got["charts"]) == set(CHART_KINDS)
        for kind, chart in got["charts"].items():
            assert chart["svg"] and chart["clean"], (name, kind)
            assert chart["tooltips"] >= 1, (name, kind)
            if kind != "heatmap":
                assert chart["grid"], (name, kind)
        # the reference line, the legend for many series, the reason
        # as the picture's own title
        assert got["charts"]["line"]["reference"]
        assert got["charts"]["line"]["legend"]
        assert got["charts"]["line"]["reason"]
        assert got["charts"]["stacked_bar"]["legend"]
        assert not got["charts"]["area"]["legend"]        # one series


def test_tables_tiles_and_dashboards_format_from_the_data(rendered):
    for name, got in rendered.items():
        table = got["bodies"]["table"]["html"]
        assert got["bodies"]["table"]["clean"]
        cells = re.findall(r'<td class="num"[^>]*>([^<]*)</td>', table)
        assert cells[:4] == ["$1,234.5", "12.5%", "100", "$2,469.0"], name
        # an identifier is text, not a grouped number
        assert 'class="text">1000</td>' in table
        assert 'class="text">1,0' not in table
        # the first fifty shown, the rest hidden behind a stateful toggle
        assert table.count("<tr hidden>") == 10
        assert 'data-limit="50" data-rows="60" data-state="some"' in table
        assert "Showing the first 50 of 60 rows" in table
        assert 'class="btn table-all" aria-expanded="false">Show all' in table
        assert "Prepared by test." in table          # the strip, the hook
        kpi = got["bodies"]["kpi"]["html"]
        assert ">$1.2M<" in kpi and 'title="$1,234,567.8"' in kpi
        assert "▼ 4.2%" in kpi and "vs last month" in kpi
        dash = got["bodies"]["dashboard"]["html"]
        assert got["bodies"]["dashboard"]["clean"]
        assert '<div class="dash-grid fixed" style="--cols:3">' in dash
        assert dash.count("--span:2") == 3 and dash.count("--span:1") == 1
        assert dash.count('class="tile-footer"') == 4
        assert "filter-opt active" in dash and ">$812K<" in dash


def test_show_all_keeps_its_state_on_the_wrapper():
    """The owner's bug: Show all did nothing visible. The old binder kept
    the state in the button's words and flipped row attributes inside a
    scroll box that already overflowed, so nothing changed on screen.
    Now the wrapper carries data-state, the rows, the count line, the
    button's words and aria-expanded all follow it, the box lifts its
    height cap, and the first revealed row scrolls into view."""
    module = MODULES["admin"].read_text(encoding="utf-8")
    binder = module.split("function bindTable(container)")[1].split(
        "function animateNumbers")[0]
    assert 'btn.textContent === "Show all"' not in binder
    assert 'wrap.dataset.state === "all"' in binder
    assert 'wrap.dataset.state = all ? "all" : "some"' in binder
    assert 'wrap.classList.toggle("all", all)' in binder
    assert 'btn.setAttribute("aria-expanded", String(all))' in binder
    assert "tr.hidden = !all && i >= limit" in binder
    assert "count.textContent = all ? `All ${totalText} rows`" in binder
    assert 'scrollIntoView({ block: "nearest" })' in binder
    assert "apply();" in binder
    for root in SURFACES.values():
        css = _block((root / "styles" / "app.css").read_text(encoding="utf-8"))
        assert ".tablev3.all { max-height: none; }" in css


def test_the_strip_under_a_number_wraps_instead_of_clipping():
    """The owner's other bug: the disclaimer under a chart or a dashboard
    tile was cut off. The stylesheet's artifacts block, last in both
    files, lets the strip wrap and the footer flow onto more lines."""
    for name, root in SURFACES.items():
        css = (root / "styles" / "app.css").read_text(encoding="utf-8")
        block = _block(css)
        rule = re.search(r"\.tile-footer \.meridian, \.artifact-footer "
                         r"\.meridian \{(.*?)\}", block, re.S).group(1)
        for piece in ("white-space: normal", "overflow: visible",
                      "text-overflow: clip", "overflow-wrap: anywhere",
                      "flex: 1 1 100%"):
            assert piece in rule, (name, piece)
        assert ".tile-footer { flex-wrap: wrap;" in block
        # the block comes after the older nowrap rule, so it wins
        assert css.index("white-space: nowrap;\n  color: var(--muted") \
            < css.index(BLOCK_START), name
        # the strip is the last thing in the file
        assert css.rstrip().endswith(BLOCK_END), name
        # themes and motion: tokens, no second copy; reduced motion stops
        # every mark
        assert "var(--line," in block and "var(--accent," in block
        assert "@media (prefers-reduced-motion: reduce)" in block
        assert ".dash-grid.fixed { grid-template-columns: repeat(var(--cols" \
            in block
