"""The composer's two knobs on both surfaces: a Thinking-effort pill
that opens a slider with one stop per depth, and a Model pill that opens
the catalog grouped by plane. Both keep their value in the hidden select
the send path already reads, so nothing downstream changed. The stops
and the models come from the dials catalog; the ladder has five stops."""

from __future__ import annotations

import sys
from pathlib import Path

from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parents[3]
ADMIN = REPO / "apps" / "synapse_admin" / "frontend"
SYNAPSE = REPO / "apps" / "synapse" / "frontend"
sys.path.insert(0, str(REPO / "apps" / "synapse_admin" / "tests"))
from test_synapse_surface import client, compiled  # noqa: E402,F401


def read(root: Path, rel: str) -> str:
    return (root / rel).read_text(encoding="utf-8")


def test_both_composers_carry_the_pills_over_hidden_selects():
    for root, who in ((ADMIN, "Synapse"), (SYNAPSE, "Radix")):
        chat = read(root, "js/pages/chat.js")
        assert 'import { mountDepthKnob, mountModelPicker } from "../knobs.js";' in chat
        # the hidden selects stay the state the send path reads
        assert '<select id="chat-model" class="chat-depth chat-plane" hidden' in chat
        assert '<select id="chat-depth" class="chat-depth" hidden' in chat
        for stop in ("minimal", "quick", "standard", "deep", "max"):
            assert f'<option value="{stop}"' in chat
        assert 'el("chat-depth").value' in chat                    # the send path, unchanged
        # the pills and their panels
        assert 'id="chat-model-btn"' in chat and 'id="chat-model-pop"' in chat
        assert 'id="chat-depth-btn"' in chat and 'id="chat-depth-pop"' in chat
        assert '<span class="pill-label">Thinking effort</span>' in chat
        assert f'title="How deeply {who} thinks on this ask"' in chat
        # fed from the dials catalog, and the switch remembered on the chat
        assert "modelKnob.setModels(models)" in chat and "depthKnob.setDepths(dials.depths || [])" in chat
        assert "api.chatSetModel(state.session.id, wanted)" in chat
        assert "modelKnob.refresh()" in chat
        assert "state.plane = boot.choice || boot.plane" in chat


def test_the_knobs_module_is_the_same_on_both_surfaces_and_does_what_it_says():
    admin = read(ADMIN, "js/knobs.js")
    assert admin == read(SYNAPSE, "js/knobs.js")
    assert "export function mountDepthKnob(" in admin and "export function mountModelPicker(" in admin
    # the slider: a native range over a rail with one dot per stop, the
    # title names the stop and opens what it means
    for piece in ('type="range" class="knob-range"', 'class="knob-fill"', 'class="knob-dots"',
                  'class="knob-title"', 'class="knob-means" hidden',
                  'select.dispatchEvent(new Event("change", { bubbles: true }))',
                  "aria-valuetext"):
        assert piece in admin, piece
    # the model list: grouped by plane, one marked, the unavailable greyed with the reason
    for piece in ('class="model-group-head"', 'class="model-row', 'role="option"',
                  'm.available ? "" : "disabled"', "m.available ? m.means : m.reason"):
        assert piece in admin, piece
    # one open panel at a time; a click outside or Escape closes it
    assert 'e.key === "Escape"' in admin and "closeAll(pop)" in admin


def test_the_styles_draw_the_pill_the_rail_and_the_list():
    for root in (ADMIN, SYNAPSE):
        css = read(root, "styles/app.css")
        for cls in (".chat-pill", ".knob-pop", ".knob-pop.below", ".knob-title", ".knob-track",
                    ".knob-rail", ".knob-fill", ".knob-dots i.on", ".knob-range::-webkit-slider-thumb",
                    ".knob-range::-moz-range-thumb", ".model-row", ".model-group-head"):
            assert cls in css, (root.name, cls)


def test_the_second_surface_has_the_switch_in_its_api():
    api = read(SYNAPSE, "js/api.js")
    assert "chatSetModel: (id, model) =>" in api
    assert '/model`, { model })' in api


def test_the_knobs_are_served_and_the_ladder_has_five_stops(client: TestClient):
    for path in ("/js/knobs.js", "/synapse/js/knobs.js"):
        response = client.get(path)
        assert response.status_code == 200 and "javascript" in response.headers["content-type"], path
    dials = client.get("/api/chat/dials").json()
    assert [d["id"] for d in dials["depths"]] == ["minimal", "quick", "standard", "deep", "max"]
    assert [d["level"] for d in dials["depths"]] == ["minimal", "low", "medium", "high", "max"]
    assert [d["default"] for d in dials["depths"]] == [False, False, True, False, False]
    assert dials["depths"][4]["label"] == "Extra deep" and dials["depths"][0]["label"] == "Minimal"
    # every model row has what the picker draws
    for m in dials["models"]:
        for key in ("id", "plane", "plane_name", "label", "means", "available", "reason", "default"):
            assert key in m, key
