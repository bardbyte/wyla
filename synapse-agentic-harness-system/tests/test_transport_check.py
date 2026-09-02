"""transport_check.py — the round-trip verdict math, pinned offline."""

from __future__ import annotations

import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "scripts"))


class Client:
    """A two-call transport: call 1 returns a signed functionCall,
    call 2 must carry the echoed parts and the functionResponse."""

    def __init__(self, second_text="Certified as sum(trans_usd_am) "
                                   "on dw.gms_transaction in USD."):
        self.bodies = []
        self.second_text = second_text

    def _post(self, body):
        self.bodies.append(body)
        if len(self.bodies) == 1:
            return {"candidates": [{"content": {"parts": [
                {"functionCall": {"name": "lookup_metric",
                                  "args": {"name": "Acquirer Net Spend"},
                                  "id": "call_1"},
                 "thoughtSignature": "sig=="}]}}]}
        assert body["contents"][1]["role"] == "model"
        assert body["contents"][1]["parts"][0]["thoughtSignature"] == "sig=="
        fr = body["contents"][2]["parts"][0]["functionResponse"]
        assert fr["name"] == "lookup_metric" and fr["id"] == "call_1"
        assert fr["response"]["status"] == "certified"
        return {"candidates": [{"content": {"parts": [
            {"thought": True, "text": "reading the result"},
            {"text": self.second_text}]}}],
            "usageMetadata": {"thoughtsTokenCount": 40}}


def test_round_trip_echoes_parts_verbatim_and_grounds_the_answer():
    import transport_check
    client = Client()
    got = transport_check.probe_round_trip(client)
    assert got["verdict"] == "ok", got
    assert got["signature_returned"] is True
    assert got["call"]["id"] == "call_1"
    assert got["thought_tokens"] == 40
    assert len(client.bodies) == 2


def test_round_trip_names_the_failing_stage():
    import transport_check
    ungrounded = transport_check.probe_round_trip(
        Client(second_text="I cannot say."))
    assert ungrounded["verdict"] == "ungrounded"

    class NoCall:
        def _post(self, body):
            return {"candidates": [{"content": {"parts": [
                {"text": "Sure, let me think about that."}]}}]}
    assert transport_check.probe_round_trip(NoCall())["verdict"] \
        == "no_call"


def test_recommendation_reads_the_verdicts():
    import transport_check
    good = {"round_trip": {"verdict": "ok", "signature_returned": True},
            "stream_tools": {"verdict": "ok"},
            "sdk": {"installed": False}, "interactions": {}}
    line = transport_check.recommend(good)
    assert line.startswith("decision 1 → the REST client")
    assert "signature was accepted" in line and "not needed" in line
    bad = {"round_trip": {"verdict": "rejected", "stage": "call_2",
                          "detail": "HTTP 400 missing thought_signature"},
           "stream_tools": {"verdict": "ok"}, "sdk": {"installed": True},
           "interactions": {}}
    assert "unresolved" in transport_check.recommend(bad)
    assert "call_2" in transport_check.recommend(bad)
    markdown = transport_check.render_markdown(good, label="t")
    assert "**round_trip**" in markdown and "REST client" in markdown
