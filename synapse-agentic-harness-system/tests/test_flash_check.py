"""flash_check.py — the probe's verdict math, pinned without a network.

The laptop answers the real question; CI pins that the script runs,
classifies HTTP outcomes the way the laptop needs them told apart
(404 model missing vs 403 permission vs 429 quota), reads a real
generateContent payload, and recommends the fastest answering Flash.
"""

from __future__ import annotations

import io
import sys
import urllib.error
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "scripts"))


def _http_error(code: int, body: str = "") -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://x", code, "err", {},
                                  io.BytesIO(body.encode()))


def test_classify_tells_the_laptop_failures_apart():
    import flash_check
    assert flash_check.classify(_http_error(404))[0] == "not_found"
    assert flash_check.classify(_http_error(403))[0] == "forbidden"
    assert flash_check.classify(_http_error(429))[0] == "quota"
    assert flash_check.classify(_http_error(400, "thinking"))[0] \
        == "rejected"
    policy = flash_check.classify(_http_error(
        400, '{"error": {"message": "Organization Policy constraint '
             'vertexai.allowedModels\n denies gemini-3.5-flash"}}'))
    assert policy[0] == "org_policy"
    assert "\n" not in policy[1]              # one line in the table
    assert flash_check.classify(RuntimeError("dead"))[0] == "error"


def test_answers_reads_a_real_payload_and_a_refusal():
    import flash_check

    class Client:
        def __init__(self, outcome):
            self.outcome = outcome

        def _post(self, body):
            assert body["generationConfig"]["maxOutputTokens"] >= 256
            if isinstance(self.outcome, Exception):
                raise self.outcome
            return self.outcome

    good = flash_check.probe_answers(Client({
        "candidates": [{"content": {"parts": [
            {"thought": True, "text": "hmm"}, {"text": "OK"}]},
            "finishReason": "STOP"}],
        "usageMetadata": {"promptTokenCount": 7,
                          "candidatesTokenCount": 1,
                          "thoughtsTokenCount": 12}}))
    assert good["verdict"] == "ok" and good["detail"] == "OK"
    assert good["thought_tokens"] == 12
    missing = flash_check.probe_answers(Client(_http_error(404)))
    assert missing["verdict"] == "not_found"
    empty = flash_check.probe_answers(Client({
        "candidates": [{"content": {"parts": []},
                        "finishReason": "MAX_TOKENS"}]}))
    assert empty["verdict"] == "empty" and "MAX_TOKENS" in empty["detail"]


def test_report_recommends_the_fastest_answering_flash():
    import flash_check
    rows = [
        {"model": "gemini-3.1-pro-preview", "listed": "ok",
         "verdict": "ok", "latency_ms": 900, "detail": "OK"},
        {"model": "gemini-3.7-flash", "listed": "not_found",
         "verdict": "not_found", "latency_ms": 120,
         "detail": "HTTP 404"},
        {"model": "gemini-3.5-flash", "listed": "ok", "verdict": "ok",
         "latency_ms": 410, "detail": "OK", "prompt_tokens": 7,
         "output_tokens": 1, "thought_tokens": 0},
        {"model": "gemini-2.5-flash", "listed": "ok", "verdict": "ok",
         "latency_ms": 380, "detail": "OK", "prompt_tokens": 7,
         "output_tokens": 1, "thought_tokens": 0},
    ]
    report = flash_check.render_markdown(rows, label="test")
    assert "VERTEX_FLASH_MODEL=gemini-2.5-flash" in report
    assert "| gemini-3.7-flash | not_found | not_found |" in report
    none = flash_check.render_markdown(rows[:2], label="test")
    assert "no Flash model answered" in none
    blocked = flash_check.render_markdown(rows[:2] + [
        {"model": "gemini-3.5-flash", "listed": "not_found",
         "verdict": "org_policy", "latency_ms": 300,
         "detail": "HTTP 400 Organization Policy constraint"}],
        label="test")
    assert "blocked by organization policy" in blocked
    assert "constraints/vertexai.allowedModels" in blocked
    assert "gemini-3.5-flash first" in blocked


def test_probe_all_swaps_the_model_id_per_candidate():
    import flash_check
    from sahs.util.auth import VertexConnection
    connection = VertexConnection(
        project="p", location="global", model="gemini-3.1-pro-preview",
        endpoint="https://aiplatform.googleapis.com", key_path=None)
    seen = []

    class Client:
        def __init__(self, conn):
            self.connection = conn

        def _token(self):
            return "t"

        def _post(self, body):
            seen.append(self.connection.model)
            if "3.7" in self.connection.model:
                raise _http_error(404)
            return {"candidates": [{"content": {"parts": [
                {"text": "OK"}]}, "finishReason": "STOP"}],
                "usageMetadata": {}}

    flash_check.probe_listed = lambda client: ("skipped", "no network")
    rows = flash_check.probe_all(connection,
                                 ["gemini-3.7-flash", "gemini-3.5-flash"],
                                 client_factory=Client)
    assert seen == ["gemini-3.7-flash", "gemini-3.5-flash"]
    assert [r["verdict"] for r in rows] == ["not_found", "ok"]
