"""eag_check — the OneIdentity signature, the token reading, the stream
classification and the whole check flow, pinned offline with a fake
gateway (the transport is injected; nothing here touches a network)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json

from sahs.util.eag import (Config, EagError, classify_probe, classify_stream,
                           extract_token, find_expiry, fingerprint,
                           hmac_signature, jwt_claims, render_report,
                           run_checks, token_headers)

SECRET = base64.b64encode(b"a-32-byte-secret-for-the-tests!!").decode()


def _jwt(claims: dict) -> str:
    seg = lambda obj: base64.urlsafe_b64encode(          # noqa: E731
        json.dumps(obj).encode()).decode().rstrip("=")
    return f"{seg({'alg': 'HS256'})}.{seg(claims)}.sig"


def test_signature_is_urlsafe_unpadded_hmac_over_app_version_timestamp():
    expected = base64.urlsafe_b64encode(hmac.new(
        base64.b64decode(SECRET), b"app-2-1700000000000",
        hashlib.sha256).digest()).decode().rstrip("=")
    assert hmac_signature("app", "2", "1700000000000", SECRET) == expected
    assert "=" not in expected and "+" not in expected and "/" not in expected
    headers = token_headers("app", SECRET, version="2",
                            timestamp="1700000000000")
    assert headers["X-Auth-Signature"] == expected
    assert headers["X-Auth-AppID"] == "app" and headers["X-Auth-Version"] == "2"
    assert headers["X-Auth-Timestamp"] == "1700000000000"


def test_token_reading_finds_the_token_the_expiry_and_the_jwt_claims():
    assert extract_token({"access_token": "abc"}) == "abc"
    assert extract_token({"data": {"token": "nested"}}) == "nested"
    assert extract_token({"nothing": 1}) == ""
    assert find_expiry({"expires_in": 300, "data": {"exp": 9}}) == {
        "expires_in": 300, "exp": 9}
    assert find_expiry({"access_token": "x"}) == {}
    token = _jwt({"exp": 1700000300, "iat": 1700000000})
    assert jwt_claims(token) == {"exp": 1700000300, "iat": 1700000000}
    assert jwt_claims("opaque-token") is None
    assert fingerprint("") == "empty"
    assert "chars · sha256" in fingerprint("secret") and "secret" not in \
        fingerprint("secret")


def test_stream_classification_tells_sse_from_a_burst():
    frame = lambda text: ("data: " + json.dumps({"candidates": [{   # noqa: E731
        "content": {"parts": [{"text": text}]},
        "finishReason": "STOP"}]}) + "\n\n").encode()
    streamed = classify_stream("text/event-stream", [
        (0.8, frame("one ")), (1.9, frame("two ")), (3.1, frame("three"))])
    assert streamed["form"] == "sse" and streamed["streamed"] is True
    assert streamed["frames"] == 3 and streamed["text"] == "one two three"
    burst = classify_stream("application/json", [(4.0, json.dumps([
        {"candidates": [{"content": {"parts": [{"text": "all"}]}}]},
        {"candidates": [{"content": {"parts": [{"text": " at once"}]}}]},
    ]).encode())])
    assert burst["form"] == "json-array" and burst["streamed"] is False
    assert burst["text"] == "all at once"
    single = classify_stream("application/json", [(2.0, json.dumps(
        {"candidates": [{"content": {"parts": [{"text": "one"}]}}]}).encode())])
    assert single["form"] == "single-json" and single["streamed"] is False
    assert classify_probe(401) == "dead" and classify_probe(400) == "alive"
    assert classify_probe(500).startswith("unclear")


class Gateway:
    """OneIdentity + EAG, scripted: the token endpoint wants SECONDS
    (a ms timestamp is refused), the model rejects includeThoughts
    once (the guide's snake_case works), tools answer with a signed
    functionCall, and the token dies 300 s after minting."""

    def __init__(self):
        self.calls = []
        self.now = 1_700_000_000.0
        self.minted = None

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds

    def http(self, method, url, headers, body, **kw):
        self.calls.append((method, url, headers, body))
        if url.endswith("/application/token"):
            stamp = headers["X-Auth-Timestamp"]
            if len(stamp) > 10:                       # milliseconds: refused
                return 401, {}, json.dumps(
                    {"error": {"message": "invalid signature"}}).encode()
            self.minted = self.now
            return 200, {}, json.dumps({"access_token": _jwt(
                {"iat": int(self.now), "exp": int(self.now) + 300}),
                "token_type": "Bearer"}).encode()
        assert headers["Authorization"].startswith("Bearer ")
        if self.now - (self.minted or self.now) >= 300:
            return 401, {}, b'{"error":{"message":"token expired"}}'
        payload = json.loads(body or b"{}")
        if not payload:
            return 400, {}, b'{"error":{"message":"contents required"}}'
        config = payload.get("generationConfig", {})
        if "includeThoughts" in config.get("thinkingConfig", {}):
            return 400, {}, json.dumps({"error": {
                "message": "Unknown name 'includeThoughts' in thinkingConfig"
            }}).encode()
        if payload.get("tools"):
            if len(payload["contents"]) == 1:
                return 200, {}, json.dumps({"candidates": [{"content": {
                    "parts": [{"functionCall": {"name": "lookup_metric",
                                                "args": {"name": "Acquirer Net Spend"},
                                                "id": "c1"},
                               "thoughtSignature": "sig=="}]},
                    "finishReason": "STOP"}]}).encode()
            assert payload["contents"][1]["parts"][0]["thoughtSignature"] == "sig=="
            assert payload["contents"][2]["parts"][0]["functionResponse"]["id"] == "c1"
            return 200, {}, json.dumps({"candidates": [{"content": {"parts": [
                {"text": "Certified as sum(trans_usd_am)."}]},
                "finishReason": "STOP"}]}).encode()
        if payload.get("systemInstruction"):
            return 200, {}, json.dumps({"candidates": [{"content": {"parts": [
                {"text": "PONG"}]}, "finishReason": "STOP"}]}).encode()
        return 200, {}, json.dumps({"candidates": [{"content": {"parts": [
            {"thought": True, "text": "thinking about cards"},
            {"text": "A cash rewards card pays back a share of spend."}]},
            "finishReason": "STOP"}],
            "usageMetadata": {"promptTokenCount": 12, "candidatesTokenCount": 20,
                              "thoughtsTokenCount": 30, "totalTokenCount": 62},
            "modelVersion": "gemini-2.5-pro"}).encode()

    def stream(self, url, headers, body, **kw):
        self.calls.append(("STREAM", url, headers, body))
        frame = lambda text: ("data: " + json.dumps({"candidates": [{   # noqa: E731
            "content": {"parts": [{"text": text}]},
            "finishReason": "STOP"}]}) + "\n\n").encode()
        return 200, {"Content-Type": "text/event-stream"}, iter([
            (0.7, frame("A cash ")), (1.6, frame("rewards ")),
            (2.9, frame("card."))])


def test_the_whole_check_against_a_scripted_gateway():
    gw = Gateway()
    cfg = Config(app_id="app", secret=SECRET, timestamp_unit="ms")
    report = run_checks(cfg, gw.http, gw.stream, now=gw.clock, clock=gw.clock,
                        sleep=gw.sleep, probe_minutes=8)
    by_name = {c["name"]: c for c in report["checks"]}
    # the token: ms refused, seconds accepted, the JWT says 300 s
    assert by_name["token"]["ok"] is True
    assert report["token"]["unit"] == "s" and report["token"]["ttl_s"] == 300
    assert [a["unit"] for a in report["token"]["attempts"]] == ["ms", "s"]
    assert "says nothing about expiry" in by_name["token"]["detail"]
    # generate: the guide's spelling of includeThoughts was needed
    assert by_name["generate"]["ok"] is True
    assert report["generate"]["thinking_key"] == "include_thoughts"
    assert report["generate"]["thought_parts"] == 1
    assert report["generate"]["usage"]["thoughts"] == 30
    # stream: real SSE frames over time
    assert by_name["stream"]["ok"] is True and report["stream"]["form"] == "sse"
    assert report["stream"]["text"] == "A cash rewards card."
    # tools: the functionCall came back signed and the round trip echoed it
    assert by_name["tools"]["ok"] is True and report["tools"]["signature"]
    assert report["tools"]["call"]["name"] == "lookup_metric"
    assert "sum(trans_usd_am)" in report["tools"]["round_trip_text"]
    # the system instruction was followed
    assert by_name["system"]["ok"] is True and report["system"]["followed"]
    # the probe watched the token die at 300 s
    assert by_name["probe"]["ok"] is True
    assert report["probe"]["verdict"] == "dead"
    assert 280 <= report["probe"]["died_at_s"] <= 320
    assert "refresh at" in report["probe"]["refresh_hint"]
    text = render_report(report)
    assert "✓ token" in text and "✓ generate" in text and "✓ stream" in text
    assert "native tools work with signatures" in text
    assert "measured lifetime" in text
    # no secret anywhere in the report
    dumped = json.dumps(report)
    assert SECRET not in dumped and "sig==" not in report["config"].values()


def test_missing_credentials_and_env_mode_are_reported_not_raised():
    gw = Gateway()
    report = run_checks(Config(), gw.http, gw.stream, now=gw.clock,
                        clock=gw.clock, sleep=gw.sleep)
    assert report["checks"] == [{"name": "token", "ok": False,
                                 "detail": "APP_ID and APP_SECRET are needed "
                                           "(AUTH_MODE=generated), or "
                                           "AUTH_MODE=env with "
                                           "GEMINI_BEARER_TOKEN"}]
    gw = Gateway()
    gw.minted = gw.now
    cfg = Config(auth_mode="env", bearer=_jwt({"iat": int(gw.now),
                                                "exp": int(gw.now) + 240}))
    report = run_checks(cfg, gw.http, gw.stream, now=gw.clock,
                        clock=gw.clock, sleep=gw.sleep, only={"token", "generate"})
    assert report["token"]["source"] == "env" and report["token"]["ttl_s"] == 240
    assert {c["name"] for c in report["checks"]} == {"token", "generate"}


def test_a_dead_gateway_is_a_recorded_failure():
    def down(*a, **k):
        raise EagError("unreachable via proxy.corp:8080")
    report = run_checks(Config(app_id="app", secret=SECRET), down, down)
    assert report["checks"][0]["ok"] is False
    assert "unreachable" in report["checks"][0]["detail"]
