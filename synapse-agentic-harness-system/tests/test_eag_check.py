"""eag_check — the OneIdentity signature, the token reading, the stream
classification and the whole check flow, pinned offline with a fake
gateway (the transport is injected; nothing here touches a network)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json

from sahs.util.eag import (Config, EagError, Route, RouteChooser,
                           classify_probe, classify_stream, env_warnings,
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
    assert extract_token({"authorization_token": "one"}) == \
        ("one", "authorization_token")                  # OneIdentity's name
    assert extract_token({"access_token": "abc"}) == ("abc", "access_token")
    assert extract_token({"data": {"token": "nested"}}) == \
        ("nested", "data.token")
    assert extract_token({"unheard_of": "x" * 64, "type": "Bearer"}) == \
        ("x" * 64, "unheard_of (by shape)")
    assert extract_token({"nothing": 1}) == ("", "")
    assert extract_token("not json") == ("", "")
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
    """OneIdentity + EAG, scripted like the laptop showed them: the
    token endpoint takes MILLISECONDS (a seconds timestamp is refused
    with UEXP001) and answers {"authorization_token": …}; the model
    takes the guide's include_thoughts and refuses includeThoughts;
    tools answer with a signed functionCall; the token dies 300 s
    after minting."""

    def __init__(self, unit="ms", token_field="authorization_token"):
        self.calls = []
        self.now = 1_700_000_000.0
        self.minted = None
        self.unit = unit
        self.token_field = token_field

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds

    def http(self, method, url, headers, body, **kw):
        self.calls.append((method, url, headers, body))
        if url.endswith("/application/token"):
            stamp = headers["X-Auth-Timestamp"]
            is_ms = len(stamp) > 10
            if is_ms != (self.unit == "ms"):
                return 403, {}, json.dumps({
                    "description": "Client credential cannot be validated "
                                   ": {Signature Expired}",
                    "error_code": "UEXP001"}).encode()
            self.minted = self.now
            return 200, {}, json.dumps({self.token_field: _jwt(
                {"iat": int(self.now), "exp": int(self.now) + 300})
            }).encode()
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
    # the token: milliseconds taken first time, read from
    # authorization_token, the JWT says 300 s, no second signature
    assert by_name["token"]["ok"] is True
    assert report["token"]["unit"] == "ms" and report["token"]["ttl_s"] == 300
    assert report["token"]["field"] == "authorization_token"
    assert [a["unit"] for a in report["token"]["attempts"]] == ["ms"]
    assert "says nothing about expiry" in by_name["token"]["detail"]
    assert "field authorization_token" in by_name["token"]["detail"]
    # generate: the guide's spelling of the thoughts flag, first try
    assert by_name["generate"]["ok"] is True
    assert report["generate"]["thinking_key"] == "include_thoughts"
    # the thoughts flag, both ways: this gateway takes the guide's only
    assert by_name["thinking"]["ok"] is False
    assert report["thinking"]["include_thoughts"]["ok"] is True
    assert report["thinking"]["includeThoughts"]["ok"] is False
    assert "the harness sends includeThoughts" in by_name["thinking"]["detail"]
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
    assert "thoughts flag: the guide's spelling only" in text
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


def test_a_dead_gateway_is_a_recorded_failure_after_one_attempt():
    """A transport failure is not a refusal: no second timestamp is
    signed for a network that did not answer, and the detail says
    reached-or-not rather than refused."""
    calls = []

    def down(*a, **k):
        calls.append(a)
        raise EagError("unreachable via proxy.corp:8080")
    report = run_checks(Config(app_id="app", secret=SECRET), down, down)
    assert report["checks"][0]["ok"] is False
    assert report["checks"][0]["detail"].startswith(
        "OneIdentity could not be reached: ms: unreachable")
    assert len(calls) == 1 and len(report["token"]["attempts"]) == 1


def test_the_seconds_fallback_only_runs_when_the_gateway_refuses():
    """A gateway that wants seconds refuses the ms signature with 403:
    then, and only then, the seconds one is sent."""
    gw = Gateway(unit="s")
    report = run_checks(Config(app_id="app", secret=SECRET), gw.http,
                        gw.stream, now=gw.clock, clock=gw.clock,
                        sleep=gw.sleep, only={"token"})
    assert report["checks"][0]["ok"] is True
    assert [a["unit"] for a in report["token"]["attempts"]] == ["ms", "s"]
    assert "UEXP001" in report["token"]["attempts"][0]["note"]


def test_a_200_with_an_unreadable_token_is_our_fault_and_says_so():
    """The laptop's case before the fix: a 200 whose token field we did
    not know. Now it is reported as a 200 with the keys named, no
    second signature is sent, and not one byte of the body leaks."""
    gw = Gateway(token_field="something_new")
    # the by-shape fallback would read a lone long string; make it two
    real_http = gw.http

    def http(method, url, headers, body, **kw):
        status, h, raw = real_http(method, url, headers, body, **kw)
        if url.endswith("/application/token") and status == 200:
            payload = json.loads(raw)
            payload["other"] = "y" * 60
            payload["something_new"] = "z" * 60
            return status, h, json.dumps(payload).encode()
        return status, h, raw

    report = run_checks(Config(app_id="app", secret=SECRET), http, gw.stream,
                        now=gw.clock, clock=gw.clock, sleep=gw.sleep)
    token = report["checks"][0]
    assert token["ok"] is False
    assert token["detail"].startswith("OneIdentity answered 200 but the "
                                      "token field was not recognized")
    assert "other" in token["detail"] and "something_new" in token["detail"]
    assert "zzzz" not in json.dumps(report) and "yyyy" not in json.dumps(report)
    assert [a["unit"] for a in report["token"]["attempts"]] == ["ms"]
    assert len(report["checks"]) == 1                # nothing else was tried


def test_the_route_is_decided_by_the_first_real_request():
    """No GET probe: the token POST itself tries direct, then the
    proxy; whichever answers with ANY status is pinned for every later
    call, and when none answers the failure names each route."""
    direct = Route({}, None, "direct")
    via = Route({"https": "http://proxy.corp:8080"}, None, "via proxy.corp")
    seen = []

    def call(route, method, url, headers, body, **kw):
        seen.append(route.label)
        if route is direct:
            raise EagError("unreachable via [Errno -2] Name or service not known")
        return 405, {}, b"method not allowed"

    chooser = RouteChooser([direct, via], call=call, stream=call)
    assert chooser.label == "undecided"
    status, _h, _b = chooser.http("POST", "https://x/token", {}, b"{}")
    assert status == 405 and chooser.label == "via proxy.corp"
    chooser.http("POST", "https://x/again", {}, b"{}")
    assert seen == ["direct", "via proxy.corp", "via proxy.corp"]
    assert chooser.failures == ["direct: unreachable via [Errno -2] Name or "
                                "service not known"]
    # while undecided, each request reports ITS failures, not a pile
    def down_all(route, *a, **k):
        raise EagError(f"dead on {route.label}")
    twice = RouteChooser([direct, via], call=down_all, stream=down_all)
    for _ in range(2):
        try:
            twice.http("POST", "https://x/token", {}, b"{}")
        except EagError:
            pass
    assert len(twice.failures) == 2

    def down(route, *a, **k):
        raise EagError(f"dead on {route.label}")
    nothing = RouteChooser([direct, via], call=down, stream=down)
    try:
        nothing.http("POST", "https://x/token", {}, b"{}")
    except EagError as e:
        assert "no route reaches the gateway" in str(e)
        assert "direct" in str(e) and "via proxy.corp" in str(e)
    else:
        raise AssertionError("no route should have raised")


def test_the_model_comes_from_eag_model_and_gemini_model_is_warned_about():
    assert Config.from_env({"EAG_MODEL": "gemini-2.5-flash",
                            "GEMINI_MODEL": "x"}).model == "gemini-2.5-flash"
    assert Config.from_env({"GEMINI_MODEL": "gemini-2.5-pro"}).model == \
        "gemini-2.5-pro"
    assert Config.from_env({}).model == "gemini-2.5-pro"
    assert env_warnings({"GEMINI_MODEL": "gemini-2.5-pro"})[0].startswith(
        "GEMINI_MODEL=gemini-2.5-pro is set and VERTEX_MODEL is not")
    assert env_warnings({"GEMINI_MODEL": "x", "VERTEX_MODEL": "y"}) == []
    assert env_warnings({"EAG_MODEL": "x"}) == []
    text = render_report({"config": {}, "checks": [],
                          "warnings": ["GEMINI_MODEL=x is set …"]})
    assert "! warning GEMINI_MODEL=x is set" in text

