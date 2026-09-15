"""The gateway plane: the signed token request, the token manager, the
route chooser, and the model client's whole-call contract."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json

import pytest

from kcx.env import ConfigError
from kcx.gateway import (MAX_CAP, GatewayConfig, GatewayError, GatewayModel,
                         Route, RouteChooser, TokenManager, default_scopes,
                         extract_token, find_expiry, fingerprint,
                         hmac_signature, jwt_claims, thinking_budgets,
                         token_headers)
from kcx.transport import TransportError
from tests.doubles import FakeGateway, jwt

SECRET = base64.b64encode(b"a-32-byte-secret-for-the-tests!!").decode()
BASE = "https://gw.example.com/genai/google/v1"
TOKEN_URL = "https://idp.example.com/v1/application/token"


def _cfg(**over) -> GatewayConfig:
    fields = dict(app_id="app", secret=SECRET, token_url=TOKEN_URL,
                  base_url=BASE, scopes=default_scopes(BASE, "gemini-2.5-pro"))
    fields.update(over)
    return GatewayConfig(**fields)


def _model(fake: FakeGateway, **over) -> GatewayModel:
    cfg = _cfg(**over)
    return GatewayModel(cfg=cfg, tokens=TokenManager(cfg, fake.http,
                                                     now=fake.clock),
                        http=fake.http, sleep=fake.sleep)


def test_signature_is_urlsafe_unpadded_hmac_over_app_version_timestamp():
    expected = base64.urlsafe_b64encode(hmac.new(
        base64.b64decode(SECRET), b"app-2-1700000000000",
        hashlib.sha256).digest()).decode().rstrip("=")
    assert hmac_signature("app", "2", "1700000000000", SECRET) == expected
    headers = token_headers("app", SECRET, version="2",
                            timestamp="1700000000000")
    assert headers["X-Auth-Signature"] == expected
    assert headers["X-Auth-AppID"] == "app" and headers["X-Auth-Version"] == "2"
    assert headers["X-Auth-Timestamp"] == "1700000000000"


def test_token_reading_finds_the_token_the_expiry_and_the_claims():
    assert extract_token({"authorization_token": "one"}) == \
        ("one", "authorization_token")
    assert extract_token({"data": {"token": "nested"}}) == ("nested", "data.token")
    assert extract_token({"unheard_of": "x" * 64, "type": "Bearer"}) == \
        ("x" * 64, "unheard_of (by shape)")
    assert extract_token({"nothing": 1}) == ("", "")
    assert extract_token("not json") == ("", "")
    assert find_expiry({"expires_in": 300, "data": {"exp": 9}}) == {
        "expires_in": 300, "exp": 9}
    assert find_expiry({"access_token": "x"}) == {}
    token = jwt({"exp": 1700000300, "iat": 1700000000})
    assert jwt_claims(token) == {"exp": 1700000300, "iat": 1700000000}
    assert jwt_claims("opaque-token") is None
    assert fingerprint("") == "empty"
    assert "chars · sha256" in fingerprint("secret")
    assert "secret" not in fingerprint("secret")


def test_config_has_no_deployment_defaults_and_validate_names_every_gap():
    cfg = GatewayConfig.from_env({})
    assert cfg.base_url == "" and cfg.token_url == "" and cfg.scopes == []
    with pytest.raises(ConfigError) as caught:
        cfg.validate()
    text = str(caught.value)
    assert "GATEWAY_BASE_URL" in text and "IDP_TOKEN_URL" in text
    assert "APP_ID and APP_SECRET" in text
    with pytest.raises(ConfigError, match="GEMINI_BEARER_TOKEN"):
        GatewayConfig.from_env({"AUTH_MODE": "env", "GATEWAY_BASE_URL": BASE}
                               ).validate()
    env = {"GATEWAY_BASE_URL": BASE + "/", "IDP_TOKEN_URL": TOKEN_URL,
           "APP_ID": "app", "APP_SECRET": SECRET}
    cfg = GatewayConfig.from_env(env)
    cfg.validate()
    assert cfg.base_url == BASE and cfg.model == "gemini-2.5-pro"
    assert cfg.scopes == ["/genai/google/v1/models/gemini-2.5-pro/**::post"]
    assert cfg.display()["app_secret"] == fingerprint(SECRET)
    assert SECRET not in json.dumps(cfg.display())
    env.update({"GATEWAY_SCOPES": "/a/**::post, /b/**::post",
                "GEMINI_MODEL": "gemini-2.5-flash"})
    cfg = GatewayConfig.from_env(env)
    assert cfg.scopes == ["/a/**::post", "/b/**::post"]
    assert cfg.model == "gemini-2.5-flash"
    env["GATEWAY_MODEL"] = "gemini-2.5-pro"
    assert GatewayConfig.from_env(env).model == "gemini-2.5-pro"
    assert default_scopes("https://gw.example.com/genai/google/v1/",
                          "gemini-2.5-pro") == \
        ["/genai/google/v1/models/gemini-2.5-pro/**::post"]


def test_the_token_manager_mints_reuses_refreshes_and_invalidates():
    fake = FakeGateway()
    tokens = TokenManager(_cfg(), fake.http, now=fake.clock)
    assert tokens.describe().startswith("no token yet")
    first = tokens.token()
    assert fake.minted == 1 and tokens.mints == 1
    assert 598 <= tokens.remaining() <= 599
    assert tokens.token() == first                  # reused, no mint
    fake.now += 400                                 # inside 80 % of 599
    assert tokens.token() == first
    fake.now += 100                                 # past 479 s: refresh
    second = tokens.token()
    assert second != first and fake.minted == 2
    assert "refresh at 479 s" in tokens.describe()
    tokens.invalidate()
    assert tokens.remaining() == 0.0
    assert tokens.token() != second and fake.minted == 3
    sent = fake.requests[0]
    assert sent["url"] == TOKEN_URL
    assert sent["body"] == {"scope": default_scopes(BASE, "gemini-2.5-pro")}
    assert sent["headers"]["X-Auth-AppID"] == "app"
    # the environment's bearer is used as it is, never minted
    given = TokenManager(_cfg(auth_mode="env", bearer=jwt({"exp": 1})),
                         fake.http, now=fake.clock)
    assert given.token() == jwt({"exp": 1}) and fake.minted == 3
    with pytest.raises(GatewayError, match="APP_ID and APP_SECRET"):
        TokenManager(_cfg(app_id=""), fake.http, now=fake.clock).token()
    with pytest.raises(GatewayError, match="IDP_TOKEN_URL"):
        TokenManager(_cfg(token_url=""), fake.http, now=fake.clock).token()


def test_the_route_is_decided_by_the_first_real_request():
    import ssl
    context = ssl.create_default_context()
    routes = [Route({}, context, "direct"),
              Route({"https": "http://proxy:8080"}, context, "via proxy")]
    seen: list = []

    def call(opener, method, url, headers, body, **kw):
        seen.append(1)
        if len(seen) == 1:
            raise TransportError("unreachable: direct is blackholed")
        return 200, {}, b"{}"
    chooser = RouteChooser(routes, call=call)
    assert chooser.label == "undecided"
    assert chooser.http("POST", "https://x", {}, b"") == (200, {}, b"{}")
    assert chooser.label == "via proxy"
    assert chooser.failures == ["direct: unreachable: direct is blackholed"]
    chooser.http("POST", "https://x", {}, b"")
    assert len(seen) == 3                           # the chosen route only

    def never(opener, *a, **kw):
        raise TransportError("no")
    with pytest.raises(GatewayError, match="no route reaches the gateway"):
        RouteChooser(routes, call=never).http("POST", "https://x", {}, b"")


def test_converse_delivers_the_events_in_one_burst_with_the_budget_under_the_cap():
    fake = FakeGateway([{"parts": [
        {"thought": True, "text": "thinking"},
        {"text": "Here: "},
        {"functionCall": {"name": "knowledge_catalog_search",
                          "args": {"query": "one"}}},
        {"functionCall": {"name": "knowledge_catalog_search",
                          "args": {"query": "two"}}}]}])
    model = _model(fake)
    events = list(model.converse(
        [{"role": "user", "parts": [{"text": "q"}]}], system="sys",
        tools=[{"name": "knowledge_catalog_search", "description": "d",
                "parameters": {"type": "OBJECT"}}], thinking_level="low"))
    assert [e["kind"] for e in events] == ["thought", "text", "call", "call",
                                           "done"]
    assert [e["args"]["query"] for e in events if e["kind"] == "call"] == \
        ["one", "two"]
    assert all(e["id"] == "" for e in events if e["kind"] == "call")
    assert events[-1]["parts"][2] == {"functionCall": {
        "name": "knowledge_catalog_search", "args": {"query": "one"}}}
    assert events[-1]["usage"]["thought_tokens"] == 30
    sent = fake.model_requests[0]
    assert sent["url"] == f"{BASE}/models/gemini-2.5-pro/generateContent"
    assert sent["body"]["generationConfig"] == {
        "maxOutputTokens": 8192 + 1024,
        "thinkingConfig": {"includeThoughts": True, "thinkingBudget": 1024}}
    assert sent["body"]["systemInstruction"] == {"parts": [{"text": "sys"}]}
    assert sent["body"]["tools"][0]["functionDeclarations"][0]["name"] == \
        "knowledge_catalog_search"
    assert model.usage["calls"] == 1 and fake.minted == 1
    colon = _model(FakeGateway(), path_form="colon")
    assert colon._url() == f"{BASE}/models/gemini-2.5-pro:generateContent"


def test_a_dead_token_is_minted_anew_once_and_a_second_refusal_is_final():
    fake = FakeGateway([{"parts": [{"text": "ok"}]}])
    model = _model(fake)
    first = model.tokens.token()
    fake.dead.add(first)
    events = list(model.converse([{"role": "user", "parts": [{"text": "q"}]}]))
    assert events[0] == {"kind": "text", "delta": "ok"}
    assert fake.minted == 2 and len(fake.model_requests) == 2
    assert fake.model_requests[1]["token"] != first

    class Always(set):
        def __contains__(self, item):
            return True
    fake = FakeGateway([{"parts": [{"text": "never"}]}])
    fake.dead = Always()
    with pytest.raises(GatewayError, match="refused: HTTP 401"):
        list(_model(fake).converse([{"role": "user", "parts": [{"text": "q"}]}]))


def test_transient_refusals_back_off_and_max_tokens_grows_the_cap_once():
    fake = FakeGateway([{"parts": [{"text": "late"}]}])
    fake.fail_next = [503]
    model = _model(fake)
    before = fake.now
    events = list(model.converse([{"role": "user", "parts": [{"text": "q"}]}]))
    assert events[0]["delta"] == "late" and fake.now - before == 2
    assert len(fake.model_requests) == 2

    fake = FakeGateway([{"parts": [{"thought": True, "text": "hmm"}],
                         "finish": "MAX_TOKENS"},
                        {"parts": [{"text": "done"}]}])
    events = list(_model(fake).converse(
        [{"role": "user", "parts": [{"text": "q"}]}], thinking_level="high"))
    assert events[-2] == {"kind": "text", "delta": "done"}
    caps = [r["body"]["generationConfig"]["maxOutputTokens"]
            for r in fake.model_requests]
    assert caps == [8192 + 16384, min(2 * (8192 + 16384), MAX_CAP)]

    fake = FakeGateway()
    fake.fail_next = [(200, b"<html>login page</html>")]
    with pytest.raises(GatewayError, match="not JSON"):
        list(_model(fake).converse([{"role": "user", "parts": [{"text": "q"}]}]))


def test_thinking_budgets_read_the_override():
    assert thinking_budgets({}) == {"low": 1024, "medium": 4096,
                                    "high": 16384}
    assert thinking_budgets({"GATEWAY_THINKING_BUDGETS": "low:512, high:x,"
                                                        "medium:2048"}) == \
        {"low": 512, "medium": 2048, "high": 16384}
