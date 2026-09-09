"""The gateway plane: the token manager, the client that delivers each
call as the loop's events in one burst, the plane switch, and a whole
assistant turn riding it — all against a scripted gateway shaped like
the laptop's (a 599 s JWT under authorization_token, the slash path,
no stream), nothing on the network."""

from __future__ import annotations

import base64
import json
import subprocess
import sys
from pathlib import Path

import pytest

from sahs.enrich.client import EnrichTransportError
from sahs.enrich.gateway_client import GatewayClient
from sahs.util.gateway import (Config, GatewayError, TokenManager, model_plane,
                           plane_note, thinking_budgets)

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"
SECRET = base64.b64encode(b"a-32-byte-secret-for-the-tests!!").decode()


def _jwt(claims: dict) -> str:
    seg = lambda obj: base64.urlsafe_b64encode(          # noqa: E731
        json.dumps(obj).encode()).decode().rstrip("=")
    return f"{seg({'alg': 'HS256'})}.{seg(claims)}.sig"


class FakeGateway:
    """The identity service + the gateway as the laptop showed them: a token that lives
    599 s, the slash path, answers scripted per model call (a list of
    parts, or a callable of the request body)."""

    def __init__(self, answers=()):
        self.answers = list(answers)
        self.now = 1_700_000_000.0
        self.calls: list[dict] = []
        self.minted = 0
        self.dead: set[str] = set()
        self.fail_next: list[int] = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds

    def http(self, method, url, headers, body, **kw):
        if url.endswith("/application/token"):
            self.minted += 1
            token = _jwt({"exp": int(self.now) + 599, "n": self.minted})
            return 200, {}, json.dumps({"authorization_token": token}).encode()
        assert "/models/gemini-2.5-pro/generateContent" in url, url
        token = headers["Authorization"].split(" ", 1)[1]
        payload = json.loads(body)
        self.calls.append({"url": url, "token": token, "body": payload})
        claims = json.loads(base64.urlsafe_b64decode(
            token.split(".")[1] + "=="))
        if token in self.dead or claims["exp"] <= self.now:
            return 401, {"WWW-Authenticate": "Bearer"}, b""
        if self.fail_next:
            return self.fail_next.pop(0), {}, b'{"error":{"message":"boom"}}'
        answer = self.answers.pop(0) if self.answers else {
            "parts": [{"text": "nothing scripted"}]}
        if callable(answer):
            answer = answer(payload)
        return 200, {}, json.dumps({
            "candidates": [{"content": {"parts": answer["parts"]},
                            "finishReason": answer.get("finish", "STOP")}],
            "usageMetadata": answer.get("usage", {
                "promptTokenCount": 100, "candidatesTokenCount": 20,
                "thoughtsTokenCount": 30})}).encode()


def _client(fake: FakeGateway, **cfg) -> GatewayClient:
    config = Config(app_id="app", secret=SECRET, **cfg)
    return GatewayClient(cfg=config, http=fake.http, sleep=fake.sleep,
                     tokens=TokenManager(config, fake.http, now=fake.clock))


def test_token_manager_mints_reuses_refreshes_and_invalidates():
    fake = FakeGateway()
    cfg = Config(app_id="app", secret=SECRET)
    tokens = TokenManager(cfg, fake.http, now=fake.clock)
    assert tokens.describe().startswith("no token yet")
    first = tokens.token()
    assert fake.minted == 1 and tokens.mints == 1
    assert 598 <= tokens.remaining() <= 599
    assert tokens.token() == first                  # reused, no mint
    fake.now += 400                                 # inside 80% of 599
    assert tokens.token() == first
    fake.now += 100                                 # past 479 s: refresh
    second = tokens.token()
    assert second != first and fake.minted == 2
    assert "refresh at 479 s" in tokens.describe()
    tokens.invalidate()
    assert tokens.remaining() == 0.0
    assert tokens.token() != second and fake.minted == 3
    # the environment's bearer is used as it is, never minted
    env_tokens = TokenManager(Config(auth_mode="env", bearer=_jwt(
        {"exp": int(fake.now) + 100})), fake.http, now=fake.clock)
    assert env_tokens.token().startswith("eyJ") and fake.minted == 3
    with pytest.raises(GatewayError):
        TokenManager(Config(auth_mode="env"), fake.http).token()
    with pytest.raises(GatewayError):
        TokenManager(Config(), fake.http).token()   # no credentials


def test_converse_delivers_one_call_as_the_loops_events():
    fake = FakeGateway([{"parts": [
        {"thought": True, "text": "search first"},
        {"text": "Looking that up."},
        {"functionCall": {"name": "search", "args": {"query": "spend"}},
         "thoughtSignature": "sig=="}]}])
    client = _client(fake)
    contents = [{"role": "user", "parts": [{"text": "spend by day?"}]}]
    tools = [{"name": "search", "description": "find", "parameters": {
        "type": "OBJECT", "properties": {"query": {"type": "STRING"}}}}]
    events = list(client.converse(contents, system="You are Synapse",
                                  tools=tools, thinking_level="low",
                                  max_output_tokens=16384))
    assert [e["kind"] for e in events] == ["thought", "text", "call", "done"]
    assert events[0]["delta"] == "search first"
    assert events[2] == {"kind": "call", "name": "search",
                         "args": {"query": "spend"}, "id": ""}
    done = events[-1]
    assert done["finish"] == "STOP"
    assert done["parts"][2]["thoughtSignature"] == "sig=="   # verbatim
    assert done["usage"] == {"prompt_tokens": 100, "output_tokens": 20,
                             "thought_tokens": 30, "cached_tokens": 0}
    assert client.usage == {"calls": 1, "prompt_tokens": 100,
                            "output_tokens": 20, "thought_tokens": 30}
    # the request: the guide's slash path, the bearer, a budget under
    # a cap that leaves room, the system instruction and the tools
    sent = fake.calls[0]
    assert sent["url"].endswith("/models/gemini-2.5-pro/generateContent")
    assert sent["body"]["systemInstruction"] == {"parts": [{"text": "You are Synapse"}]}
    assert sent["body"]["tools"] == [{"functionDeclarations": tools}]
    assert sent["body"]["generationConfig"] == {
        "maxOutputTokens": 16384 + 1024,
        "thinkingConfig": {"includeThoughts": True, "thinkingBudget": 1024}}


def test_a_dead_token_mid_turn_is_minted_anew_and_the_call_retried_once():
    fake = FakeGateway([{"parts": [{"text": "after the refresh"}]}])
    client = _client(fake)
    first = client.tokens.token()
    fake.dead.add(first)                            # the gateway says 401
    events = list(client.converse(
        [{"role": "user", "parts": [{"text": "hi"}]}], thinking_level="medium"))
    assert events[-1]["kind"] == "done"
    assert events[0] == {"kind": "text", "delta": "after the refresh"}
    assert fake.minted == 2
    assert [c["token"] == first for c in fake.calls] == [True, False]
    # a second 401 in the same call is a refusal, not a loop
    fake.dead.add(client.tokens.token())
    fake.answers = []
    fake2 = FakeGateway()
    client2 = _client(fake2)
    token2 = client2.tokens.token()
    fake2.dead.add(token2)
    fake2.http_orig = fake2.http

    def always_401(method, url, headers, body, **kw):
        if url.endswith("/application/token"):
            return fake2.http_orig(method, url, headers, body, **kw)
        return 401, {}, b""
    client2.http = always_401
    with pytest.raises(EnrichTransportError) as err:
        list(client2.converse([{"role": "user", "parts": [{"text": "hi"}]}]))
    assert "401" in str(err.value)


def test_transient_refusals_back_off_and_max_tokens_grows_the_cap_once():
    fake = FakeGateway([{"parts": [{"thought": True, "text": "…"}],
                     "finish": "MAX_TOKENS"},
                    {"parts": [{"text": "done"}]}])
    fake.fail_next = [503]
    client = _client(fake)
    events = list(client.converse(
        [{"role": "user", "parts": [{"text": "hi"}]}],
        thinking_level="high", max_output_tokens=8192))
    assert [e["kind"] for e in events] == ["text", "done"]
    caps = [c["body"]["generationConfig"]["maxOutputTokens"]
            for c in fake.calls]
    assert caps == [8192 + 16384, 8192 + 16384, (8192 + 16384) * 2]
    assert fake.now > 1_700_000_000.0                 # it slept the backoff


def test_the_one_shot_json_path_and_the_burst_stream():
    from sahs.ask.model import VertexModel
    fake = FakeGateway([{"parts": [{"text": '{"ok": true}'}]},
                    {"parts": [{"text": "A whole answer at once."}]}])
    client = _client(fake)
    assert VertexModel(client).json('Return {"ok": true}') == {"ok": True}
    config = fake.calls[0]["body"]["generationConfig"]
    assert config["responseMimeType"] == "application/json"
    assert config["thinkingConfig"] == {"includeThoughts": False,
                                        "thinkingBudget": 512}
    assert config["maxOutputTokens"] == 1024 + 512
    assert list(client.generate_stream("tell me")) == ["A whole answer at once."]


def test_the_plane_switch_and_the_budgets():
    assert model_plane({}) == "vertex"
    assert model_plane({"APP_ID": "a", "APP_SECRET": "s"}) == "gateway"
    assert model_plane({"GEMINI_BEARER_TOKEN": "t"}) == "gateway"
    assert model_plane({"APP_ID": "a", "APP_SECRET": "s",
                        "SAHS_MODEL_PLANE": "vertex"}) == "vertex"
    assert model_plane({"SAHS_MODEL_PLANE": "gateway"}) == "gateway"
    assert plane_note({"APP_ID": "a", "APP_SECRET": "s"}) == \
        "SAHS_MODEL_PLANE unset: the gateway credentials present"
    assert plane_note({"SAHS_MODEL_PLANE": "gateway"}) == "SAHS_MODEL_PLANE=gateway"
    assert thinking_budgets({})["medium"] == 4096
    assert thinking_budgets({"GATEWAY_THINKING_BUDGETS": "low:512, high:8192",
                             "GATEWAY_JSON_THINKING_BUDGET": "256"}) == {
        "low": 512, "medium": 4096, "high": 8192, "json": 256}


def test_the_agent_factory_picks_the_plane_and_teaches_when_unconfigured(
        monkeypatch):
    from sahs.ask.model import ModelUnavailable
    from sahs.assistant.agent import GatewayAgent, agent_from_env
    monkeypatch.delenv("APP_ID", raising=False)
    monkeypatch.delenv("APP_SECRET", raising=False)
    monkeypatch.delenv("GEMINI_BEARER_TOKEN", raising=False)
    monkeypatch.setenv("SAHS_MODEL_PLANE", "gateway")
    with pytest.raises(ModelUnavailable) as err:
        agent_from_env()
    assert "APP_ID and APP_SECRET" in str(err.value)
    monkeypatch.setenv("APP_ID", "app")
    monkeypatch.setenv("APP_SECRET", SECRET)
    agent = agent_from_env()
    assert isinstance(agent, GatewayAgent) and agent.client.plane == "gateway"
    assert agent.client.cfg.model == "gemini-2.5-pro"


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("gateway_plane")
    graph_dir = tmp / "graph"
    result = subprocess.run(
        [sys.executable, str(SILO / "scripts" / "laptop.py"),
         "build-graph", "--graph", str(graph_dir),
         "--crosswalk", str(FX / "identity" / "crosswalk.jsonl"),
         "--bq-archive", str(FX / "real_extractions_production"),
         "--mdm-archive", str(FX / "mdm_46_patched_v2"),
         "--sources-dir", str(FX / "sources"),
         "--registry", str(FX / "sources" / "tables_registry.txt"),
         "--out", str(tmp / "run"), "--plain", "--run-id", "eagplane"],
        capture_output=True, text=True, cwd=SILO)
    assert result.returncode == 0, result.stderr[-800:]
    from sahs.compiler.compile import compile_build
    from sahs.tools.api import Build
    _d, _m, failures = compile_build(graph_dir, tmp / "builds")
    assert not failures
    return Build.open(tmp / "builds"), tmp


def test_a_whole_turn_rides_the_gateway_plane(compiled):
    """The loop, the hooks, the store and the events, with the model
    on the gateway: a data question that searches, then answers with chips —
    two calls, each delivered whole, the thinking kept, the token
    minted once."""
    from sahs.assistant import AssistantRuntime
    from sahs.assistant.agent import GatewayAgent
    build, tmp = compiled

    def first_call(body):
        assert body["systemInstruction"]["parts"][0]["text"].startswith(
            "<identity>")
        assert any(t["name"] == "search" for t in
                   body["tools"][0]["functionDeclarations"])
        return {"parts": [
            {"thought": True, "text": "Find the spend metric first."},
            {"functionCall": {"name": "search", "args": {"query": "spend"}},
             "thoughtSignature": "sig=="}]}

    def second_call(body):
        # the echo: the model turn verbatim, then the tool's answer
        assert body["contents"][-2]["role"] == "model"
        assert body["contents"][-2]["parts"][-1]["thoughtSignature"] == "sig=="
        fr = body["contents"][-1]["parts"][0]["functionResponse"]
        assert fr["name"] == "search" and "results" in fr["response"]
        return {"parts": [
            {"thought": True, "text": "One certified spend metric."},
            {"text": "The certified spend metric is **Acquirer Net Spend** "
                     "on dw.gms_transaction."},
            {"functionCall": {"name": "suggest_next", "args": {
                "options": ["chart it by day"]}}}]}

    fake = FakeGateway([first_call, second_call])
    client = _client(fake)
    runtime = AssistantRuntime(
        builds_root=build.root.parent, graph_root=tmp / "graph",
        store_path=tmp / "chat.sqlite3",
        model_factory=lambda budget: GatewayAgent(client, budget))
    session = runtime.create_session()
    runtime.start_turn(session["id"], "which spend metric is certified?")
    assert runtime.wait(session["id"], 60)
    events = runtime.runtime(session["id"]).bus.since(0)
    by = lambda name: [e for e in events if e["ev"] == name]   # noqa: E731
    assert by("turn_done")[-1]["status"] == "answered"
    assert [e["delta"] for e in by("thinking")] == [
        "Find the spend metric first.", "One certified spend metric."]
    assert by("tool_call")[0]["tool"] == "search"
    assert "Acquirer Net Spend" in "".join(e["delta"] for e in by("say_token"))
    assert by("chips")[0]["suggestions"] == ["chart it by day"]
    assert len(fake.calls) == 2 and fake.minted == 1
    assert client.usage["calls"] == 2
    stored = runtime.store.messages(session["id"])[-1]
    assert stored["payload"]["trace"][0]["kind"] == "thought"
    assert runtime.model_label == "scripted"     # a factory is a factory


def test_the_plane_catalog_names_both_planes_and_why_one_cannot_be_ridden(
        monkeypatch, tmp_path):
    """The composer's catalog: both planes always listed, availability
    read from the environment each time, the reason when a plane is
    not configured, and which one a new chat starts on."""
    from sahs.assistant.agent import agent_for, plane_catalog
    from sahs.ask.model import ModelUnavailable
    for var in ("APP_ID", "APP_SECRET", "GEMINI_BEARER_TOKEN",
                "SAHS_MODEL_PLANE", "VERTEX_PROJECT_ID",
                "SYNAPSE_VERTEX_PROJECT", "GOOGLE_CLOUD_PROJECT",
                "SYNAPSE_VERTEX_SA_KEY", "GOOGLE_APPLICATION_CREDENTIALS",
                "VERTEX_MODEL", "SYNAPSE_VERTEX_MODEL", "GEMINI_MODEL",
                "GATEWAY_MODEL"):
        monkeypatch.delenv(var, raising=False)
    rows = {r["id"]: r for r in plane_catalog()}
    assert list(rows) == ["vertex", "gateway"]
    assert rows["vertex"]["label"] == "Gemini 3.1 Pro Preview"
    assert rows["gateway"]["label"] == "Gemini 2.5 Pro"
    assert rows["vertex"]["plane_name"] == "Vertex"
    assert rows["gateway"]["plane_name"] == "Gateway"
    assert not rows["vertex"]["available"] and "SYNAPSE_VERTEX_SA_KEY" in \
        rows["vertex"]["reason"]
    assert not rows["gateway"]["available"] and "APP_ID" in rows["gateway"]["reason"]
    assert rows["vertex"]["default"] and not rows["gateway"]["default"]
    assert rows["vertex"]["feel"] == "streams"
    assert rows["gateway"]["feel"] == "whole calls"
    # the gateway configured: available, and the default for a new chat
    monkeypatch.setenv("APP_ID", "app")
    monkeypatch.setenv("APP_SECRET", SECRET)
    rows = {r["id"]: r for r in plane_catalog()}
    assert rows["gateway"]["available"] and rows["gateway"]["default"]
    assert not rows["vertex"]["default"]
    # Vertex configured too (a key file that exists): both available,
    # the .env still names the default
    key = tmp_path / "sa.json"
    key.write_text("{}")
    monkeypatch.setenv("SYNAPSE_VERTEX_SA_KEY", str(key))
    monkeypatch.setenv("VERTEX_PROJECT_ID", "prj")
    monkeypatch.setenv("SAHS_MODEL_PLANE", "vertex")
    rows = {r["id"]: r for r in plane_catalog()}
    assert rows["vertex"]["available"] and rows["vertex"]["default"]
    assert rows["gateway"]["available"] and not rows["gateway"]["default"]
    # the factory by name: an unknown plane is a typed refusal
    with pytest.raises(ModelUnavailable) as err:
        agent_for("gpt")
    assert "vertex and gateway" in str(err.value)
    assert agent_for("gateway").client.plane == "gateway"


def test_a_chat_switches_planes_from_the_composer(compiled, monkeypatch,
                                                  tmp_path):
    """The switch is remembered on the chat and rides the next message;
    a message can name a plane for itself; the turn record says which
    plane served it; the dials catalog explains all three dials; and a
    plane this machine cannot ride is refused with the reason before
    anything is stored."""
    from sahs.ask.model import ModelUnavailable
    from sahs.assistant import AssistantRuntime
    from sahs.assistant.agent import ScriptedAgent
    build, tmp = compiled
    monkeypatch.setenv("APP_ID", "app")
    monkeypatch.setenv("APP_SECRET", SECRET)
    monkeypatch.setenv("SAHS_MODEL_PLANE", "auto")
    heard: list[str] = []

    def factory(budget, plane):        # a factory that hears the switch
        heard.append(plane)
        return ScriptedAgent(steps=[[{"text": f"answered on {plane}"}]])

    runtime = AssistantRuntime(
        builds_root=build.root.parent, graph_root=tmp / "graph",
        store_path=tmp_path / "chat.sqlite3", model_factory=factory)
    session = runtime.create_session()
    assert session["model"] == ""                  # the .env default
    assert runtime.plane_for(session) == "gateway"     # auto → the gateway here
    dials = runtime.dials()
    assert [d["id"] for d in dials["depths"]] == ["quick", "standard",
                                                  "deep"]
    assert dials["depths"][2]["on"] == {"vertex": "thinking level high",
                                        "gateway": "16,384 thinking tokens "
                                               "per call"}
    assert [m["id"] for m in dials["modes"]] == ["chat", "autopilot"]
    assert [p["id"] for p in dials["planes"]] == ["vertex", "gateway"]
    # the first message names Vertex for itself: remembered
    started = runtime.start_turn(session["id"], "hello", model="vertex")
    assert started["plane"] == "vertex"
    assert runtime.wait(session["id"], 30)
    assert runtime.store.get_session(session["id"])["model"] == "vertex"
    events = runtime.runtime(session["id"]).bus.since(0)
    assert events[0]["ev"] == "turn_started" and events[0]["plane"] == "vertex"
    assert "answered on vertex" in "".join(
        e.get("delta", "") for e in events if e["ev"] == "say_token")
    # the next message names nothing: it rides the remembered plane
    runtime.start_turn(session["id"], "again")
    assert runtime.wait(session["id"], 30)
    assert heard == ["vertex", "vertex"]
    # the switch from the composer, then a message on the new plane
    assert runtime.set_session_model(session["id"], "gateway") == {
        "ok": True, "plane": "gateway", "model": "scripted"}
    runtime.start_turn(session["id"], "and now")
    assert runtime.wait(session["id"], 30)
    assert heard[-1] == "gateway"
    # '' forgets the switch: back to the .env default (the gateway here)
    assert runtime.set_session_model(session["id"], "")["plane"] == "gateway"
    assert runtime.store.get_session(session["id"])["model"] == ""
    # an unknown plane is refused before anything is stored
    before = len(runtime.store.messages(session["id"]))
    with pytest.raises(ModelUnavailable):
        runtime.start_turn(session["id"], "on gpt", model="gpt")
    assert len(runtime.store.messages(session["id"])) == before
    # without a factory the environment decides: Vertex is not
    # configured on this machine, so picking it is refused with why
    runtime._model_factory = None
    monkeypatch.delenv("SYNAPSE_VERTEX_SA_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    with pytest.raises(ModelUnavailable) as err:
        runtime.set_session_model(session["id"], "vertex")
    assert "not configured on this machine" in str(err.value)
    assert runtime.label_for("gateway") == "Gemini 2.5 Pro"
    assert runtime.model_label == "Gemini 2.5 Pro"
