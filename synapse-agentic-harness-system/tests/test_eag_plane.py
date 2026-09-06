"""The EAG plane: the token manager, the client that delivers each
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
from sahs.enrich.eag_client import EagClient
from sahs.util.eag import (Config, EagError, TokenManager, model_plane,
                           plane_note, thinking_budgets)

SILO = Path(__file__).resolve().parents[1]
FX = SILO / "tests" / "fixtures"
SECRET = base64.b64encode(b"a-32-byte-secret-for-the-tests!!").decode()


def _jwt(claims: dict) -> str:
    seg = lambda obj: base64.urlsafe_b64encode(          # noqa: E731
        json.dumps(obj).encode()).decode().rstrip("=")
    return f"{seg({'alg': 'HS256'})}.{seg(claims)}.sig"


class FakeEag:
    """OneIdentity + EAG as the laptop showed them: a token that lives
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


def _client(fake: FakeEag, **cfg) -> EagClient:
    config = Config(app_id="app", secret=SECRET, **cfg)
    return EagClient(cfg=config, http=fake.http, sleep=fake.sleep,
                     tokens=TokenManager(config, fake.http, now=fake.clock))


def test_token_manager_mints_reuses_refreshes_and_invalidates():
    fake = FakeEag()
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
    with pytest.raises(EagError):
        TokenManager(Config(auth_mode="env"), fake.http).token()
    with pytest.raises(EagError):
        TokenManager(Config(), fake.http).token()   # no credentials


def test_converse_delivers_one_call_as_the_loops_events():
    fake = FakeEag([{"parts": [
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
    fake = FakeEag([{"parts": [{"text": "after the refresh"}]}])
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
    fake2 = FakeEag()
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
    fake = FakeEag([{"parts": [{"thought": True, "text": "…"}],
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
    fake = FakeEag([{"parts": [{"text": '{"ok": true}'}]},
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
    assert model_plane({"APP_ID": "a", "APP_SECRET": "s"}) == "eag"
    assert model_plane({"GEMINI_BEARER_TOKEN": "t"}) == "eag"
    assert model_plane({"APP_ID": "a", "APP_SECRET": "s",
                        "SAHS_MODEL_PLANE": "vertex"}) == "vertex"
    assert model_plane({"SAHS_MODEL_PLANE": "eag"}) == "eag"
    assert plane_note({"APP_ID": "a", "APP_SECRET": "s"}) == \
        "SAHS_MODEL_PLANE unset: EAG credentials present"
    assert plane_note({"SAHS_MODEL_PLANE": "eag"}) == "SAHS_MODEL_PLANE=eag"
    assert thinking_budgets({})["medium"] == 4096
    assert thinking_budgets({"EAG_THINKING_BUDGETS": "low:512, high:8192",
                             "EAG_JSON_THINKING_BUDGET": "256"}) == {
        "low": 512, "medium": 4096, "high": 8192, "json": 256}


def test_the_agent_factory_picks_the_plane_and_teaches_when_unconfigured(
        monkeypatch):
    from sahs.ask.model import ModelUnavailable
    from sahs.assistant.agent import EagAgent, agent_from_env
    monkeypatch.delenv("APP_ID", raising=False)
    monkeypatch.delenv("APP_SECRET", raising=False)
    monkeypatch.delenv("GEMINI_BEARER_TOKEN", raising=False)
    monkeypatch.setenv("SAHS_MODEL_PLANE", "eag")
    with pytest.raises(ModelUnavailable) as err:
        agent_from_env()
    assert "APP_ID and APP_SECRET" in str(err.value)
    monkeypatch.setenv("APP_ID", "app")
    monkeypatch.setenv("APP_SECRET", SECRET)
    agent = agent_from_env()
    assert isinstance(agent, EagAgent) and agent.client.plane == "eag"
    assert agent.client.cfg.model == "gemini-2.5-pro"


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("eag_plane")
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


def test_a_whole_turn_rides_the_eag_plane(compiled):
    """The loop, the hooks, the store and the events, with the model
    on EAG: a data question that searches, then answers with chips —
    two calls, each delivered whole, the thinking kept, the token
    minted once."""
    from sahs.assistant import AssistantRuntime
    from sahs.assistant.agent import EagAgent
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

    fake = FakeEag([first_call, second_call])
    client = _client(fake)
    runtime = AssistantRuntime(
        builds_root=build.root.parent, graph_root=tmp / "graph",
        store_path=tmp / "chat.sqlite3",
        model_factory=lambda budget: EagAgent(client, budget))
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
