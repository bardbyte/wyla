"""One plane, several engines: the gateway's model list from the
environment, each model's engine map (its thinking style, the levels
it accepts, the dial folded onto them, its cap), the composer's
catalog of choices (a plane, or plane:model), the sampling policy and
the style the Gemini 3 family gets, and the runtime's switch between
models. Nothing here touches a network."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_gateway_plane import SECRET, compiled  # noqa: E402,F401

from sahs.assistant.agent import join_choice, model_catalog, split_choice  # noqa: E402
from sahs.enrich.gateway_client import GatewayClient  # noqa: E402
from sahs.util.gateway import (DEFAULT_MODEL, DEFAULT_SCOPES,  # noqa: E402
                               EMBEDDING_SCOPES, Config, GatewayError,
                               gateway_models, output_cap, scopes_for,
                               thinking_kind, thinking_levels)
from sahs.util.profiles import (PROFILES, profile_for, prompt_style,  # noqa: E402
                                temperature_for)

HOSTS = {"IDP_TOKEN_URL": "https://identity.example/security/digital/v1/application/token",
         "GATEWAY_BASE_URL": "https://gateway.example/genai/google/v1"}
MODELS = "gemini-3.7-flash gemini-3.5-flash,gemini-3.1-flash-lite"
LEGACY = "gemini-2.5-pro"           # retiring: an .env may still name it
SCOPE = "/genai/google/v1/models/{}/**::post"


def test_the_model_list_comes_from_the_environment_default_first():
    assert DEFAULT_MODEL == "gemini-3.7-flash"                        # 2.5 Pro is retiring
    assert gateway_models({}) == ["gemini-3.7-flash"]
    assert gateway_models({"GATEWAY_MODEL": "gemini-3.5-flash"}) == ["gemini-3.5-flash"]
    listed = gateway_models({"GATEWAY_MODELS": MODELS})
    assert listed == ["gemini-3.7-flash", "gemini-3.5-flash", "gemini-3.1-flash-lite"]
    # GATEWAY_MODEL names the default: it comes first whatever the list's order
    assert gateway_models({"GATEWAY_MODELS": MODELS, "GATEWAY_MODEL": "gemini-3.5-flash"})[0] == "gemini-3.5-flash"
    # a default the list does not name is added, first — a 2.5 the .env still names included
    assert gateway_models({"GATEWAY_MODELS": "gemini-3.5-flash", "GATEWAY_MODEL": LEGACY}) == [
        LEGACY, "gemini-3.5-flash"]


def test_scopes_follow_the_models_unless_given():
    assert scopes_for({}) == DEFAULT_SCOPES == [SCOPE.format("gemini-3.7-flash"), *EMBEDDING_SCOPES]
    derived = scopes_for({"GATEWAY_MODELS": "gemini-3.7-flash gemini-3.5-flash"})
    assert derived[:2] == [SCOPE.format("gemini-3.7-flash"), SCOPE.format("gemini-3.5-flash")]
    assert derived[2:] == EMBEDDING_SCOPES                            # the embedding scopes ride along
    # GATEWAY_MODEL alone derives its own scope: no fixed list to fall back on
    assert scopes_for({"GATEWAY_MODEL": "gemini-3.1-flash-lite"})[0] == SCOPE.format("gemini-3.1-flash-lite")
    given = scopes_for({"GATEWAY_MODELS": MODELS, "GATEWAY_SCOPES": "/a/**::post, /b/**::post"})
    assert given == ["/a/**::post", "/b/**::post"]                    # the gateway team's list wins
    cfg = Config.from_env({"GATEWAY_MODELS": MODELS, **HOSTS})
    assert cfg.model == "gemini-3.7-flash" and cfg.models == gateway_models({"GATEWAY_MODELS": MODELS})
    assert cfg.display()["models"] == cfg.models


def test_each_engine_has_its_map_and_the_dial_folds_onto_it():
    # the table is keyed by stem: a preview suffix finds its row
    pro = profile_for("gemini-3.1-pro-preview")
    assert pro.thinking == "level" and pro.accepts == ("low", "medium", "high")
    assert pro.family == "gemini-3" and pro.source == "docs"
    assert pro.depth_levels == {"minimal": "low", "low": "low", "medium": "medium",
                                "high": "high", "max": "high", "json": "low"}
    flash37 = profile_for("gemini-3.7-flash")
    assert flash37.accepts == ("low", "medium", "high") and flash37.level_for("minimal") == "low"
    flash35 = profile_for("gemini-3.5-flash")
    assert flash35.accepts == ("medium", "high")                     # medium is its floor
    assert flash35.depth_levels == {"minimal": "medium", "low": "medium", "medium": "medium",
                                    "high": "high", "max": "high", "json": "medium"}
    lite = profile_for("gemini-3.1-flash-lite")
    assert lite.accepts == ("minimal", "low", "medium", "high")
    assert lite.level_for("minimal") == "minimal" and lite.level_for("json") == "minimal"
    assert "JSON" in lite.facts and "quick checks" in lite.fit
    # a model the table does not name gets its family's map
    assert profile_for("gemini-3.9-ultra").accepts == ("low", "medium", "high")
    legacy = profile_for(LEGACY)
    assert legacy.thinking == "budget" and legacy.accepts == ()
    assert legacy.family == "gemini-2.5" and "Retiring" in legacy.facts
    assert "compatibility" in legacy.fit
    assert legacy.level_for("minimal") == "minimal"                   # nothing to fold onto
    assert profile_for("gpt-5").thinking == "budget" and profile_for("gpt-5").family == "other"
    # the environment's word: the probe's levels, a forced style, a cap
    env = {"GATEWAY_MODEL_LEVELS": "gemini-3.5-flash:low|medium|high, gemini-3.7-flash:medium high",
           "GATEWAY_MODEL_THINKING": "gemini-3.1-flash-lite:none",
           "GATEWAY_MODEL_CAPS": "gemini-3.1-flash-lite:8192"}
    probed = profile_for("gemini-3.5-flash", env)
    assert probed.accepts == ("low", "medium", "high") and probed.source == "env"
    assert profile_for("gemini-3.7-flash", env).level_for("minimal") == "medium"
    forced = profile_for("gemini-3.1-flash-lite", env)
    assert forced.thinking == "none" and forced.cap == 8192
    # nonsense in the .env leaves the table's word
    assert profile_for("gemini-3.5-flash", {"GATEWAY_MODEL_LEVELS": "gemini-3.5-flash:sideways"}).accepts == ("medium", "high")
    assert profile_for("gemini-3.5-flash", {"GATEWAY_MODEL_THINKING": "gemini-3.5-flash:sideways"}).thinking == "level"
    # the rows the composer reads
    assert all(p.as_row()["levels"] == list(p.accepts) for p in PROFILES.values())


def test_thinking_style_levels_and_cap_read_the_engine_map():
    assert thinking_kind(LEGACY) == "budget"
    assert thinking_kind("gemini-2.5-flash-lite") == "budget"
    assert thinking_kind("gemini-3.5-flash") == "level"
    assert thinking_kind("gemini-3.1-flash-lite") == "level"
    assert thinking_kind("gemini-3.1-flash-lite", {"GATEWAY_MODEL_THINKING": "gemini-3.1-flash-lite:none"}) == "none"
    assert thinking_kind("gemini-3.5-flash", {"GATEWAY_MODEL_THINKING": "gemini-3.5-flash:sideways"}) == "level"
    # no model named: the common three, the ends folded in, the JSON one-shots shallow
    assert thinking_levels({}) == {"minimal": "low", "low": "low", "medium": "medium",
                                   "high": "high", "max": "high", "json": "low"}
    # a model named: ITS levels
    assert thinking_levels({}, "gemini-3.5-flash")["low"] == "medium"
    assert thinking_levels({}, "gemini-3.1-flash-lite")["minimal"] == "minimal"
    assert thinking_levels({}, "gemini-3.1-flash-lite")["json"] == "minimal"
    # GATEWAY_THINKING_LEVELS is the deployment's last word, for every model
    assert thinking_levels({"GATEWAY_THINKING_LEVELS": "medium:high,minimal:minimal,bogus:x"}) == {
        "minimal": "minimal", "low": "low", "medium": "high", "high": "high", "max": "high", "json": "low"}
    assert thinking_levels({"GATEWAY_THINKING_LEVELS": "minimal:low"}, "gemini-3.1-flash-lite")["minimal"] == "low"
    assert output_cap(LEGACY) == 65536
    assert output_cap("gemini-3.1-flash-lite", {"GATEWAY_MODEL_CAPS": "gemini-3.1-flash-lite:8192"}) == 8192
    assert output_cap("gemini-3.1-flash-lite", {"GATEWAY_MODEL_CAPS": "gemini-3.1-flash-lite:lots"}) == 65536


def _client(model: str = "gemini-3.7-flash") -> GatewayClient:
    cfg = Config(app_id="app", secret=SECRET, model=model,
                 models=["gemini-3.7-flash", "gemini-3.5-flash", "gemini-3.1-flash-lite", LEGACY], **{
                     "token_url": HOSTS["IDP_TOKEN_URL"], "base_url": HOSTS["GATEWAY_BASE_URL"]})
    # for_model reads the engine map the way from_env does
    return GatewayClient(cfg=cfg, tokens=None, http=None).for_model(model)


def test_each_model_takes_its_depth_its_own_way(monkeypatch):
    monkeypatch.setenv("GATEWAY_MODEL_THINKING", "gemini-3.1-flash-lite:none")
    monkeypatch.setenv("GATEWAY_MODEL_CAPS", "gemini-3.1-flash-lite:8192")
    flash = _client()
    assert flash.thinking == "level" and flash.model == "gemini-3.7-flash"
    assert flash._url().endswith("/models/gemini-3.7-flash/generateContent")
    assert flash._config("low", 8192) == {"maxOutputTokens": 8192,
                                          "thinkingConfig": {"includeThoughts": True, "thinkingLevel": "low"}}
    # minimal folds onto low: 3.7 Flash refuses minimal
    assert flash._config("minimal", 8192)["thinkingConfig"]["thinkingLevel"] == "low"
    # the JSON one-shots ride the shallowest level, never the literal "json";
    # a None extra (the temperature left at the model's default) is left out
    assert flash._config("json", 1024, False, temperature=None,
                         responseMimeType="application/json") == {
        "maxOutputTokens": 1024, "responseMimeType": "application/json",
        "thinkingConfig": {"includeThoughts": False, "thinkingLevel": "low"}}
    assert flash._config("json", 1024, False, temperature=0.2)["temperature"] == 0.2
    flash35 = flash.for_model("gemini-3.5-flash")
    assert flash35.model == "gemini-3.5-flash"
    assert flash35._config("low", 8192)["thinkingConfig"]["thinkingLevel"] == "medium"    # its floor
    assert flash35._config("medium", 8192, False)["thinkingConfig"] == {"includeThoughts": False, "thinkingLevel": "medium"}
    lite = flash.for_model("gemini-3.1-flash-lite")
    assert lite.thinking == "none" and lite.cap == 8192
    assert lite._config("high", 65536) == {"maxOutputTokens": 8192}     # no thinkingConfig, the model's cap
    monkeypatch.delenv("GATEWAY_MODEL_THINKING")
    lite = flash.for_model("gemini-3.1-flash-lite")
    assert lite.thinking == "level"
    assert lite._config("minimal", 100)["thinkingConfig"]["thinkingLevel"] == "minimal"   # reaches the model
    # a 2.5 the .env still names: the budget dialect, the cap raised by the budget
    legacy = flash.for_model(LEGACY)
    assert legacy.thinking == "budget"
    assert legacy._config("high", 8192) == {"maxOutputTokens": 8192 + 16384,
                                            "thinkingConfig": {"includeThoughts": True, "thinkingBudget": 16384}}
    # the token manager and the route are shared; the counters are not
    assert flash35.tokens is flash.tokens and flash35.usage is not flash.usage
    # a rejected thinkingConfig turns any style off for the run
    flash35.thinking_ok = False
    assert "thinkingConfig" not in flash35._config("high", 100)
    with pytest.raises(GatewayError, match="no gateway model called 'gpt'"):
        flash.for_model("gpt")


def test_gemini_3_keeps_the_default_temperature_and_gets_a_style(monkeypatch):
    monkeypatch.delenv("SAHS_TEMPERATURE_POLICY", raising=False)
    assert temperature_for("gemini-3.7-flash", 0.0) is None            # the model's default (Google)
    assert temperature_for("gemini-3.1-pro-preview", 0.2) is None
    assert temperature_for(LEGACY, 0.2) == 0.2                          # the older dialect takes the number
    assert temperature_for("gemini-3.7-flash", 0.0, {"SAHS_TEMPERATURE_POLICY": "explicit"}) == 0.0
    assert "Lead with the answer" in prompt_style("gemini-3.5-flash")
    assert prompt_style(LEGACY) == "" and prompt_style("") == ""


class _Fake:
    """A client with a model name, usage counters and a JSON answer."""

    def __init__(self, model: str):
        self.model = model
        self.usage = {"calls": 0, "prompt_tokens": 0, "output_tokens": 0}
        self.seen: list[dict] = []
        self.notes: list[str] = []

    def generate(self, prompt, *, temperature=0.2, max_output_tokens=1024):
        self.seen.append({"temperature": temperature, "max": max_output_tokens})
        return '{"ok": true}'

    def for_model(self, model):
        if model != "gemini-3.1-flash-lite":
            raise GatewayError(f"no gateway model called {model!r}")
        return _Fake(model)

    def _note(self, message):
        self.notes.append(message)


def test_the_agents_apply_the_sampling_policy_and_route_the_json_calls(monkeypatch):
    from sahs.assistant.agent import GatewayAgent, VertexAgent
    monkeypatch.delenv("SAHS_TEMPERATURE_POLICY", raising=False)
    monkeypatch.delenv("GATEWAY_JSON_MODEL", raising=False)
    flash = _Fake("gemini-3.7-flash")
    assert VertexAgent(flash).json("q", temperature=0.0) == {"ok": True}
    assert flash.seen[-1]["temperature"] is None                       # Gemini 3: the model's default
    legacy = _Fake(LEGACY)
    VertexAgent(legacy).json("q", temperature=0.1)
    assert legacy.seen[-1]["temperature"] == 0.1
    monkeypatch.setenv("SAHS_TEMPERATURE_POLICY", "explicit")
    VertexAgent(flash).json("q", temperature=0.0)
    assert flash.seen[-1]["temperature"] == 0.0
    monkeypatch.delenv("SAHS_TEMPERATURE_POLICY")
    # the gateway's one-shots ride GATEWAY_JSON_MODEL when the plane serves it
    agent = GatewayAgent(_Fake("gemini-3.7-flash"))
    assert agent._json_client() is agent.client
    monkeypatch.setenv("GATEWAY_JSON_MODEL", "gemini-3.1-flash-lite")
    lite = agent._json_client()
    assert lite.model == "gemini-3.1-flash-lite" and agent._json_client() is lite   # cached
    assert agent.json("q") == {"ok": True}
    assert lite.seen and not agent.client.seen
    # a model the plane does not serve: the calls stay home, the log says why
    other = GatewayAgent(_Fake("gemini-3.7-flash"))
    monkeypatch.setenv("GATEWAY_JSON_MODEL", "gemini-9")
    assert other._json_client() is other.client
    assert "GATEWAY_JSON_MODEL=gemini-9 not used" in other.client.notes[0]


def test_the_style_section_rides_after_the_mode_for_a_gemini_3_engine(compiled):  # noqa: F811
    from sahs.assistant.loop import system_prompt
    build, _tmp = compiled
    plain = system_prompt(build)
    assert "<style>" not in plain
    styled = system_prompt(build, style=prompt_style("gemini-3.7-flash"))
    assert "<style>\nLead with the answer" in styled
    assert styled.index("</mode>") < styled.index("<style>") < styled.index("<graph>")
    # everything before the style is byte-identical: the cached prefix holds
    assert styled[:styled.index("<style>")] == plain[:plain.index("<graph>")]


def test_the_catalog_lists_every_model_on_every_plane(monkeypatch):
    for var in ("APP_ID", "APP_SECRET", "GEMINI_BEARER_TOKEN", "SAHS_MODEL_PLANE",
                "SYNAPSE_VERTEX_SA_KEY", "GOOGLE_APPLICATION_CREDENTIALS", "VERTEX_PROJECT_ID",
                "GATEWAY_MODEL", "GEMINI_MODEL", "GATEWAY_MODEL_THINKING", "GATEWAY_MODEL_LEVELS",
                "VERTEX_MODEL", "SYNAPSE_VERTEX_MODEL"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.delenv("GATEWAY_MODELS", raising=False)
    rows = model_catalog()
    assert [r["id"] for r in rows] == ["vertex", "gateway"]              # one model each: the planes as before
    by = {r["id"]: r for r in rows}
    # the Vertex engine's map rides its row: 3.1 Pro takes a level, not a budget
    assert by["vertex"]["thinking"] == "level" and by["vertex"]["levels"] == ["low", "medium", "high"]
    assert by["vertex"]["family"] == "gemini-3" and "Most careful" in by["vertex"]["fit"]
    assert "streams on Vertex" in by["vertex"]["facts"]
    monkeypatch.setenv("GATEWAY_MODELS", MODELS)
    for key, value in HOSTS.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("APP_ID", "app")
    monkeypatch.setenv("APP_SECRET", SECRET)
    rows = model_catalog()
    assert [r["id"] for r in rows] == ["vertex", "gateway", "gateway:gemini-3.5-flash",
                                       "gateway:gemini-3.1-flash-lite"]
    by = {r["id"]: r for r in rows}
    assert by["gateway"]["model"] == "gemini-3.7-flash" and by["gateway"]["default"]
    assert by["gateway"]["thinking"] == "level" and by["gateway"]["levels"] == ["low", "medium", "high"]
    assert by["gateway:gemini-3.5-flash"]["label"] == "Gemini 3.5 Flash"
    assert by["gateway:gemini-3.5-flash"]["levels"] == ["medium", "high"]
    assert by["gateway:gemini-3.1-flash-lite"]["levels"][0] == "minimal"
    assert "JSON" in by["gateway:gemini-3.1-flash-lite"]["facts"]
    assert all(r["plane"] == "gateway" and r["available"] and not r["default"]
               for r in rows[2:])
    assert not by["vertex"]["available"] and "SYNAPSE_VERTEX_SA_KEY" in by["vertex"]["reason"]
    assert split_choice("gateway:gemini-3.5-flash") == ("gateway", "gemini-3.5-flash")
    assert split_choice("Vertex") == ("vertex", "") and split_choice("") == ("", "")
    assert join_choice("gateway", "") == "gateway" and join_choice("gateway", "x") == "gateway:x"


def test_the_runtime_switches_between_models_and_refuses_unknown_ones(compiled, tmp_path, monkeypatch):  # noqa: F811
    from sahs.ask.model import ModelUnavailable
    from sahs.assistant import AssistantRuntime
    from sahs.assistant.agent import ScriptedAgent
    build, tmp = compiled
    monkeypatch.setenv("APP_ID", "app")
    monkeypatch.setenv("APP_SECRET", SECRET)
    for key, value in HOSTS.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("SAHS_MODEL_PLANE", "gateway")
    monkeypatch.setenv("GATEWAY_MODELS", MODELS)
    monkeypatch.delenv("GATEWAY_MODEL", raising=False)
    heard: list[str] = []

    def factory(budget, plane):
        heard.append(plane)
        return ScriptedAgent(steps=[[{"text": f"answered on {plane}"}]])

    runtime = AssistantRuntime(
        builds_root=build.root.parent, graph_root=tmp / "graph",
        store_path=tmp_path / "chat.sqlite3", model_factory=factory)
    session = runtime.create_session()
    assert runtime.choice_of(session) == "gateway" and runtime.plane_of(session) == "gateway"
    assert [m["id"] for m in runtime.dials()["models"]][:3] == ["vertex", "gateway", "gateway:gemini-3.5-flash"]
    # a model on the plane: remembered as plane:model
    got = runtime.set_session_model(session["id"], "gateway:gemini-3.5-flash")
    assert got == {"ok": True, "plane": "gateway", "choice": "gateway:gemini-3.5-flash", "model": "scripted"}
    assert runtime.store.get_session(session["id"])["model"] == "gateway:gemini-3.5-flash"
    # the plane's default model is the plane itself
    assert runtime.set_session_model(session["id"], "gateway:gemini-3.7-flash")["choice"] == "gateway"
    # a model the plane does not serve is refused, naming what it does
    with pytest.raises(ModelUnavailable, match="it serves gemini-3.7-flash, gemini-3.5-flash"):
        runtime.set_session_model(session["id"], "gateway:gemini-9")
    with pytest.raises(ModelUnavailable, match="no model plane called 'gpt'"):
        runtime.set_session_model(session["id"], "gpt:x")
    # a message names a model: it rides it and the chat remembers it
    started = runtime.start_turn(session["id"], "hello", model="gateway:gemini-3.1-flash-lite")
    assert started["choice"] == "gateway:gemini-3.1-flash-lite" and started["plane"] == "gateway"
    assert runtime.wait(session["id"], 30)
    assert runtime.store.get_session(session["id"])["model"] == "gateway:gemini-3.1-flash-lite"
    events = runtime.runtime(session["id"]).bus.since(0)
    assert events[0]["ev"] == "turn_started" and events[0]["plane"] == "gateway"
    assert events[0]["model"] == "scripted"
    assert heard == ["gateway"]
    # the engine a choice rides, for the prompt style: none for a double
    assert runtime.model_name_for("gateway") == ""
    runtime._model_factory = None
    assert runtime.model_name_for("gateway") == "gemini-3.7-flash"
    assert runtime.model_name_for("gateway:gemini-3.5-flash") == "gemini-3.5-flash"


def test_the_granted_scopes_are_exactly_what_the_models_derive():
    """The gateway team grants one path pattern per model; with the
    three 3.x models named, the derived scopes are those patterns (plus
    the embedding scopes), so GATEWAY_SCOPES need not repeat them. 2.5
    Pro is retiring and no longer asked for."""
    env = {"GATEWAY_MODELS": "gemini-3.7-flash gemini-3.5-flash gemini-3.1-flash-lite"}
    derived = scopes_for(env)
    for granted in (SCOPE.format("gemini-3.7-flash"), SCOPE.format("gemini-3.5-flash"),
                    SCOPE.format("gemini-3.1-flash-lite")):
        assert granted in derived, granted
    assert SCOPE.format(LEGACY) not in derived
    assert derived[-2:] == EMBEDDING_SCOPES
    assert [thinking_kind(m, env) for m in gateway_models(env)] == ["level", "level", "level"]
