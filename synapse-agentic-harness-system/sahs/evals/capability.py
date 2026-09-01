"""E19 capability matrix (reconstructed) — the baseline E21 gates on.

RECONSTRUCTION NOTICE, deliberately loud: the E19 instruction itself
never landed in this repo; E20, the answering ladder, and E21 all cite
it (tiers T1-T7 and T10, "the two-number line", "the three-
configuration ablation"). This module rebuilds that suite from those
citations so Step 0b can publish a baseline now. If the real E19 text
differs, reconcile THIS file to it — the tier ids below are the
contract every later PR's delta line refers to.

The tiers, mapped to what the built loop actually does:

  T1  vocabulary        acronyms expand, scope narrows, unscoped
                        ambiguity refuses to bind (L0)
  T2  certified bind    the resolver binds the certified metric, fast
  T3  clarification     below-evidence stops with a question whose
                        options carry evidence — never a guess
  T4  mutation          "same for Canada" moves one slot, delta-only
  T5  contract          acceptance before work, default-FAIL, the
                        judge failing closed
  T6  join preview      fan-out judged from build facts, pre-SQL
  T7  receipts          answers carry meridian line, grain, SQL,
                        limits — or refuse
  T8  composition (L3)  NOT BUILT — reported absent, never scored
  T9  exploratory (L4)  NOT BUILT — reported absent, never scored
  T10 abstention        out-of-scope questions get no answer, and the
                        refusal names itself
  T11 conversation      greetings, thanks, meta and feedback land like
                        a colleague, touch no plan, and (for the
                        deterministic ones) cost no model call (E22)

Two SUT levels, each task naming its own: ``resolver`` tasks call
resolve() directly (T1, T3 disambiguation); ``loop`` tasks drive a
fresh Ask session end to end; ``preview`` tasks call the fan-out
judgement. The model transport in the loop is scripted HERE, in the
measurement layer — the same substitution every harness test makes:
the transport is what is replaced, never the data. ``--real`` on the
CLI swaps in Vertex for the laptop baseline.

The two-number line, per configuration:
  answered%          of answerable tasks, how many produced an answer
  wrong-when-answered%  of those answers, how many were wrong
plus, reported beside it: false-abstain% (answerable tasks that got
nothing) and false-answer% (abstain tasks that got an answer).

The three configurations vary ONE knob — the resolver margin — by
copying the build manifest, never by touching the ranker:
  pinned 0.15 (shipped) · looser 0.05 · strict 0.30
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

TIERS: dict[str, dict[str, Any]] = {
    "T1": {"title": "vocabulary (L0)", "built": True},
    "T2": {"title": "certified bind (L1)", "built": True},
    "T3": {"title": "clarification", "built": True},
    "T4": {"title": "single-slot mutation", "built": True},
    "T5": {"title": "contract, default-FAIL", "built": True},
    "T6": {"title": "join and grain preview", "built": True},
    "T7": {"title": "answer receipts", "built": True},
    "T8": {"title": "composition (L3)", "built": False},
    "T9": {"title": "exploratory (L4)", "built": False},
    "T10": {"title": "abstention and honesty", "built": True},
    "T11": {"title": "conversation quality (E22)", "built": True},
}

CONFIGS: dict[str, float | None] = {
    "pinned": None,          # the build's own constants, untouched
    "looser": 0.05,
    "strict": 0.30,
}

GOOD_SQL = ("SELECT part_dt, sum(trans_usd_am) AS acquirer_net_spend "
            "FROM dw.gms_transaction GROUP BY part_dt")


class ScriptedTransport:
    """Measurement-only stand-in for Vertex (A1 discipline: the
    transport is replaced, the data never is). ``judge_unusable``
    exercises the fail-closed path."""

    def __init__(self, judge_unusable: bool = False,
                 mixed: str = "") -> None:
        self.judge_unusable = judge_unusable
        self.mixed = mixed            # the data half of a mixed turn
        self.calls: list[str] = []    # E22: chat turns must not add here

    def json(self, prompt, *, system="", temperature=0.0, max_tokens=1024):
        if "conversational voice" in system:
            self.calls.append("chat")
            return {"reply": "That one is outside the data I hold, but "
                             "point me at the numbers behind it and I "
                             "will dig in."}
        if "You classify ONE turn" in system:
            self.calls.append("classify")
            low = prompt.lower()
            if self.mixed:
                return {"kind": "mixed", "chat": "Hello.",
                        "question": self.mixed, "edits": [],
                        "why": "a greeting and a question"}
            if "off_topic_probe" in low:
                return {"kind": "off_topic", "question": "", "edits": [],
                        "why": "outside the data world"}
            if "canada" in low:
                return {"kind": "mutate", "why": "a filter changed",
                        "edits": [{"slot": "filters.country",
                                   "value": "CA"}]}
            return {"kind": "new_question", "question": prompt,
                    "why": "fresh subject", "edits": []}
        if "You compose ONE BigQuery SELECT" in system:
            self.calls.append("sql")
            return {"sql": GOOD_SQL, "why": "certified expression"}
        if "skeptical reviewer" in system:
            self.calls.append("judge")
            if self.judge_unusable:
                return {"nonsense": True}
            return {"grounded": True, "why": "claims trace"}
        return {}

    def stream(self, prompt, *, system="", temperature=0.3,
               max_tokens=1500):
        self.calls.append("stream")
        yield ("Validated against the promoted build; not executed "
               "live, so no figure is stated.")


def build_with_margin(build: Any, margin: float | None) -> Any:
    """A configuration is a copied manifest, nothing else. The ranker
    is untouched; resolve() reads its constants from the manifest it
    is handed (E6)."""
    if margin is None:
        return build
    from dataclasses import replace
    constants = dict(build.manifest.get("resolver_constants") or {})
    if not constants:
        from sahs.tools.constants import RESOLVER_CONSTANTS
        constants = dict(RESOLVER_CONSTANTS)
    constants = {**constants, "margin_threshold": margin}
    return replace(build, manifest={**build.manifest,
                                    "resolver_constants": constants})


# ── task outcomes ────────────────────────────────────────────
@dataclass
class TaskResult:
    task_id: str
    tier: str
    passed: bool
    answered: bool          # an answer_payload was emitted
    wrong: bool             # answered, and the answer failed its check
    expects_answer: bool = False
    detail: str = ""


@dataclass
class MatrixResult:
    config: str
    margin: float | None
    results: list[TaskResult] = field(default_factory=list)

    def tier_scores(self) -> dict[str, dict[str, int]]:
        out: dict[str, dict[str, int]] = {}
        for r in self.results:
            slot = out.setdefault(r.tier, {"passed": 0, "total": 0})
            slot["total"] += 1
            slot["passed"] += int(r.passed)
        return out

    def line(self) -> dict[str, Any]:
        # answerable = tasks whose PASS condition is an answer. A task
        # that is supposed to end at a clarifying question has not
        # "failed to answer": stopping to ask is its success state, so
        # it must not deflate the answer rate.
        answerable = [r for r in self.results if r.expects_answer]
        abstain = [r for r in self.results if r.tier == "T10"]
        answered = [r for r in answerable if r.answered]
        wrong = [r for r in answered if r.wrong]
        false_abstain = [r for r in answerable if not r.answered]
        false_answer = [r for r in abstain if r.answered]
        pct = lambda n, d: round(100 * n / d, 1) if d else None  # noqa: E731
        return {
            "answered_pct": pct(len(answered), len(answerable)),
            "wrong_when_answered_pct": pct(len(wrong), len(answered)),
            "false_abstain_pct": pct(len(false_abstain), len(answerable)),
            "false_answer_pct": pct(len(false_answer), len(abstain)),
        }


# ── the runner ───────────────────────────────────────────────
def _drive_loop(build: Any, tmp: Path, task: dict,
                model_factory: Callable[..., Any]) -> tuple[list, Any]:
    """One task, one FRESH session (memory must not leak between
    tasks), driven to completion; returns (events, runtime)."""
    from sahs.ask import AskRuntime
    runtime = AskRuntime(
        builds_root=build.root.parent, graph_root=tmp / "graph",
        store_path=tmp / f"{task['id']}.sqlite3",
        model_factory=lambda budget: model_factory(task))
    current = build.root.parent / "CURRENT"
    runtime._build = build                 # the configured copy
    runtime._build_stamp = (current.stat().st_mtime
                            if current.exists() else -1.0)
    session = runtime.create_session("analyst")
    rt = runtime.runtime(session["id"])
    runtime.session_id = session["id"]      # for the plan-count pin
    for turn in task["turns"]:
        runtime.start_turn(session["id"], turn["text"],
                           choice=turn.get("choice"))
        assert runtime.wait(session["id"], 30), "turn never finished"
    return rt.bus.since(0), runtime


def _grade_loop(events: list[dict], expect: dict) -> tuple[bool, str]:
    kinds = [e["ev"] for e in events]
    answers = [e for e in events if e["ev"] == "answer_payload"]
    clarifies = [e for e in events if e["ev"] == "clarify_request"]
    problems: list[str] = []

    if expect.get("no_answer") and answers:
        problems.append("answered when it must not")
    if expect.get("answered") and not answers:
        problems.append("no answer_payload")
    if "bound_label" in expect:
        bound = next((e.get("bound") for e in events
                      if e["ev"] == "resolve_result" and e.get("bound")),
                     None)
        if not bound or bound.get("label") != expect["bound_label"]:
            problems.append(f"bound {bound and bound.get('label')!r}, "
                            f"wanted {expect['bound_label']!r}")
    if "resolve_under_ms" in expect:
        slow = [e.get("elapsed_ms") for e in events
                if e["ev"] == "resolve_result"
                and (e.get("elapsed_ms") or 0) > expect["resolve_under_ms"]]
        if slow:
            problems.append(f"resolve took {slow}ms")
    if "clarify_slot" in expect:
        if not clarifies:
            problems.append("no clarifying question")
        elif clarifies[0].get("slot") != expect["clarify_slot"]:
            problems.append(f"asked about {clarifies[0].get('slot')!r}")
    if expect.get("options_carry_evidence"):
        opts = (clarifies[0].get("options") if clarifies else []) or []
        if not opts or not all(o.get("why") or o.get("evidence")
                               for o in opts):
            problems.append("options without evidence")
    if "one_slot_moved" in expect:
        # the pin is about the FINAL turn: earlier turns may each have
        # legitimately moved their own single slot (a chip answer is a
        # grain edit). Scope the check to events after the last start.
        last_start = max(i for i, e in enumerate(events)
                         if e["ev"] == "turn_started")
        deltas = [c["slot"] for e in events[last_start:]
                  if e["ev"] == "plan_delta"
                  for c in (e.get("changes") or [])]
        if deltas != [expect["one_slot_moved"]]:
            problems.append(f"slots moved in the last turn: {deltas}")
    if expect.get("contract_before_generate"):
        if "contract_ready" not in kinds:
            problems.append("no contract")
        elif "generate_token" in kinds and kinds.index(
                "contract_ready") > kinds.index("generate_token"):
            problems.append("work began before acceptance")
        else:
            contract = next(e for e in events
                            if e["ev"] == "contract_ready")
            if any(c["passed"] for c in
                   contract["contract"]["will_verify"]):
                problems.append("a criterion was born true")
    if "verdict" in expect:
        verdicts = [e.get("verdict") for e in events
                    if e["ev"] == "verify_verdict"]
        if not verdicts or verdicts[-1] != expect["verdict"]:
            problems.append(f"verdict {verdicts}")
    if expect.get("grounded_fail_closed"):
        grounded = next((c for e in events if e["ev"] == "verify_verdict"
                         for c in e.get("will_verify", [])
                         if c["id"] == "grounded"), None)
        if grounded is None or grounded["passed"]:
            problems.append("an unusable judge did not fail closed")
    if expect.get("receipts") and answers:
        payload = answers[-1]["payload"]
        for key in ("meridian_line", "grain", "sql"):
            if not payload.get(key):
                problems.append(f"answer missing {key}")
        if not isinstance(payload.get("limits"), list):
            problems.append("no limits list")
    if expect.get("honest_exit") and not (clarifies or [
            e for e in events if e["ev"] == "error"]):
        problems.append("neither a question nor an error card")

    # ── E22 conversation pins ────────────────────────────────
    classifications = [e for e in events
                       if e["ev"] == "classify_result"]
    classified = classifications[-1] if classifications else None
    if "classified" in expect:
        got = classified.get("kind") if classified else None
        if got != expect["classified"]:
            problems.append(f"classified {got!r}, wanted "
                            f"{expect['classified']!r}")
    if expect.get("no_plan"):
        if "contract_ready" in kinds or "plan_delta" in kinds:
            problems.append("a chat turn touched the plan")
    if expect.get("no_resolver") and "resolve_started" in kinds:
        problems.append("a chat turn reached the resolver")
    if expect.get("spoke"):
        prose = "".join(e.get("delta", "") for e in events
                        if e["ev"] == "generate_token")
        if not prose.strip():
            problems.append("a chat turn said nothing")
        for banned in expect.get("must_not_say", []):
            if banned.lower() in prose.lower():
                problems.append(f"said {banned!r}, which it must not")
        for wanted in expect.get("must_say", []):
            if wanted.lower() not in prose.lower():
                problems.append(f"did not mention {wanted!r}")
    return (not problems), "; ".join(problems)


def _grade_resolver(build: Any, task: dict) -> tuple[bool, str, bool]:
    """→ (passed, detail, wrongly_bound). Resolver-level tiers."""
    from sahs.tools.resolver import resolve
    expect = task["expect"]
    raw = resolve(build, task["prompt"], task.get("context") or {})
    expanded = raw.get("acronyms_expanded") or []
    bound = raw.get("metrics") or []
    problems: list[str] = []
    if "expanded_exact" in expect and len(expanded) != expect[
            "expanded_exact"]:
        problems.append(f"expanded {len(expanded)}x")
    if "expanded_min" in expect and len(expanded) < expect["expanded_min"]:
        problems.append("no expansion")
    if expect.get("must_not_bind") and bound:
        problems.append(f"bound {bound[0]['label']!r} on ambiguity")
    wrongly = bool(expect.get("must_not_bind") and bound)
    return (not problems), "; ".join(problems), wrongly


def _grade_curated(build: Any, curated_id: str,
                   curated_path: Path) -> tuple[bool, str]:
    """T3 disambiguation and T10 resolver-level abstention reuse the
    P1 graders and the checked-in curated golds verbatim — a proven
    judge over the same evidence, never a lookalike."""
    from sahs.evals.grading import grade
    from sahs.evals.schema import Task
    from sahs.tools.resolver import resolver_sut
    row = next((json.loads(line) for line in
                curated_path.read_text(encoding="utf-8").splitlines()
                if line.strip() and json.loads(line)["id"] == curated_id),
               None)
    if row is None:
        return False, f"curated task {curated_id!r} missing"
    task = Task.model_validate(row)
    trial = grade(task, resolver_sut(build)(task), None)
    return str(trial.verdict).lower() == "pass", trial.reason


def run_matrix(build: Any, tmp: Path, tasks: list[dict], *,
               config: str, margin: float | None,
               model_factory: Callable[..., Any] | None = None
               ) -> MatrixResult:
    from sahs.ask.preview import join_grain_preview
    from sahs.ask.plan import Plan

    configured = build_with_margin(build, margin)
    transports: dict[str, ScriptedTransport] = {}

    def default_factory(task):
        transport = ScriptedTransport(
            judge_unusable=bool(task.get("judge_unusable")),
            mixed=str(task.get("mixed") or ""))
        transports[task["id"]] = transport
        return transport

    factory = model_factory or default_factory
    out = MatrixResult(config=config, margin=margin)

    for task in tasks:
        tier, level = task["tier"], task["level"]
        started = time.perf_counter()
        try:
            if level == "resolver":
                passed, detail, wrong = _grade_resolver(configured, task)
                answered = False
            elif level == "curated":
                passed, detail = _grade_curated(
                    configured, task["curated_id"],
                    Path(task["curated_path"]))
                answered, wrong = False, False
            elif level == "preview":
                plan = Plan(question="q", metric_id="m", grain="transaction",
                            table=task["base"],
                            provenance={"grain": "resolver"})
                preview = join_grain_preview(configured, plan,
                                             task["tables"])
                passed = preview["verdict"] == task["expect"]["verdict"]
                detail = "" if passed else (
                    f"verdict {preview['verdict']!r}")
                answered, wrong = False, False
            else:                                   # loop
                events, rt = _drive_loop(configured, tmp, task, factory)
                passed, detail = _grade_loop(events, task["expect"])
                transport = transports.get(task["id"])
                if task["expect"].get("zero_model_calls") and transport \
                        and transport.calls:
                    passed = False
                    detail = (f"{len(transport.calls)} model call(s) on a "
                              f"turn that must cost none: "
                              f"{transport.calls}")
                if task["expect"].get("plans") is not None:
                    plans = len(rt.store.plan_versions(rt.session_id)) \
                        if hasattr(rt, "session_id") else None
                    if plans is not None and plans != task["expect"][
                            "plans"]:
                        passed = False
                        detail = f"{plans} plan versions"
                answered = any(e["ev"] == "answer_payload"
                               for e in events)
                wrong = answered and not passed
        except Exception as e:                       # a crash is a fail
            passed, detail = False, f"{type(e).__name__}: {e}"
            answered, wrong = False, False
        out.results.append(TaskResult(
            task_id=task["id"], tier=tier, passed=passed,
            answered=answered, wrong=wrong,
            expects_answer=bool(task.get("expect", {}).get("answered")),
            detail=detail or f"ok in {round((time.perf_counter()-started)*1000)}ms"))
    return out


# ── the published report ─────────────────────────────────────
def render_report(runs: list[MatrixResult], *, build_id: str,
                  transport: str) -> str:
    lines = [
        "# E19 capability baseline (E21 Step 0b)",
        "",
        f"- build: `{build_id}` · transport: **{transport}**",
        "- RECONSTRUCTION: the E19 instruction is cited by E20/E21 but "
        "its text never landed in this repo; this suite rebuilds it "
        "from those citations (see `sahs/evals/capability.py`). "
        "Reconcile against the real E19 if it differs.",
        "",
        "## the two-number line, per configuration",
        "",
        "| config | margin | answered% | wrong-when-answered% | "
        "false-abstain% | false-answer% |",
        "|---|---|---|---|---|---|",
    ]
    for run in runs:
        line = run.line()
        margin = run.margin if run.margin is not None else "0.15 (shipped)"
        lines.append(
            f"| {run.config} | {margin} | {line['answered_pct']} | "
            f"{line['wrong_when_answered_pct']} | "
            f"{line['false_abstain_pct']} | {line['false_answer_pct']} |")
    lines += ["", "## tiers (pinned configuration)", "",
              "| tier | capability | score |", "|---|---|---|"]
    pinned = next(r for r in runs if r.config == "pinned")
    scores = pinned.tier_scores()
    for tier, meta in TIERS.items():
        if not meta["built"]:
            lines.append(f"| {tier} | {meta['title']} | absent — not "
                         "built, not scored |")
            continue
        score = scores.get(tier, {"passed": 0, "total": 0})
        lines.append(f"| {tier} | {meta['title']} | "
                     f"{score['passed']}/{score['total']} |")
    failures = [r for r in pinned.results if not r.passed]
    if failures:
        lines += ["", "## failures (pinned)", ""]
        for r in failures:
            lines.append(f"- `{r.task_id}` ({r.tier}): {r.detail}")
    flat = len({json.dumps(r.line(), sort_keys=True)
                for r in runs}) == 1
    if flat:
        lines += ["", "**Ablation note:** the three configurations "
                  "produce identical lines on this build — none of "
                  "these tasks sits near the margin boundary at this "
                  "fixture's scale. On the full graph (3,000+ mined "
                  "classes) the margin knob is expected to "
                  "differentiate; a flat line there would be a "
                  "finding about the knob, not the suite."]
    lines += ["", "Every later E21 step re-runs this suite and reports "
              "its delta against this file on the same line.", ""]
    return "\n".join(lines)


def load_tasks(path: Path) -> list[dict]:
    return [json.loads(line) for line in
            path.read_text(encoding="utf-8").splitlines() if line.strip()]
