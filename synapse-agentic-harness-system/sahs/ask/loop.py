"""The turn pipeline (E18 Stage A):

    classify → apply → delta-resolve → validate → generate → verify → render

One stateful loop carrying the plan; stateless workers around it. The
loop is the only thing that knows the conversation; the generator and
the verifier each see one turn's artifacts and nothing else.

Every step emits its event before moving on, so the stream IS the
progress report — the UI is a pure consumer, and a replay of the
events file reproduces the turn exactly.
"""

from __future__ import annotations

import os
import time
import traceback
from typing import Any

from sahs.tools.api import Build

from .budget import Abort, Aborted, Budget
from .classify import classify
from .converse import (CHAT_SYSTEM, ChatTurn, chat_prompt,
                       pre_classify, world)
from .contract import build_contract
from .events import EventBus
from .generate import generate
from .model import ModelUnavailable
from .plan import Plan, PlanError, apply_edit
from .preview import join_grain_preview
from .render import RenderRefused, render_answer
from .resolve import resolve_plan
from .store import SessionStore
from .verify import verify

FULL_RESOLVE = ["metric", "grain", "table"]


def _error(bus: EventBus, turn_id: str, code: str, message: str,
           *, next_actions: list[str] | None = None,
           retryable: bool = True) -> None:
    """Errors are cards with next actions. Never a stack trace, never
    a dead end."""
    bus.emit("error", turn_id=turn_id, code=code, message=message,
             retryable=retryable,
             next_actions=next_actions or ["rephrase", "regenerate"])


def _greeting_fallback(facts: dict[str, Any]) -> str:
    return (f"Hi. I work over build {facts.get('build_id', '?')}: "
            f"{facts.get('tables', 0)} tables, "
            f"{facts.get('metrics', 0)} metrics.")


def _compose_chat(model: Any, cls: Any, text: str,
                  facts: dict[str, Any]) -> ChatTurn:
    """A chat kind the deterministic matcher declined. One small model
    call, JSON, no tools, no data. If it returns nothing usable the
    turn still answers: a conversation must not dead-end on a parse."""
    fallback = {
        "help": "Ask for a measure and how you want it cut, like "
                "\"spend by month for Canada\". If I cannot tell which "
                "definition you mean, I will show you the candidates "
                "with their evidence.",
        "off_topic": "That one is outside the data I hold. If there is "
                     "something in the company's numbers behind it, "
                     "point me at that and I will dig in.",
        "feedback": "Noted, and recorded against that answer.",
    }.get(cls.kind, "I am here for questions about the company's data. "
                    "What would you like to look at?")
    try:
        answer = model.json(chat_prompt(cls.kind, text, facts),
                            system=CHAT_SYSTEM, temperature=0.3,
                            max_tokens=300)
    except Exception:
        answer = None                 # a chat turn never breaks a turn
    reply = ""
    if isinstance(answer, dict):
        reply = str(answer.get("reply") or "").strip()
    feedback = ({"vote": "down", "subject": "answer", "note": text[:2000]}
                if cls.kind == "feedback" else None)
    return ChatTurn(kind=cls.kind, text=reply or fallback,
                    model_used=isinstance(answer, dict), facts=facts,
                    feedback=feedback)


def _say(bus: EventBus, store: SessionStore, session: dict[str, Any],
         turn_id: str, spoken: ChatTurn, *, close: bool = True) -> str:
    """Stream a conversational reply. NO plan version, NO resolver, NO
    contract, NO answer payload: a chat turn cannot emit data, and the
    absence of those events is what the UI reads to keep the theater
    strip hidden."""
    for chunk in _sentences(spoken.text):
        bus.emit("generate_token", turn_id=turn_id, delta=chunk)
    store.add_message(session["id"], "assistant", spoken.text,
                      turn_id=turn_id,
                      payload={"chat": {"kind": spoken.kind}})
    if spoken.feedback:
        store.add_feedback(session["id"], spoken.feedback["subject"],
                           spoken.feedback["vote"], turn_id=turn_id,
                           note=spoken.feedback.get("note", ""))
    return spoken.kind if close else ""


def _sentences(text: str):
    """Stream in readable chunks so a chat reply arrives like the rest
    of the product rather than as one block."""
    buffer = ""
    for word in text.split(" "):
        buffer += word + " "
        if len(buffer) >= 48 or word.endswith((".", "?", "!", ":")):
            yield buffer
            buffer = ""
    if buffer:
        yield buffer


def _finish(*, build: Build, store: SessionStore, bus: EventBus,
            budget: Budget, abort: Abort, model: Any,
            session: dict[str, Any], turn_id: str, plan: Plan,
            tables: list[str]) -> str:
    """contract → generate → verify → render: the ONE exit every
    answered turn shares. The fast path reaches it when the opening
    resolved everything; the agent loop reaches it at ``final``. The
    generator never sees the verdict; the verifier gets fresh context;
    the renderer refuses an ungoverned payload — same as always."""
    session_id = session["id"]

    # the contract, before any work: the join and grain preview rides
    # on contract_ready because that IS the acceptance moment
    contract = build_contract(plan, multi_table=len(tables) > 1)
    preview = join_grain_preview(build, plan, tables)
    store.add_plan_version(session_id, plan.to_dict(),
                           parent=plan.parent, turn_id=turn_id,
                           summary=plan.summary())
    bus.emit("contract_ready", turn_id=turn_id,
             contract=contract.to_dict(), plan=plan.to_dict(),
             preview=preview)

    # generate (streams; never sees the verdict)
    abort.check()
    gen = generate(
        model, build, plan,
        on_token=lambda chunk: bus.emit(
            "generate_token", turn_id=turn_id, delta=chunk),
        abort_check=abort.check)
    bus.emit("budget_tick", turn_id=turn_id, **budget.tick())

    # verify (fresh context, default-FAIL)
    abort.check()
    contract = verify(
        build, plan, contract, gen, model,
        on_progress=lambda criterion: bus.emit(
            "verify_progress", turn_id=turn_id, criterion=criterion),
        abort_check=abort.check)
    bus.emit("verify_verdict", turn_id=turn_id, **contract.to_dict())

    # render (refuses an ungoverned payload)
    payload = render_answer(build, plan, gen, contract)
    bus.emit("answer_payload", turn_id=turn_id, payload=payload)
    store.add_message(session_id, "assistant", gen.prose,
                      turn_id=turn_id, payload=payload)
    _autotitle(store, session, plan)
    return "answered"


def _resume_context(store: SessionStore, session_id: str,
                    choice: dict[str, Any]) -> dict[str, Any]:
    """What the loop needs to continue after its own ask_user: the
    question it asked and the notes it had, both riding the stored
    clarify message rather than any in-memory state."""
    question, notes = "", []
    for row in reversed(store.messages(session_id)):
        payload = row.get("payload") or {}
        clarify = payload.get("clarify") or {}
        if clarify.get("slot") == "agent":
            question = clarify.get("question", "")
            notes = list(payload.get("loop_notes") or [])
            break
    return {"question": question, "answer": choice.get("value", ""),
            "notes": notes}


def _autotitle(store: SessionStore, session: dict[str, Any],
               plan: Plan) -> None:
    """The sidebar needs a name as soon as there is something real to
    name — a plan that bound its metric and then stopped to ask a
    question is still that session's subject."""
    if (session.get("title") or "").strip() or not plan.metric_id:
        return
    title = plan.summary()
    session["title"] = title
    store.set_title(session["id"], title)


def run_turn(*, build: Build, store: SessionStore, bus: EventBus,
             budget: Budget, abort: Abort, model: Any,
             session: dict[str, Any], turn_id: str, text: str,
             choice: dict[str, Any] | None = None,
             navigate: bool | None = None) -> None:
    session_id = session["id"]
    started = time.perf_counter()
    bus.emit("turn_started", turn_id=turn_id, text=text,
             kind=session.get("kind", "analyst"), build_id=build.version)
    budget.start_turn()
    status = "error"
    # the navigation lane (Agent Loop v1 §2) engages when the
    # deterministic opening cannot complete the plan. Behind a flag
    # until §9.4's navigation tasks grade it: T3 pins the
    # no-candidates chips, and gates move on evidence, not excitement.
    wants_nav = navigate if navigate is not None else (
        os.environ.get("SYNAPSE_NAVIGATE") == "1")
    try:
        prior_row = store.latest_plan(session_id)
        prior = Plan.from_dict(prior_row["plan"]) if prior_row else None

        def finish(plan: Plan, tables: list[str]) -> str:
            return _finish(build=build, store=store, bus=bus,
                           budget=budget, abort=abort, model=model,
                           session=session, turn_id=turn_id,
                           plan=plan, tables=tables)

        # ── a chip answer to the loop's OWN question resumes it ─
        # (resolver chips name a real slot; the loop's ask names
        # "agent", which no apply_edit could ever move)
        if choice and choice.get("slot") == "agent":
            from sahs.loop.loop import navigate_loop
            if prior is None:
                _error(bus, turn_id, "nothing_to_resume",
                       "that answer belongs to a question no plan "
                       "remembers: ask the question again",
                       next_actions=["rephrase"])
                status = "refused"
                return
            bus.emit("classify_result", turn_id=turn_id, kind="mutate",
                     question=prior.question, edits=[],
                     why="loop resume: the chip answer re-enters "
                         "navigation", degraded=False,
                     model_used=False, chat_turn=False)
            status = navigate_loop(
                build=build, store=store, bus=bus, budget=budget,
                abort=abort, model=model, session=session,
                turn_id=turn_id, plan=prior, finish=finish,
                resume=_resume_context(store, session_id, choice))
            return

        # ── classify ─────────────────────────────────────────
        # E22: the conversational half is tried in CODE first. A
        # greeting, a thanks, or "what can you do" costs nothing and
        # cannot drift, which is exactly why those are not prompts.
        abort.check()
        facts = world(build)
        spoken = None if choice else pre_classify(
            text, facts, first_turn=prior is None)
        classified_in_code = False
        if spoken is not None and spoken.kind == "mixed":
            # a greeting glued to a request, split in code: hello
            # first, then the data half runs the full pipeline. One
            # classify_result for the turn; the split cost nothing.
            bus.emit("classify_result", turn_id=turn_id, kind="mixed",
                     question=spoken.question, edits=[],
                     why="split in code, no model call",
                     degraded=False, model_used=False, chat_turn=False)
            _say(bus, store, session, turn_id,
                 ChatTurn(kind="chat", text=spoken.text,
                          facts=facts), close=False)
            text = spoken.question
            spoken = None
            classified_in_code = True
        if spoken is not None:
            bus.emit("classify_result", turn_id=turn_id,
                     kind=spoken.kind, question="", edits=[],
                     why="matched in code, no model call",
                     degraded=False, model_used=False, chat_turn=True)
            status = _say(bus, store, session, turn_id, spoken)
            return

        cls = classify(model, text, prior, choice=choice,
                       allow_chat=not choice and not classified_in_code)
        if not classified_in_code:
            bus.emit("classify_result", turn_id=turn_id,
                     **cls.to_event())
        if cls.model_used:
            bus.emit("budget_tick", turn_id=turn_id, **budget.tick())

        # ── the chat kinds never plan ────────────────────────
        if cls.is_chat:
            status = _say(bus, store, session, turn_id,
                          _compose_chat(model, cls, text, facts))
            bus.emit("budget_tick", turn_id=turn_id, **budget.tick())
            return
        if cls.kind == "mixed":
            # one turn, both halves, in order: the pleasantry is
            # answered and THEN the data question runs the full
            # pipeline. The chat half can never carry a number.
            _say(bus, store, session, turn_id,
                 ChatTurn(kind="chat", text=cls.chat
                          or _greeting_fallback(facts), facts=facts),
                 close=False)
            text = cls.question

        # ── apply (deterministic, one slot per edit) ──────────
        abort.check()
        touched: list[str] = []
        if cls.kind == "mutate" and prior is not None:
            plan = prior
            for edit in cls.edits:
                plan, delta = apply_edit(plan, edit["slot"], edit["value"])
                touched.append(edit["slot"])
                bus.emit("plan_delta", turn_id=turn_id,
                         version=plan.version, parent=plan.parent,
                         changes=delta, summary=plan.summary())
        else:
            plan = Plan(question=cls.question or text,
                        version=(prior.version + 1) if prior else 1,
                        parent=prior.version if prior else None)
            touched = list(FULL_RESOLVE)
            bus.emit("plan_delta", turn_id=turn_id, version=plan.version,
                     parent=plan.parent, changes=[], summary="new plan",
                     reset=True)

        # ── delta resolution (deterministic, must be instant) ─
        abort.check()
        bus.emit("resolve_started", turn_id=turn_id, slots=touched)
        outcome = resolve_plan(build, plan, touched=touched)
        plan = outcome.plan
        bus.emit("resolve_result", turn_id=turn_id, **outcome.result)

        # ── the loop's middle: navigate where the opening stopped ─
        if wants_nav and (outcome.clarify or not plan.ready()[0]):
            from sahs.loop.loop import navigate_loop
            status = navigate_loop(
                build=build, store=store, bus=bus, budget=budget,
                abort=abort, model=model, session=session,
                turn_id=turn_id, plan=plan, finish=finish,
                resolver=outcome.result, clarify=outcome.clarify)
            return

        if outcome.clarify:
            # one question, chips carry evidence, the turn stops here
            # with partial state kept: this is a success state
            store.add_plan_version(session_id, plan.to_dict(),
                                   parent=plan.parent, turn_id=turn_id,
                                   summary=plan.summary())
            _autotitle(store, session, plan)
            bus.emit("clarify_request", turn_id=turn_id,
                     **outcome.clarify)
            store.add_message(session_id, "assistant", "",
                              turn_id=turn_id,
                              payload={"clarify": outcome.clarify})
            status = "clarify"
            return

        ready, missing = plan.ready()
        if not ready:
            _error(bus, turn_id, "plan_incomplete",
                   "this plan still needs " + ", ".join(missing),
                   next_actions=["answer the question above"])
            status = "incomplete"
            return

        # ── the one shared exit (fast path edition) ──────────
        status = finish(plan, outcome.result.get("tables") or [])

    except Aborted as e:
        status = "stopped"
        bus.emit("budget_tick", turn_id=turn_id, **budget.tick())
    except ModelUnavailable as e:
        status = "error"
        _error(bus, turn_id, "model_unavailable", str(e),
               next_actions=["check the Vertex contract in the silo .env",
                             "python scripts/vertex_check.py"],
               retryable=False)
    except (PlanError, RenderRefused) as e:
        status = "refused"
        _error(bus, turn_id, "refused", str(e),
               next_actions=["ask a narrower question"])
    except Exception as e:                       # never a stack trace
        status = "error"
        _error(bus, turn_id, "internal", f"{type(e).__name__}: {e}")
        bus.emit("error", turn_id=turn_id, code="trace",
                 message=traceback.format_exc(limit=3)[-800:],
                 retryable=False, next_actions=[])
    finally:
        if budget.needs_grace():
            bus.emit("budget_grace", turn_id=turn_id,
                     message="this session is near its budget: wrap up "
                             "or start a new one",
                     **budget.tick())
        tripped = budget.exceeded()
        if tripped:
            bus.emit("budget_grace", turn_id=turn_id,
                     message=f"stopped by the {tripped}", **budget.tick())
        bus.emit("turn_done", turn_id=turn_id, status=status,
                 elapsed_ms=round((time.perf_counter() - started) * 1000, 1),
                 **budget.tick())
