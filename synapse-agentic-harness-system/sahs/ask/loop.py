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

import time
import traceback
from typing import Any

from sahs.tools.api import Build

from .budget import Abort, Aborted, Budget
from .classify import classify
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
             choice: dict[str, Any] | None = None) -> None:
    session_id = session["id"]
    started = time.perf_counter()
    bus.emit("turn_started", turn_id=turn_id, text=text,
             kind=session.get("kind", "analyst"), build_id=build.version)
    budget.start_turn()
    status = "error"
    try:
        prior_row = store.latest_plan(session_id)
        prior = Plan.from_dict(prior_row["plan"]) if prior_row else None

        # ── classify ─────────────────────────────────────────
        abort.check()
        cls = classify(model, text, prior, choice=choice)
        bus.emit("classify_result", turn_id=turn_id, **cls.to_event())
        if cls.model_used:
            bus.emit("budget_tick", turn_id=turn_id, **budget.tick())

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

        # ── the contract, before any work ────────────────────
        # the join and grain preview rides on contract_ready because
        # that IS the acceptance moment: a fan-out warning is only
        # useful while the plan can still change.
        tables = outcome.result.get("tables") or []
        contract = build_contract(plan, multi_table=len(tables) > 1)
        preview = join_grain_preview(build, plan, tables)
        store.add_plan_version(session_id, plan.to_dict(),
                               parent=plan.parent, turn_id=turn_id,
                               summary=plan.summary())
        bus.emit("contract_ready", turn_id=turn_id,
                 contract=contract.to_dict(), plan=plan.to_dict(),
                 preview=preview)

        # ── generate (streams; never sees the verdict) ───────
        abort.check()
        gen = generate(
            model, build, plan,
            on_token=lambda chunk: bus.emit(
                "generate_token", turn_id=turn_id, delta=chunk),
            abort_check=abort.check)
        bus.emit("budget_tick", turn_id=turn_id, **budget.tick())

        # ── verify (fresh context, default-FAIL) ─────────────
        abort.check()
        contract = verify(
            build, plan, contract, gen, model,
            on_progress=lambda criterion: bus.emit(
                "verify_progress", turn_id=turn_id, criterion=criterion),
            abort_check=abort.check)
        bus.emit("verify_verdict", turn_id=turn_id,
                 **contract.to_dict())

        # ── render (refuses an ungoverned payload) ───────────
        payload = render_answer(build, plan, gen, contract)
        bus.emit("answer_payload", turn_id=turn_id, payload=payload)
        store.add_message(session_id, "assistant", gen.prose,
                          turn_id=turn_id, payload=payload)
        _autotitle(store, session, plan)
        status = "answered"

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
