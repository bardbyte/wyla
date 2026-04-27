"""Smoke test for the Aggregator CustomAgent."""

from __future__ import annotations

import pytest

from lumi.agents.aggregator import Aggregator


async def _seeded_session(
    ss: object, state: dict, user_id: str = "u", session_id: str = "s"
) -> object:
    """Create an ADK session and seed state via append_event, the ONLY way ADK
    guarantees state visibility inside an agent's run."""
    from google.adk.events import Event, EventActions

    session = await ss.create_session(app_name="t", user_id=user_id, session_id=session_id)  # type: ignore[attr-defined]
    await ss.append_event(  # type: ignore[attr-defined]
        session,
        Event(author="test-seed", actions=EventActions(state_delta=state)),
    )
    return session


@pytest.mark.asyncio
async def test_aggregator_collects_enriched_views() -> None:
    from google.adk.runners import Runner
    from google.adk.sessions import InMemorySessionService
    from google.genai import types

    ss: InMemorySessionService = InMemorySessionService()  # type: ignore[no-untyped-call]
    agent = Aggregator()
    runner = Runner(app_name="t", agent=agent, session_service=ss)

    await _seeded_session(
        ss,
        {
            "enriched_view__view_a": {"view_name": "a", "fields": []},
            "enriched_view__view_b": {"view_name": "b", "fields": []},
            "unrelated_key": {"x": 1},
        },
    )

    msg = types.Content(role="user", parts=[types.Part(text="go")])
    async for _ in runner.run_async(user_id="u", session_id="s", new_message=msg):
        pass

    # Re-fetch the session so we see post-run state.
    session = await ss.get_session(app_name="t", user_id="u", session_id="s")
    assert session is not None
    flat = session.state.get("enriched_views", {})
    assert set(flat.keys()) == {"view_a", "view_b"}
    assert isinstance(session.state.get("enriched_views_for_prompt"), str)


@pytest.mark.asyncio
async def test_aggregator_raises_when_no_views() -> None:
    from google.adk.runners import Runner
    from google.adk.sessions import InMemorySessionService
    from google.genai import types

    ss: InMemorySessionService = InMemorySessionService()  # type: ignore[no-untyped-call]
    agent = Aggregator()
    runner = Runner(app_name="t", agent=agent, session_service=ss)
    await ss.create_session(app_name="t", user_id="u", session_id="s")

    msg = types.Content(role="user", parts=[types.Part(text="go")])
    with pytest.raises(RuntimeError, match="no enriched views"):
        async for _ in runner.run_async(user_id="u", session_id="s", new_message=msg):
            pass
