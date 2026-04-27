"""Smoke test for the Validator CustomAgent."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lumi.agents.validator import Validator


@pytest.mark.asyncio
async def test_validator_writes_all_artifacts(tmp_path: Path) -> None:
    from google.adk.runners import Runner
    from google.adk.sessions import InMemorySessionService
    from google.genai import types

    cfg = {
        "git": {
            "repo": "https://example.com/x.git",
            "branch": "main",
            "model_file": "models/analytics.model.lkml",
            "view_files": ["views/a.view.lkml"],
        },
        "mdm": {
            "endpoint": "https://mdm.example.com/api",
            "view_to_mdm_entity": {"a": "entity_a"},
        },
        "gold_queries": {"file": "data/q.xlsx"},
        "output": {"directory": str(tmp_path / "out")},
    }
    enriched = {
        "view_name": "a",
        "view_label": "A",
        "view_description": "test view",
        "fields": [
            {
                "name": "x",
                "kind": "dimension",
                "label": "X",
                "description": "a column",
                "origin": "gold_query",
            }
        ],
    }
    gold_query = {
        "query_id": "q_0001",
        "user_prompt": "list x",
        "expected_sql": "SELECT x FROM t",
        "primary_table": "t",
        "dimensions": ["x"],
    }

    from google.adk.events import Event, EventActions

    ss: InMemorySessionService = InMemorySessionService()  # type: ignore[no-untyped-call]
    agent = Validator()
    runner = Runner(app_name="t", agent=agent, session_service=ss)
    session = await ss.create_session(app_name="t", user_id="u", session_id="s")
    await ss.append_event(
        session,
        Event(
            author="test-seed",
            actions=EventActions(
                state_delta={
                    "lumi_config": cfg,
                    "gold_queries": [gold_query],
                    "enriched_view__a": enriched,
                    "view_name_to_table": {"a": "t"},
                    "join_graphs": [],
                    "model_file_text_enriched": "# generated explores",
                }
            ),
        ),
    )

    msg = types.Content(role="user", parts=[types.Part(text="go")])
    async for _ in runner.run_async(user_id="u", session_id="s", new_message=msg):
        pass

    out = tmp_path / "out"
    assert (out / "views" / "a.view.lkml").exists()
    assert (out / "models" / "analytics.model.lkml").exists()
    coverage = json.loads((out / "reports" / "coverage_report.json").read_text())
    assert coverage["total_queries"] == 1
    assert coverage["passed"] == 1

    gap = json.loads((out / "reports" / "gap_report.json").read_text())
    assert gap["passed"] == 1
