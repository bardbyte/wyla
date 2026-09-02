"""turn_doctor: the event file says where a turn sits."""

from __future__ import annotations

import datetime as _dt
import json
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))
sys.path.insert(0, str(SILO / "scripts"))

import turn_doctor  # noqa: E402


def _ev(ts, ev, **f):
    return {"ts": f"2026-09-02T07:00:{ts:02d}+00:00", "ev": ev,
            "turn_id": "t1", **f}


def test_timeline_finds_the_open_model_call(tmp_path):
    events = [
        _ev(0, "turn_started", text="how many customers enrolled?"),
        _ev(0, "model_prompt", kind="call", n=1),
        _ev(3, "thinking", delta="Find the enrolment metric."),
        _ev(5, "tool_call", n=1, tool="search", args='{"query": "x"}'),
        _ev(6, "tool_step", n=1, tool="search", summary="16 results"),
        _ev(6, "model_prompt", kind="call", n=2),
        _ev(9, "tool_call", n=2, tool="run_sql", args="{}"),
        _ev(21, "tool_step", n=2, tool="run_sql",
            summary="valid; bytes 82737261052"),
        _ev(21, "budget_tick", tokens=48000),
        _ev(21, "model_prompt", kind="call", n=3),
    ]
    now = _dt.datetime(2026, 9, 2, 7, 3, 21, tzinfo=_dt.timezone.utc)
    turns = turn_doctor.timeline(events, now=now)
    assert len(turns) == 1
    t = turns[0]
    whats = [(s["what"], s["seconds"]) for s in t["segments"]]
    assert whats == [("model call #1", 5.0), ("tool search", 1.0),
                     ("model call #2", 3.0), ("tool run_sql", 12.0)]
    assert t["segments"][0]["first_sign_after"] == 3.0
    assert t["segments"][3]["outcome"].startswith("valid")
    assert t["open"]["what"] == "model call #3"
    assert t["open"]["for_seconds"] == 180.0
    text = turn_doctor.render(turns)
    assert "OPEN: in model call #3 for 180.0s — nothing has arrived" in text
    assert "tokens so far: 48000" in text
    # a finished turn closes its open segment and shows the status
    events.append(_ev(40, "say_token", delta="About 1.2M."))
    events.append(_ev(41, "turn_done", status="answered"))
    turns = turn_doctor.timeline(events, now=now)
    assert turns[0]["open"] is None and turns[0]["status"] == "answered"
    assert turns[0]["segments"][-1]["what"] == "model call #3"
    assert turns[0]["segments"][-1]["first_sign_after"] == 19.0
    assert "answered · 41.0s" in turn_doctor.render(turns)


def test_main_reads_the_newest_file(tmp_path, capsys):
    old = tmp_path / "s_old.jsonl"
    new = tmp_path / "s_new.jsonl"
    old.write_text(json.dumps(_ev(0, "turn_started", text="old")) + "\n")
    new.write_text("\n".join(json.dumps(e) for e in [
        _ev(0, "turn_started", text="new"),
        _ev(0, "model_prompt", kind="call", n=1),
        _ev(2, "tool_call", n=1, tool="read", args="{}"),
    ]) + "\n")
    import os
    os.utime(old, (1, 1))
    assert turn_doctor.main(["--events", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "s_new.jsonl" in out and "'new'" in out
    assert "OPEN: in tool read" in out
    assert "the tool has not returned" in out
