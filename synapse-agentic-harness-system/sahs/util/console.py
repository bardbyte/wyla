"""Production console (E10) — the terminal is a projection; the stream is
the record.

Every laptop.py subcommand routes its progress through one RunConsole:
events append to graph/runs/<run_id>/events.jsonl as ``meridian.event/1``
records (committed with all run outputs), while the terminal renders either
a single in-place TTY line with EMA-smoothed rate and ETA, or plain
non-ANSI heartbeats for CI/non-TTY. A summary block prints on every exit —
success or failure — with items in/ok/quarantined-by-category, gate
results, outputs written, elapsed, and the next command to run.

Exit codes (pinned): 0 ok · 1 gate failure · 2 validation error ·
3 env/auth · 4 interrupted-with-checkpoint. Runbooks and CI branch on
codes, not grep.

Checkpointing: long loops call ``checkpoint_state``/``load_state`` on a
``_state.json`` beside the run outputs; ``--resume`` is the default
posture, ``--fresh`` overrides.

Stdlib only. ~150 lines of behavior; no rich, no TUI.
"""

from __future__ import annotations

import json
import sys
import time
import datetime as _dt
from collections import Counter
from pathlib import Path
from typing import Any, TextIO

SCHEMA = "meridian.event/1"
EMA_ALPHA = 0.2                       # pinned (E10)

EXIT_OK = 0
EXIT_GATE_FAILURE = 1
EXIT_VALIDATION_ERROR = 2
EXIT_ENV_AUTH = 3
EXIT_INTERRUPTED = 4

_HEARTBEAT_ITEMS = 1_000
_HEARTBEAT_SECONDS = 10.0


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def _fmt_eta(seconds: float | None) -> str:
    if seconds is None or seconds != seconds or seconds < 0:
        return "--:--"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


class RunConsole:
    """One per subcommand invocation. Also usable as a context manager."""

    def __init__(self, run_id: str, *, script_version: str,
                 canon_version: str, events_path: Path | None = None,
                 plain: bool = False, stream: TextIO | None = None) -> None:
        self.run_id = run_id
        self.script_version = script_version
        self.canon_version = canon_version
        self.stream = stream or sys.stderr
        self.plain = plain or not self._isatty()
        self.events_path = events_path
        if events_path is not None:
            events_path.parent.mkdir(parents=True, exist_ok=True)
        self._t0 = time.monotonic()
        self._phase: str | None = None
        self._n_done = 0
        self._total_done = 0
        self._n_total: int | None = None
        self._quarantined: Counter = Counter()
        self._gates: list[dict[str, Any]] = []
        self._outputs: list[str] = []
        self._ema_rate: float | None = None
        self._last_tick = self._t0
        self._last_beat_items = 0
        self._last_beat_time = self._t0
        self._line_open = False
        self.emit("run_start")

    # ── event stream (the record) ────────────────────────────

    def emit(self, ev: str, **fields: Any) -> None:
        record = {"schema": SCHEMA, "ts": _now_iso(), "run_id": self.run_id,
                  "script_version": self.script_version,
                  "canon_version": self.canon_version, "ev": ev}
        if self._phase is not None:
            record.setdefault("phase", self._phase)
        record.update({k: v for k, v in fields.items() if v is not None})
        if self.events_path is not None:
            with self.events_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

    # ── phases + items ───────────────────────────────────────

    def phase(self, name: str, total: int | None = None) -> None:
        self._close_line()
        self._phase = name
        self._n_done = 0
        self._n_total = total
        self._ema_rate = None
        self._last_tick = time.monotonic()
        self._last_beat_items = 0
        self._last_beat_time = self._last_tick
        self.emit("phase_start", n_total=total)
        self._println(f"── {name}" + (f" ({total:,} items)" if total else ""))

    def item_ok(self, n: int = 1) -> None:
        self._advance(n)

    def item_quarantined(self, category: str, detail: str = "") -> None:
        self._quarantined[category] += 1
        self.emit("item_quarantined", category=category,
                  detail=detail[:200] or None)
        self._advance(1)

    def _advance(self, n: int) -> None:
        self._n_done += n
        self._total_done += n
        now = time.monotonic()
        dt = now - self._last_tick
        if dt > 0:
            inst = n / dt
            self._ema_rate = (inst if self._ema_rate is None
                              else EMA_ALPHA * inst
                              + (1 - EMA_ALPHA) * self._ema_rate)
        self._last_tick = now
        if self.plain:
            if (self._n_done - self._last_beat_items >= _HEARTBEAT_ITEMS
                    or now - self._last_beat_time >= _HEARTBEAT_SECONDS):
                self._heartbeat(now)
        else:
            self._render_line()

    def _eta(self) -> float | None:
        if not self._n_total or not self._ema_rate:
            return None
        return max(0.0, (self._n_total - self._n_done) / self._ema_rate)

    def _progress_text(self) -> str:
        q = sum(self._quarantined.values())
        total = f"/{self._n_total:,}" if self._n_total else ""
        rate = f" · {self._ema_rate:,.0f}/s" if self._ema_rate else ""
        quar = (f" · quar {q:,} ({q / max(self._n_done, 1):.1%})" if q else "")
        eta = (f" · ETA {_fmt_eta(self._eta())}" if self._n_total else "")
        return (f"{self._phase or 'run'} {self._n_done:,}{total}"
                f"{rate}{quar}{eta}")

    def _heartbeat(self, now: float) -> None:
        self._last_beat_items = self._n_done
        self._last_beat_time = now
        self.emit("heartbeat", n_done=self._n_done, n_total=self._n_total,
                  rate=round(self._ema_rate, 1) if self._ema_rate else None,
                  eta_s=round(self._eta(), 1) if self._eta() else None)
        self._println(self._progress_text())

    # ── gates + outputs ──────────────────────────────────────

    def gate(self, name: str, passed: bool, detail: str = "") -> bool:
        self._gates.append({"gate": name, "passed": passed, "detail": detail})
        self.emit("gate_result", detail=f"{name}: "
                  + ("PASS" if passed else "FAIL")
                  + (f" — {detail}" if detail else ""))
        self._close_line()
        mark = "✓" if passed else "✗"
        self._println(f"  {mark} gate {name}"
                      + (f" — {detail}" if detail else ""))
        return passed

    def output(self, path: Path | str) -> None:
        self._outputs.append(str(path))

    # ── checkpointing (E10) ──────────────────────────────────

    @staticmethod
    def checkpoint_state(path: Path, state: dict[str, Any]) -> None:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)

    @staticmethod
    def load_state(path: Path, *, resume: bool = True) -> dict[str, Any]:
        if resume and path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                return {}
        return {}

    # ── finish ───────────────────────────────────────────────

    def finish(self, exit_code: int, *, next_command: str = "",
               extra: dict[str, Any] | None = None) -> dict[str, Any]:
        """Summary block + run_end event. Returns the machine summary
        (what --json prints). Always call, success or failure."""
        self._close_line()
        elapsed = time.monotonic() - self._t0
        summary = {
            "schema": SCHEMA, "run_id": self.run_id, "exit_code": exit_code,
            "elapsed_s": round(elapsed, 1),
            "items_done": self._total_done,
            "quarantined": dict(self._quarantined),
            "gates": self._gates,
            "outputs": self._outputs,
            "next_command": next_command or None,
        }
        if extra:
            summary.update(extra)
        self.emit("run_end", n_done=self._total_done,
                  detail=f"exit={exit_code}")
        self._println("── summary")
        self._println(f"  items: {self._total_done:,} done · "
                      f"{sum(self._quarantined.values()):,} quarantined "
                      + (str(dict(self._quarantined))
                         if self._quarantined else ""))
        for g in self._gates:
            self._println(f"  gate {g['gate']}: "
                          + ("PASS" if g["passed"] else "FAIL"))
        for o in self._outputs:
            self._println(f"  wrote {o}")
        self._println(f"  elapsed {_fmt_eta(elapsed)} · exit {exit_code}")
        if next_command:
            self._println(f"  next: {next_command}")
        return summary

    def __enter__(self) -> "RunConsole":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is KeyboardInterrupt:
            self.finish(EXIT_INTERRUPTED)

    # ── terminal rendering ───────────────────────────────────

    def _isatty(self) -> bool:
        try:
            return bool(self.stream.isatty())
        except Exception:
            return False

    def _render_line(self) -> None:
        self.stream.write("\r\x1b[2K  " + self._progress_text())
        self.stream.flush()
        self._line_open = True

    def _close_line(self) -> None:
        if self._line_open:
            self.stream.write("\n")
            self.stream.flush()
            self._line_open = False

    def _println(self, text: str) -> None:
        self._close_line()
        self.stream.write(text + "\n")
        self.stream.flush()
