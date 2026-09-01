"""Budgets and breakers for Ask (E18) — in code, never in prompts.

The research's runaway cases are prevented in the orchestration
layer: per-session token caps, per-turn caps, no worker spawning
workers, oversized results truncated. The stop button and the
breaker share ONE abort path, so "stop" is exercised by every test
that exercises the cap.

Cost is reported only when a rate is configured (SYNAPSE_COST_IN /
SYNAPSE_COST_OUT, dollars per million tokens). No rate, no invented
number: the meter shows tokens and says so.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass, field


class Aborted(RuntimeError):
    """The turn stopped between steps: user stop, or a breaker."""

    def __init__(self, reason: str = "stopped") -> None:
        super().__init__(reason)
        self.reason = reason


class Abort:
    """One signal, two callers: the stop button and the breaker."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self.reason = ""

    def fire(self, reason: str = "stopped") -> None:
        self.reason = reason
        self._event.set()

    def fired(self) -> bool:
        return self._event.is_set()

    def check(self) -> None:
        """Called between pipeline steps: partial state is kept."""
        if self._event.is_set():
            raise Aborted(self.reason or "stopped")


@dataclass
class Budget:
    """Session-scoped accounting. Turn caps bound one turn; session
    caps bound the conversation. Grace fires once, at the threshold,
    so the loop can wrap up instead of dying mid-sentence."""

    session_tokens: int = 400_000
    session_calls: int = 120
    turn_tokens: int = 60_000
    # sized for a navigation turn: up to 24 loop steps (each one model
    # call) plus compose and the verifier. The fixed pipeline used ≤12;
    # the cap still exists and still shares the abort path.
    turn_calls: int = 40
    grace_at: float = 0.85

    tokens_in: int = 0
    tokens_out: int = 0
    calls: int = 0
    turn_tokens_used: int = 0
    turn_calls_used: int = 0
    grace_sent: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock,
                                  repr=False)

    def start_turn(self) -> None:
        with self._lock:
            self.turn_tokens_used = 0
            self.turn_calls_used = 0

    def charge(self, tokens_in: int = 0, tokens_out: int = 0,
               calls: int = 1) -> None:
        with self._lock:
            self.tokens_in += max(0, tokens_in)
            self.tokens_out += max(0, tokens_out)
            self.calls += calls
            self.turn_tokens_used += max(0, tokens_in) + max(0, tokens_out)
            self.turn_calls_used += calls

    @property
    def tokens(self) -> int:
        return self.tokens_in + self.tokens_out

    def cost(self) -> float | None:
        """Dollars, or None when no rate is configured (we do not
        invent prices; the meter then reports tokens)."""
        rate_in = os.environ.get("SYNAPSE_COST_IN")
        rate_out = os.environ.get("SYNAPSE_COST_OUT")
        if not rate_in and not rate_out:
            return None
        try:
            per_in = float(rate_in or 0) / 1_000_000
            per_out = float(rate_out or 0) / 1_000_000
        except ValueError:
            return None
        return round(self.tokens_in * per_in + self.tokens_out * per_out, 4)

    def exceeded(self) -> str:
        """The breaker: '' when clear, else the cap that tripped."""
        if self.tokens >= self.session_tokens:
            return "session token cap"
        if self.calls >= self.session_calls:
            return "session model-call cap"
        if self.turn_tokens_used >= self.turn_tokens:
            return "turn token cap"
        if self.turn_calls_used >= self.turn_calls:
            return "turn model-call cap"
        return ""

    def needs_grace(self) -> bool:
        if self.grace_sent:
            return False
        share = self.tokens / max(1, self.session_tokens)
        if share >= self.grace_at:
            self.grace_sent = True
            return True
        return False

    def tick(self) -> dict:
        """The budget_tick payload: real counters, honest cost."""
        return {
            "tokens_in": self.tokens_in, "tokens_out": self.tokens_out,
            "tokens": self.tokens, "calls": self.calls,
            "session_tokens_cap": self.session_tokens,
            "turn_tokens": self.turn_tokens_used,
            "turn_tokens_cap": self.turn_tokens,
            "cost_usd": self.cost(),
            "cost_note": ("rate not configured: the meter reports tokens"
                          if self.cost() is None else ""),
        }


def truncate_rows(rows: list, cap: int = 200) -> tuple[list, int]:
    """Oversized results never enter context whole; the count of what
    was withheld travels with them so nothing is silently dropped."""
    if len(rows) <= cap:
        return rows, 0
    return rows[:cap], len(rows) - cap
