"""Plan v2 (E18): the versioned semantic plan — the stateful spine of
a session. Conversation history is NOT the state; this is.

Two pins live here:
  * **grain is required.** A plan without a grain cannot reach the
    contract, so no answer can be rendered without saying what one
    row means. The serializer refuses; it is not a lint.
  * **mutation is single-slot and deterministic.** "same for Canada"
    changes exactly one slot, computed in code, never re-planned by a
    model. ``apply_edit`` raises if an edit would move more than one.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

# every mutable named part of a plan. filters are addressed as
# "filters.<name>" so a mutation names one slot exactly.
BASE_SLOTS: tuple[str, ...] = (
    "metric", "grain", "table", "time_window", "lob", "dimensions",
    "checks",
)


class PlanError(ValueError):
    """A plan refused to become something a steward could not defend."""


@dataclass(frozen=True)
class Plan:
    question: str = ""
    metric_id: str = ""
    metric_fp: str = ""
    metric_label: str = ""
    metric_sql: str = ""
    grain: str = ""                     # REQUIRED before contract
    table: str = ""
    time_window: str = ""
    lob: str = ""
    dimensions: tuple[str, ...] = ()
    # what the answer must survive (Agent Loop v1): reconciliation and
    # sanity checks the model wrote down while navigating. The verifier
    # reads them; an empty tuple is legal and means "the contract's
    # defaults only".
    checks: tuple[str, ...] = ()
    filters: dict[str, str] = field(default_factory=dict)
    # filter name → the real column/expression the build binds it to.
    # Resolved evidence, not a slot: it never shows up in a diff.
    filter_bindings: dict[str, str] = field(default_factory=dict)
    version: int = 1
    parent: int | None = None
    # slot → how it got its value: user | resolver | inherited | default
    provenance: dict[str, str] = field(default_factory=dict)
    unresolved: tuple[str, ...] = ()

    # ── slots ────────────────────────────────────────────────
    def slots(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "metric": self.metric_id, "grain": self.grain,
            "table": self.table, "time_window": self.time_window,
            "lob": self.lob, "dimensions": list(self.dimensions),
            "checks": list(self.checks),
        }
        for name, value in self.filters.items():
            out[f"filters.{name}"] = value
        return out

    def get_slot(self, slot: str) -> Any:
        return self.slots().get(slot)

    # ── serialization ────────────────────────────────────────
    def to_dict(self) -> dict[str, Any]:
        return {
            "question": self.question, "metric_id": self.metric_id,
            "metric_fp": self.metric_fp, "metric_label": self.metric_label,
            "metric_sql": self.metric_sql, "grain": self.grain,
            "table": self.table, "time_window": self.time_window,
            "lob": self.lob, "dimensions": list(self.dimensions),
            "checks": list(self.checks),
            "filters": dict(self.filters),
            "filter_bindings": dict(self.filter_bindings),
            "version": self.version,
            "parent": self.parent, "provenance": dict(self.provenance),
            "unresolved": list(self.unresolved),
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Plan":
        return Plan(
            question=data.get("question", ""),
            metric_id=data.get("metric_id", ""),
            metric_fp=data.get("metric_fp", ""),
            metric_label=data.get("metric_label", ""),
            metric_sql=data.get("metric_sql", ""),
            grain=data.get("grain", ""),
            table=data.get("table", ""),
            time_window=data.get("time_window", ""),
            lob=data.get("lob", ""),
            dimensions=tuple(data.get("dimensions", ())),
            checks=tuple(data.get("checks", ())),
            filters=dict(data.get("filters", {})),
            filter_bindings=dict(data.get("filter_bindings", {})),
            version=int(data.get("version", 1)),
            parent=data.get("parent"),
            provenance=dict(data.get("provenance", {})),
            unresolved=tuple(data.get("unresolved", ())),
        )

    # ── the contract gate ────────────────────────────────────
    def ready(self) -> tuple[bool, list[str]]:
        """Can this plan be contracted? Missing REQUIRED parts named."""
        missing = []
        if not self.metric_id:
            missing.append("metric")
        if not self.grain:
            missing.append("grain")     # pinned: no grain, no answer
        if not self.table:
            missing.append("table")
        missing += [s for s in self.unresolved if s not in missing]
        return (not missing), missing

    def summary(self) -> str:
        """The one-line plan summary (also the session auto-title)."""
        bits = [self.metric_label or self.metric_id or "?"]
        if self.grain:
            bits.append(f"by {self.grain}")
        for name, value in sorted(self.filters.items()):
            bits.append(f"{name}={value}")
        if self.time_window:
            bits.append(self.time_window)
        return " · ".join(bits)


def diff(before: Plan, after: Plan) -> list[dict[str, Any]]:
    """Slot-level diff, the payload behind the plan_delta event."""
    old, new = before.slots(), after.slots()
    changes = []
    for slot in sorted(set(old) | set(new)):
        a, b = old.get(slot), new.get(slot)
        if a != b:
            changes.append({"slot": slot, "from": a, "to": b})
    return changes


def apply_edit(plan: Plan, slot: str, value: Any, *,
               actor: str = "user") -> tuple[Plan, list[dict[str, Any]]]:
    """Deterministic single-slot mutation → (new plan, delta).

    The version increments and the parent is recorded, so the version
    stepper can scrub and a restore is just another edit."""
    fields: dict[str, Any] = {}
    if slot.startswith("filters."):
        name = slot.split(".", 1)[1]
        if not name:
            raise PlanError("a filter slot needs a name")
        filters = dict(plan.filters)
        if value in (None, ""):
            filters.pop(name, None)
        else:
            filters[name] = value
        fields["filters"] = filters
    elif slot == "metric":
        fields["metric_id"] = value or ""
    elif slot in ("dimensions", "checks"):
        fields[slot] = tuple(value or ())
    elif slot in BASE_SLOTS:
        fields[slot] = value or ""
    else:
        raise PlanError(f"unknown slot {slot!r}: plans mutate by named "
                        f"slot, never by free-form rewrite")

    provenance = dict(plan.provenance)
    provenance[slot] = actor
    candidate = replace(plan, version=plan.version + 1, parent=plan.version,
                        provenance=provenance, **fields)
    changes = diff(plan, candidate)
    # the pin, enforced rather than requested: one edit moves one slot
    if len(changes) > 1:
        raise PlanError(
            "an edit moved more than one slot "
            f"({', '.join(c['slot'] for c in changes)}): mutations are "
            "single-slot so a diff is always defensible")
    return candidate, changes
