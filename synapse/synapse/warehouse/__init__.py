"""Gated warehouse execution — the agent asks, the gate chain decides.

Execution is a WORKFLOW, not agent discretion: every query passes the
same deterministic checks in the same order, and every attempt lands on
an append-only audit ledger. The agent's only powers are `dry_run` and
`execute`; the gates are code.
"""

from synapse.warehouse.runner import GateConfig, WarehouseRunner

__all__ = ["GateConfig", "WarehouseRunner"]
