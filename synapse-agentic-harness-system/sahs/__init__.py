"""Meridian — the siloed agentic-analytics platform (import package: sahs).

Layers: canon (L1) → graph (L2) → compiler (L3) → tools (L4), with evals as
the ground under everything. Nothing here imports from synapse/, apps/, or
lumi_final/ — the silo's only sanctioned carry-overs are the SVC-ID
connectivity contract (util.auth) and the ADK plain-function tool pattern,
both implemented fresh.
"""

__version__ = "0.1.0"
CANON_RULESET = 1
