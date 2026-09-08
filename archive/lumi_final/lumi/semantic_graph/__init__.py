"""LUMI semantic graph — production knowledge layer.

Apache AGE on PostgreSQL. Same Postgres instance as Radix → federation
free. Confidence-typed, event-sourced, append-only with provenance
on every claim.

Layout:
    config.py       — AGE_ENABLED flag, conn string, source weights, decay windows
    schema.py       — Cypher DDL: node + edge labels, indexes; idempotent bootstrap
    projector.py    — event_type → Cypher MERGE templates (the only translator)
    writer.py       — dual-write: JSONL audit + AGE materialized view
    replay.py       — JSONL → AGE rehydration (recovery + bootstrap from scratch)

The graph is the product. LookML is the first consumer rendering. The
JSONL event log is the durable audit trail; AGE is a queryable
projection that can be wiped and rebuilt from the events with no data
loss. This is the same pattern that makes Wikidata operationally safe.
"""

from __future__ import annotations
