"""Persistent, event-sourced ontology store.

The ontology is the system of record. We learn it from every signal
source (parse_sqls, fetch_mdm, parse_baseline, approve_plan, critic) and
gate promotion of candidates into the canonical ontology with explicit
evidence thresholds. Nothing fancy — just versioned JSON on disk so we
can scale from 29 tables to 8000 without rewriting the storage layer.

Layout under ``data/ontology/``:

    current.json            the canonical DomainOntology Radix sees today
    versions/
      v0001.json            snapshot at promotion time
      v0002.json
      ...
    events/
      2026-05-03.jsonl      one OntologyEvent per line, append-only
    candidates.json         tallied evidence per (entity, signal-type)
                            promoted to current.json on threshold

The four event hooks are co-located here as helpers so callers don't
have to know how to construct events:

    record_equivalences_from_fingerprints(fps)
    record_entity_hints_from_mdm(mdm_columns, table)
    record_curated_synonyms_from_baseline(ctx)
    record_approval_lock(approval, plan, ctx)

Each hook returns the number of events recorded — useful for logging
and tests.

Public API:

    OntologyStore(root: Path)
        .record(event)                            -> None (appends)
        .record_many(events)                      -> int
        .candidates() -> list[dict]               -> read tallied candidates
        .promote_candidates(threshold=...)        -> DomainOntology
        .snapshot(ontology, reason)               -> Path (versions/v00NN.json)
        .current() -> DomainOntology | None
        .save_current(ontology)                   -> Path

    record_equivalences_from_fingerprints(...)
    record_entity_hints_from_mdm(...)
    record_curated_synonyms_from_baseline(...)
    record_approval_lock(...)
"""

from __future__ import annotations

import contextlib
import datetime as _dt
import hashlib
import json
import logging
import re
import threading
from pathlib import Path
from typing import Any, Iterator

from lumi.schemas import (
    DomainOntology,
    OntologyEntity,
    OntologyEvent,
    PlanApproval,
    TableContext,
)
from lumi.sql_to_context import SQLFingerprint

logger = logging.getLogger("lumi.ontology_store")


_DEFAULT_ROOT = Path("data/ontology")


# ─── Store ───────────────────────────────────────────────────


class OntologyStore:
    """Versioned, event-sourced store on the local filesystem.

    Bounded mutability: anyone can append events. Promotion of candidates
    into ``current.json`` requires explicit evidence threshold OR human
    approval (vocabulary_lock event). Snapshots in ``versions/`` are the
    audit trail.
    """

    def __init__(self, root: Path = _DEFAULT_ROOT) -> None:
        self.root = Path(root)
        self.events_dir = self.root / "events"
        self.versions_dir = self.root / "versions"
        self.current_path = self.root / "current.json"
        self.candidates_path = self.root / "candidates.json"
        self.lock_path = self.root / ".lock"
        self.seen_hashes_path = self.root / "seen_hashes.txt"
        self.root.mkdir(parents=True, exist_ok=True)
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.versions_dir.mkdir(parents=True, exist_ok=True)
        # Process-local hash cache. Persisted to seen_hashes.txt across
        # runs. Keeps idempotency cheap: O(1) skip on duplicate.
        self._hash_lock = threading.Lock()
        self._seen_hashes: set[str] = self._load_seen_hashes()

    # ─── events: append-only, idempotent ────────────────────

    def record(self, event: OntologyEvent) -> bool:
        """Append one event + tally; idempotent on content hash.

        Returns True if recorded, False if already seen (deduped).
        """
        if not event.observed_at:
            event.observed_at = _now_iso()
        if not event.content_hash:
            event.content_hash = _compute_content_hash(event)

        with self._hash_lock:
            if event.content_hash in self._seen_hashes:
                return False
            self._seen_hashes.add(event.content_hash)

        # Persist hash + append to JSONL + tally — all under process+file lock
        # so concurrent plan agents don't corrupt candidates.json.
        with self._file_lock():
            self._persist_hash(event.content_hash)
            path = self._events_path_for(event.observed_at)
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(event.model_dump(), default=str) + "\n")
            self._tally_into_candidates(event)
        return True

    def record_many(self, events: list[OntologyEvent]) -> int:
        n = 0
        for ev in events:
            if self.record(ev):
                n += 1
        return n

    # ─── candidates: tallied evidence ───────────────────────

    def candidates(self) -> dict[str, Any]:
        """Read the candidates dict (creating an empty one if missing)."""
        if not self.candidates_path.exists():
            return {
                "entities": {},      # name -> {synonyms{}, grain_columns{}, evidence_count}
                "equivalences": [],  # list of {a_table, a_col, b_table, b_col, count}
                "synonyms": {},      # canonical -> {alt -> count}
                "primary_keys": {},  # table -> {column -> count}
                "cardinalities": {}, # key -> {kind, votes:{kind:count}, evidence:[]}
                "join_paths": [],    # list of {base, chain:[(table, jt)], freq}
            }
        try:
            data: dict[str, Any] = json.loads(
                self.candidates_path.read_text(encoding="utf-8"),
            )
            return data
        except Exception as e:  # noqa: BLE001
            logger.warning("candidates.json unparseable, starting fresh: %s", e)
            return {
                "entities": {}, "equivalences": [],
                "synonyms": {}, "primary_keys": {},
                "cardinalities": {}, "join_paths": [],
            }

    def _save_candidates(self, c: dict[str, Any]) -> None:
        self.candidates_path.write_text(
            json.dumps(c, indent=2, default=str), encoding="utf-8",
        )

    def _tally_into_candidates(self, event: OntologyEvent) -> None:
        c = self.candidates()
        if event.event_type == "equivalence_observed":
            a_t = event.payload.get("a_table") or event.table_name
            a_c = event.payload.get("a_column") or event.column_name
            b_t = event.payload.get("b_table")
            b_c = event.payload.get("b_column")
            if not all((a_t, a_c, b_t, b_c)):
                return
            # Deterministic ordering so (a,b) and (b,a) tally to one bucket.
            pair = sorted([(a_t, a_c), (b_t, b_c)])
            key = f"{pair[0][0]}.{pair[0][1]}↔{pair[1][0]}.{pair[1][1]}"
            existing = next(
                (e for e in c["equivalences"] if e.get("key") == key), None,
            )
            if existing is None:
                c["equivalences"].append({
                    "key": key,
                    "a_table": pair[0][0], "a_column": pair[0][1],
                    "b_table": pair[1][0], "b_column": pair[1][1],
                    "count": 1,
                })
            else:
                existing["count"] = (existing.get("count") or 0) + 1
        elif event.event_type == "entity_hint":
            ent = event.entity_name or event.payload.get("entity")
            if not ent:
                return
            bucket = c["entities"].setdefault(ent, {
                "synonyms": {}, "grain_columns": {},
                "evidence_count": 0, "evidence": [],
            })
            bucket["evidence_count"] = bucket.get("evidence_count", 0) + 1
            tbl = event.table_name
            col = event.column_name
            if tbl and col:
                bucket["grain_columns"].setdefault(tbl, [])
                if col not in bucket["grain_columns"][tbl]:
                    bucket["grain_columns"][tbl].append(col)
            if event.evidence and len(bucket["evidence"]) < 8:
                bucket["evidence"].append(event.evidence)
        elif event.event_type in {"synonym_candidate", "curated_synonym"}:
            canonical = (event.payload.get("canonical")
                         or event.entity_name or "")
            alt = event.payload.get("synonym") or ""
            if not (canonical and alt):
                return
            syns = c["synonyms"].setdefault(canonical, {})
            syns[alt] = syns.get(alt, 0) + 1
        elif event.event_type == "curated_pk":
            tbl = event.table_name
            col = event.column_name
            if not (tbl and col):
                return
            pks = c["primary_keys"].setdefault(tbl, {})
            pks[col] = pks.get(col, 0) + 1
        elif event.event_type == "vocabulary_lock":
            # Approvals carry strong weight — bump the matching entity.
            ent = event.entity_name
            if ent:
                bucket = c["entities"].setdefault(ent, {
                    "synonyms": {}, "grain_columns": {},
                    "evidence_count": 0, "evidence": [],
                })
                # +5 weight for human approval — promotes faster.
                bucket["evidence_count"] = bucket.get("evidence_count", 0) + 5
        elif event.event_type == "entity_refinement":
            # Critic findings — nudge entity + synonym candidates. The
            # entity itself gets a small evidence bump so a critic-only
            # signal can promote a marginal entity.
            canonical = event.entity_name or ""
            alt = event.payload.get("synonym") or ""
            if canonical:
                bucket = c["entities"].setdefault(canonical, {
                    "synonyms": {}, "grain_columns": {},
                    "evidence_count": 0, "evidence": [],
                })
                bucket["evidence_count"] = bucket.get("evidence_count", 0) + 1
                if event.evidence and len(bucket["evidence"]) < 8:
                    bucket["evidence"].append(event.evidence)
                # If the refinement points at a specific table+column,
                # capture it as a grain hint too.
                tbl = event.table_name
                col = event.column_name
                if tbl and col:
                    bucket["grain_columns"].setdefault(tbl, [])
                    if col not in bucket["grain_columns"][tbl]:
                        bucket["grain_columns"][tbl].append(col)
            if canonical and alt:
                syns = c["synonyms"].setdefault(canonical, {})
                syns[alt] = syns.get(alt, 0) + 1
        elif event.event_type == "cardinality_observed":
            # payload: {left_table, left_column, right_table, right_column,
            #           cardinality, vote_breakdown, observations}
            p = event.payload or {}
            lt, lc = p.get("left_table"), p.get("left_column")
            rt, rc = p.get("right_table"), p.get("right_column")
            kind = p.get("cardinality") or "unknown"
            if not all([lt, lc, rt, rc]):
                return
            # Normalize key so a→b and b→a fold to one bucket; we keep
            # the directional kind separately.
            sorted_pair = sorted([(lt, lc), (rt, rc)])
            key = f"{sorted_pair[0][0]}.{sorted_pair[0][1]}↔{sorted_pair[1][0]}.{sorted_pair[1][1]}"
            bucket = c["cardinalities"].setdefault(key, {
                "left_table": sorted_pair[0][0],
                "left_column": sorted_pair[0][1],
                "right_table": sorted_pair[1][0],
                "right_column": sorted_pair[1][1],
                "votes": {}, "evidence": [], "observations": 0,
            })
            # If event direction differs from canonical, invert kind.
            if (lt, lc) != sorted_pair[0]:
                kind = {
                    "one_to_many": "many_to_one",
                    "many_to_one": "one_to_many",
                }.get(kind, kind)
            bucket["votes"][kind] = bucket["votes"].get(kind, 0) + 1
            bucket["observations"] = bucket.get("observations", 0) + (
                p.get("observations") or 1
            )
            if event.evidence and len(bucket["evidence"]) < 6:
                bucket["evidence"].append(event.evidence)
        elif event.event_type == "join_path_observed":
            # payload: {base, chain:[(table, jt)], frequency}
            p = event.payload or {}
            base = p.get("base")
            chain = p.get("chain")
            if not (base and chain):
                return
            chain_tup = tuple(tuple(item) for item in chain)
            key = f"{base}::{'/'.join(t for t, _ in chain_tup)}"
            existing = next(
                (e for e in c["join_paths"] if e.get("key") == key), None,
            )
            if existing is None:
                c["join_paths"].append({
                    "key": key, "base": base, "chain": [list(x) for x in chain_tup],
                    "frequency": int(p.get("frequency") or 1),
                })
            else:
                existing["frequency"] = int(existing.get("frequency") or 0) + int(
                    p.get("frequency") or 1
                )
        self._save_candidates(c)

    # ─── promotion + snapshot ───────────────────────────────

    def promote_candidates(
        self,
        *,
        evidence_threshold: int = 2,
        equivalence_threshold: int = 1,
    ) -> DomainOntology:
        """Materialize candidates into a DomainOntology.

        Promotion rules:
          - An entity with evidence_count >= ``evidence_threshold`` is
            promoted into the ontology with all its tallied synonyms +
            grain_columns.
          - Equivalence pairs with count >= ``equivalence_threshold`` are
            recorded as evidence on the matching entities.
          - Curated PKs from baseline are surfaced under
            grain_columns[table] for the matching entity.
        """
        c = self.candidates()
        entities: list[OntologyEntity] = []
        for name, bucket in c.get("entities", {}).items():
            ev_count = bucket.get("evidence_count", 0)
            if ev_count < evidence_threshold:
                continue
            syns_for_entity = c.get("synonyms", {}).get(name, {})
            sorted_syns = sorted(
                syns_for_entity.items(), key=lambda kv: -kv[1],
            )
            entities.append(OntologyEntity(
                name=name,
                synonyms=[s for s, _ in sorted_syns[:8]],
                grain_columns=bucket.get("grain_columns", {}),
                description="",
                evidence=bucket.get("evidence", []),
            ))
        # Map each table to its strongest entity by column count.
        table_to_primary: dict[str, str] = {}
        for ent in entities:
            for tbl, cols in ent.grain_columns.items():
                cur = table_to_primary.get(tbl)
                if cur is None:
                    table_to_primary[tbl] = ent.name
                else:
                    cur_ent = next((e for e in entities if e.name == cur), None)
                    cur_n = (
                        len(cur_ent.grain_columns.get(tbl, []))
                        if cur_ent else 0
                    )
                    if len(cols) > cur_n:
                        table_to_primary[tbl] = ent.name
        return DomainOntology(
            entities=entities,
            relationships=[],  # promoted later from equivalence + critic events
            table_to_primary_entity=table_to_primary,
            authoring={
                "mode": "event_sourced",
                "reason": (
                    f"promoted from candidates with threshold "
                    f"evidence>={evidence_threshold}, equivalence>={equivalence_threshold}"
                ),
            },
        )

    def snapshot(self, ontology: DomainOntology, reason: str = "") -> Path:
        """Write a versioned snapshot of the given ontology."""
        existing = sorted(self.versions_dir.glob("v*.json"))
        next_n = 1
        if existing:
            last = existing[-1].stem
            m = re.match(r"v(\d+)", last)
            if m:
                next_n = int(m.group(1)) + 1
        target = self.versions_dir / f"v{next_n:04d}.json"
        target.write_text(
            json.dumps(
                {
                    "snapshot_reason": reason,
                    "snapshot_at": _now_iso(),
                    "ontology": ontology.model_dump(),
                },
                indent=2, default=str,
            ),
            encoding="utf-8",
        )
        return target

    def current(self) -> DomainOntology | None:
        if not self.current_path.exists():
            return None
        try:
            return DomainOntology(
                **json.loads(self.current_path.read_text(encoding="utf-8")),
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("current.json unparseable: %s", e)
            return None

    def save_current(self, ontology: DomainOntology) -> Path:
        self.current_path.write_text(
            json.dumps(ontology.model_dump(), indent=2, default=str),
            encoding="utf-8",
        )
        return self.current_path

    # ─── unified refresh — single entry point ───────────────

    def refresh(
        self,
        *,
        seed_fn: Any = None,
        force_seed: bool = False,
        evidence_threshold: int = 2,
        equivalence_threshold: int = 1,
        snapshot_reason: str = "",
    ) -> DomainOntology:
        """Promote candidates → save current → snapshot.

        This is the SINGLE entry point that produces a usable
        DomainOntology. Pipeline code reads ``store.current()`` to
        consume the result; nothing reads the legacy ``data/ontology.json``.

        Args:
            seed_fn: optional callable(store) -> int that emits seed
                events (e.g. one-shot LLM ontology synthesis on cold
                start). Called only if current.json is missing.
            evidence_threshold / equivalence_threshold: see promote_candidates
            snapshot_reason: short note on why we're refreshing — lands
                in versions/<vNNNN>.json.
        """
        if seed_fn is not None and (
            force_seed or not self.current_path.exists()
        ):
            try:
                n = seed_fn(self)
                logger.info("seeded %d events from seed_fn", n)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "seed_fn failed (continuing with non-seed events): %s", e,
                )
        ontology = self.promote_candidates(
            evidence_threshold=evidence_threshold,
            equivalence_threshold=equivalence_threshold,
        )
        # Add deterministic + curated relationships from candidates.
        ontology.relationships = list(self._infer_relationships(ontology))
        self.save_current(ontology)
        if snapshot_reason:
            self.snapshot(ontology, reason=snapshot_reason)
        return ontology

    def _infer_relationships(
        self, ontology: DomainOntology,
    ) -> Iterator[Any]:
        """Convert tallied equivalences + cardinalities into relationships.

        Strategy:
          1. Find entity pairs connected by an equivalence.
          2. For each such pair, look up the strongest cardinality vote
             from the cardinalities bucket. If found, emit with that
             cardinality + evidence. Otherwise, emit unknown.
        """
        from lumi.schemas import OntologyRelationship  # local import

        c = self.candidates()
        cols_by_entity: dict[str, set[tuple[str, str]]] = {}
        for ent in ontology.entities:
            cols_by_entity[ent.name] = {
                (t, col)
                for t, cols in ent.grain_columns.items() for col in cols
            }

        # Index cardinalities by sorted column pair so we can look up
        # the inferred kind for any (a→b) edge between entities.
        card_lookup: dict[
            tuple[tuple[str, str], tuple[str, str]], dict[str, Any],
        ] = {}
        for bucket in (c.get("cardinalities") or {}).values():
            lt = bucket.get("left_table")
            lc = bucket.get("left_column")
            rt = bucket.get("right_table")
            rc = bucket.get("right_column")
            if not all([lt, lc, rt, rc]):
                continue
            sorted_pair = tuple(sorted([(lt, lc), (rt, rc)]))
            card_lookup[(sorted_pair[0], sorted_pair[1])] = bucket

        seen_pairs: set[tuple[str, str]] = set()
        for eq in c.get("equivalences", []):
            a = (eq.get("a_table"), eq.get("a_column"))
            b = (eq.get("b_table"), eq.get("b_column"))
            ent_a = next(
                (n for n, cols in cols_by_entity.items() if a in cols), None,
            )
            ent_b = next(
                (n for n, cols in cols_by_entity.items() if b in cols), None,
            )
            if not (ent_a and ent_b) or ent_a == ent_b:
                continue
            pair = tuple(sorted([ent_a, ent_b]))
            pair_key: tuple[str, str] = (pair[0], pair[1])
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            # Look up cardinality.
            sorted_cols = tuple(sorted([a, b]))
            card_bucket = card_lookup.get((sorted_cols[0], sorted_cols[1]))
            if card_bucket and card_bucket.get("votes"):
                votes = card_bucket["votes"]
                top_kind = max(votes, key=lambda k: votes[k])
                # Direction handling: card_bucket stores from sorted[0] to sorted[1].
                # If our entity ordering matches sorted ordering, take as-is.
                if sorted_cols[0] == a:
                    from_e, to_e = ent_a, ent_b
                else:
                    from_e, to_e = ent_b, ent_a
                ev_lines = card_bucket.get("evidence", [])[:2]
                ev_str = "; ".join(ev_lines) or "no evidence captured"
                yield OntologyRelationship(
                    from_entity=from_e,
                    to_entity=to_e,
                    cardinality=top_kind,
                    evidence=(
                        f"votes={votes}, observations="
                        f"{card_bucket.get('observations', 0)}; {ev_str}"
                    ),
                )
            else:
                yield OntologyRelationship(
                    from_entity=ent_a,
                    to_entity=ent_b,
                    cardinality="unknown",
                    evidence=(
                        f"observed via {eq.get('count', 1)}× JOIN ON "
                        f"{a[0]}.{a[1]} ↔ {b[0]}.{b[1]}"
                    ),
                )

    # ─── helpers ─────────────────────────────────────────────

    def _events_path_for(self, iso_ts: str) -> Path:
        date_part = iso_ts.split("T")[0] if "T" in iso_ts else iso_ts[:10]
        return self.events_dir / f"{date_part}.jsonl"

    @contextlib.contextmanager
    def _file_lock(self) -> Iterator[None]:
        """Cross-process exclusive lock on the store. POSIX fcntl.

        Plan stage runs N parallel agents; each emits events that
        read-modify-write candidates.json. Without this lock, last writer
        wins → silent data loss. fcntl is process-blocking but lock
        granularity is per-event (~milliseconds), so contention is low.
        Falls back gracefully on platforms without fcntl (Windows).
        """
        try:
            import fcntl
        except ImportError:
            yield
            return
        # Touch lockfile so we have an FD to lock.
        self.lock_path.touch(exist_ok=True)
        with self.lock_path.open("a") as fh:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                yield
            finally:
                with contextlib.suppress(Exception):
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    def _load_seen_hashes(self) -> set[str]:
        if not self.seen_hashes_path.exists():
            return set()
        try:
            return {
                line.strip()
                for line in self.seen_hashes_path.read_text(
                    encoding="utf-8",
                ).splitlines()
                if line.strip()
            }
        except Exception:  # noqa: BLE001
            return set()

    def _persist_hash(self, h: str) -> None:
        with self.seen_hashes_path.open("a", encoding="utf-8") as f:
            f.write(h + "\n")

    def reset_for_test(self) -> None:
        """Wipe all state — for tests only. Safe because it touches
        only the configured root path."""
        for p in [
            self.candidates_path, self.current_path, self.seen_hashes_path,
            self.lock_path,
        ]:
            if p.exists():
                p.unlink()
        for d in [self.events_dir, self.versions_dir]:
            if d.exists():
                for f in d.iterdir():
                    f.unlink()
        self._seen_hashes = set()


# ─── Per-signal-source hooks ─────────────────────────────────


# Cardmember/customer/account naming patterns — same hint set as the
# ontology builder. Used to assign a canonical entity to a column when
# only naming evidence is available.
_ENTITY_HINTS: dict[str, list[str]] = {
    "cardmember": [
        "cardmember", "card_member", "cm_", "cm11", "cm15", "cmember",
    ],
    "customer": ["customer", "cust_", "cust_id", "cust_xref"],
    "account": ["account", "acct_", "acct_id", "acct_xref"],
    "transaction": ["transaction", "txn_", "trans_", "billed", "spend"],
    "merchant": ["merchant", "merch_", "merchant_id"],
    "product": ["product", "prod_", "prdct_"],
    "risk": ["risk_", "fico_", "delinquency", "default_"],
}


def _classify_to_entity(col: str) -> str | None:
    if not col:
        return None
    cl = col.lower()
    matches: list[tuple[str, int]] = []
    for ent, hints in _ENTITY_HINTS.items():
        for h in hints:
            if h in cl:
                matches.append((ent, len(h)))
    if not matches:
        return None
    matches.sort(key=lambda kv: -kv[1])
    return matches[0][0]


def record_cardinalities_from_fingerprints(
    store: OntologyStore, fingerprints: list[SQLFingerprint],
) -> int:
    """Hook 5 — corpus-wide JOIN cardinality + path inference.

    Runs after equivalences are recorded. Uses ``lumi.joins`` to infer
    one_to_many / many_to_one / many_to_many for each unique JOIN pair
    (majority vote across all 138 query observations) AND extracts the
    top canonical multi-hop chains.

    Emits ``cardinality_observed`` events (one per pair) and
    ``join_path_observed`` events (one per chain). These are the
    signals the LLM planner uses to author correct
    proposed_explore.relationship values, and the critic uses to block
    plans that contradict observed cardinality.
    """
    from lumi.joins import infer_canonical_paths, infer_join_cardinalities

    cards = infer_join_cardinalities(fingerprints)
    paths = infer_canonical_paths(fingerprints, top_k=20)

    events: list[OntologyEvent] = []
    for c in cards:
        if c.cardinality == "unknown" and c.confidence < 0.5:
            continue
        events.append(OntologyEvent(
            event_type="cardinality_observed",
            source="parse_sqls",
            table_name=c.left_table,
            column_name=c.left_column,
            payload={
                "left_table": c.left_table,
                "left_column": c.left_column,
                "right_table": c.right_table,
                "right_column": c.right_column,
                "cardinality": c.cardinality,
                "vote_breakdown": c.vote_breakdown,
                "observations": c.observations,
            },
            confidence=c.confidence,
            evidence=c.evidence[0] if c.evidence else "",
        ))
    for p in paths:
        events.append(OntologyEvent(
            event_type="join_path_observed",
            source="parse_sqls",
            table_name=p.base_table,
            payload={
                "base": p.base_table,
                "chain": [[t, jt or "inner"] for t, jt in p.chain],
                "frequency": p.frequency,
            },
            confidence=min(0.95, 0.5 + 0.05 * p.frequency),
            evidence=f"observed in {p.frequency} quer(y/ies)",
        ))
    store.record_many(events)
    return len(events)


def record_equivalences_from_fingerprints(
    store: OntologyStore, fingerprints: list[SQLFingerprint],
) -> int:
    """Hook 1 — every JOIN ON pair is an equivalence claim.

    Emits one event per unique (table.col ↔ table.col) pair seen across
    the corpus. Confidence proportional to observation count.
    """
    pair_counts: dict[tuple[tuple[str, str], tuple[str, str]], int] = {}
    for fp in fingerprints:
        if fp.parse_error:
            continue
        from_t = fp.primary_table
        for j in fp.joins or []:
            other_t = j.get("right_table") or j.get("other_table")
            lk = j.get("left_key")
            rk = j.get("right_key")
            if not (from_t and other_t and lk and rk):
                continue
            sorted_pair = sorted([(from_t, lk), (other_t, rk)])
            pair: tuple[tuple[str, str], tuple[str, str]] = (
                sorted_pair[0], sorted_pair[1],
            )
            pair_counts[pair] = pair_counts.get(pair, 0) + 1

    events: list[OntologyEvent] = []
    for (a, b), count in pair_counts.items():
        confidence = min(0.95, 0.5 + 0.05 * count)
        events.append(OntologyEvent(
            event_type="equivalence_observed",
            source="parse_sqls",
            table_name=a[0],
            column_name=a[1],
            payload={
                "a_table": a[0], "a_column": a[1],
                "b_table": b[0], "b_column": b[1],
                "count": count,
            },
            confidence=confidence,
            evidence=f"observed in {count} JOIN ON pairs across the corpus",
        ))
    store.record_many(events)
    return len(events)


def record_corpus_facts(
    store: OntologyStore, fingerprints: list[SQLFingerprint],
) -> int:
    """Hook 1b — every corpus signal becomes a graph event (the "verb layer").

    Symmetric with ``record_entity_hints_from_mdm``: where MDM gives us
    nouns (Table/Column/Entity/PII/Partition), the 122 queries give us
    verbs (Metric / Threshold / Filter / FilterValue / TimeGrain /
    QuestionPattern / Cohort). Without this, the graph has structure but
    not semantics.

    Pattern follows ``record_equivalences_from_fingerprints``: pre-aggregate
    counts across the corpus, then emit ONE event per unique fact with
    ``count`` in the payload. Idempotent re-runs accumulate count via MERGE.

    Emits (per unique fact across all fps):
      - metric_observed         (table, column, function, distinct)
      - threshold_observed      (table, source_column, kind, value, label)
      - filter_observed         (table, column, operator, values, is_structural)
      - time_grain_observed     (table, column, grain)
      - cohort_observed         (cohort_name, tables, source_filters)
      - question_pattern_observed (cluster signature, member_query_ids)
    """
    # ── Per-fact aggregation ──
    metrics: dict[tuple[str, str, str, bool, str], int] = {}
    thresholds: dict[tuple[str, str, str, Any, str], int] = {}
    filters: dict[tuple[str, str, str, bool, tuple], int] = {}
    grains: dict[tuple[str, str, str], int] = {}
    cohorts: dict[str, dict[str, Any]] = {}

    for fp in fingerprints:
        if fp.parse_error:
            continue
        primary = fp.primary_table or (fp.tables[0] if fp.tables else None)
        if not primary:
            continue

        # 1. metric_observed — aggregations
        for agg in fp.aggregations or []:
            fn = (agg.get("function") or "").upper()
            col = agg.get("column")
            alias = agg.get("alias") or ""
            distinct = bool(agg.get("distinct"))
            if not (fn and col):
                continue
            key = (primary, str(col), fn, distinct, str(alias))
            metrics[key] = metrics.get(key, 0) + 1

        # 2. threshold_observed — case_whens + derived_dim_proposals
        for cw in fp.case_whens or []:
            src = cw.get("source_column")
            if not src:
                continue
            for label, sql_or_bound in (cw.get("mapped_values") or {}).items():
                key = (primary, str(src), "boundary", sql_or_bound, str(label))
                thresholds[key] = thresholds.get(key, 0) + 1
        for prop in getattr(fp, "derived_dim_proposals", None) or []:
            src = prop.get("source_column")
            if not src:
                continue
            for bucket in (prop.get("buckets") or []):
                if isinstance(bucket, dict):
                    val = bucket.get("threshold") or bucket.get("value")
                    label = bucket.get("label") or bucket.get("name") or ""
                    kind = bucket.get("kind") or "boundary"
                else:
                    val, label, kind = bucket, "", "boundary"
                key = (primary, str(src), kind, val, str(label))
                thresholds[key] = thresholds.get(key, 0) + 1

        # 3. filter_observed — WHERE predicates (skip is_structural=None safety)
        for flt in fp.filters or []:
            col = flt.get("column")
            if not col:
                continue
            op = (flt.get("operator") or "=").upper()
            is_struct = bool(flt.get("is_structural"))
            val = flt.get("value")
            # Normalize IN-list values for stable hash
            if op == "IN" and isinstance(val, str):
                vals = tuple(sorted(
                    v.strip().strip("'\"") for v in
                    val.strip("()").split(",") if v.strip()
                ))
            elif val is None or val == "":
                vals = ()
            else:
                vals = (str(val).strip().strip("'\""),)
            key = (primary, str(col), op, is_struct, vals)
            filters[key] = filters.get(key, 0) + 1

        # 4. time_grain_observed — date_functions
        for dfn in fp.date_functions or []:
            col = dfn.get("column")
            fn = (dfn.get("function") or "").upper()
            if not (col and fn):
                continue
            grain = ""
            if fn.startswith("DATE_TRUNC_"):
                grain = fn.replace("DATE_TRUNC_", "").lower()
            elif fn in {"YEAR", "MONTH", "WEEK", "QUARTER", "DAY"}:
                grain = fn.lower()
            elif fn == "EXTRACT":
                grain = (dfn.get("granularity") or "").lower()
            if not grain:
                continue
            key = (primary, str(col), grain)
            grains[key] = grains.get(key, 0) + 1

        # 5. cohort_observed — cohort_scope_signals
        for ch in getattr(fp, "cohort_scope_signals", None) or []:
            name = ch.get("cohort_name") or ch.get("name")
            if not name:
                continue
            entry = cohorts.setdefault(name, {
                "tables": set(), "source_filters": [], "count": 0,
            })
            entry["count"] += 1
            for t in (ch.get("tables") or [primary]):
                entry["tables"].add(t)
            for sf in (ch.get("source_filters") or []):
                if sf not in entry["source_filters"]:
                    entry["source_filters"].append(sf)

    events: list[OntologyEvent] = []

    for (table, col, fn, distinct, alias), count in metrics.items():
        events.append(OntologyEvent(
            event_type="metric_observed",
            source="parse_sqls",
            table_name=table,
            column_name=col,
            payload={
                "function": fn, "alias": alias, "distinct": distinct,
                "count": count,
            },
            confidence=min(0.95, 0.5 + 0.05 * count),
            evidence=f"{fn}({col}) observed in {count} query(ies)",
        ))

    for (table, col, kind, value, label), count in thresholds.items():
        events.append(OntologyEvent(
            event_type="threshold_observed",
            source="parse_sqls",
            table_name=table,
            column_name=col,
            payload={
                "kind": kind, "value": value, "label": label, "count": count,
            },
            confidence=min(0.9, 0.5 + 0.05 * count),
            evidence=f"CASE WHEN on {col} → {label or value} ({count}x)",
        ))

    for (table, col, op, is_struct, vals), count in filters.items():
        events.append(OntologyEvent(
            event_type="filter_observed",
            source="parse_sqls",
            table_name=table,
            column_name=col,
            payload={
                "operator": op,
                "is_structural": is_struct,
                "values": list(vals),
                "count": count,
            },
            confidence=0.85 if is_struct else min(0.9, 0.5 + 0.05 * count),
            evidence=(
                f"WHERE {col} {op} {list(vals) or '?'} in {count} query(ies)"
                + (" (structural — CTE-scoped)" if is_struct else "")
            ),
        ))

    for (table, col, grain), count in grains.items():
        events.append(OntologyEvent(
            event_type="time_grain_observed",
            source="parse_sqls",
            table_name=table,
            column_name=col,
            payload={"grain": grain, "count": count},
            confidence=min(0.9, 0.5 + 0.05 * count),
            evidence=f"{grain.upper()} grain on {col} in {count} query(ies)",
        ))

    for name, entry in cohorts.items():
        events.append(OntologyEvent(
            event_type="cohort_observed",
            source="parse_sqls",
            entity_name=name,
            payload={
                "cohort_name": name,
                "tables": sorted(entry["tables"]),
                "source_filters": entry["source_filters"],
                "count": entry["count"],
            },
            confidence=min(0.85, 0.5 + 0.05 * entry["count"]),
            evidence=f"cohort '{name}' observed in {entry['count']} query(ies)",
        ))

    # 6. question_pattern_observed — corpus-level clustering pass
    try:
        from lumi.explore_clusters import cluster_queries

        clusters = cluster_queries(fingerprints, min_cluster_size=1)
        for cl in clusters:
            members = cl.member_query_indices
            events.append(OntologyEvent(
                event_type="question_pattern_observed",
                source="parse_sqls",
                payload={
                    "cluster_id": cl.cluster_id,
                    "tables": cl.tables,
                    "group_by_keys": cl.group_by_keys,
                    "structural_filters": cl.structural_filters,
                    "canonical_filters": cl.canonical_filters,
                    "aggregation_columns": cl.aggregation_columns,
                    "member_query_ids": [f"Q{i+1:02d}" for i in members],
                    "frequency": cl.frequency,
                    "sample_query": (cl.sample_queries or [""])[0],
                },
                confidence=min(0.9, 0.5 + 0.05 * cl.frequency),
                evidence=(
                    f"cluster {cl.cluster_id} — {cl.frequency} member queries "
                    f"over {len(cl.tables)} table(s)"
                ),
            ))
    except ImportError:
        pass  # explore_clusters may not be available in minimal envs

    store.record_many(events)
    return len(events)


def record_entity_hints_from_mdm(
    store: OntologyStore, ctx: TableContext,
) -> int:
    """Hook 2 — every MDM signal becomes a graph event.

    Originally just entity_hint + synonym_candidate. Now exhaustive over
    the MDM digest surface (the patentable graph IS the product — every
    MDM fact a grounded claim):

      Per column:
        - entity_hint            (column naming pattern → Entity)
        - synonym_candidate      (business_name ≠ column_name)
        - curated_pk             (is_primary OR is_dedupe_key)
        - column_governance_observed
                                 (PII, CDE, GDPR, sensitive, mandatory,
                                  clustered, format, length, publish_code)
        - partition_observed     (is_partitioned → TimeGrain + always_filter)
        - derived_formula_observed
                                 (derived_logic → Metric)
        - cardinality_observed   (external_references → declared FK)
        - deprecation_observed   (is_decommissioned)

      Per table:
        - table_metadata_observed
                                 (table_type, feed_type, data_category,
                                  bq_fqn, ownership)
        - deprecation_observed   (table-level is_decommissioned)

    Every event flows through ``store.record`` → JSONL + (optional) AGE.
    Source weight ``mdm = 3`` is multiplied into evidence accumulation
    by the promotion gate.
    """
    events: list[OntologyEvent] = []
    table = ctx.table_name

    ds = ctx.mdm_dataset_details or {}
    cat = (ds.get("data_category") or "").lower()
    sub = (ds.get("data_sub_category") or "").lower()
    blob = f"{cat} {sub}"
    table_entity: str | None = None
    for cand, hints in _ENTITY_HINTS.items():
        if any(h.replace("_", "") in blob.replace("_", "") for h in hints):
            table_entity = cand
            break

    # ── Table-level metadata + deprecation ──
    table_meta_payload = {
        k: ds.get(k) for k in (
            "table_type", "feed_type", "load_type",
            "data_category", "data_sub_category",
            "is_internal", "is_sor_certified", "is_searchable",
            "is_transactional", "retention_period",
            "bq_project", "bq_dataset", "bq_table",
            "table_business_name", "table_description",
        ) if ds.get(k) is not None
    }
    bq_fqn = ".".join(filter(None, [
        ds.get("bq_project"), ds.get("bq_dataset"), ds.get("bq_table"),
    ]))
    if bq_fqn:
        table_meta_payload["bq_fqn"] = bq_fqn
    own = ctx.mdm_ownership or {}
    if own:
        table_meta_payload["ownership_imr_queue"] = own.get("imr_queue")
        table_meta_payload["ownership_aim_id"] = own.get("aim_id")
    if table_meta_payload:
        events.append(OntologyEvent(
            event_type="table_metadata_observed",
            source="fetch_mdm",
            table_name=table,
            payload=table_meta_payload,
            confidence=0.85,
            evidence=f"MDM table-level facts: {sorted(table_meta_payload.keys())}",
        ))
    if ds.get("is_decommissioned"):
        events.append(OntologyEvent(
            event_type="deprecation_observed",
            source="fetch_mdm",
            table_name=table,
            payload={"reason": "MDM is_decommissioned=true"},
            confidence=0.95,
            evidence="MDM dataset_details.is_decommissioned",
        ))

    # ── Per-column passes ──
    for col in (ctx.mdm_columns or []):
        cn = col.get("name")
        if not cn:
            continue
        ent = _classify_to_entity(cn) or table_entity

        # 1. entity_hint (pre-existing behavior)
        if ent:
            events.append(OntologyEvent(
                event_type="entity_hint",
                source="fetch_mdm",
                table_name=table,
                column_name=cn,
                entity_name=ent,
                payload={
                    "mdm_business_name": col.get("business_name"),
                    "data_category": cat or None,
                },
                confidence=0.6 if _classify_to_entity(cn) else 0.4,
                evidence=(
                    f"MDM business_name={col.get('business_name')!r}, "
                    f"category={cat or '(none)'}"
                ),
            ))

        # 2. synonym_candidate (pre-existing behavior)
        bn = (col.get("business_name") or "").strip()
        if bn and bn.lower() != cn.lower() and ent:
            events.append(OntologyEvent(
                event_type="synonym_candidate",
                source="fetch_mdm",
                table_name=table,
                column_name=cn,
                entity_name=ent,
                payload={"canonical": ent, "synonym": bn},
                confidence=0.55,
                evidence=f"MDM business_name='{bn}' for column '{cn}'",
            ))

        # 3. curated_pk (is_primary OR is_dedupe_key)
        if col.get("is_primary") or col.get("is_dedupe_key"):
            role = "pk" if col.get("is_primary") else "dedupe_key"
            events.append(OntologyEvent(
                event_type="curated_pk",
                source="fetch_mdm",
                table_name=table,
                column_name=cn,
                payload={
                    "role": role,
                    "is_primary": bool(col.get("is_primary")),
                    "is_dedupe_key": bool(col.get("is_dedupe_key")),
                },
                confidence=0.9 if role == "pk" else 0.75,
                evidence=f"MDM sensitivity_details.{role}=true",
            ))

        # 4. column_governance_observed (PII/CDE/GDPR/mandatory/clustered/format)
        gov_keys = {
            "is_pii": col.get("is_pii"),
            "pii_role_id": col.get("pii_role_id"),
            "is_critical_data_element": col.get("is_critical_data_element"),
            "is_gdpr": col.get("is_gdpr"),
            "is_sensitive": col.get("is_sensitive"),
            "is_mandatory": col.get("is_mandatory"),
            "is_clustered": col.get("is_clustered"),
            "cluster_position": col.get("cluster_position"),
            "attribute_format": col.get("format"),
            "attribute_length": col.get("length"),
            "publish_code": col.get("publish_code"),
            "is_meta_column": col.get("is_meta_column"),
        }
        gov_facts = {k: v for k, v in gov_keys.items() if v not in (None, False)}
        if gov_facts:
            events.append(OntologyEvent(
                event_type="column_governance_observed",
                source="fetch_mdm",
                table_name=table,
                column_name=cn,
                payload=gov_facts,
                confidence=0.85,
                evidence=f"MDM governance facts: {sorted(gov_facts.keys())}",
            ))

        # 5. partition_observed
        if col.get("is_partitioned") or col.get("partition_column"):
            events.append(OntologyEvent(
                event_type="partition_observed",
                source="fetch_mdm",
                table_name=table,
                column_name=cn,
                payload={
                    "partition_position": col.get("partition_position"),
                    "time_partition_type": col.get("time_partition_type"),
                },
                confidence=0.95,
                evidence="MDM attribute_details.is_partitioned=true",
            ))

        # 6. derived_formula_observed
        derived = col.get("derived_logic")
        if derived:
            events.append(OntologyEvent(
                event_type="derived_formula_observed",
                source="fetch_mdm",
                table_name=table,
                column_name=cn,
                payload={"derived_logic": str(derived)},
                confidence=0.8,
                evidence=(
                    f"MDM derived_logic length={len(str(derived))} chars"
                ),
            ))

        # 7. cardinality_observed for declared external_references (FKs)
        for ref in (col.get("external_references") or []):
            other_t = (ref.get("table") or ref.get("ref_table")
                       or ref.get("target_table"))
            other_c = (ref.get("column") or ref.get("ref_column")
                       or ref.get("target_column"))
            if not (other_t and other_c):
                continue
            events.append(OntologyEvent(
                event_type="cardinality_observed",
                source="fetch_mdm",
                payload={
                    "left_table": table, "left_column": cn,
                    "right_table": other_t, "right_column": other_c,
                    "cardinality": ref.get("cardinality", "many_to_one"),
                    "role": "external_reference",
                },
                confidence=0.9,
                evidence=(
                    f"MDM external_references: {table}.{cn} → {other_t}.{other_c}"
                ),
            ))

        # 8. column-level deprecation
        if col.get("is_decommissioned") or col.get("status") == "decommissioned":
            events.append(OntologyEvent(
                event_type="deprecation_observed",
                source="fetch_mdm",
                table_name=table,
                column_name=cn,
                payload={"reason": "MDM column is_decommissioned/status"},
                confidence=0.95,
                evidence="MDM column-level decommission flag",
            ))

    store.record_many(events)
    return len(events)


def record_curated_synonyms_from_baseline(
    store: OntologyStore, ctx: TableContext,
) -> int:
    """Hook 3 — baseline LookML carries human-curated signals.

    `baseline_sql_aliases` ({dim_name: source_column}) is gold: the human
    chose to rename ``bus_seg`` to ``customer_segment``, telling us
    ``customer_segment`` is the analyst-facing synonym.

    `baseline_primary_key_column` is a trusted PK candidate.
    """
    events: list[OntologyEvent] = []
    table = ctx.table_name

    # Curated synonyms from sql_aliases.
    for dim_name, source_col in (ctx.baseline_sql_aliases or {}).items():
        if not (dim_name and source_col):
            continue
        ent = _classify_to_entity(source_col) or _classify_to_entity(dim_name)
        if not ent:
            # Treat the dim name as a synonym for the column name itself.
            events.append(OntologyEvent(
                event_type="curated_synonym",
                source="parse_baseline",
                table_name=table,
                column_name=source_col,
                payload={"canonical": source_col, "synonym": dim_name},
                confidence=0.7,
                evidence=(
                    f"baseline LookML renames {source_col} to {dim_name} — "
                    "human-curated synonym"
                ),
            ))
            continue
        events.append(OntologyEvent(
            event_type="curated_synonym",
            source="parse_baseline",
            table_name=table,
            column_name=source_col,
            entity_name=ent,
            payload={"canonical": ent, "synonym": dim_name},
            confidence=0.75,
            evidence=(
                f"baseline LookML renames {source_col} to {dim_name} for "
                f"entity {ent}"
            ),
        ))

    # Curated PK.
    pk = ctx.baseline_primary_key_column
    if pk:
        events.append(OntologyEvent(
            event_type="curated_pk",
            source="parse_baseline",
            table_name=table,
            column_name=pk,
            confidence=0.9,
            evidence="baseline LookML declares this column as primary_key",
        ))

    store.record_many(events)
    return len(events)


def record_approval_lock(
    store: OntologyStore,
    approval: PlanApproval,
    ctx: TableContext | None = None,
    primary_entity: str | None = None,
    plan: Any = None,
) -> int:
    """Hook 4 — human approval locks vocabulary into the ontology.

    Approval is the strongest signal in the system: a human verified
    the plan against ground truth and ticked APPROVED. We mine three
    things:

      1. Vocabulary lock at entity level (the plan's primary entity
         gets a +5 boost in candidate evidence).
      2. Per-dimension synonyms (when a plan dim renames the source
         column, that's a curated synonym).
      3. Per-measure curated PKs and value formats.

    The plan param is optional — when missing we degrade to entity-only
    lock, preserving back-compat with callers that don't have the plan.
    """
    if not approval.approved:
        return 0

    n = 0
    ent: str | None = primary_entity
    if not ent and ctx:
        ent = _classify_to_entity(ctx.table_name)

    if ent:
        store.record(OntologyEvent(
            event_type="vocabulary_lock",
            source="approve_plan",
            table_name=approval.table_name,
            entity_name=ent,
            payload={"approved_by": approval.approver},
            confidence=0.95,
            evidence=(
                f"plan for {approval.table_name} approved by "
                f"{approval.approver}"
            ),
        ))
        n += 1

    # Mine plan content if available — every dimension where name !=
    # source_column is a human-blessed synonym. Every measure with a
    # primary_key flag is a curated PK.
    if plan is not None:
        for d in (getattr(plan, "proposed_dimensions", None) or []):
            name = (d.get("name") or "").strip()
            src = (d.get("source_column") or "").strip()
            if not (name and src) or name == src:
                continue
            # Best-effort entity assignment: classify the source column.
            target_ent = _classify_to_entity(src) or ent
            if not target_ent:
                continue
            store.record(OntologyEvent(
                event_type="curated_synonym",
                source="approve_plan",
                table_name=approval.table_name,
                column_name=src,
                entity_name=target_ent,
                payload={"canonical": target_ent, "synonym": name},
                confidence=0.85,
                evidence=(
                    f"approved plan renamed {src} → {name} for entity {target_ent}"
                ),
            ))
            n += 1

        # Curated PK mined from primary_key=yes dims.
        for d in (getattr(plan, "proposed_dimensions", None) or []):
            if not d.get("primary_key"):
                continue
            src = (d.get("source_column") or "").strip()
            if not src:
                continue
            store.record(OntologyEvent(
                event_type="curated_pk",
                source="approve_plan",
                table_name=approval.table_name,
                column_name=src,
                confidence=0.95,
                evidence="approved plan declares this as primary_key",
            ))
            n += 1
    return n


# ─── helpers ─────────────────────────────────────────────────


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def _compute_content_hash(event: OntologyEvent) -> str:
    """Deterministic key over event identity (NOT timestamp / confidence).

    Two events from the same source about the same fact are considered
    equal. Re-running ``parse_sqls`` recomputes the same equivalence
    pairs → same hashes → no double-counting.
    """
    payload_normalized = json.dumps(
        event.payload or {}, sort_keys=True, default=str,
    )
    blob = "|".join([
        event.event_type,
        event.source,
        event.table_name or "",
        event.column_name or "",
        event.entity_name or "",
        payload_normalized,
    ])
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


