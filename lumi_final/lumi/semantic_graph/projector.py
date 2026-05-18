"""Event → Cypher MERGE templates. The only translator from
``OntologyEvent`` to graph nodes/edges.

Discipline:
- One function per event_type. Exhaustive — adding a new event type
  requires a new function or `_DISPATCH` entry; CI gates this.
- Every emitted Cypher uses MERGE, never CREATE — re-projection is
  idempotent (replay-safe).
- Every promoted node + edge carries the provenance envelope
  (canonical_uri, confidence, evidence_count, source_quality, etc.)
  set at projection time from the event metadata.
- The single Event node is also MERGEd, with ASSERTS edges back to
  whatever nodes/edges this event touches.

When AGE is not enabled (``config.is_age_enabled() == False``), every
projector call is a no-op — JSONL stays the source of truth and the
graph can be rebuilt later via ``replay.py``.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Iterator
from contextlib import contextmanager

from lumi.schemas import OntologyEvent
from lumi.semantic_graph import config as gconfig

logger = logging.getLogger("lumi.semantic_graph.projector")


# ─── psycopg connection (lazy import, AGE-aware) ─────────────


@contextmanager
def _connect() -> Iterator[Any]:
    """Yield a psycopg connection with AGE loaded and search_path set."""
    try:
        import psycopg
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "psycopg required for AGE projection. "
            "Install via `pip install 'psycopg[binary]'`."
        ) from e
    conn_params = gconfig.AGEConnection()
    with psycopg.connect(conn_params.conninfo()) as pgconn:
        pgconn.autocommit = False
        with pgconn.cursor() as cur:
            cur.execute("LOAD 'age';")
            cur.execute('SET search_path = ag_catalog, "$user", public;')
        try:
            yield pgconn
        finally:
            pgconn.commit()


# ─── Cypher helpers ──────────────────────────────────────────


def _cypher(pgconn: Any, query: str) -> list[Any]:
    """Run a Cypher statement via AGE's cypher() wrapper. Returns rows."""
    conn_params = gconfig.AGEConnection()
    # AGE wraps Cypher inside cypher('graph_name', $$ ... $$) AS (col agtype)
    wrapped = (
        f"SELECT * FROM cypher('{conn_params.graph_name}', $$ "
        f"{query} "
        f"$$) AS (result ag_catalog.agtype);"
    )
    with pgconn.cursor() as cur:
        cur.execute(wrapped)
        try:
            return cur.fetchall()
        except Exception:
            return []


def _safe(s: Any) -> str:
    """Escape a value for inline Cypher string literal embedding.

    For simple props we inline; for complex we'd parameterize, but AGE's
    Cypher-in-Postgres path makes parameter binding awkward. The values
    we project come from controlled OntologyEvent objects (not user input),
    so inline escaping is safe.
    """
    if s is None:
        return "null"
    if isinstance(s, bool):
        return "true" if s else "false"
    if isinstance(s, (int, float)):
        return str(s)
    # String — escape single quotes + backslashes
    escaped = str(s).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _props(props: dict[str, Any]) -> str:
    """Render a Python dict as a Cypher property map literal."""
    parts = [f"{k}: {_safe(v)}" for k, v in props.items() if v is not None]
    return "{" + ", ".join(parts) + "}"


# ─── Provenance envelope helpers ─────────────────────────────


def _envelope_for(event: OntologyEvent) -> dict[str, Any]:
    """Build the provenance envelope every promoted node/edge carries."""
    source = event.source
    source_quality = _classify_source(source)
    return {
        "promoted_by": source,
        "source_quality": source_quality,
        "confidence": event.payload.get("confidence_label", "inferred") or "inferred",
        "evidence_count": 1,  # bumped on MERGE via SET below
        "schema_version": gconfig.SCHEMA_VERSION,
        "tenant_id": gconfig.default_tenant_id(),
        "last_reconfirmed_at": event.observed_at,
        "status": "candidate",  # promotion logic will flip to 'promoted' later
    }


def _classify_source(source: str) -> str:
    """Map event.source string to one of SOURCE_WEIGHTS keys."""
    s = (source or "").lower()
    if "approval" in s:
        return "human_approval"
    if "mdm" in s:
        return "mdm"
    if "baseline" in s:
        return "baseline_lookml"
    if "parse" in s or "sql" in s or "corpus" in s:
        return "corpus_sql"
    if "llm" in s or "seed" in s:
        return "llm_inferred"
    if "bq_probe" in s:
        return "bq_probe_confirm"
    return "corpus_sql"  # safe default


def _canonical_uri(node_type: str, *parts: str) -> str:
    """Stable URI for a node — external consumers reference by this."""
    safe_parts = "/".join(str(p).lower().replace(" ", "_") for p in parts if p)
    return f"lumi://{node_type.lower()}/{safe_parts}"


# ─── Per-event-type projectors ───────────────────────────────


def _project_equivalence_observed(pgconn: Any, event: OntologyEvent) -> None:
    """JOIN ON pair → MERGE both Columns + EQUIVALENT_TO edge + Event/ASSERTS."""
    p = event.payload
    a_t, a_c = p.get("a_table"), p.get("a_column")
    b_t, b_c = p.get("b_table"), p.get("b_column")
    if not all([a_t, a_c, b_t, b_c]):
        return

    env = _envelope_for(event)
    a_uri = _canonical_uri("column", a_t, a_c)
    b_uri = _canonical_uri("column", b_t, b_c)
    # Merge both Columns by canonical_uri
    for table, col, uri in ((a_t, a_c, a_uri), (b_t, b_c, b_uri)):
        col_props = {
            "canonical_uri": uri,
            "name": col,
            "table_name": table,
            **env,
        }
        # Also merge the Table node
        t_uri = _canonical_uri("table", table)
        _cypher(pgconn, (
            f"MERGE (t:Table {{canonical_uri: {_safe(t_uri)}}}) "
            f"ON CREATE SET t += {_props({'name': table, **env})} "
            f"MERGE (c:Column {{canonical_uri: {_safe(uri)}}}) "
            f"ON CREATE SET c += {_props(col_props)} "
            f"MERGE (t)-[:CONTAINS]->(c)"
        ))
    # Merge EQUIVALENT_TO edge (deterministic ordering: lex)
    pair = sorted([(a_uri, a_c), (b_uri, b_c)])
    left_uri, _ = pair[0]
    right_uri, _ = pair[1]
    _cypher(pgconn, (
        f"MATCH (a:Column {{canonical_uri: {_safe(left_uri)}}}), "
        f"(b:Column {{canonical_uri: {_safe(right_uri)}}}) "
        f"MERGE (a)-[r:EQUIVALENT_TO]->(b) "
        f"ON CREATE SET r += {_props({'evidence_count': 1, **env})} "
        f"ON MATCH SET r.evidence_count = coalesce(r.evidence_count, 0) + 1, "
        f"r.last_reconfirmed_at = {_safe(event.observed_at)}"
    ))
    _project_event_provenance(pgconn, event, target_label="Column", target_uri=a_uri)


def _project_entity_hint(pgconn: Any, event: OntologyEvent) -> None:
    """Naming-pattern entity hint → MERGE Entity + Column + IDENTIFIES."""
    entity_name = event.entity_name
    table = event.table_name
    col = event.column_name
    if not entity_name:
        return
    env = _envelope_for(event)
    e_uri = _canonical_uri("entity", entity_name)
    _cypher(pgconn, (
        f"MERGE (e:Entity {{canonical_uri: {_safe(e_uri)}}}) "
        f"ON CREATE SET e += {_props({'name': entity_name, **env})} "
        f"ON MATCH SET e.evidence_count = coalesce(e.evidence_count, 0) + 1"
    ))
    if table and col:
        t_uri = _canonical_uri("table", table)
        c_uri = _canonical_uri("column", table, col)
        _cypher(pgconn, (
            f"MERGE (t:Table {{canonical_uri: {_safe(t_uri)}}}) "
            f"ON CREATE SET t += {_props({'name': table, **env})} "
            f"MERGE (c:Column {{canonical_uri: {_safe(c_uri)}}}) "
            f"ON CREATE SET c += {_props({'name': col, 'table_name': table, **env})} "
            f"MERGE (t)-[:CONTAINS]->(c) "
            f"WITH c "
            f"MATCH (e:Entity {{canonical_uri: {_safe(e_uri)}}}) "
            f"MERGE (c)-[r:IDENTIFIES {{role: 'hint'}}]->(e) "
            f"ON CREATE SET r += {_props({'evidence_count': 1, **env})} "
            f"ON MATCH SET r.evidence_count = coalesce(r.evidence_count, 0) + 1"
        ))
    _project_event_provenance(pgconn, event, target_label="Entity", target_uri=e_uri)


def _project_synonym(pgconn: Any, event: OntologyEvent) -> None:
    """MDM business_name / baseline alias / curated synonym → Synonym + HAS_SYNONYM."""
    p = event.payload
    canonical = p.get("canonical") or event.entity_name
    syn_text = p.get("synonym")
    if not canonical or not syn_text:
        return
    env = _envelope_for(event)
    syn_uri = _canonical_uri("synonym", canonical, syn_text)
    _cypher(pgconn, (
        f"MERGE (s:Synonym {{canonical_uri: {_safe(syn_uri)}}}) "
        f"ON CREATE SET s += {_props({'text': syn_text, 'canonical': canonical, **env})} "
        f"ON MATCH SET s.evidence_count = coalesce(s.evidence_count, 0) + 1"
    ))
    # Link to entity if it exists; otherwise to column (best-effort)
    e_uri = _canonical_uri("entity", canonical)
    _cypher(pgconn, (
        f"MATCH (s:Synonym {{canonical_uri: {_safe(syn_uri)}}}) "
        f"OPTIONAL MATCH (target {{canonical_uri: {_safe(e_uri)}}}) "
        f"WITH s, target WHERE target IS NOT NULL "
        f"MERGE (target)-[r:HAS_SYNONYM]->(s) "
        f"ON CREATE SET r += {_props(env)}"
    ))
    _project_event_provenance(pgconn, event, target_label="Synonym", target_uri=syn_uri)


def _project_curated_pk(pgconn: Any, event: OntologyEvent) -> None:
    """baseline_primary_key → IDENTIFIES edge with role='pk'."""
    table = event.table_name
    col = event.column_name
    if not (table and col):
        return
    env = _envelope_for(event)
    t_uri = _canonical_uri("table", table)
    c_uri = _canonical_uri("column", table, col)
    _cypher(pgconn, (
        f"MERGE (t:Table {{canonical_uri: {_safe(t_uri)}}}) "
        f"ON CREATE SET t += {_props({'name': table, **env})} "
        f"MERGE (c:Column {{canonical_uri: {_safe(c_uri)}}}) "
        f"ON CREATE SET c += {_props({'name': col, 'table_name': table, 'is_pk_hint': True, **env})} "
        f"ON MATCH SET c.is_pk_hint = true "
        f"MERGE (t)-[:CONTAINS]->(c)"
    ))
    _project_event_provenance(pgconn, event, target_label="Column", target_uri=c_uri)


def _project_vocabulary_lock(pgconn: Any, event: OntologyEvent) -> None:
    """Human approval → Approval node + LOCKS edge to entity."""
    entity_name = event.entity_name
    if not entity_name:
        return
    env = _envelope_for(event)
    env["confidence"] = "human_asserted"
    env["status"] = "promoted"
    e_uri = _canonical_uri("entity", entity_name)
    approval_uri = _canonical_uri(
        "approval", event.table_name or "", event.observed_at,
    )
    _cypher(pgconn, (
        f"MERGE (e:Entity {{canonical_uri: {_safe(e_uri)}}}) "
        f"ON CREATE SET e += {_props({'name': entity_name, **env})} "
        f"SET e.confidence = 'human_asserted', e.status = 'promoted' "
        f"MERGE (a:Approval {{canonical_uri: {_safe(approval_uri)}}}) "
        f"ON CREATE SET a += {_props({'approver': event.payload.get('approved_by'), 'observed_at': event.observed_at, **env})} "
        f"MERGE (a)-[:LOCKS {{lock_kind: 'vocabulary'}}]->(e)"
    ))
    _project_event_provenance(pgconn, event, target_label="Entity", target_uri=e_uri)


def _project_cardinality_observed(pgconn: Any, event: OntologyEvent) -> None:
    """JoinCardinality → RELATES_TO edge between entities (if known) or
    a properties bump on the EQUIVALENT_TO edge."""
    p = event.payload
    lt, lc = p.get("left_table"), p.get("left_column")
    rt, rc = p.get("right_table"), p.get("right_column")
    if not all([lt, lc, rt, rc]):
        return
    env = _envelope_for(event)
    cardinality = p.get("cardinality", "unknown")
    # Bump cardinality vote on the EQUIVALENT_TO edge
    pair = sorted([
        (_canonical_uri("column", lt, lc), lc),
        (_canonical_uri("column", rt, rc), rc),
    ])
    left_uri, _ = pair[0]
    right_uri, _ = pair[1]
    _cypher(pgconn, (
        f"MATCH (a:Column {{canonical_uri: {_safe(left_uri)}}}), "
        f"(b:Column {{canonical_uri: {_safe(right_uri)}}}) "
        f"MERGE (a)-[r:EQUIVALENT_TO]->(b) "
        f"ON CREATE SET r += {_props({'evidence_count': 1, **env})} "
        f"SET r.observed_cardinality = {_safe(cardinality)}, "
        f"r.cardinality_confidence = {_safe(event.confidence)}"
    ))
    _project_event_provenance(pgconn, event, target_label="Column", target_uri=left_uri)


def _project_join_path_observed(pgconn: Any, event: OntologyEvent) -> None:
    """Multi-hop chain — stored as a sequence of JOIN_PATH edges."""
    p = event.payload
    base = p.get("base")
    chain = p.get("chain") or []
    if not (base and chain):
        return
    env = _envelope_for(event)
    base_uri = _canonical_uri("table", base)
    _cypher(pgconn, (
        f"MERGE (t:Table {{canonical_uri: {_safe(base_uri)}}}) "
        f"ON CREATE SET t += {_props({'name': base, **env})}"
    ))
    prev_uri = base_uri
    for ord_idx, step in enumerate(chain, start=1):
        if isinstance(step, list) and step:
            next_table = step[0]
        elif isinstance(step, str):
            next_table = step
        else:
            continue
        next_uri = _canonical_uri("table", next_table)
        _cypher(pgconn, (
            f"MERGE (t:Table {{canonical_uri: {_safe(next_uri)}}}) "
            f"ON CREATE SET t += {_props({'name': next_table, **env})} "
            f"WITH t "
            f"MATCH (prev:Table {{canonical_uri: {_safe(prev_uri)}}}) "
            f"MERGE (prev)-[r:JOIN_PATH {{step_ordinal: {ord_idx}}}]->(t) "
            f"ON CREATE SET r += {_props({'frequency': p.get('frequency', 1), **env})}"
        ))
        prev_uri = next_uri
    _project_event_provenance(pgconn, event, target_label="Table", target_uri=base_uri)


def _project_entity_refinement(pgconn: Any, event: OntologyEvent) -> None:
    """Critic refinement → entity_hint-style. Just MERGE the entity + bump."""
    _project_entity_hint(pgconn, event)


def _project_event_provenance(
    pgconn: Any, event: OntologyEvent,
    *, target_label: str, target_uri: str,
) -> None:
    """MERGE the Event node and ASSERTS edge back to the touched node.

    This is what makes every claim auditable — given any node, you can
    query (Event)-[ASSERTS]->(node) to see every event that produced it.
    """
    if not event.content_hash:
        return
    env = {
        "content_hash": event.content_hash,
        "event_type": event.event_type,
        "source": event.source,
        "confidence": event.confidence,
        "observed_at": event.observed_at,
        "evidence": (event.evidence or "")[:240],
        "schema_version": gconfig.SCHEMA_VERSION,
    }
    event_uri = _canonical_uri("event", event.content_hash)
    _cypher(pgconn, (
        f"MERGE (ev:Event {{canonical_uri: {_safe(event_uri)}}}) "
        f"ON CREATE SET ev += {_props(env)} "
        f"WITH ev "
        f"MATCH (target:{target_label} {{canonical_uri: {_safe(target_uri)}}}) "
        f"MERGE (ev)-[:ASSERTS {{delta: 'reinforce'}}]->(target)"
    ))


# ─── Dispatch table ──────────────────────────────────────────


_DISPATCH: dict[str, Callable[[Any, OntologyEvent], None]] = {
    "equivalence_observed": _project_equivalence_observed,
    "entity_hint": _project_entity_hint,
    "synonym_candidate": _project_synonym,
    "curated_synonym": _project_synonym,
    "curated_pk": _project_curated_pk,
    "vocabulary_lock": _project_vocabulary_lock,
    "entity_refinement": _project_entity_refinement,
    "cardinality_observed": _project_cardinality_observed,
    "join_path_observed": _project_join_path_observed,
}


# ─── Public API ──────────────────────────────────────────────


def project(event: OntologyEvent) -> bool:
    """Project a single event into AGE. No-op if AGE disabled.

    Returns True if projection happened (or attempted), False if AGE off.
    Errors are logged + swallowed — JSONL remains the source of truth.
    """
    if not gconfig.is_age_enabled():
        return False
    fn = _DISPATCH.get(event.event_type)
    if fn is None:
        logger.warning(
            "no projector for event_type=%s — skipping AGE write",
            event.event_type,
        )
        return False
    try:
        with _connect() as pgconn:
            fn(pgconn, event)
        return True
    except Exception as e:  # noqa: BLE001
        logger.error(
            "AGE projection failed for event_type=%s: %s",
            event.event_type, e,
        )
        return False


def project_many(events: list[OntologyEvent]) -> int:
    """Project a batch of events; returns count successfully projected."""
    if not gconfig.is_age_enabled():
        return 0
    n_ok = 0
    for ev in events:
        if project(ev):
            n_ok += 1
    return n_ok


def covered_event_types() -> tuple[str, ...]:
    """Return event types this projector knows how to translate. Used by
    CI tests to ensure exhaustiveness vs `schemas.OntologyEventType`."""
    return tuple(_DISPATCH.keys())
