"""Domain Ontology Builder — ONE upfront Gemini call, used by 29+ downstream calls.

Solves the cardmember↔customer↔cust_xref_id semantic-equivalence problem
at the SYSTEM level, not per-table. While ``lumi.ontology`` provides the
deterministic-only equivalence closure (from JOIN ON pairs), this module
adds the synonym/grain/description layer the LLM is uniquely good at:

  - "cardmember", "card member", "cm", "cust", "customer" → same entity
  - "account", "acct", "acct_xref_id" → same entity at account-grain
  - "transaction", "txn" → same entity

The builder reads:
  - Every table's MDM data_desc + business_name + data_category
  - Every column's business_name across all tables
  - Cross-table JOIN ON pairs (frequency-weighted)
  - The deterministic equivalence classes (compute_equivalence_classes)
  - Pre-computed table-level grain hints from baseline view descriptions

Outputs ``data/ontology.json`` (a DomainOntology). Persisted on disk;
re-build only when ``--refresh-ontology`` is passed or the file is
missing.

Defensive fallbacks:
  - Vertex unreachable → returns deterministic-only ontology built
    from the equivalence closure + naming-pattern clustering
  - LLM JSON parse failure → same fallback
  - Tests can pass ``with_llm=False`` for the deterministic path

Public API:
    build_domain_ontology(contexts, all_fps, *, with_llm=True,
                           insecure=False, config=None) -> DomainOntology
    save_ontology(ontology, path) / load_ontology(path)
    ensure_ontology(...) -> DomainOntology  # build+save if missing
    render_ontology_for_table(ontology, table_name) -> str
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

from lumi.config import LumiConfig
from lumi.ontology import EquivalenceMap, compute_equivalence_classes
from lumi.schemas import (
    DomainOntology,
    OntologyEntity,
    OntologyRelationship,
    TableContext,
)
from lumi.sql_to_context import SQLFingerprint

if TYPE_CHECKING:
    from lumi.ontology_store import OntologyStore

logger = logging.getLogger("lumi.ontology_builder")


_DEFAULT_PATH = Path("data/ontology.json")


# ─── Public API ──────────────────────────────────────────────


def build_domain_ontology(
    contexts: dict[str, TableContext],
    all_fingerprints: list[SQLFingerprint],
    *,
    with_llm: bool = True,
    insecure: bool = False,
    config: LumiConfig | None = None,
) -> DomainOntology:
    """Build the domain ontology from the corpus.

    Args:
        contexts: dict[table_name, TableContext] for every discovered table
        all_fingerprints: every parsed SQL — for join evidence
        with_llm: when True, calls Gemini for synonym/grain refinement.
            When False, returns a deterministic-only ontology built
            from the equivalence closure + naming-pattern clustering.
        insecure: TLS bypass for corp-MITM networks (sets LUMI_INSECURE_TLS)
        config: LumiConfig override (model, temperature, etc.)

    Returns:
        DomainOntology. Always succeeds — falls back to deterministic
        construction on any LLM error so the pipeline never blocks.
    """
    cfg = config or LumiConfig()
    eq_map = compute_equivalence_classes(all_fingerprints)

    # Always build the deterministic skeleton first; it's the floor.
    skeleton = _build_deterministic_ontology(contexts, eq_map)

    if not with_llm:
        return skeleton

    if insecure:
        import os
        os.environ["LUMI_INSECURE_TLS"] = "1"

    try:
        refined = _author_ontology_with_llm(
            contexts, all_fingerprints, eq_map, skeleton, cfg,
        )
        if refined is not None:
            return refined
    except Exception as e:  # noqa: BLE001
        logger.warning("LLM ontology authoring failed: %s — using deterministic", e)

    skeleton.authoring = {"mode": "deterministic", "reason": "LLM unavailable"}
    return skeleton


def save_ontology(ontology: DomainOntology, path: Path = _DEFAULT_PATH) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(ontology.model_dump(), indent=2, default=str),
        encoding="utf-8",
    )
    return path


def load_ontology(path: Path = _DEFAULT_PATH) -> DomainOntology | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return DomainOntology(**data)
    except Exception as e:  # noqa: BLE001
        logger.warning("could not load ontology from %s: %s", path, e)
        return None


def ensure_ontology(
    contexts: dict[str, TableContext],
    all_fingerprints: list[SQLFingerprint],
    *,
    path: Path = _DEFAULT_PATH,
    refresh: bool = False,
    with_llm: bool = True,
    config: LumiConfig | None = None,
) -> DomainOntology:
    """DEPRECATED — use ``OntologyStore.refresh(seed_fn=...)`` instead.

    Kept for the ``python -m lumi ontology`` standalone CLI. The active
    pipeline now reads from the unified store at ``data/ontology/``,
    not the legacy single-file path. This function still works for
    backward-compatible CLI use but routes through the store under the
    hood so seed events feed the candidates index.
    """
    from lumi.ontology_store import OntologyStore

    # Backward-compat: honor the legacy path-file cache when caller
    # passed a custom path (tests use tmp_path). The unified store path
    # below is the canonical flow for the pipeline.
    if not refresh:
        existing_at_path = load_ontology(path)
        if existing_at_path is not None:
            return existing_at_path

    store = OntologyStore()

    def _seed(s: "OntologyStore") -> int:
        ontology = build_domain_ontology(
            contexts, all_fingerprints,
            with_llm=with_llm, config=config,
        )
        return _emit_seed_events(s, ontology)

    out = store.refresh(
        seed_fn=_seed,
        force_seed=refresh,  # refresh=True forces rebuild even if current.json exists
        evidence_threshold=1,
        snapshot_reason="ensure_ontology() seed",
    )
    save_ontology(out, path)
    return out


def _emit_seed_events(store: "OntologyStore", ontology: DomainOntology) -> int:
    """Convert a built DomainOntology into events recorded in the store."""
    from lumi.schemas import OntologyEvent

    n = 0
    for ent in ontology.entities:
        for tbl, cols in ent.grain_columns.items():
            for col in cols:
                store.record(OntologyEvent(
                    event_type="entity_hint",
                    source="llm_seed",
                    table_name=tbl,
                    column_name=col,
                    entity_name=ent.name,
                    confidence=0.85,
                    evidence=f"LLM-authored ontology seed: {ent.name}",
                ))
                n += 1
        for syn in ent.synonyms:
            store.record(OntologyEvent(
                event_type="synonym_candidate",
                source="llm_seed",
                entity_name=ent.name,
                payload={"canonical": ent.name, "synonym": syn},
                confidence=0.85,
                evidence=f"LLM-authored seed synonym for {ent.name}",
            ))
            n += 1
    return n


# ─── Deterministic skeleton ──────────────────────────────────


_ENTITY_NAMING_HINTS = {
    "cardmember": ["cardmember", "card member", "card_member", "cm_", "cm-",
                   "cm11", "cm15", "cmember"],
    "customer": ["customer", "cust_", "cust ", "cust-", "cust_id",
                 "cust_xref_id", "cust_master"],
    "account": ["account", "acct_", "acct ", "acct-", "acct_id", "acct_xref"],
    "transaction": ["transaction", "txn", "trans_", "billed", "spend"],
    "merchant": ["merchant", "merch_", "merchant_id"],
    "product": ["product", "prod_", "pmdl_", "prdct_"],
    "risk": ["risk_", "fico", "delinquency", "default"],
}


def _build_deterministic_ontology(
    contexts: dict[str, TableContext],
    eq_map: EquivalenceMap,
) -> DomainOntology:
    """Construct an ontology from purely deterministic signals.

    Each equivalence class becomes a candidate entity. Naming-pattern
    heuristics + MDM data_category cluster columns by entity. Used as
    the floor when Vertex is unavailable, and as the seed the LLM
    refines.
    """
    # Group columns into entity buckets by naming hints.
    entity_buckets: dict[str, dict[str, list[str]]] = {
        name: {} for name in _ENTITY_NAMING_HINTS
    }

    for table_name, ctx in contexts.items():
        for col in (ctx.columns_referenced or []):
            entity_name = _classify_column_to_entity(col, ctx)
            if entity_name:
                entity_buckets.setdefault(entity_name, {}).setdefault(
                    table_name, [],
                ).append(col)

    # Augment with equivalence classes — each class is a candidate entity.
    for ec in eq_map.classes:
        # Try to assign the class to a known entity bucket via majority vote.
        votes: dict[str, int] = {}
        for table, col in ec.members:
            ctx = contexts.get(table)
            if ctx is None:
                continue
            ent = _classify_column_to_entity(col, ctx)
            if ent:
                votes[ent] = votes.get(ent, 0) + 1
        if not votes:
            continue
        winner = max(votes.items(), key=lambda kv: kv[1])[0]
        for table, col in ec.members:
            entity_buckets.setdefault(winner, {}).setdefault(
                table, [],
            )
            if col not in entity_buckets[winner][table]:
                entity_buckets[winner][table].append(col)

    # Convert buckets into OntologyEntities (drop empty / single-table ones).
    entities: list[OntologyEntity] = []
    for name, by_table in entity_buckets.items():
        if not by_table:
            continue
        # Skip entities that only appear in 1 table — they're not really
        # cross-table and don't need ontology-level treatment.
        if len(by_table) < 2 and sum(len(v) for v in by_table.values()) < 2:
            continue
        synonyms = _ENTITY_NAMING_HINTS.get(name, [])[:5]
        entities.append(OntologyEntity(
            name=name,
            synonyms=synonyms,
            grain_description=_default_grain(name),
            grain_columns=by_table,
            description=_default_entity_description(name),
            evidence=[
                f"naming-pattern match across {len(by_table)} tables",
                f"{sum(len(v) for v in by_table.values())} columns matched",
            ],
        ))

    # Deterministic relationships from equivalence-class membership +
    # naming-pattern cardinality hints.
    relationships = _infer_relationships(entities, eq_map, contexts)

    # Map each table to its primary entity (the one with the most
    # columns in that table).
    table_to_primary: dict[str, str] = {}
    for table_name in contexts:
        best_entity: str | None = None
        best_count = 0
        for entity in entities:
            count = len(entity.grain_columns.get(table_name, []))
            if count > best_count:
                best_count = count
                best_entity = entity.name
        if best_entity is not None:
            table_to_primary[table_name] = best_entity

    return DomainOntology(
        entities=entities,
        relationships=relationships,
        table_to_primary_entity=table_to_primary,
        authoring={"mode": "deterministic", "reason": None},
    )


def _classify_column_to_entity(col: str, ctx: TableContext) -> str | None:
    """Best-effort entity classification by name pattern + MDM hint."""
    if not col:
        return None
    cl = col.lower()
    # Name-pattern matches (longest-prefix wins).
    matches: list[tuple[str, int]] = []
    for entity, hints in _ENTITY_NAMING_HINTS.items():
        for h in hints:
            if h in cl:
                matches.append((entity, len(h)))
    if matches:
        matches.sort(key=lambda kv: -kv[1])
        return matches[0][0]
    return None


def _default_grain(entity_name: str) -> str:
    grains = {
        "cardmember": "one row per cardmember (point-in-time)",
        "customer": "one row per customer (point-in-time)",
        "account": "one row per account (point-in-time)",
        "transaction": "one row per transaction event",
        "merchant": "one row per merchant",
        "product": "one row per product variant",
        "risk": "risk metric per cardmember-account-snapshot",
    }
    return grains.get(entity_name, "(grain not yet inferred)")


def _default_entity_description(entity_name: str) -> str:
    desc = {
        "cardmember": "Individual American Express cardmember (cardholder).",
        "customer": "Customer entity, often equivalent to cardmember in this domain.",
        "account": "Cardmember account — financial account identifier.",
        "transaction": "Card transaction event.",
        "merchant": "Merchant accepting card transactions.",
        "product": "Card product or add-on service.",
        "risk": "Risk metric or scorable attribute.",
    }
    return desc.get(entity_name, f"{entity_name} entity.")


def _infer_relationships(
    entities: list[OntologyEntity],
    eq_map: EquivalenceMap,
    contexts: dict[str, TableContext],
) -> list[OntologyRelationship]:
    """Heuristic relationships — full ontology comes from the LLM call."""
    rels: list[OntologyRelationship] = []
    entity_names = {e.name for e in entities}

    # Common cardmember-domain relationships.
    if "cardmember" in entity_names and "account" in entity_names:
        rels.append(OntologyRelationship(
            from_entity="cardmember",
            to_entity="account",
            cardinality="one_to_many",
            evidence="domain default; one cardmember can have multiple accounts",
        ))
    if "cardmember" in entity_names and "customer" in entity_names:
        rels.append(OntologyRelationship(
            from_entity="cardmember",
            to_entity="customer",
            cardinality="one_to_one",
            evidence="cardmember and customer often refer to the same entity in this domain",
        ))
    if "account" in entity_names and "transaction" in entity_names:
        rels.append(OntologyRelationship(
            from_entity="account",
            to_entity="transaction",
            cardinality="one_to_many",
            evidence="domain default; one account has many transactions",
        ))
    return rels


# ─── LLM-authored ontology ───────────────────────────────────


def _author_ontology_with_llm(
    contexts: dict[str, TableContext],
    all_fingerprints: list[SQLFingerprint],
    eq_map: EquivalenceMap,
    skeleton: DomainOntology,
    cfg: LumiConfig,
) -> DomainOntology | None:
    """Call Gemini once with the corpus + skeleton; parse + return."""
    # Lazy imports — only paid when LLM path is taken.
    try:
        from google.adk.agents import LlmAgent
        from google.adk.runners import InMemoryRunner
        from google.adk.sessions import InMemorySessionService
        from google.genai import types as genai_types
    except ImportError as e:
        logger.warning("ADK not importable, deterministic ontology only: %s", e)
        return None

    prompt = _build_ontology_prompt(contexts, all_fingerprints, eq_map, skeleton)
    safe_prompt = prompt.replace("{", "{{").replace("}", "}}")

    agent = LlmAgent(
        name="domain_ontology_builder",
        model=cfg.model_name,
        description="Synthesizes a domain ontology from corpus metadata.",
        instruction=safe_prompt,
        output_schema=DomainOntology,
        generate_content_config=genai_types.GenerateContentConfig(
            temperature=cfg.temperature,
            max_output_tokens=12000,
            response_mime_type="application/json",
        ),
    )
    runner = InMemoryRunner(agent=agent, app_name="lumi_ontology")
    session_service: InMemorySessionService = runner.session_service  # type: ignore[assignment]
    import asyncio
    user_id, session_id = "lumi_ontology", "build"

    async def _run() -> str | None:
        await session_service.create_session(
            app_name="lumi_ontology", user_id=user_id, session_id=session_id,
        )
        last_text: str | None = None
        async for event in runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(
                    text="Synthesize the domain ontology now.",
                )],
            ),
        ):
            if (event.content and event.content.parts
                    and event.content.parts[0].text):
                last_text = event.content.parts[0].text
        return last_text

    try:
        text = asyncio.run(_run())
    except Exception as e:  # noqa: BLE001
        logger.warning("ontology agent invocation failed: %s", e)
        return None

    if not text:
        return None

    parsed = _parse_ontology_response(text)
    if parsed is None:
        return None
    parsed.authoring = {"mode": "llm", "reason": None}
    return parsed


def _build_ontology_prompt(
    contexts: dict[str, TableContext],
    all_fps: list[SQLFingerprint],
    eq_map: EquivalenceMap,
    skeleton: DomainOntology,
) -> str:
    """Compose the upfront ontology prompt."""
    parts: list[str] = [
        "# Domain ontology synthesis task",
        "",
        "You are a senior data architect. Read the inputs below and "
        "synthesize the **domain ontology** for this 29-table BigQuery "
        "semantic layer. The output drives downstream LookML enrichment "
        "for an NL-to-SQL retrieval system (Radix).",
        "",
        "## Output contract",
        "Return a DomainOntology JSON object. Identify 5-10 dominant "
        "business entities, their synonyms (cardmember = card member = "
        "cm = cust = customer is the canonical example), the grain of "
        "each (one row per X), which columns across which tables "
        "identify each entity, and the key relationships between "
        "entities with cardinality.",
        "",
        "## Tables in scope",
        "",
    ]

    for table_name, ctx in sorted(contexts.items()):
        ds = ctx.mdm_dataset_details or {}
        bus_name = ds.get("business_name") or "(no MDM business_name)"
        desc = ctx.mdm_table_description or ds.get("data_desc") or "(no MDM description)"
        cat = ds.get("data_category", "")
        sub = ds.get("data_sub_category", "")
        cat_str = f"{cat} / {sub}" if sub else cat
        sample_cols = []
        for col in (ctx.mdm_columns or [])[:8]:
            cn = col.get("name")
            bn = col.get("business_name")
            if cn:
                sample_cols.append(f"{cn}" + (f' ("{bn}")' if bn else ""))
        parts.append(
            f"### `{table_name}`\n"
            f"- business_name: {bus_name}\n"
            f"- description: {desc[:280]}{'…' if len(desc) > 280 else ''}\n"
            + (f"- category: {cat_str}\n" if cat_str else "")
            + (f"- sample columns: {', '.join(sample_cols[:8])}\n" if sample_cols else "")
        )

    # Cross-table JOIN evidence
    parts.append("\n## Observed JOIN ON pairs (cross-table equivalence claims)")
    parts.append("")
    join_pairs: dict[frozenset, int] = {}
    for fp in all_fps:
        if fp.parse_error:
            continue
        from_t = fp.primary_table
        for j in fp.joins or []:
            right_t = j.get("right_table") or j.get("other_table")
            lk = j.get("left_key")
            rk = j.get("right_key")
            if not (right_t and lk and rk and from_t):
                continue
            key = frozenset({(from_t, lk), (right_t, rk)})
            join_pairs[key] = join_pairs.get(key, 0) + 1
    for pair, count in sorted(join_pairs.items(), key=lambda kv: -kv[1])[:25]:
        members = sorted(pair)
        a, b = members[0], members[1]
        parts.append(
            f"  - `{a[0]}.{a[1]}` ↔ `{b[0]}.{b[1]}` ({count}× in queries)"
        )

    # Pre-computed deterministic equivalence classes (skeleton hint)
    if eq_map.classes:
        parts.append("\n## Deterministic equivalence classes (transitive closure)")
        parts.append("")
        parts.append(
            "These are connected components of JOIN ON pairs — proven "
            "equivalences. Use as anchors for entity grain_columns."
        )
        for ec in eq_map.classes[:10]:
            members_str = ", ".join(
                f"`{t}.{c}`" for t, c in sorted(ec.members)
            )
            parts.append(
                f"  - {{ {members_str} }} (strength: {ec.query_count})"
            )

    # Skeleton seed
    parts.append("\n## Deterministic skeleton (your starting point)")
    parts.append("")
    parts.append(
        "We pre-classified columns by naming pattern. Refine this with "
        "your domain knowledge — add missing entities, merge synonyms, "
        "fix grain descriptions. Don't lose information; refine."
    )
    if skeleton.entities:
        for e in skeleton.entities:
            parts.append(
                f"  - **{e.name}** (synonyms: {e.synonyms[:5]}): "
                f"{len(e.grain_columns)} tables, "
                f"{sum(len(v) for v in e.grain_columns.values())} columns"
            )

    parts.append("\n## Authoring rules")
    parts.append(
        "1. ENTITIES: 5-10 dominant ones. Use snake_case names. Include "
        "ALL observed synonyms in `synonyms`. Each entity needs at least "
        "2 columns across the corpus to qualify.\n"
        "2. GRAIN: describe what one row represents — 'one row per "
        "cardmember per day', 'one row per transaction', etc.\n"
        "3. GRAIN_COLUMNS: for each entity, map every table to its "
        "identifier columns (the primary-key-ish columns that mean "
        "'this row is about this entity').\n"
        "4. RELATIONSHIPS: identify the key edges — cardmember → account "
        "(one_to_many), account → transaction (one_to_many), etc. "
        "Cardinality must be one of one_to_one, one_to_many, "
        "many_to_one, many_to_many, unknown.\n"
        "5. TABLE_TO_PRIMARY_ENTITY: for each of the 29 tables, name "
        "the entity its rows are PRIMARILY about — usually the entity "
        "that determines the table's grain.\n"
        "6. EVIDENCE: for each entity, cite specific JOIN ON pairs, "
        "MDM business_names, or naming patterns. Don't make up evidence.\n"
        "7. Be CONSERVATIVE — if the evidence doesn't support an entity, "
        "don't invent it. Better to have 5 well-grounded entities than "
        "10 speculative ones.",
    )
    return "\n".join(parts)


def _parse_ontology_response(text: str) -> DomainOntology | None:
    """Tolerant parser, same pattern as plan_builder._parse_plan_response."""
    # Strategy 1: as-is
    try:
        return DomainOntology(**json.loads(text))
    except Exception:  # noqa: BLE001
        pass

    # Strategy 2: strip code fence
    stripped = text.strip()
    if stripped.startswith("```"):
        first_nl = stripped.find("\n")
        if first_nl != -1:
            stripped = stripped[first_nl + 1:]
        if stripped.endswith("```"):
            stripped = stripped[:-3]
        stripped = stripped.strip()
        try:
            return DomainOntology(**json.loads(stripped))
        except Exception:  # noqa: BLE001
            pass

    # Strategy 3: extract first balanced object
    extracted = _extract_first_json_object(stripped)
    if extracted:
        try:
            return DomainOntology(**json.loads(extracted))
        except Exception:  # noqa: BLE001
            # Strategy 4: strip trailing commas
            repaired = re.sub(r",(\s*[}\]])", r"\1", extracted)
            try:
                return DomainOntology(**json.loads(repaired))
            except Exception:  # noqa: BLE001
                pass

    logger.warning(
        "ontology agent returned unparseable output; first 200 chars: %r",
        text[:200],
    )
    return None


def _extract_first_json_object(text: str) -> str | None:
    """Quote-aware brace-balanced extractor; same as plan_builder."""
    start = -1
    depth = 0
    in_string = False
    escape_next = False
    for i, ch in enumerate(text):
        if escape_next:
            escape_next = False
            continue
        if in_string:
            if ch == "\\":
                escape_next = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            if start == -1:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start != -1:
                return text[start:i + 1]
    return None


# ─── Render to prompt section ────────────────────────────────


def render_ontology_for_table(
    ontology: DomainOntology, table_name: str, *, max_entities: int = 8,
) -> str:
    """Dense Markdown section for a single table's plan/enrich prompt.

    Shows: this table's primary entity + that entity's synonyms + grain,
    and the few related entities the table may join into. Filtered to
    keep the prompt focused — we don't dump the whole ontology for every
    table.
    """
    primary = ontology.primary_entity_for_table(table_name)
    related = ontology.related_entities_for_table(table_name, limit=max_entities - 1)

    if primary is None and not related:
        return ""

    lines = [
        "## Domain ontology — entity context for this table",
        "",
        "Names of the same entity vary across tables in this domain. "
        "Use the entity context below to ground descriptions and pick "
        "consistent vocabulary. When a column on this table is "
        "equivalent to a column on another table (per the equivalences "
        "section below), describe both as the same entity.",
        "",
    ]

    if primary is not None:
        lines.append(f"### Primary entity: **{primary.name}**")
        if primary.synonyms:
            lines.append(
                f"Synonyms in this domain: {', '.join(primary.synonyms)}"
            )
        if primary.grain_description:
            lines.append(f"Grain: _{primary.grain_description}_")
        if primary.description:
            lines.append(primary.description)
        if primary.grain_columns.get(table_name):
            cols = primary.grain_columns[table_name]
            lines.append(
                f"On this table, this entity is identified by: "
                f"{', '.join(f'`{c}`' for c in cols)}"
            )
        # Cross-table grain columns (this is the cardmember-equivalence signal!)
        cross_table_cols = [
            (t, cols)
            for t, cols in primary.grain_columns.items()
            if t != table_name
        ]
        if cross_table_cols:
            lines.append("")
            lines.append(
                "Same entity identified on other tables by:"
            )
            for t, cols in cross_table_cols[:6]:
                lines.append(
                    f"  - `{t}`: {', '.join(f'`{c}`' for c in cols)}"
                )
        lines.append("")

    if related:
        lines.append(f"### Related entities ({len(related)})")
        for ent in related:
            syn = (
                f" (a.k.a. {', '.join(ent.synonyms[:3])})"
                if ent.synonyms else ""
            )
            lines.append(f"  - **{ent.name}**{syn}: {ent.description[:120]}")
        # Relationships involving primary
        if primary is not None:
            primary_rels = [
                r for r in ontology.relationships
                if r.from_entity == primary.name or r.to_entity == primary.name
            ]
            if primary_rels:
                lines.append("")
                lines.append("Key relationships involving this entity:")
                for r in primary_rels[:6]:
                    lines.append(
                        f"  - `{r.from_entity}` → `{r.to_entity}` "
                        f"({r.cardinality})"
                        + (f" — {r.evidence}" if r.evidence else "")
                    )
        lines.append("")

    return "\n".join(lines)
