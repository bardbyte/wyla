"""Evidence bundle assembler.

Pulls the seven sources into a single typed EvidenceBundle the LLM
will draw from for entity proposal. No filtering by domain — the LLM
sees the whole picture and proposes entities scoped to wherever the
evidence supports them.

The seven sources, in priority order:

    1. Glossary CSV            (curated acronyms with disambiguation context)
    2. Metric catalog CSV      (business-vouched metric definitions)
    3. Table catalog CSV       (scope + domain)
    4. MDM cache               (per-table digests; reuses lumi_final's cache)
    5. SQL corpus              (analyst behavior aggregated as nouns)
    6. Baseline LookML         (deferred — handled by extractor layer)
    7. BigQuery samples        (deferred — handled by BQ probe)

The bundle is intentionally lossy on (6) and (7) — they're better
suited as confirmations against the proposed entities, not as
contributions to the proposal. Including them here would bloat the
prompt without adding entity-naming signal.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from synapse.registry import (
    EvidenceBundle,
    GlossaryEntry,
    MDMTableDigest,
    MetricCatalogEntry,
    TableCatalogEntry,
)
from synapse.registry.corpus_signals import aggregate_corpus_signals
from synapse.registry.glossary import load_glossary
from synapse.registry.metric_catalog import load_metric_catalog
from synapse.registry.table_catalog import load_table_catalog

# Make lumi_final available for MDM cache access.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_LUMI_FINAL = _REPO_ROOT / "lumi_final"
if _LUMI_FINAL.exists() and str(_LUMI_FINAL) not in sys.path:
    sys.path.insert(0, str(_LUMI_FINAL))


def load_mdm_digests(
    cache_dir: Path, *, table_names: list[str] | None = None,
    max_sample_columns: int = 20,
) -> list[MDMTableDigest]:
    """Load the per-table MDM cache and distill to MDMTableDigest.

    If table_names is provided, only those tables are loaded. The
    sample_columns list is capped at max_sample_columns to keep the
    prompt size bounded; key columns (is_primary / is_dedupe_key) are
    always included and never truncated."""
    if not cache_dir.exists():
        return []

    out: list[MDMTableDigest] = []
    for path in sorted(cache_dir.glob("*.json")):
        name = path.stem
        if table_names is not None and name not in table_names:
            continue
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(blob, dict):
            continue

        columns = blob.get("columns") or []
        valid_cols = [c for c in columns if isinstance(c, dict)]

        key_cols = [
            {
                "name": c.get("name"),
                "business_name": c.get("business_name"),
                "description": c.get("description") or c.get("attribute_desc"),
                "role": (
                    "pk" if c.get("is_primary")
                    else "dedupe_key" if c.get("is_dedupe_key")
                    else "?"
                ),
            }
            for c in valid_cols
            if c.get("is_primary") or c.get("is_dedupe_key")
        ]

        pii_cols = [
            c.get("name") for c in valid_cols
            if (c.get("is_pii") or c.get("pii_role_id")) and c.get("name")
        ]

        # Sample columns — prefer those with descriptions, then business_name
        scored = sorted(
            valid_cols,
            key=lambda c: -(
                (1 if c.get("description") else 0)
                + (1 if c.get("business_name") else 0)
                + (1 if c.get("is_primary") else 0)
            ),
        )
        sample = [
            {
                "name": c.get("name"),
                "business_name": c.get("business_name"),
                "description": c.get("description") or c.get("attribute_desc"),
                "type": c.get("type") or c.get("attribute_type"),
            }
            for c in scored[:max_sample_columns]
        ]

        bq_fqn = ".".join(filter(None, [
            blob.get("bq_project"), blob.get("bq_dataset"), blob.get("bq_table"),
        ])) or None

        out.append(MDMTableDigest(
            table_name=blob.get("table_name") or name,
            bq_fqn=bq_fqn,
            table_business_name=blob.get("table_business_name"),
            table_description=blob.get("table_description"),
            data_category=blob.get("data_category"),
            data_sub_category=blob.get("data_sub_category"),
            n_columns=len(valid_cols),
            key_columns=key_cols,
            pii_columns=pii_cols,
            sample_columns=sample,
        ))
    return out


def assemble_evidence_bundle(
    *,
    glossary_path: Path,
    metric_catalog_path: Path,
    table_catalog_path: Path,
    mdm_cache_dir: Path,
    sql_corpus_dir: Path,
    scope_description: str = "Enterprise-wide entity curation",
    corpus_top_n: int = 200,
    mdm_max_sample_columns: int = 20,
) -> EvidenceBundle:
    """Build the EvidenceBundle from every source we have.

    All paths must exist. The function does NOT filter by domain at
    this stage — the LLM sees the whole catalog and proposes entities
    where the evidence supports them."""
    glossary: list[GlossaryEntry] = load_glossary(glossary_path)
    metrics: list[MetricCatalogEntry] = load_metric_catalog(metric_catalog_path)
    tables: list[TableCatalogEntry] = load_table_catalog(table_catalog_path)

    # Restrict MDM digests to tables present in the table catalog —
    # avoids polluting the prompt with tables outside scope.
    in_catalog = {t.table_name for t in tables}
    mdm_digests = load_mdm_digests(
        mdm_cache_dir,
        table_names=sorted(in_catalog) if in_catalog else None,
        max_sample_columns=mdm_max_sample_columns,
    )

    # Corpus: walk every gold SQL, aggregate noun frequency.
    # If sqlglot isn't installed (or lumi.sql_to_context fails to import),
    # we degrade gracefully — bundle still assembles with empty corpus
    # signals plus a logger warning. The prompt will note 0 corpus
    # queries analyzed, which the LLM can interpret.
    import logging as _logging
    _log = _logging.getLogger("synapse.curation.bundle")
    sql_files = sorted(sql_corpus_dir.glob("*.sql")) if sql_corpus_dir.exists() else []
    if sql_files:
        try:
            corpus_signals, n_queries = aggregate_corpus_signals(
                sql_files, top_n=corpus_top_n,
            )
        except RuntimeError as e:
            _log.warning(
                "corpus_signals aggregation skipped (%s) — "
                "bundle proceeds with empty corpus", e,
            )
            corpus_signals, n_queries = [], 0
    else:
        corpus_signals, n_queries = [], 0

    return EvidenceBundle(
        scope_description=scope_description,
        table_catalog=tables,
        glossary=glossary,
        metric_catalog=metrics,
        mdm_digests=mdm_digests,
        corpus_signals=corpus_signals,
        n_queries_analyzed=n_queries,
    )
