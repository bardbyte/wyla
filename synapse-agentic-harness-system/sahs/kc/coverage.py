"""Coverage: every kind of knowledge the graph holds, rowed against the
catalog construct that receives it.

This registry is the module's contract with the graph. One row per
item; an item is a node kind or one of its props, a relation or one of
its edge props, a provenance field, a witness family, an index field,
a card section, or a run-report field. Each row says WHERE in the
Knowledge Catalog that item lands (in the catalog's own words), HOW it
is represented, WHICH Meridian statuses may flow, WHAT the copied text
must disclose, and WHICH extractor in ``assemble.py`` produces it. An
item with no catalog home is rowed too, with the reason.

The completeness gate walks the current graph and build and fails when
any item present is unrowed: knowledge the graph learned that this
module has not decided about is a build failure, never a silent gap.
Rows may use shell-style patterns (``node:table.*_atlas``) where a
family of attributes shares one destination.

The catalog's documentation was not reachable from the environment
this was written in; construct names follow the catalog's UI and API
vocabulary (entry description, entry overview, glossary term, synonym,
related term, related entry, aspect type and field, query, contact,
data quality rule) and ``export.py`` names the pages to verify against.
"""

from __future__ import annotations

import fnmatch
import json
import re
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

from sahs.graph.ids import ID_PATTERNS
from sahs.graph.quads import RELATIONS, WITNESSES, GraphDir, Prov
from sahs.tools.api import Build

# ── representation vocabulary (pinned) ──────────────────────────
PROSE = "prose"                  # entry description / overview text
GLOSSARY = "glossary"            # glossary category, term, synonym, related term
ASPECT = "aspect field"          # a field of a custom or system aspect
QUERY = "query"                  # the queries aspect (source=User)
DQ = "DQ rule"                   # a data quality rule suggestion
CONTACT = "contact"              # the contacts aspect
RELATED = "related entry"        # term ↔ table/column link
SUGGESTION = "suggestion-only"   # shown in Suggestions, never in a copy block
EXCLUDED = "excluded"            # no catalog home; reason required
REPRESENTATIONS = (PROSE, GLOSSARY, ASPECT, QUERY, DQ, CONTACT, RELATED,
                   SUGGESTION, EXCLUDED)

# ── status rules (which Meridian statuses may flow) ─────────────
ANY = "any (structural fact)"
CERT = "certified only"
CERT_OR_LABELED = "certified; pending/team_candidate only with status label"
WITNESSED = "witnessed by ≥1 non-model family"
REVIEW = "review-only: never copied without a human decision"
NEVER = "never leaves the graph"
NA = "n/a"

# ── extractors: one per row family; assemble.py must define each ──
EXTRACTORS: tuple[str, ...] = (
    "identity", "description", "stewardship", "usage", "columns", "terms",
    "lineage", "keys", "metrics", "concepts", "joins", "sensitivity",
    "status", "queries", "domains", "provenance", "enrichment")


@dataclass(frozen=True)
class CoverageRow:
    item: str            # graph item, see module docstring for the grammar
    kc_target: str       # construct + field, in the catalog's words
    representation: str  # one of REPRESENTATIONS
    status_rule: str     # which statuses may flow
    disclosure: str      # what the copied text must say about itself
    extractor: str       # assemble.py extractor name ('' when excluded)
    reason: str = ""     # why there is no catalog home (excluded rows)

    def matches(self, item: str) -> bool:
        return self.item == item or fnmatch.fnmatchcase(item, self.item)


def _r(item: str, target: str, rep: str, status: str, disclosure: str,
       extractor: str, reason: str = "") -> CoverageRow:
    assert rep in REPRESENTATIONS, rep
    assert (rep == EXCLUDED) == (extractor == ""), item
    assert rep != EXCLUDED or reason, item
    assert extractor in EXTRACTORS or extractor == "", item
    return CoverageRow(item, target, rep, status, disclosure, extractor,
                       reason)


_PROV = "Aspect meridian-provenance"
_STATUS = "Aspect definition-status"
_USAGE = "Aspect usage-profile"
_METRICS = "Aspect governed-metrics"
_BIND = "Aspect concept-bindings"
_JOIN = "Aspect join-paths"
_SENS = "Aspect sensitivity-consensus"
_LIN = "Aspect lineage-notes"
_DOM = "Aspect value-domain (column entry)"
_OVW = "Entry overview §"
_W = "each line carries [witness · status]"

ROWS: tuple[CoverageRow, ...] = (
    # ── witness families: how each renders inside a disclosure ──
    _r("witness:bq", "disclosure text 'BigQuery catalog'", ASPECT, ANY, _W, "provenance"),
    _r("witness:lumi", "disclosure text 'MDM profile'", ASPECT, ANY, _W, "provenance"),
    _r("witness:atlas", "disclosure text 'Atlas catalog'", ASPECT, ANY, _W, "provenance"),
    _r("witness:steward", "disclosure text 'steward decision'", ASPECT, ANY, _W, "provenance"),
    _r("witness:dmp", "disclosure text 'metric catalog (dmp)'", ASPECT, ANY, _W, "provenance"),
    _r("witness:gmns", "disclosure text 'GMNS spec'", ASPECT, ANY, _W, "provenance"),
    _r("witness:skill_contract", "disclosure text 'skill contract'", ASPECT, ANY, _W, "provenance"),
    _r("witness:catalog_mined", "disclosure text 'mined from the measures catalog'", ASPECT, ANY, _W, "provenance"),
    _r("witness:jobs_30d", "disclosure text 'from 30-day query history'", ASPECT, ANY, _W, "provenance"),
    _r("witness:audit_30d", "disclosure text 'audit log corroboration'", ASPECT, ANY, "corroborates, never votes", "provenance"),
    _r("witness:snippet", "disclosure text 'insight snippet'", ASPECT, ANY, _W, "provenance"),
    _r("witness:studio", "disclosure text 'observed in Studio SQL'", ASPECT, ANY, _W, "provenance"),
    _r("witness:llm_enriched", "label 'Suggested — unreviewed'", SUGGESTION, REVIEW, "model text is never copied without its flag", "enrichment"),
    _r("witness:gold_attested", "Query (attested sample) only", QUERY, WITNESSED, "attested SQL; prompts and answers never leave", "queries"),
    _r("witness:user_variant", "none", EXCLUDED, NEVER, "", "", "a person's on-the-fly variant is not shared truth"),
    _r("witness:kc", "none (the read-back witness)", EXCLUDED, NA, "", "", "what the catalog already says is input to the census, never output"),

    # ── provenance fields ──
    _r("prov:source", f"{_PROV}.witnesses[].source", ASPECT, ANY, "named per fact", "provenance"),
    _r("prov:witness", f"{_PROV}.witnesses[].family", ASPECT, ANY, "named per fact", "provenance"),
    _r("prov:run", f"{_PROV}.graph_run", ASPECT, ANY, "the graph run id", "provenance"),
    _r("prov:retrieved", f"{_PROV}.retrieved_at", ASPECT, ANY, "", "provenance"),
    _r("prov:support", "support counts inside aspect rows", ASPECT, ANY, "count of independent observations", "provenance"),
    _r("prov:status", "quad status filter (active only)", EXCLUDED, NA, "", "", "superseded/retracted quads are never assembled"),
    _r("prov:evidence", "expandable evidence on every fact", ASPECT, ANY, "the source file or record", "provenance"),
    _r("prov:actor", "Contact (steward decisions); push record signature", CONTACT, ANY, "named steward", "stewardship"),
    _r("prov:valid_for", "none", EXCLUDED, NA, "", "", "schema-version validity is a serving concern"),

    # ── table node ──
    _r("node:table", "Entry (BigQuery table)", PROSE, ANY, "", "identity"),
    _r("node:table.object_type", f"{_OVW} Grain & keys; {_PROV}.object_type", ASPECT, ANY, "", "identity"),
    _r("node:table.total_rows", f"{_OVW} Grain & keys; {_PROV}.row_count", ASPECT, ANY, "absent means unknown, never zero", "identity"),
    _r("node:table.n_partitions", f"{_PROV}.partitions", ASPECT, ANY, "", "identity"),
    _r("node:table.partition_latest", f"{_OVW} Grain & keys; DQ rule freshness", DQ, ANY, "latest partition as observed", "identity"),
    _r("node:table.lifecycle_status", "Entry description (status posture); definition-status.lifecycle", ASPECT, ANY, "", "identity"),
    _r("node:table.schema_fingerprint", f"{_PROV}.schema_fingerprint", ASPECT, ANY, "", "identity"),
    _r("node:table.project", f"{_PROV}.project", ASPECT, ANY, "", "identity"),
    _r("node:table.layer_type", f"{_OVW} Purpose; {_PROV}.layer", ASPECT, ANY, "", "identity"),
    _r("node:table.environment", f"{_PROV}.environment", ASPECT, ANY, "", "identity"),
    _r("node:table.feed_type", f"{_LIN}.feed_type", ASPECT, ANY, "", "lineage"),
    _r("node:table.source_system", f"{_LIN}.source_system", ASPECT, ANY, "", "lineage"),
    _r("node:table.pipeline_name", f"{_LIN}.pipeline", ASPECT, ANY, "", "lineage"),
    _r("node:table.appl_id", f"{_PROV}.application_id", ASPECT, ANY, "", "identity"),
    _r("node:table.stub", "none", EXCLUDED, NA, "", "", "loader placeholder flag, no meaning"),
    _r("node:table.table_meta_logical", f"{_PROV}.catalog_attributes (map)", ASPECT, ANY, "", "identity"),
    _r("node:table.table_metrics", f"{_PROV}.catalog_attributes (map)", ASPECT, ANY, "", "identity"),
    _r("node:table.answerability", f"{_STATUS}.answerability", ASPECT, ANY, "", "status"),
    _r("node:table.cost_prior", f"{_USAGE}.bytes_per_query", ASPECT, ANY, "30-day history", "usage"),
    _r("node:table.top_users", f"{_USAGE}.distinct_users (count only)", ASPECT, ANY, "names never leave; the count does", "usage"),
    _r("node:table.usage_rhythm", f"{_USAGE}.peak_hours", ASPECT, ANY, "from 30-day history", "usage"),
    _r("node:table.description_atlas", "Entry description; overview § Purpose", PROSE, ANY, "Atlas catalog", "description"),
    _r("node:table.description_bq", "Entry description (fallback); overview § Purpose", PROSE, ANY, "BigQuery catalog", "description"),
    _r("node:table.description_mdm", "overview § Purpose (supplementary)", PROSE, ANY, "MDM profile", "description"),
    _r("node:table.business_name_atlas", "Entry display name suggestion; glossary term", GLOSSARY, ANY, "Atlas catalog", "description"),
    _r("node:table.business_unit", f"Contact (business unit); {_OVW} Ownership & usage", CONTACT, ANY, "", "stewardship"),
    _r("node:table.ownership_atlas", "Contact (business owner / steward)", CONTACT, ANY, "Atlas catalog", "stewardship"),
    _r("node:table.data_category", f"{_PROV}.catalog_attributes.data_category", ASPECT, ANY, "", "identity"),
    _r("node:table.data_sub_category", f"{_PROV}.catalog_attributes.data_sub_category", ASPECT, ANY, "", "identity"),
    _r("node:table.has_pii_atlas", f"{_SENS}.declared.pii", ASPECT, ANY, "Atlas declaration", "sensitivity"),
    _r("node:table.has_gdpr_atlas", f"{_SENS}.declared.gdpr", ASPECT, ANY, "Atlas declaration", "sensitivity"),
    _r("node:table.has_oncop_atlas", f"{_SENS}.declared.oncop", ASPECT, ANY, "Atlas declaration", "sensitivity"),
    _r("node:table.is_lineage_exist_atlas", f"{_LIN}.catalog_declares_lineage", ASPECT, ANY, "", "lineage"),
    _r("node:table.*_atlas", f"{_PROV}.catalog_attributes (map, Atlas plane)", ASPECT, ANY, "Atlas catalog", "identity"),
    _r("node:table.*_mdm", f"{_PROV}.catalog_attributes (map, MDM plane)", ASPECT, ANY, "MDM profile", "identity"),

    # ── column node ──
    _r("node:col", "Column entry (schema aspect field)", PROSE, ANY, "", "columns"),
    _r("node:col.data_type", "Column description (type shown); schema field", ASPECT, ANY, "BigQuery is authoritative on type", "columns"),
    _r("node:col.data_type_atlas", f"{_STATUS}.type_disagreements (D3)", ASPECT, ANY, "catalog says; BigQuery wins", "columns"),
    _r("node:col.data_type_mdm", f"{_STATUS}.type_disagreements (D3)", ASPECT, ANY, "MDM says; BigQuery wins", "columns"),
    _r("node:col.description_atlas", "Column description", PROSE, ANY, "Atlas catalog", "columns"),
    _r("node:col.description_bq", "Column description (fallback)", PROSE, ANY, "BigQuery catalog", "columns"),
    _r("node:col.description_mdm", "Column description (supplementary)", PROSE, ANY, "MDM profile", "columns"),
    _r("node:col.business_name", "Column description (business name prefix); glossary term candidate", GLOSSARY, ANY, "", "columns"),
    _r("node:col.business_name_atlas", "Column description (business name prefix)", PROSE, ANY, "Atlas catalog", "columns"),
    _r("node:col.column_name_atlas", f"{_STATUS}.name_disagreements", ASPECT, ANY, "", "columns"),
    _r("node:col.is_pii_mdm", f"{_SENS}.columns[].declared_by (MDM)", ASPECT, ANY, "MDM declaration", "sensitivity"),
    _r("node:col.pii_role_id", f"{_SENS}.columns[].pii_role", ASPECT, ANY, "MDM declaration", "sensitivity"),
    _r("node:col.sde_group", f"{_SENS}.columns[].sde_group", ASPECT, ANY, "MDM declaration", "sensitivity"),
    _r("node:col.is_primary_key", "DQ rule uniqueness; overview § Grain & keys", DQ, ANY, "declared key", "keys"),
    _r("node:col.is_primary_key_atlas", "DQ rule uniqueness (catalog-declared)", DQ, ANY, "Atlas declaration", "keys"),
    _r("node:col.is_partitioning", "overview § Grain & keys; DQ rule freshness", DQ, ANY, "", "keys"),
    _r("node:col.is_partitioning_atlas", "overview § Grain & keys (catalog view)", ASPECT, ANY, "Atlas declaration", "keys"),
    _r("node:col.nullable_atlas", "DQ rule not-null (catalog-declared)", DQ, ANY, "Atlas declaration", "columns"),
    _r("node:col.null_count", "DQ rule not-null (profile evidence)", DQ, ANY, "profile scan", "columns"),
    _r("node:col.approx_distinct", f"{_DOM}.distinct_estimate", ASPECT, ANY, "profile scan", "domains"),
    _r("node:col.profile_coverage", f"{_DOM}.profile_coverage", ASPECT, ANY, "how much was profiled", "domains"),
    _r("node:col.derived_logic", f"{_LIN}.column_derivations", ASPECT, ANY, "MDM lineage", "lineage"),
    _r("node:col.ordinal", "none", EXCLUDED, NA, "", "", "the catalog owns column order from the schema"),
    _r("node:col.ordinal_atlas", "none", EXCLUDED, NA, "", "", "the catalog owns column order from the schema"),
    _r("node:col.column_length", "none", EXCLUDED, NA, "", "", "physical length is already in the schema aspect"),
    _r("node:col.nested_path", "Column description (nested path shown)", PROSE, ANY, "", "columns"),
    _r("node:col.observed_via", f"{_STATUS}.columns_observed_via", ASPECT, ANY, "which plane saw the column", "columns"),
    _r("node:col.stub", "none", EXCLUDED, NA, "", "", "loader placeholder flag, no meaning"),
    _r("node:col.*_atlas", f"{_PROV}.catalog_attributes (column, Atlas plane)", ASPECT, ANY, "Atlas catalog", "columns"),
    _r("node:col.*_mdm", f"{_PROV}.catalog_attributes (column, MDM plane)", ASPECT, ANY, "MDM profile", "columns"),

    # ── metric node + index ──
    _r("node:metric", f"Glossary term (certified) / Candidate terms (pending); {_METRICS}", GLOSSARY, CERT_OR_LABELED, "status inside the definition", "metrics"),
    _r("node:metric.label", "Glossary term display name", GLOSSARY, CERT_OR_LABELED, "", "metrics"),
    _r("node:metric.canonical_sql", f"Query (certified); {_METRICS}.canonical_sql", QUERY, CERT, "certified SQL only as a query; others in the aspect", "metrics"),
    _r("node:metric.canon_version", f"{_METRICS}.fingerprint_version", ASPECT, ANY, "", "metrics"),
    _r("node:metric.grain", f"Glossary term definition (grain); {_METRICS}.grain", GLOSSARY, CERT_OR_LABELED, "", "metrics"),
    _r("node:metric.grain_declared", f"{_METRICS}.grain (catalog-declared)", ASPECT, ANY, "", "metrics"),
    _r("node:metric.grain_observed", f"{_METRICS}.grain_observed", ASPECT, ANY, "observed in Studio SQL", "metrics"),
    _r("node:metric.question_answered", "Glossary term definition (question)", GLOSSARY, CERT_OR_LABELED, "", "metrics"),
    _r("node:metric.description", "Glossary term overview", GLOSSARY, CERT_OR_LABELED, "", "metrics"),
    _r("node:metric.expression_prose", "Glossary term overview (calculation note)", GLOSSARY, CERT_OR_LABELED, "", "metrics"),
    _r("node:metric.approved_dimensions", f"{_METRICS}.approved_dimensions", ASPECT, CERT_OR_LABELED, "", "metrics"),
    _r("node:metric.sign_convention", f"{_METRICS}.sign_convention", ASPECT, ANY, "", "metrics"),
    _r("node:metric.common_filters", f"{_METRICS}.filters", ASPECT, ANY, "filters are part of identity", "metrics"),
    _r("node:metric.author", "Contact (metric author)", CONTACT, CERT_OR_LABELED, "", "metrics"),
    _r("node:metric.author_id", "none", EXCLUDED, NA, "", "", "an internal id behind the author name"),
    _r("node:metric.requestor", "Contact (requestor)", CONTACT, CERT_OR_LABELED, "", "metrics"),
    _r("node:metric.data_owners", "Contact (data owner)", CONTACT, ANY, "Studio export", "metrics"),
    _r("node:metric.data_owners_dmp", "Contact (data owner)", CONTACT, ANY, "metric catalog", "metrics"),
    _r("node:metric.approval", f"{_METRICS}.approval", ASPECT, ANY, "the catalog's own approval record", "metrics"),
    _r("node:metric.business_unit", "Glossary category (business unit)", GLOSSARY, ANY, "", "metrics"),
    _r("node:metric.line_of_business", "Glossary category (LOB)", GLOSSARY, ANY, "", "metrics"),
    _r("node:metric.domain", "Glossary category (domain)", GLOSSARY, ANY, "", "metrics"),
    _r("node:metric.data_category", f"{_METRICS}.data_category", ASPECT, ANY, "", "metrics"),
    _r("node:metric.scope", f"{_METRICS}.scope", ASPECT, ANY, "", "metrics"),
    _r("node:metric.products", f"{_METRICS}.products", ASPECT, ANY, "", "metrics"),
    _r("node:metric.product_ids", f"{_METRICS}.products", ASPECT, ANY, "", "metrics"),
    _r("node:metric.base_tables", f"{_METRICS}.base_tables", ASPECT, ANY, "", "metrics"),
    _r("node:metric.joined_tables", f"{_JOIN} (tier: candidate)", SUGGESTION, ANY, "which tables, never how", "joins"),
    _r("node:metric.join_condition", f"{_METRICS}.join_condition", ASPECT, ANY, "Studio export", "metrics"),
    _r("node:metric.join_conditions", f"{_METRICS}.join_conditions", ASPECT, ANY, "metric catalog", "metrics"),
    _r("node:metric.tables_associated_not_referenced", f"{_METRICS}.tables_associated", ASPECT, ANY, "associated, not referenced", "metrics"),
    _r("node:metric.execution_count", f"{_USAGE}.metric_runs", ASPECT, ANY, "catalog usage texture", "usage"),
    _r("node:metric.group_by_patterns", f"{_METRICS}.group_by_patterns", ASPECT, ANY, "", "metrics"),
    _r("node:metric.query_shape", f"{_METRICS}.query_shape", ASPECT, ANY, "", "metrics"),
    _r("node:metric.confidence", f"{_METRICS}.miner_confidence", ASPECT, ANY, "the miner's word", "metrics"),
    _r("node:metric.label_usage", f"{_USAGE}.label_usage", ASPECT, ANY, "", "usage"),
    _r("node:metric.sql_source", f"{_METRICS}.sql_source", ASPECT, ANY, "", "metrics"),
    _r("node:metric.question_enriched", "Suggestion: term definition (question)", SUGGESTION, REVIEW, "Suggested — unreviewed", "enrichment"),
    _r("node:metric.grain_enriched", "Suggestion: term definition (grain)", SUGGESTION, REVIEW, "Suggested — unreviewed", "enrichment"),
    _r("node:metric.*_enriched", "Suggestion", SUGGESTION, REVIEW, "Suggested — unreviewed", "enrichment"),
    _r("node:metric.enrich_*", "Suggestion metadata (confidence, caveat, prompt version)", SUGGESTION, REVIEW, "", "enrichment"),

    # ── other node kinds ──
    _r("node:mgroup", "Glossary term (the catalog name of a metric family)", GLOSSARY, CERT_OR_LABELED, "", "metrics"),
    _r("node:mgroup.label", "Glossary term display name", GLOSSARY, CERT_OR_LABELED, "", "metrics"),
    _r("node:mgroup.group_key", f"{_METRICS}.catalog_id", ASPECT, ANY, "", "metrics"),
    _r("node:pred", f"{_BIND}.predicate_sql", ASPECT, WITNESSED, "", "concepts"),
    _r("node:pred.canonical_sql", f"{_BIND}.predicate_sql", ASPECT, WITNESSED, "", "concepts"),
    _r("node:pred.canon_version", f"{_BIND}.fingerprint_version", ASPECT, ANY, "", "concepts"),
    _r("node:pred.kind", f"{_BIND}.kind", ASPECT, ANY, "", "concepts"),
    _r("node:tmpl", f"{_USAGE}.query_templates (count + shape)", ASPECT, ANY, "mined shapes are never sample queries", "usage"),
    _r("node:tmpl.normalized_sql", f"{_USAGE}.query_templates[].shape", ASPECT, ANY, "parameterized shape, not a runnable query", "usage"),
    _r("node:tmpl.occurrences", f"{_USAGE}.query_templates[].runs", ASPECT, ANY, "30-day history", "usage"),
    _r("node:tmpl.table", "join key to the table", EXCLUDED, NA, "", "", "the template's own table pointer"),
    _r("node:concept", f"Glossary term (certified/witnessed concept); {_BIND}", GLOSSARY, WITNESSED, "", "concepts"),
    _r("node:concept.label", "Glossary term display name", GLOSSARY, WITNESSED, "", "concepts"),
    _r("node:concept.description_enriched", "Suggestion: concept definition", SUGGESTION, REVIEW, "Suggested — unreviewed", "enrichment"),
    _r("node:concept.disambiguation_enriched", "Suggestion: concept disambiguation", SUGGESTION, REVIEW, "Suggested — unreviewed", "enrichment"),
    _r("node:concept.*_enriched", "Suggestion", SUGGESTION, REVIEW, "Suggested — unreviewed", "enrichment"),
    _r("node:concept.enrich_*", "Suggestion metadata", SUGGESTION, REVIEW, "", "enrichment"),
    _r("node:term", "Glossary term (business term)", GLOSSARY, ANY, "Atlas catalog", "terms"),
    _r("node:term.name", "Glossary term display name", GLOSSARY, ANY, "", "terms"),
    _r("node:term.description", "Glossary term description", GLOSSARY, ANY, "", "terms"),
    _r("node:term.status", "Glossary term description (status prefix)", GLOSSARY, ANY, "the catalog's own status word", "terms"),
    _r("node:term.term_id", f"{_PROV}.term_ids (source id)", ASPECT, ANY, "", "terms"),
    _r("node:acr", "Synonym of its expansion", GLOSSARY, WITNESSED, "scope in the definition", "terms"),
    _r("node:acr.symbol", "Synonym text", GLOSSARY, WITNESSED, "", "terms"),
    _r("node:acr.definition", "Glossary term description (expansion)", GLOSSARY, WITNESSED, "", "terms"),
    _r("node:acr.business_unit", "Glossary term description (scope: business unit)", GLOSSARY, ANY, "", "terms"),
    _r("node:acr.region", "Glossary term description (scope: region)", GLOSSARY, ANY, "", "terms"),
    _r("node:acr.entry_type", "none", EXCLUDED, NA, "", "", "the source sheet's row type"),
    _r("node:acr.common_word", "none (the anti-alias guard)", EXCLUDED, NEVER, "", "", "a common word flagged as acronym must never become a synonym"),
    _r("node:domain", f"{_DOM}.values; DQ rule set membership", DQ, ANY, "profile scan", "domains"),
    _r("node:domain.values", f"{_DOM}.values[] (value, share)", ASPECT, ANY, "profile scan", "domains"),
    _r("node:domain.meanings", f"{_DOM}.values[].meaning", ASPECT, ANY, "MDM value index", "domains"),
    _r("node:domain.distinct_estimate", f"{_DOM}.distinct_estimate", ASPECT, ANY, "", "domains"),
    _r("node:domain.profiled", f"{_DOM}.profiled", ASPECT, ANY, "", "domains"),
    _r("node:owner", "Contact", CONTACT, ANY, "", "stewardship"),
    _r("node:owner.owner", "Contact identity", CONTACT, ANY, "", "stewardship"),
    _r("node:owner.role", "Contact role", CONTACT, ANY, "", "stewardship"),
    _r("node:lob", "Glossary category (LOB → org unit)", GLOSSARY, ANY, "", "stewardship"),
    _r("node:lob.code", "Glossary category display name", GLOSSARY, ANY, "", "stewardship"),
    _r("node:lob.name", "Glossary category description", GLOSSARY, ANY, "", "stewardship"),
    _r("node:lob.kind", "Glossary category level (lob | org_unit)", GLOSSARY, ANY, "", "stewardship"),
    _r("node:lob.parent", "Glossary category parent", GLOSSARY, ANY, "", "stewardship"),
    _r("node:mdom", "Glossary category (metric domain)", GLOSSARY, ANY, "", "stewardship"),
    _r("node:mdom.name", "Glossary category display name", GLOSSARY, ANY, "", "stewardship"),
    _r("node:schema", f"{_PROV}.schema_version", ASPECT, ANY, "", "identity"),
    _r("node:schema.fingerprint", f"{_PROV}.schema_fingerprint", ASPECT, ANY, "", "identity"),
    _r("node:schema.n_columns", f"{_PROV}.schema_columns", ASPECT, ANY, "", "identity"),
    _r("node:doc", "Query (SQL docs); lineage-notes (view SQL)", QUERY, WITNESSED, "which kind of SQL it is", "queries"),
    _r("node:doc.kind", "Query description / lineage note kind", QUERY, WITNESSED, "", "queries"),
    _r("node:doc.sql", "Query SQL", QUERY, WITNESSED, "attested SQL only", "queries"),
    _r("node:doc.text", f"{_LIN}.notes", ASPECT, WITNESSED, "", "queries"),
    # the read-back loader's docs: what the catalog said, pending
    _r("node:doc.name", "none (read-back: the catalog's own term name)", EXCLUDED, NA, "", "", "catalog read-back is census input, never output"),
    _r("node:doc.category", "none (read-back)", EXCLUDED, NA, "", "", "catalog read-back is census input, never output"),
    _r("node:doc.review_status", "none (read-back review state)", EXCLUDED, NA, "", "", "the pending flag on read-back records"),
    _r("node:doc.*", "none (read-back fields)", EXCLUDED, NA, "", "", "catalog read-back is census input, never output"),
    _r("node:review", f"{_STATUS}.open_reviews", ASPECT, ANY, "open steward items about this table", "status"),
    _r("node:review.kind", f"{_STATUS}.open_reviews[].kind", ASPECT, ANY, "", "status"),
    _r("node:review.subject", f"{_STATUS}.open_reviews[].subject", ASPECT, ANY, "", "status"),
    _r("node:review.proposal", f"{_STATUS}.open_reviews[].proposal", ASPECT, ANY, "", "status"),
    _r("node:review.evidence", f"{_STATUS}.open_reviews[].evidence", ASPECT, ANY, "", "status"),
    _r("node:review.priority", f"{_STATUS}.open_reviews[].priority", ASPECT, ANY, "", "status"),
    _r("node:review.status", f"{_STATUS}.open_reviews[].status", ASPECT, ANY, "", "status"),
    _r("node:review.agent_recommendation", "none", EXCLUDED, REVIEW, "", "", "a recommendation to the steward, not a fact about the table"),
    _r("node:review.*", f"{_STATUS}.open_reviews[] (decision fields)", ASPECT, ANY, "", "status"),
    _r("node:skill", f"{_METRICS}.skill_pack", ASPECT, ANY, "", "metrics"),
    _r("node:skill.*", f"{_METRICS}.skill_pack (attributes)", ASPECT, ANY, "", "metrics"),
    _r("node:status", "status word inside every glossary definition", GLOSSARY, ANY, "", "metrics"),
    _r("node:policy", f"{_SENS}.policy", ASPECT, ANY, "UNKNOWN → 'treat as restricted'", "sensitivity"),
    _r("node:run", f"{_PROV}.graph_run", ASPECT, ANY, "", "provenance"),

    # ── relations ──
    _r("edge:has_column", "Column entries (schema)", PROSE, ANY, "", "columns"),
    _r("edge:has_schema", f"{_PROV}.schema_version", ASPECT, ANY, "", "identity"),
    _r("edge:bound_to", f"{_BIND}.bindings[]; Related entry term→table", RELATED, WITNESSED, "conflicts marked contested", "concepts"),
    _r("edge:bound_to.*", f"{_BIND}.bindings[].first_seen/last_seen/runs", ASPECT, ANY, "", "concepts"),
    _r("edge:defines_metric", f"{_METRICS}.catalog_id", ASPECT, ANY, "", "metrics"),
    _r("edge:measured_on", f"Related entry term→table; {_METRICS}.metrics[]", RELATED, CERT_OR_LABELED, "", "metrics"),
    _r("edge:measured_on.*", f"{_METRICS}.metrics[].first_seen/last_seen/runs", ASPECT, ANY, "", "metrics"),
    _r("edge:variant_of", "Related term ('variant of <parent>')", GLOSSARY, CERT_OR_LABELED, "delta named", "metrics"),
    _r("edge:mapped_term", "Related entry term→column", RELATED, ANY, "Atlas mapping", "terms"),
    _r("edge:mapped_term.confidence", "Related entry note (mapping confidence)", RELATED, ANY, "", "terms"),
    _r("edge:mapped_term.mapping_source", "Related entry note", RELATED, ANY, "", "terms"),
    _r("edge:mapped_term.mapping_type", "Related entry note (declared | inferred)", RELATED, ANY, "declared vs inferred stated", "terms"),
    _r("edge:mapped_term.matched_on", "Related entry note", RELATED, ANY, "", "terms"),
    _r("edge:alias_of", "Synonym", GLOSSARY, WITNESSED, "anti-aliases never", "terms"),
    _r("edge:joins_via", f"{_JOIN}.paths[] (tiered)", ASPECT, WITNESSED, "witnessed vs candidate labeled; candidates never as joins", "joins"),
    _r("edge:joins_via.on", f"{_JOIN}.paths[].on", ASPECT, WITNESSED, "", "joins"),
    _r("edge:joins_via.scope", f"{_JOIN}.paths[].scope", ASPECT, WITNESSED, "scoped_only = not raw-safe", "joins"),
    _r("edge:joins_via.join_type", f"{_JOIN}.paths[].join_type", ASPECT, WITNESSED, "", "joins"),
    _r("edge:joins_via.purpose", f"{_JOIN}.paths[].purpose", ASPECT, WITNESSED, "", "joins"),
    _r("edge:joins_via.preconditions", f"{_JOIN}.paths[].preconditions", ASPECT, WITNESSED, "", "joins"),
    _r("edge:joins_via.witness_metrics", f"{_JOIN}.paths[].seen_in_metrics", ASPECT, ANY, "", "joins"),
    _r("edge:joins_via.measures", f"{_JOIN}.paths[].seen_in_measures", ASPECT, ANY, "", "joins"),
    _r("edge:joins_via.confidence", f"{_JOIN}.paths[].confidence", ASPECT, ANY, "the miner's word", "joins"),
    _r("edge:joins_via.how", f"{_JOIN}.paths[].how", ASPECT, ANY, "'unknown' keeps the path a candidate", "joins"),
    _r("edge:joins_via.*", f"{_JOIN}.paths[].first_seen/last_seen", ASPECT, ANY, "", "joins"),
    _r("edge:co_queried_with", "Suggestion: proposed join (co-usage)", SUGGESTION, ANY, "co-usage alone is not a join", "joins"),
    _r("edge:derived_from", f"{_LIN}.column_derivations[]", ASPECT, ANY, "MDM lineage", "lineage"),
    _r("edge:derived_from.derivation_logic", f"{_LIN}.column_derivations[].logic", ASPECT, ANY, "", "lineage"),
    _r("edge:upstream_of", f"{_LIN}.upstream[]; overview § Provenance", ASPECT, ANY, "API lineage needs process events: notes only", "lineage"),
    _r("edge:owned_by", "Contact", CONTACT, ANY, "", "stewardship"),
    _r("edge:owned_by.role", "Contact role", CONTACT, ANY, "", "stewardship"),
    _r("edge:certified_as", "status word in every term / aspect row", GLOSSARY, ANY, "", "metrics"),
    _r("edge:certified_as.note", f"{_METRICS}.metrics[].status_note", ASPECT, ANY, "steward note", "metrics"),
    _r("edge:has_policy", f"{_SENS}.policy", ASPECT, ANY, "UNKNOWN → 'treat as restricted'", "sensitivity"),
    _r("edge:has_domain", f"{_DOM}", ASPECT, ANY, "", "domains"),
    _r("edge:evidenced_by", "Query (attested SQL for a metric)", QUERY, WITNESSED, "", "queries"),
    _r("edge:evidenced_by.*", "Query description (first/last seen)", QUERY, ANY, "", "queries"),
    _r("edge:valid_in", "none", EXCLUDED, NA, "", "", "schema-version validity is a serving concern"),
    _r("edge:member_of", f"{_METRICS}.metrics[].families", ASPECT, ANY, "", "metrics"),
    _r("edge:member_of.*", f"{_METRICS}.metrics[].first_seen/last_seen/runs", ASPECT, ANY, "", "metrics"),
    _r("edge:described_by", f"{_LIN}.view_sql; Query (view definition)", QUERY, WITNESSED, "", "queries"),
    _r("edge:described_by.*", "none (read-back link fields)", EXCLUDED, NA, "", "", "review_status / link kind on read-back edges"),
    _r("edge:concerns", f"{_STATUS}.open_reviews[]", ASPECT, ANY, "", "status"),
    _r("edge:in_lob", "Glossary category membership; Contact (LOB)", GLOSSARY, ANY, "home | shared stated", "stewardship"),
    _r("edge:in_lob.role", "overview § Ownership & usage (home | shared)", PROSE, ANY, "", "stewardship"),
    _r("edge:in_lob.note", "overview § Ownership & usage (steward note)", PROSE, ANY, "", "stewardship"),
    _r("edge:in_lob.*", "none", EXCLUDED, NA, "", "", "first/last seen of a membership: bookkeeping"),
    _r("edge:in_domain", "Glossary category (metric domain)", GLOSSARY, ANY, "", "stewardship"),
    _r("edge:in_domain.*", "none", EXCLUDED, NA, "", "", "first/last seen of a membership: bookkeeping"),
    _r("edge:used_by", f"{_USAGE}.used_by_lob[]; overview § Ownership & usage", ASPECT, ANY, "usage ≠ ownership", "usage"),
    _r("edge:used_by.*", f"{_USAGE}.used_by_lob[].first_seen/last_seen", ASPECT, ANY, "", "usage"),
    _r("edge:fk_references", f"{_JOIN}.paths[] (tier: declared); DQ rule referential", DQ, ANY, "declared constraint", "keys"),
    _r("edge:fk_references.constraint", f"{_JOIN}.paths[].constraint_name", ASPECT, ANY, "", "keys"),
    _r("edge:kc_pushed", "Push record (Synapse side)", EXCLUDED, NA, "", "", "the record of what left; the catalog holds the content itself"),
    _r("edge:kc_pushed.*", "Push record fields", EXCLUDED, NA, "", "", "the record of what left"),

    # ── compiled indexes ──
    _r("index:metrics.*", f"{_METRICS} (row fields mirror node props)", ASPECT, CERT_OR_LABELED, "", "metrics"),
    _r("index:metrics.status_served", "status word in every term / aspect row", GLOSSARY, ANY, "governance vs evidence split", "metrics"),
    _r("index:metrics.evidence_origin", f"{_METRICS}.metrics[].evidence_origin", ASPECT, ANY, "", "metrics"),
    _r("index:metrics.support_by_witness", f"{_METRICS}.metrics[].witnesses", ASPECT, ANY, "", "metrics"),
    _r("index:metrics.witness_agreement", f"{_METRICS}.metrics[].agreement", ASPECT, ANY, "", "metrics"),
    _r("index:bindings.*", f"{_BIND}.bindings[]", ASPECT, WITNESSED, "", "concepts"),
    _r("index:vocab.*", "Glossary term / synonym (compiled view)", GLOSSARY, WITNESSED, "", "terms"),
    _r("index:joins.*", f"{_JOIN}.paths[]", ASPECT, WITNESSED, "", "joins"),
    _r("index:joins.also_witnessed_by", f"{_JOIN}.paths[].corroborated_by", ASPECT, ANY, "", "joins"),
    _r("index:joins.also_in_catalog", f"{_JOIN}.paths[].corroborated_by (catalog)", ASPECT, ANY, "", "joins"),
    _r("index:joins.note", f"{_JOIN}.paths[].note", ASPECT, ANY, "", "joins"),
    _r("index:tables.*", f"{_PROV} / overview § Grain & keys", ASPECT, ANY, "", "identity"),
    _r("index:tables.primary_key", "DQ rule uniqueness", DQ, ANY, "declared key", "keys"),
    _r("index:lob.*", "Glossary categories; Contact (LOB)", GLOSSARY, ANY, "", "stewardship"),
    _r("index:domains.*", f"{_DOM}; DQ rule set membership", DQ, ANY, "", "domains"),
    _r("index:value_meanings.*", f"{_DOM}.values[].meaning", ASPECT, ANY, "", "domains"),
    _r("index:cost_priors.*", f"{_USAGE}.bytes_per_query", ASPECT, ANY, "30-day history", "usage"),
    _r("index:columns.*", "Column description; column sensitivity; definition-status", PROSE, ANY, "", "columns"),
    _r("index:acl.pii_columns", f"{_SENS}.columns[] (most restrictive)", ASPECT, ANY, "union, most restrictive", "sensitivity"),
    _r("index:acl.restricted", f"{_SENS}.policy", ASPECT, ANY, "UNKNOWN → 'treat as restricted'", "sensitivity"),
    _r("index:schema", "Column entries (servable columns)", PROSE, ANY, "", "columns"),
    _r("index:graph_map.*", "none", EXCLUDED, NA, "", "", "a rendering of the graph for the sky, not table knowledge"),
    _r("index:sources.*", f"{_PROV}.witnesses[] (display names)", ASPECT, ANY, "", "provenance"),

    # ── cards (served text) ──
    _r("card:table.preamble", "Entry description; overview § Purpose", PROSE, ANY, "", "description"),
    _r("card:table.columns", "Column descriptions", PROSE, ANY, "", "columns"),
    _r("card:table.joined_with", f"{_JOIN}; overview § Joins", ASPECT, WITNESSED, "witnessed vs candidate", "joins"),
    _r("card:table.common_filters", f"{_BIND}; overview § Concepts & filters", ASPECT, WITNESSED, "", "concepts"),
    _r("card:table.metrics_available", f"{_METRICS}; overview § Metrics on this table", ASPECT, CERT_OR_LABELED, "certified first; pending labeled", "metrics"),
    _r("card:table.access", f"{_SENS}; overview § Sensitivity", ASPECT, ANY, "", "sensitivity"),
    _r("card:table.conflicts", f"{_STATUS}; overview § What we don't know yet", ASPECT, ANY, "", "status"),
    _r("card:metric.preamble", "Glossary term definition + overview", GLOSSARY, CERT_OR_LABELED, "", "metrics"),
    _r("card:metric.variants", "Related terms (metric family)", GLOSSARY, CERT_OR_LABELED, "", "metrics"),
    _r("card:metric.conflicts", f"{_STATUS}.metric_conflicts", ASPECT, ANY, "contested — steward pending", "status"),
    _r("card:metric.*", f"{_METRICS} (other card sections)", ASPECT, ANY, "", "metrics"),
    _r("card:concept.preamble", "Glossary term (concept)", GLOSSARY, WITNESSED, "", "concepts"),
    _r("card:concept.bindings", f"{_BIND}.bindings[]", ASPECT, WITNESSED, "", "concepts"),
    _r("card:concept.conflicts", f"{_STATUS}.concept_conflicts", ASPECT, ANY, "contested — steward pending", "status"),

    # ── run reports and build bookkeeping ──
    _r("report:census.summary.*", f"{_STATUS}.census (build-wide counts)", ASPECT, ANY, "build-wide, not per table", "status"),
    _r("report:census.structural.per_table", f"{_STATUS}.d_counts; overview § What we don't know yet", ASPECT, ANY, "D1..D5 explained", "status"),
    _r("report:census.structural.totals", "none", EXCLUDED, NA, "", "", "build-wide totals; the per-table cell is what an entry carries"),
    _r("report:census.structural.handlers", f"{_STATUS}.d_counts (handler text)", ASPECT, ANY, "", "status"),
    _r("report:census.meta", "none", EXCLUDED, NA, "", "", "scope notes about the census counters themselves"),
    _r("report:census.schema", "none", EXCLUDED, NA, "", "", "envelope"),
    _r("report:tickets.*", f"{_STATUS}.tickets[]; DQ rule suggestions", DQ, ANY, "", "status"),
    _r("report:manifest.build_id", f"{_PROV}.build_id", ASPECT, ANY, "", "provenance"),
    _r("report:manifest.graph_hash", f"{_PROV}.graph_hash", ASPECT, ANY, "", "provenance"),
    _r("report:manifest.*", "none", EXCLUDED, NA, "", "", "build bookkeeping (counts, gates, resolver constants), not table knowledge"),
    _r("report:diff.*", "none", EXCLUDED, NA, "", "", "the build-to-build diff is Synapse's steward surface"),
    _r("report:run.run_id", f"{_PROV}.graph_run", ASPECT, ANY, "", "provenance"),
    _r("report:run.roots", "none", EXCLUDED, NA, "", "", "machine paths of the inputs"),
    _r("report:run.reports.*", "none", EXCLUDED, NA, "", "", "loader counters: ingestion health, not table knowledge"),
    _r("report:run.utilization", "none", EXCLUDED, NA, "", "", "the input ledger: ingestion health, not table knowledge"),
    _r("report:run.archived", "none", EXCLUDED, NA, "", "", "run housekeeping"),
    _r("report:validation.*", "none", EXCLUDED, NA, "", "", "graph integrity report, not table knowledge"),
    _r("report:enrich.*", "Suggestion metadata (gate tier, prompt version)", SUGGESTION, REVIEW, "", "enrichment"),
    _r("report:feedback", "none", EXCLUDED, NA, "", "", "talk-back records are Synapse's own loop"),
    _r("report:kc.*", "Push record (Synapse side)", EXCLUDED, NA, "", "", "this module's own run records"),
)


def rows_for(item: str) -> list[CoverageRow]:
    """Every row that claims the item, exact matches first."""
    exact = [r for r in ROWS if r.item == item]
    if exact:
        return exact
    return [r for r in ROWS if r.matches(item)]


def row_for(item: str) -> CoverageRow | None:
    hits = rows_for(item)
    return hits[0] if hits else None


# ── walking what actually exists ─────────────────────────────────

def _slug(header: str) -> str:
    head = header.split("(", 1)[0].split(":", 1)[0].strip().lower()
    return re.sub(r"[^a-z0-9]+", "_", head).strip("_")


def _card_sections(build_root: Path) -> set[str]:
    items: set[str] = set()
    for kind in ("tables", "metrics", "concepts"):
        singular = kind[:-1]
        for path in sorted((build_root / "cards" / kind).glob("*.md"))[:200]:
            items.add(f"card:{singular}.preamble")
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.startswith("## "):
                    items.add(f"card:{singular}.{_slug(line[3:])}")
    return items


def _jsonl_fields(path: Path) -> set[str]:
    fields: set[str] = set()
    if not path.exists():
        return fields
    for line in path.read_text(encoding="utf-8").split("\n"):
        if line.strip():
            fields.update(json.loads(line).keys())
    return fields


def observed_items(graph_root: Path, build_root: Path) -> set[str]:
    """Every item present in this graph and build, plus the static
    registries (witness enum, relation registry, id grammar, prov
    fields) so a kind that exists in code but not yet in data is still
    rowed before its first quad lands."""
    items: set[str] = set()
    items.update(f"witness:{w}" for w in WITNESSES)
    items.update(f"edge:{r}" for r in RELATIONS)
    items.update(f"node:{k}" for k in ID_PATTERNS)
    items.update(f"prov:{f}" for f in Prov.model_fields)

    graph = GraphDir(graph_root)
    for record in graph.iter_nodes():
        kind = record.id.split(":", 1)[0]
        items.add(f"node:{kind}")
        items.update(f"node:{kind}.{p}" for p in record.props)
    for quad in graph.iter_edges():
        items.add(f"edge:{quad.r}")
        items.update(f"edge:{quad.r}.{p}" for p in quad.props)

    build_root = Path(build_root)
    for name in ("metrics", "bindings", "vocab", "joins", "tables", "lob",
                 "domains", "value_meanings"):
        items.update(f"index:{name}.{f}" for f in
                     _jsonl_fields(build_root / "indexes" / f"{name}.jsonl"))
    priors = build_root / "indexes" / "cost_priors.json"
    if priors.exists():
        for entry in json.loads(priors.read_text(encoding="utf-8")).values():
            items.update(f"index:cost_priors.{f}" for f in entry)
    gmap = build_root / "indexes" / "graph_map.json"
    if gmap.exists():
        items.update(f"index:graph_map.{k}" for k in
                     json.loads(gmap.read_text(encoding="utf-8")))
    sources = build_root / "indexes" / "sources.json"
    if sources.exists():
        items.update(f"index:sources.{k}" for k in
                     json.loads(sources.read_text(encoding="utf-8")))
    columns = build_root / "columns.json"
    if columns.exists():
        for rows in json.loads(columns.read_text(encoding="utf-8")).values():
            for row in rows:
                items.update(f"index:columns.{f}" for f in row)
    if (build_root / "schema.json").exists():
        items.add("index:schema")
    acl = build_root / "acl.json"
    if acl.exists():
        for entry in json.loads(acl.read_text(encoding="utf-8")).values():
            items.update(f"index:acl.{f}" for f in entry)
    items.update(_card_sections(build_root))

    census = build_root / "census.json"
    if census.exists():
        payload = json.loads(census.read_text(encoding="utf-8"))
        for key, value in payload.items():
            if key in ("summary", "structural") and isinstance(value, dict):
                items.update(f"report:census.{key}.{k}" for k in value)
            else:
                items.add(f"report:census.{key}")
    items.update(f"report:tickets.{f}" for f in
                 _jsonl_fields(build_root / "tickets.jsonl"))
    manifest = build_root / "manifest.json"
    if manifest.exists():
        items.update(f"report:manifest.{k}" for k in
                     json.loads(manifest.read_text(encoding="utf-8")))
    diff = build_root / "DIFF_vs_prev.md"
    if diff.exists():
        for line in diff.read_text(encoding="utf-8").splitlines():
            if line.startswith("## "):
                items.add(f"report:diff.{_slug(line[3:])}")
    for run_manifest in sorted((graph_root / "runs").glob("*/manifest.json")):
        payload = json.loads(run_manifest.read_text(encoding="utf-8"))
        for key, value in payload.items():
            if key == "reports" and isinstance(value, dict):
                items.update(f"report:run.reports.{k}" for k in value)
            else:
                items.add(f"report:run.{key}")
        validation = run_manifest.parent / "validation.json"
        if validation.exists():
            items.update(f"report:validation.{k}" for k in json.loads(
                validation.read_text(encoding="utf-8")))
        enrich = run_manifest.parent / "enrich_report.json"
        if enrich.exists():
            items.update(f"report:enrich.{k}" for k in json.loads(
                enrich.read_text(encoding="utf-8")))
    if (graph_root / "runs" / "feedback").exists():
        items.add("report:feedback")
    if (graph_root / "runs" / "kc").exists():
        items.add("report:kc.records")
    return items


def missing_rows(graph_root: Path, build_root: Path) -> list[str]:
    """The completeness gate: items present with no row. Empty = green."""
    return sorted(i for i in observed_items(graph_root, build_root)
                  if row_for(i) is None)


def item_counts(graph_root: Path) -> dict[str, int]:
    """How many records each node kind / relation holds: the weights
    behind the coverage percentages."""
    counts: Counter[str] = Counter()
    graph = GraphDir(graph_root)
    for record in graph.iter_nodes():
        counts[f"node:{record.id.split(':', 1)[0]}"] += 1
    for quad in graph.iter_edges():
        counts[f"edge:{quad.r}"] += 1
    return dict(counts)


def coverage_report(graph_root: Path, build_root: Path) -> dict[str, Any]:
    """The machine-readable coverage: rows, observed items with their
    row, the missing list, and the counts the tab publishes."""
    observed = observed_items(graph_root, build_root)
    manifest = Path(build_root) / "manifest.json"
    build_id = (json.loads(manifest.read_text(encoding="utf-8")).get("build_id", "")
                if manifest.exists() else "")
    resolved = []
    by_rep: Counter[str] = Counter()
    for item in sorted(observed):
        row = row_for(item)
        resolved.append({"item": item,
                         "row": row.item if row else "",
                         "representation": row.representation if row else "",
                         "kc_target": row.kc_target if row else ""})
        by_rep[row.representation if row else "MISSING"] += 1
    total = max(1, len(observed))
    targeted = sum(n for rep, n in by_rep.items()
                   if rep not in (SUGGESTION, EXCLUDED, "MISSING"))
    return {
        "schema": "meridian.kc_coverage/1",
        "build_id": build_id,
        "rows": [asdict(r) for r in ROWS],
        "observed": resolved,
        "missing": [r["item"] for r in resolved if not r["row"]],
        "counts": {
            "rows": len(ROWS),
            "observed": len(observed),
            "by_representation": dict(sorted(by_rep.items())),
            "pct_targeted": round(100 * targeted / total, 1),
            "pct_suggestion_only": round(100 * by_rep[SUGGESTION] / total, 1),
            "pct_excluded_with_reason": round(100 * by_rep[EXCLUDED] / total, 1),
        },
        "item_counts": item_counts(graph_root),
    }


def dictionary(report: dict[str, Any] | None = None) -> dict[str, Any]:
    """The two-way terminology map the tab renders: Meridian item →
    catalog construct, and catalog construct → Meridian items. Built
    from the rows alone, so the page can never say more than the
    registry does."""
    forward = [{"meridian": r.item, "kc": r.kc_target,
                "representation": r.representation,
                "status_rule": r.status_rule, "disclosure": r.disclosure,
                "extractor": r.extractor, "reason": r.reason}
               for r in ROWS]
    reverse: dict[str, list[str]] = {}
    for r in ROWS:
        construct = r.kc_target.split(";")[0].split(".")[0].strip()
        reverse.setdefault(construct, []).append(r.item)
    return {"forward": forward,
            "reverse": [{"kc": k, "meridian": sorted(v)}
                        for k, v in sorted(reverse.items())],
            "observed": (report or {}).get("observed", [])}


# ── the documents ────────────────────────────────────────────────

def render_markdown(report: dict[str, Any]) -> str:
    counts = report["counts"]
    lines = [
        "# Knowledge Catalog coverage",
        "",
        "One row per kind of knowledge the graph holds, against the catalog "
        "construct that receives it. Generated by `pipeline.py kc-coverage` "
        "from `sahs/kc/coverage.py`; the completeness gate fails the build "
        "when the current graph carries an item with no row.",
        "",
        f"- generated against build **{report.get('build_id') or '?'}**: the "
        "observed items and the per-kind counts below are that build's; the "
        "rows are the registry and do not depend on the build",
        f"- rows: **{counts['rows']}** · items observed in the current graph "
        f"and build: **{counts['observed']}** · missing: "
        f"**{len(report['missing'])}**",
        f"- targeted at a catalog construct: **{counts['pct_targeted']}%** · "
        f"suggestion-only: **{counts['pct_suggestion_only']}%** · "
        f"excluded with a reason: **{counts['pct_excluded_with_reason']}%**",
        "",
        "| representation | items |", "|---|---|",
    ]
    for rep, n in counts["by_representation"].items():
        lines.append(f"| {rep} | {n} |")
    if report["missing"]:
        lines += ["", "## MISSING (gate red)", ""]
        lines += [f"- `{m}`" for m in report["missing"]]
    lines += ["", "## Rows", "",
              "| item | KC target | representation | statuses that flow | "
              "disclosure | extractor | reason |",
              "|---|---|---|---|---|---|---|"]
    for r in report["rows"]:
        lines.append("| `{}` | {} | {} | {} | {} | {} | {} |".format(
            r["item"], r["kc_target"], r["representation"], r["status_rule"],
            r["disclosure"] or "", r["extractor"] or "", r["reason"] or ""))
    lines += ["", "## Facts per kind in the current graph", "",
              "| kind | records |", "|---|---|"]
    for item, n in sorted(report["item_counts"].items()):
        lines.append(f"| `{item}` | {n} |")
    return "\n".join(lines) + "\n"


def write_coverage_docs(graph_root: Path, build_root: Path,
                        docs_dir: Path) -> dict[str, Any]:
    report = coverage_report(graph_root, build_root)
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "kc_coverage.json").write_text(
        json.dumps(report, indent=1, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8")
    (docs_dir / "kc_coverage.md").write_text(render_markdown(report),
                                             encoding="utf-8")
    return report


def build_root_of(builds_root: Path) -> Path:
    return Build.open(builds_root).root
