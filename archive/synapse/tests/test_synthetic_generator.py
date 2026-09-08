"""Tests for synapse.synthetic — schema + generator are deterministic + correct."""

from __future__ import annotations

import csv
import json
from pathlib import Path


from synapse.synthetic import SYNTHETIC_TABLES, generate_all_sources
from synapse.synthetic.schema import SyntheticTable


def test_synthetic_tables_loaded():
    assert len(SYNTHETIC_TABLES) >= 12
    # Every table has the required identity
    for t in SYNTHETIC_TABLES:
        assert isinstance(t, SyntheticTable)
        assert t.name
        assert t.columns
        assert t.company_domain
        assert t.data_domain


def test_synthetic_tables_have_realistic_pk_shapes():
    """Tables that should have a PK actually declare one."""
    name_to_table = {t.name: t for t in SYNTHETIC_TABLES}
    # Lookup / dim tables MUST have a PK
    dim_like = [
        "drm_product_hier", "gms_merchant_full_hier",
    ]
    for name in dim_like:
        t = name_to_table.get(name)
        assert t is not None, f"missing fixture table: {name}"
        pks = [c for c in t.columns if c.is_primary]
        assert pks, f"{name} should have a primary key"


def test_synthetic_tables_have_pii_taxonomy_on_sensitive_columns():
    """cm11, ssn-like, financial-amount columns must carry PII taxonomy."""
    found = False
    for t in SYNTHETIC_TABLES:
        for c in t.columns:
            if c.name == "cm11":
                assert c.pii_taxonomy.startswith("Sensitive")
                found = True
    assert found, "no cm11 column found across synthetic tables"


def test_synthetic_tables_cover_all_expected_domains():
    """Schema must exercise every domain so the graph has spread."""
    domains = {t.company_domain for t in SYNTHETIC_TABLES}
    for required in (
        "Finance", "Risk", "Loyalty", "Acquisitions Tracking",
        "Merchant", "Travel", "Cardmember",
    ):
        assert required in domains, f"missing domain: {required}"


def test_generate_all_sources_writes_every_artifact(tmp_path: Path):
    counts = generate_all_sources(tmp_path)
    # All source categories returned counts (8 original + 2 Dataplex parallels)
    expected_keys = {
        "glossary", "metric_catalog", "table_catalog",
        "mdm_cache", "sql_corpus", "bq_profile",
        "usage_history", "baseline_lookml",
        "dq_rules", "ai_descriptions",
    }
    assert set(counts.keys()) == expected_keys
    # Every count is positive
    for src, n in counts.items():
        assert n > 0, f"{src} produced 0 artifacts"
    # File-level checks
    assert (tmp_path / "registries" / "raw" / "glossary.csv").exists()
    assert (tmp_path / "registries" / "raw" / "metric_catalog.csv").exists()
    assert (tmp_path / "registries" / "raw" / "table_catalog.csv").exists()
    n_mdm = len(list((tmp_path / "mdm_cache").glob("*.json")))
    n_sql = len(list((tmp_path / "gold_queries").glob("*.sql")))
    n_bq = len(list((tmp_path / "bq_cache").glob("*.json")))
    n_use = len(list((tmp_path / "usage_history").glob("*.json")))
    n_lkml = len(list((tmp_path / "baseline_views").glob("*.view.lkml")))
    assert n_mdm == len(SYNTHETIC_TABLES)
    assert n_bq == len(SYNTHETIC_TABLES)
    assert n_use == len(SYNTHETIC_TABLES)
    assert n_lkml == len(SYNTHETIC_TABLES)
    assert n_sql > 0


def test_generation_is_deterministic(tmp_path: Path):
    """Same generator → same bytes. Reproducibility anchor."""
    out_a = tmp_path / "a"
    out_b = tmp_path / "b"
    generate_all_sources(out_a)
    generate_all_sources(out_b)

    # All registries CSVs byte-identical
    for rel in (
        "registries/raw/glossary.csv",
        "registries/raw/metric_catalog.csv",
        "registries/raw/table_catalog.csv",
    ):
        assert (out_a / rel).read_bytes() == (out_b / rel).read_bytes(), rel

    # MDM cache JSONs byte-identical
    for json_a in (out_a / "mdm_cache").glob("*.json"):
        json_b = out_b / "mdm_cache" / json_a.name
        assert json_a.read_bytes() == json_b.read_bytes(), json_a.name


def test_synthetic_glossary_has_ambiguous_symbols(tmp_path: Path):
    """The glossary must include ambiguous symbols (CM, AA, DM) — these
    drive the disambiguation tests downstream."""
    generate_all_sources(tmp_path)
    csv_path = tmp_path / "registries" / "raw" / "glossary.csv"
    with csv_path.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # CM should appear at least twice with different definitions
    cm_rows = [r for r in rows if r["Symbol"] == "CM"]
    assert len(cm_rows) >= 2
    assert len({r["Definition"] for r in cm_rows}) >= 2


def test_synthetic_metric_catalog_includes_key_metrics(tmp_path: Path):
    generate_all_sources(tmp_path)
    csv_path = tmp_path / "registries" / "raw" / "metric_catalog.csv"
    with csv_path.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    technical_names = {r["technical_name"] for r in rows}
    # The canonical metrics must be there
    for required in ("total_billed_business", "active_cardmembers", "fico_band"):
        assert required in technical_names, f"missing metric: {required}"


def test_synthetic_mdm_cache_has_pii_flags(tmp_path: Path):
    """Verify the MDM digest correctly marks PII columns from taxonomy."""
    generate_all_sources(tmp_path)
    cm_json = tmp_path / "mdm_cache" / "custins_customer_insights_cardmember.json"
    blob = json.loads(cm_json.read_text(encoding="utf-8"))
    cm11_col = next(c for c in blob["columns"] if c["name"] == "cm11")
    assert cm11_col["is_pii"] is True
    assert "Sensitive" in (cm11_col.get("pii_role_id") or "")


def test_synthetic_bq_profile_provides_distinct_values(tmp_path: Path):
    """For columns with sample_distinct in schema, BQ profile must surface them."""
    generate_all_sources(tmp_path)
    blob = json.loads(
        (tmp_path / "bq_cache" / "drm_product_hier.json").read_text(),
    )
    distinct = blob.get("distinct_values", {})
    assert "card_product_id" in distinct
    values = [v["value"] for v in distinct["card_product_id"]]
    assert "005" in values


def test_synthetic_usage_history_has_top_users(tmp_path: Path):
    generate_all_sources(tmp_path)
    blob = json.loads(
        (tmp_path / "usage_history" / "custins_customer_insights_cardmember.json")
        .read_text(),
    )
    assert blob["total_queries"] > 0
    assert len(blob["top_users"]) > 0
    for u in blob["top_users"]:
        assert "email" in u and "team" in u and "query_count" in u


def test_synthetic_baseline_lookml_has_primary_key(tmp_path: Path):
    """Tables with a PK should declare it in baseline LookML."""
    generate_all_sources(tmp_path)
    lkml = (tmp_path / "baseline_views" / "drm_product_hier.view.lkml").read_text()
    assert "primary_key: yes" in lkml
    assert "view: drm_product_hier" in lkml
