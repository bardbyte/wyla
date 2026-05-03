"""Session 1 end-to-end test against the real on-disk inputs.

Marked `integration` so it only runs with `--run-integration` (per the
conftest.py marker convention). This test reads:

  data/gold_queries/Q*.sql       — produced by scripts/excel_to_queries.py
  data/looker_master/**/*.lkml   — produced by scripts/import_lookml_local.py
                                    (or scripts/fetch_lookml_master.py)
  data/mdm_cache/*.json          — produced by scripts/probe_mdm.py

If any of those is missing, the test SKIPS rather than fails — the
unit tests in test_sql_to_context.py prove the code logic; this test
proves it works against the real corpus on Saheb's work laptop.

Run with:
    pytest tests/test_session1_e2e.py -v --run-integration
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lumi.config import LumiConfig
from lumi.mdm import CachedMDMClient
from lumi.schemas import TableContext
from lumi.sql_to_context import prepare_enrichment_context

pytestmark = pytest.mark.integration


def _load_sqls(d: Path) -> list[str]:
    return [f.read_text(encoding="utf-8") for f in sorted(d.glob("*.sql"))]


@pytest.fixture
def cfg() -> LumiConfig:
    return LumiConfig()


@pytest.fixture
def gold_queries_dir(cfg) -> Path:
    p = Path(cfg.gold_queries_dir)
    if not p.exists() or not list(p.glob("*.sql")):
        pytest.skip(
            "data/gold_queries/ has no .sql files. "
            "Run: python scripts/excel_to_queries.py <your.xlsx>"
        )
    return p


@pytest.fixture
def baseline_dir(cfg) -> Path:
    p = Path(cfg.baseline_views_dir)
    if not p.exists():
        pytest.skip(
            f"{p} doesn't exist. "
            f"Run: python scripts/import_lookml_local.py /path/to/looker_repo"
        )
    return p


@pytest.fixture
def mdm_cache_dir(cfg) -> Path:
    p = Path(cfg.mdm_cache_dir)
    if not p.exists() or not list(p.glob("*.json")):
        pytest.skip(
            f"{p} has no MDM cache files. "
            f"Run: python scripts/probe_mdm.py --save data/mdm_cache/"
        )
    return p


# ─── End-to-end ──────────────────────────────────────────────


def test_session1_e2e_no_crashes(gold_queries_dir, baseline_dir, mdm_cache_dir):
    """The whole Stage 1+2 pipeline runs against real disk inputs without
    raising. This is the smoke check — semantics-level assertions follow.
    """
    sqls = _load_sqls(gold_queries_dir)
    mdm = CachedMDMClient(mdm_cache_dir)
    contexts = prepare_enrichment_context(sqls, mdm, str(baseline_dir))
    assert isinstance(contexts, dict)
    assert len(contexts) > 0
    for name, ctx in contexts.items():
        assert isinstance(ctx, TableContext)
        assert ctx.table_name == name


def test_session1_e2e_at_least_one_baseline_loaded(
    gold_queries_dir, baseline_dir, mdm_cache_dir
):
    """At least one of the discovered tables should have a baseline view
    loaded (otherwise the merge step downstream has nothing to merge into).
    Tells us the recursive baseline lookup actually finds files.
    """
    sqls = _load_sqls(gold_queries_dir)
    mdm = CachedMDMClient(mdm_cache_dir)
    contexts = prepare_enrichment_context(sqls, mdm, str(baseline_dir))
    with_baselines = [n for n, c in contexts.items() if c.existing_view_lkml]
    assert with_baselines, (
        f"No baseline .view.lkml found for any of {sorted(contexts.keys())} "
        f"under {baseline_dir}. Check the directory structure or table names."
    )


def test_session1_e2e_at_least_one_mdm_hit(
    gold_queries_dir, baseline_dir, mdm_cache_dir
):
    """At least one table should have non-zero MDM coverage. Otherwise
    either the cache is empty or the table-name → cache-file mapping is off.
    """
    sqls = _load_sqls(gold_queries_dir)
    mdm = CachedMDMClient(mdm_cache_dir)
    contexts = prepare_enrichment_context(sqls, mdm, str(baseline_dir))
    with_mdm = {
        n: c.mdm_coverage_pct for n, c in contexts.items() if c.mdm_coverage_pct > 0
    }
    assert with_mdm, (
        f"All {len(contexts)} tables came back with mdm_coverage_pct=0. "
        f"Check {mdm_cache_dir} files match the table names."
    )


def test_session1_e2e_every_query_has_a_primary_table(
    gold_queries_dir, baseline_dir, mdm_cache_dir
):
    """Every parsed query should have at least one queries_using_this entry
    on some context — i.e., we didn't drop any queries on the floor.
    """
    sqls = _load_sqls(gold_queries_dir)
    n_sqls = len(sqls)
    mdm = CachedMDMClient(mdm_cache_dir)
    contexts = prepare_enrichment_context(sqls, mdm, str(baseline_dir))
    all_attributed = set()
    for ctx in contexts.values():
        all_attributed.update(ctx.queries_using_this)
    expected = {f"Q{i:02d}" for i in range(1, n_sqls + 1)}
    missing = expected - all_attributed
    assert not missing, (
        f"{len(missing)} queries didn't attribute to any table — likely "
        f"sqlglot parse errors. Missing: {sorted(missing)}"
    )
