"""Smoke test — wires the whole pipeline end-to-end without needing real
.env or Vertex access. Validates the code paths import + execute."""

from __future__ import annotations

import sys
from pathlib import Path


_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))


def test_imports_clean():
    """Every module imports without side-effects-on-disk."""
    from semantic_graph import __version__
    from semantic_graph.config import Config  # noqa: F401
    from semantic_graph.loaders import load_all_sources  # noqa: F401
    from semantic_graph.graph import build_and_save_graph, load_cached_graph  # noqa: F401
    from semantic_graph.enrichment import enrich_table  # noqa: F401
    from semantic_graph.tools import inspect_table  # noqa: F401
    assert __version__


def test_skills_files_exist():
    skills = _REPO / "skills"
    assert (skills / "enrichment_skill.md").exists()
    assert (skills / "agent_skill.md").exists()
    # Sanity — non-empty
    assert (skills / "enrichment_skill.md").stat().st_size > 1000
    assert (skills / "agent_skill.md").stat().st_size > 1000


def test_env_example_exists():
    assert (_REPO / ".env.example").exists()


def test_pipeline_runs_on_synthetic(tmp_path, monkeypatch):
    """Spin up a full pipeline against the synthetic generator (no real
    Vertex, no real BQ). Confirms the whole call graph works."""
    # Reach the synapse sibling for synthetic data generation
    synapse_root = _REPO.parent / "synapse"
    sys.path.insert(0, str(synapse_root))
    from synapse.synthetic import generate_all_sources

    demo = tmp_path / "synth_sources"
    generate_all_sources(demo)

    # Stub config — point everything at the synthetic demo dir
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/fake_sa.json")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    monkeypatch.setenv("TABLE_NAME", "custins_customer_insights_cardmember")
    monkeypatch.setenv("BQ_PROJECT", "test-project")
    monkeypatch.setenv("BQ_DATASET", "dw")
    monkeypatch.setenv("BQ_EXTRACTION_DIR", str(demo))
    monkeypatch.setenv("MDM_JSON_PATH", str(demo / "mdm_cache" / "custins_customer_insights_cardmember.json"))
    monkeypatch.setenv("SQL_QUERIES_DIR", str(demo / "gold_queries"))
    monkeypatch.setenv("GRAPH_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("ENRICHMENT_MEMORY_PATH", str(tmp_path / "cache" / "memory.json"))
    monkeypatch.setenv("ENTITY_PROPOSALS_PATH", str(tmp_path / "cache" / "proposals.json"))
    monkeypatch.setenv("ENRICHMENT_DRY_RUN", "1")
    monkeypatch.setenv("ENRICHMENT_BATCH_SIZE", "100")
    monkeypatch.setenv("ENRICHMENT_MAX_CALLS", "5")

    # Reset cached config
    import semantic_graph.config as _cfg_mod
    _cfg_mod._CACHED = None

    from semantic_graph.config import load_config

    # Build the graph WITHOUT calling the loaders (synthetic data IS
    # already in the canonical layout the builder expects). Skip the
    # loaders here and use the existing build_graph_from_sources directly.
    cfg = load_config()
    from synapse.graph import build_graph_from_sources
    store = build_graph_from_sources(demo)
    assert store.stats()["n_nodes"] > 50

    # Save the snapshot the tool will load
    (cfg.graph_cache_dir / "graph_snapshot.json").write_text(
        store.model_dump_json(indent=2), encoding="utf-8"
    )

    # Enrichment in dry-run mode — should produce a non-empty memory
    from semantic_graph.enrichment import enrich_table
    result = enrich_table(cfg, store)
    assert len(result["bundle"].column_observations) > 0
    assert cfg.enrichment_memory_path.exists()

    # The tool reads the cached snapshot — confirm it returns the inspection
    import semantic_graph.tools.inspect_table_tool as tool_mod
    tool_mod._STORE = None  # reset lazy cache
    out = tool_mod.inspect_table(cfg.table_name)
    assert out["identity"]["table"] == cfg.table_name
    assert len(out["columns"]) > 0
