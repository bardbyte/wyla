"""LUMI configuration.

Uses Vertex AI direct. All enterprise-specific endpoints (GitHub, MDM,
BQ project) are read from env vars at runtime; the in-code defaults
are placeholders only.
"""

import os
from dataclasses import dataclass


@dataclass
class LumiConfig:
    """Pipeline configuration. Override via lumi_config.yaml or env vars."""

    # Model — Vertex AI direct
    model_name: str = "gemini-3.1-pro-preview"
    vertex_project: str = os.environ.get("LUMI_VERTEX_PROJECT", "your-vertex-project")
    vertex_location: str = "global"
    temperature: float = 0.0

    # Parallelism
    max_concurrent_enrichments: int = 5  # semaphore for ParallelAgent
    max_concurrent_plans: int = 5        # semaphore for plan stage

    # Self-repair plan loop
    plan_repair_max_rounds: int = 2      # critique → retry → re-critique cap

    # Quality thresholds
    coverage_target_pct: float = 90.0
    max_evaluator_iterations: int = 3
    description_min_chars: int = 15
    description_max_chars: int = 200

    # Batching (for future use with >150 column tables)
    field_batch_size: int = 30

    # NL question generation
    nl_questions_per_sql: int = 8

    # GitHub (Enterprise or .com — set via env)
    github_api_base: str = os.environ.get(
        "LUMI_GITHUB_API_BASE", "https://api.github.com",
    )
    github_repo: str = os.environ.get("LUMI_GITHUB_REPO", "owner/repo")
    github_branch_prefix: str = "lumi/enriched"
    github_create_pr: bool = True

    # MDM API endpoint (set via env)
    mdm_api_base: str = os.environ.get(
        "LUMI_MDM_API_BASE",
        "https://example.invalid/mdm-api/datasets/schemas",
    )

    # Paths
    # baseline_views_dir is now the full Looker mirror by default — same
    # files, just in their original directory layout. discover_tables()
    # finds <table>.view.lkml under any subdir.
    baseline_views_dir: str = "data/looker_master"
    mdm_cache_dir: str = "data/mdm_cache"
    gold_queries_dir: str = "data/gold_queries"
    output_dir: str = "output"
    learnings_path: str = "data/learnings.md"

    # BigQuery project (for sql_table_name in LookML)
    bq_project: str = os.environ.get("LUMI_BQ_PROJECT", "your-bq-project")
    bq_dataset: str = "dw"
