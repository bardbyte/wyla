"""LUMI configuration.

Uses Vertex AI direct (NOT SafeChain). See CLAUDE.md for rationale.
"""

from dataclasses import dataclass


@dataclass
class LumiConfig:
    """Pipeline configuration. Override via lumi_config.yaml or env vars."""

    # Model — Vertex AI direct (no SafeChain)
    model_name: str = "gemini-3.1-pro-preview"
    vertex_project: str = "prj-d-ea-poc"
    vertex_location: str = "global"
    temperature: float = 0.0

    # Parallelism
    max_concurrent_enrichments: int = 5  # semaphore for ParallelAgent

    # Quality thresholds
    coverage_target_pct: float = 90.0
    max_evaluator_iterations: int = 3
    description_min_chars: int = 15
    description_max_chars: int = 200

    # Batching (for future use with >150 column tables)
    field_batch_size: int = 30

    # NL question generation
    nl_questions_per_sql: int = 8

    # GitHub Enterprise
    github_api_base: str = "https://github.aexp.com/api/v3"
    github_repo: str = "amex-eng/prj-d-lumi-gpt-semantic"
    github_branch_prefix: str = "lumi/enriched"
    github_create_pr: bool = True

    # MDM API
    mdm_api_base: str = "https://lumimdmapi-guse4.aexp.com/api/v1/ngbd/mdm-api/datasets/schemas"

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
    bq_project: str = "axp-lumi"
    bq_dataset: str = "dw"
