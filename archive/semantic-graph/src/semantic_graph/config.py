"""Configuration loaded from .env."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel, Field


_ROOT = Path(__file__).resolve().parents[2]   # semantic-graph/
_ENV_PATH = _ROOT / ".env"


class Config(BaseModel):
    # ── Vertex AI ──
    google_credentials_path: Path
    google_project: str
    google_location: str = "global"
    gemini_model: str = "gemini-3.1-pro-preview"

    # ── Target table ──
    table_name: str
    bq_project: str
    bq_dataset: str

    # ── Sources (one of two modes) ──
    # Mode A — lumi-fused single file (preferred, simpler)
    lumi_output_path: Path | None = None
    # Mode B — three separate raw sources (BQ extraction + MDM API + SQL folder)
    bq_extraction_dir: Path | None = None
    mdm_json_path: Path | None = None
    sql_queries_dir: Path | None = None

    # ── Outputs ──
    graph_cache_dir: Path
    enrichment_memory_path: Path
    entity_proposals_path: Path

    # ── Enrichment controls ──
    enrichment_batch_size: int = 20
    enrichment_max_calls: int = 20
    enrichment_dry_run: bool = False

    # ── Repo paths (computed) ──
    repo_root: Path = Field(default_factory=lambda: _ROOT)
    skills_dir: Path = Field(default_factory=lambda: _ROOT / "skills")

    @property
    def fqn(self) -> str:
        return f"{self.bq_project}.{self.bq_dataset}.{self.table_name}"

    @property
    def enrichment_skill_path(self) -> Path:
        return self.skills_dir / "enrichment_skill.md"

    @property
    def agent_skill_path(self) -> Path:
        return self.skills_dir / "agent_skill.md"


_CACHED: Config | None = None


def load_config(*, env_path: Path | None = None, override: dict[str, Any] | None = None) -> Config:
    """Load .env once; return a typed Config. Idempotent."""
    global _CACHED
    if _CACHED is not None and override is None:
        return _CACHED

    env_file = env_path or _ENV_PATH
    if env_file.exists():
        # override=False: existing process env (set by shell export OR by
        # pytest monkeypatch.setenv) wins over the .env file. This keeps
        # tests hermetic and lets users override per-shell.
        load_dotenv(env_file, override=False)

    def _req(key: str) -> str:
        v = os.getenv(key)
        if not v:
            raise RuntimeError(
                f"Missing required env var {key} (expected in {env_file}). "
                "Copy semantic-graph/.env.example to .env and fill in real paths."
            )
        return v

    def _opt(key: str, default: str) -> str:
        return os.getenv(key, default)

    # Sources are additive — every configured loader fires; the graph
    # builder fuses them via per-fact Provenance. Require at least one.
    lumi_path = os.getenv("LUMI_OUTPUT_PATH", "").strip()
    bq_dir    = os.getenv("BQ_EXTRACTION_DIR", "").strip()
    mdm_path  = os.getenv("MDM_JSON_PATH", "").strip()
    sql_dir   = os.getenv("SQL_QUERIES_DIR", "").strip()
    if not any([lumi_path, bq_dir, mdm_path, sql_dir]):
        raise RuntimeError(
            "No source configured. Set at least one of "
            "LUMI_OUTPUT_PATH, BQ_EXTRACTION_DIR, MDM_JSON_PATH, SQL_QUERIES_DIR "
            f"in {env_file}."
        )
    source_paths = {
        "lumi_output_path":  Path(lumi_path) if lumi_path else None,
        "bq_extraction_dir": Path(bq_dir)    if bq_dir    else None,
        "mdm_json_path":     Path(mdm_path)  if mdm_path  else None,
        "sql_queries_dir":   Path(sql_dir)   if sql_dir   else None,
    }

    raw: dict[str, Any] = {
        "google_credentials_path": Path(_req("GOOGLE_APPLICATION_CREDENTIALS")),
        "google_project":          _req("GOOGLE_CLOUD_PROJECT"),
        "google_location":         _opt("GOOGLE_CLOUD_LOCATION", "global"),
        "gemini_model":            _opt("GEMINI_MODEL", "gemini-3.1-pro-preview"),
        "table_name":              _req("TABLE_NAME"),
        "bq_project":              _req("BQ_PROJECT"),
        "bq_dataset":              _req("BQ_DATASET"),
        **source_paths,
        "graph_cache_dir":         Path(_opt("GRAPH_CACHE_DIR", "./data/cache")),
        "enrichment_memory_path":  Path(_opt("ENRICHMENT_MEMORY_PATH", "./data/cache/enrichment_memory.json")),
        "entity_proposals_path":   Path(_opt("ENTITY_PROPOSALS_PATH", "./data/cache/entity_proposals.json")),
        "enrichment_batch_size":   int(_opt("ENRICHMENT_BATCH_SIZE", "20")),
        "enrichment_max_calls":    int(_opt("ENRICHMENT_MAX_CALLS", "20")),
        "enrichment_dry_run":      _opt("ENRICHMENT_DRY_RUN", "0") in ("1", "true", "True"),
    }
    if override:
        raw.update(override)

    cfg = Config(**raw)

    # Set the four Vertex env vars Google's google-genai client expects
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(cfg.google_credentials_path)
    os.environ["GOOGLE_GENAI_USE_VERTEXAI"]      = "true"
    os.environ["GOOGLE_CLOUD_PROJECT"]           = cfg.google_project
    os.environ["GOOGLE_CLOUD_LOCATION"]          = cfg.google_location

    # Ensure output dirs exist
    cfg.graph_cache_dir.mkdir(parents=True, exist_ok=True)

    _CACHED = cfg
    return cfg
