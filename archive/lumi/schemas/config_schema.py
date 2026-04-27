"""Validates lumi_config.yaml. If this fails, the pipeline cannot start."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field, field_validator


class GitConfig(BaseModel):
    """Where the LookML views live. Cloned once, pulled on re-runs."""

    repo: str = Field(..., description="Git URL, e.g. github.com/org/project or https URL.")
    branch: str = Field("main", description="Branch to check out.")
    model_file: str = Field(..., description="Path within repo to the .model.lkml to read.")
    view_files: list[str] = Field(
        ..., min_length=1, description="Paths within repo to .view.lkml files to enrich."
    )
    clone_dir: str = Field(
        ".git_cache", description="Local directory for the cloned working tree."
    )

    @field_validator("view_files")
    @classmethod
    def views_not_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("view_files cannot be empty")
        return v


class MdmConfig(BaseModel):
    """MDM API access. `auth_env`, if set, names the env var holding the bearer token."""

    endpoint: str = Field(..., description="Base URL, e.g. https://mdm.internal/api/v2")
    auth_env: str | None = Field(
        None,
        description=(
            "Name of env var containing a bearer token. None = no auth. "
            "Confirmed per-deployment; see docs/PLAN.md contradiction #1."
        ),
    )
    view_to_mdm_entity: dict[str, str] = Field(
        ..., description="Map view_name → MDM entity name to query."
    )
    cache_dir: str = Field(".mdm_cache", description="Local cache for responses.")
    cache_ttl_hours: int = Field(24, ge=1, le=168)


class GoldQueriesConfig(BaseModel):
    """Where the 137 (or N) gold queries live and the column names to read."""

    file: str = Field(..., description="Path to Excel file.")
    sheet: str | None = Field(None, description="Sheet name; None = first sheet.")
    prompt_column: str = "user_prompt"
    sql_column: str = "expected_query"
    difficulty_column: str | None = "difficulty"


class LlmConfig(BaseModel):
    """Which SafeChain model indices to use for which task."""

    strong_model_idx: str = Field("1", description='Pro tier. "1" = gemini-2.5-pro.')
    fast_model_idx: str = Field("3", description='Flash tier. "3" = gemini-2.5-flash.')
    temperature: float = Field(0.0, ge=0.0, le=1.0)


class OutputConfig(BaseModel):
    """Where enriched files + reports go."""

    directory: str = Field("output", description="Output root.")
    views_subdir: str = "views"
    model_subdir: str = "models"
    reports_subdir: str = "reports"
    git_branch: str | None = Field(None, description="Optional branch to commit enriched files.")


class BatchingConfig(BaseModel):
    """Controls how large views are split for LLM enrichment."""

    field_threshold: int = Field(
        150, ge=10, description="Views with more fields than this are batched."
    )
    batch_size: int = Field(30, ge=5, description="Target fields per batch.")


class LumiConfig(BaseModel):
    """Root config. Load via lumi.config.load_config()."""

    git: GitConfig
    mdm: MdmConfig
    gold_queries: GoldQueriesConfig
    llm: LlmConfig = Field(default_factory=LlmConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    batching: BatchingConfig = Field(default_factory=BatchingConfig)

    def resolved_view_names(self) -> list[str]:
        """Return view names (file stem minus .view) in config order."""
        return [Path(p).name.removesuffix(".view.lkml").removesuffix(".lkml") for p in self.git.view_files]
