"""Loads and validates lumi_config.yaml via pydantic."""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from lumi.schemas import LumiConfig

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path("lumi_config.yaml")


class ConfigError(Exception):
    """Raised when the config is missing or invalid."""


def load_config(path: str | Path | None = None) -> LumiConfig:
    """Load and validate LumiConfig from YAML.

    Raises ConfigError with a clear message if the file is missing or malformed.
    """
    cfg_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not cfg_path.exists():
        raise ConfigError(
            f"Config not found at {cfg_path}. Copy lumi_config.example.yaml to "
            f"lumi_config.yaml and fill in values."
        )

    try:
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in {cfg_path}: {e}") from e

    if not isinstance(raw, dict):
        raise ConfigError(f"Config at {cfg_path} must be a YAML mapping at the top level.")

    try:
        cfg = LumiConfig.model_validate(raw)
    except Exception as e:
        raise ConfigError(f"Config validation failed: {e}") from e

    logger.info("Loaded config from %s (%d views)", cfg_path, len(cfg.git.view_files))
    return cfg
