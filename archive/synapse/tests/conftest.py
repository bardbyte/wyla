"""Shared pytest fixtures."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNAPSE_ROOT = REPO_ROOT / "synapse"
FIXTURES = SYNAPSE_ROOT / "tests" / "fixtures"

# Make synapse importable when running pytest from the repo root.
if str(SYNAPSE_ROOT) not in sys.path:
    sys.path.insert(0, str(SYNAPSE_ROOT))


@pytest.fixture
def glossary_csv() -> Path:
    return FIXTURES / "glossary_sample.csv"


@pytest.fixture
def metric_catalog_csv() -> Path:
    return FIXTURES / "metric_catalog_sample.csv"


@pytest.fixture
def table_catalog_csv() -> Path:
    return FIXTURES / "table_catalog_sample.csv"


@pytest.fixture
def llm_response_yaml() -> Path:
    return FIXTURES / "llm_response_sample.yaml"
