"""Shared pytest fixtures."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_queries_xlsx() -> Path:
    path = FIXTURES / "sample_queries.xlsx"
    if not path.exists():
        pytest.skip("sample_queries.xlsx missing; run tests/fixtures/build_sample_queries.py")
    return path


@pytest.fixture
def sample_view_lkml() -> Path:
    return FIXTURES / "sample_view.lkml"


@pytest.fixture
def sample_mdm_response() -> dict[str, Any]:
    return json.loads((FIXTURES / "sample_mdm_response.json").read_text())
