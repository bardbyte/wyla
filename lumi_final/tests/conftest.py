"""LUMI test configuration.

Fixtures are designed so tests read like specs:
    def test_cte_detection(q9_fingerprint):
        assert len(q9_fingerprint.ctes) == 2
"""
import pytest
from tests.fixtures.sample_sqls import (
    Q1_SQL, Q9_SQL, Q10_SQL,
    ALL_SQLS, EASY_SQLS, HARD_SQLS,
)


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration", action="store_true", default=False,
        help="Run integration tests (requires real APIs on work laptop)",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: needs real API access (work laptop only)"
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-integration"):
        skip = pytest.mark.skip(reason="needs --run-integration")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)


# ─── SQL fixtures ────────────────────────────────────────────

@pytest.fixture
def q1_sql(): return Q1_SQL

@pytest.fixture
def q9_sql(): return Q9_SQL

@pytest.fixture
def q10_sql(): return Q10_SQL

@pytest.fixture
def all_sqls(): return ALL_SQLS

@pytest.fixture
def easy_sqls(): return EASY_SQLS

@pytest.fixture
def hard_sqls(): return HARD_SQLS
