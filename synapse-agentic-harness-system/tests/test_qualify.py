"""The graph says dw.table; the warehouse wants project.dataset.table.

qualify.py rewrites every table the build knows with the data
project before a trip to BigQuery — deterministic, never a prose rule
— and physical_of accepts the qualified form on the way back so the
validator, the sandbox, and the canon all agree on one physical name.
"""

from __future__ import annotations

import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))

from sahs.tools.api import Build                          # noqa: E402
from sahs.tools.qualify import qualify_tables             # noqa: E402


class Stub:
    """Just enough of a Build: the schema and the real resolver."""
    schema = {"dw.gms_transaction": {}, "dw.wwcas_authorization": {}}
    physical_of = Build.physical_of
    short_table = Build.short_table


def test_two_part_and_bare_names_get_the_data_project():
    sql = ("SELECT t.part_dt, sum(t.trans_usd_am) AS spend "
           "FROM dw.gms_transaction t "
           "JOIN wwcas_authorization w ON t.id = w.id "
           "WHERE t.country_cd = 'US' GROUP BY t.part_dt")
    sent, changes = qualify_tables(sql, Stub(), "demo-warehouse")
    assert [c["to"] for c in changes] == [
        "demo-warehouse.dw.gms_transaction", "demo-warehouse.dw.wwcas_authorization"]
    assert "`demo-warehouse`.dw.gms_transaction AS t" in sent
    assert "`demo-warehouse`.dw.wwcas_authorization AS w" in sent
    assert "'US'" in sent and "GROUP BY" in sent      # the rest survives


def test_qualified_names_ctes_and_unknowns_pass_through():
    already = "SELECT 1 FROM `demo-warehouse`.dw.gms_transaction"
    assert qualify_tables(already, Stub(), "demo-warehouse") == (already, [])
    with_cte = ("WITH x AS (SELECT 1 FROM dw.gms_transaction) "
                "SELECT * FROM x")
    sent, changes = qualify_tables(with_cte, Stub(), "demo-warehouse")
    assert len(changes) == 1 and changes[0]["from"] == "dw.gms_transaction"
    assert "FROM x" in sent and "`demo-warehouse`.x" not in sent
    unknown = "SELECT 1 FROM dw.ghost_table"
    assert qualify_tables(unknown, Stub(), "demo-warehouse") == (unknown, [])
    # no data project configured: nothing moves
    sql = "SELECT 1 FROM dw.gms_transaction"
    assert qualify_tables(sql, Stub(), "") == (sql, [])


def test_the_textual_fallback_handles_unparseable_sql():
    from sahs.tools.qualify import _qualify_textually
    sql = "SELECT 1 FROM dw.gms_transaction WHERE ??? JOIN `dw.wwcas_authorization`"
    sent, changes = _qualify_textually(sql, Stub(), "demo-warehouse")
    assert "FROM `demo-warehouse.dw.gms_transaction`" in sent
    assert "JOIN `demo-warehouse.dw.wwcas_authorization`" in sent
    assert len(changes) == 2


def test_physical_of_accepts_the_qualified_form():
    stub = Stub()
    assert stub.physical_of("demo-warehouse.dw.gms_transaction") \
        == "dw.gms_transaction"
    assert stub.physical_of("`demo-warehouse`.dw.gms_transaction") \
        == "dw.gms_transaction"
    assert stub.physical_of("`demo-warehouse.dw.gms_transaction`") \
        == "dw.gms_transaction"
    assert stub.physical_of("gms_transaction") == "dw.gms_transaction"
    assert stub.physical_of("other.dw.ghost") is None
