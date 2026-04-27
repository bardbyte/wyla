from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from lumi.tools.mdm_tools import column_label_fallback, query_mdm_api


def _mock_ok_response(payload: dict) -> MagicMock:
    m = MagicMock()
    m.ok = True
    m.status_code = 200
    m.json.return_value = payload
    return m


def _mock_404() -> MagicMock:
    m = MagicMock()
    m.ok = False
    m.status_code = 404
    m.text = "not found"
    return m


def test_happy_path_api(sample_mdm_response: dict, tmp_path: Path) -> None:
    with patch("lumi.tools.mdm_tools.requests.get") as g:
        g.return_value = _mock_ok_response(sample_mdm_response)
        r = query_mdm_api(
            endpoint="https://mdm.example/api",
            entity_name="acquisition_us_accounts",
            cache_dir=tmp_path,
        )
    assert r["status"] == "success"
    assert r["source"] == "api"
    assert r["metadata"]["canonical_name"] == "Acquisition US Accounts"
    assert "account_id" in r["columns"]
    assert (tmp_path / "acquisition_us_accounts.json").exists()


def test_cache_is_used(sample_mdm_response: dict, tmp_path: Path) -> None:
    # Seed cache
    cache_file = tmp_path / "acquisition_us_accounts.json"
    cache_file.write_text(
        json.dumps(
            {
                "metadata": {"canonical_name": "From Cache", "definition": "", "synonyms": []},
                "columns": {},
            }
        )
    )
    with patch("lumi.tools.mdm_tools.requests.get") as g:
        r = query_mdm_api(
            endpoint="https://mdm.example/api",
            entity_name="acquisition_us_accounts",
            cache_dir=tmp_path,
        )
        g.assert_not_called()
    assert r["source"] == "cache"
    assert r["metadata"]["canonical_name"] == "From Cache"


def test_404_falls_back(tmp_path: Path) -> None:
    with patch("lumi.tools.mdm_tools.requests.get") as g:
        g.return_value = _mock_404()
        r = query_mdm_api(
            endpoint="https://mdm.example/api",
            entity_name="risk_account_history",
            cache_dir=tmp_path,
        )
    assert r["status"] == "success"
    assert r["source"] == "fallback"
    assert r["metadata"]["canonical_name"] == "Risk Account History"


def test_network_error_falls_back(tmp_path: Path) -> None:
    import requests as rq

    with patch("lumi.tools.mdm_tools.requests.get") as g:
        g.side_effect = rq.ConnectionError("dns fail")
        r = query_mdm_api(
            endpoint="https://mdm.example/api",
            entity_name="risk_indv_cust_hist",
            cache_dir=tmp_path,
        )
    assert r["source"] == "fallback"
    assert "dns fail" in r["metadata"]["fallback_reason"]


def test_column_label_fallback() -> None:
    assert column_label_fallback("acct_bal_age_mth01_cd") == "Acct Bal Age Mth01 Cd"
