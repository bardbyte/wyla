from __future__ import annotations

import pytest
from pydantic import ValidationError

from lumi.schemas import LumiConfig


def _base_cfg() -> dict:
    return {
        "git": {
            "repo": "https://example.com/x.git",
            "branch": "main",
            "model_file": "models/a.model.lkml",
            "view_files": ["views/a.view.lkml"],
        },
        "mdm": {
            "endpoint": "https://mdm.example.com/api",
            "view_to_mdm_entity": {"a": "entity_a"},
        },
        "gold_queries": {"file": "data/q.xlsx"},
    }


def test_valid_config_minimal() -> None:
    cfg = LumiConfig.model_validate(_base_cfg())
    assert cfg.llm.strong_model_idx == "1"
    assert cfg.llm.fast_model_idx == "3"
    assert cfg.batching.field_threshold == 150


def test_rejects_empty_view_files() -> None:
    raw = _base_cfg()
    raw["git"]["view_files"] = []
    with pytest.raises(ValidationError):
        LumiConfig.model_validate(raw)


def test_resolved_view_names_strips_suffix() -> None:
    raw = _base_cfg()
    raw["git"]["view_files"] = [
        "views/acqdw_acquisition_us.view.lkml",
        "views/custins_customer_insights_cardmember.view.lkml",
    ]
    cfg = LumiConfig.model_validate(raw)
    assert cfg.resolved_view_names() == [
        "acqdw_acquisition_us",
        "custins_customer_insights_cardmember",
    ]


def test_cache_ttl_bounds() -> None:
    raw = _base_cfg()
    raw["mdm"]["cache_ttl_hours"] = 0
    with pytest.raises(ValidationError):
        LumiConfig.model_validate(raw)
