"""The rule that lets this folder be lifted out: nothing here reaches
outside it."""

from __future__ import annotations

import importlib.resources
import re
from pathlib import Path

from kcx.agent import load_instruction
from kcx.gateway import GatewayConfig

ROOT = Path(__file__).resolve().parents[1]


def test_nothing_imports_or_locates_the_monorepo():
    pattern = re.compile(r"^\s*(from|import)\s+sahs\b", re.M)
    for folder in ("kcx", "scripts", "tests"):
        for path in (ROOT / folder).rglob("*.py"):
            if path.name == Path(__file__).name:
                continue                    # this file names the strings
            text = path.read_text(encoding="utf-8")
            assert not pattern.search(text), path
            assert "MERIDIAN_SILO_DIR" not in text, path
            assert "synapse-agentic-harness-system" not in text, path


def test_the_instruction_ships_with_the_package():
    assert importlib.resources.files("kcx").joinpath("SKILL.md").is_file()
    text = load_instruction()
    assert text.startswith("---\nname: knowledge_catalog_discovery_agent")
    assert "knowledge_catalog_search(query: str)" in text


def test_the_gateway_module_carries_no_deployment_url():
    source = (ROOT / "kcx" / "gateway.py").read_text(encoding="utf-8")
    assert re.findall(r"https?://[\w.-]+", source) == []
    cfg = GatewayConfig.from_env({})
    assert cfg.base_url == "" and cfg.token_url == ""
