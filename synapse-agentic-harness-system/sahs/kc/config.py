"""``config/kc.yaml`` — the catalog this module targets, resolved once.

Resolution: ``KC_CONFIG`` (a path) → ``<silo>/config/kc.yaml`` →
built-in defaults. Missing keys fall back field by field so a partial
file is a valid file; an empty ``tables`` list is legal and means "every
table in the build, and say so"."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

SILO = Path(__file__).resolve().parents[2]
PROMPT_VERSION = "kc.1"

ASPECT_TYPES: tuple[str, ...] = (
    "meridian-provenance", "governed-metrics", "concept-bindings",
    "join-paths", "usage-profile", "sensitivity-consensus",
    "definition-status", "lineage-notes", "value-domain")


@dataclass
class KcConfig:
    project: str = ""
    location: str = "us"
    glossary: str = ""
    tables: list[str] = field(default_factory=list)
    aspect_types: dict[str, str] = field(
        default_factory=lambda: {a: a for a in ASPECT_TYPES})
    push_record_dir: str = "runs/kc"
    llm: bool = True
    prompt_version: str = PROMPT_VERSION
    turn_tokens: int = 120_000
    turn_calls: int = 8
    gate_halt_below: float = 0.60
    gate_review_below: float = 0.80
    path: str = ""

    def aspect_id(self, name: str) -> str:
        """The id the catalog knows an aspect type by. A bare id is
        qualified with project.location when the project is set, the
        way an ``aspects`` map key is spelled in an entries.patch."""
        raw = self.aspect_types.get(name, name)
        if "." in raw or not self.project:
            return raw
        return f"{self.project}.{self.location}.{raw}"

    def glossary_path(self) -> str:
        if self.glossary.startswith("projects/"):
            return self.glossary
        if self.project and self.glossary:
            return (f"projects/{self.project}/locations/{self.location}"
                    f"/glossaries/{self.glossary}")
        return self.glossary


def config_path() -> Path:
    env = os.environ.get("KC_CONFIG")
    return Path(env) if env else SILO / "config" / "kc.yaml"


def load_config(path: Path | None = None) -> KcConfig:
    path = path or config_path()
    raw: dict[str, Any] = {}
    if path.exists():
        loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        raw = dict(loaded.get("kc") or loaded)
    budget = raw.get("budget") or {}
    gate = raw.get("gate") or {}
    aspects = dict(KcConfig().aspect_types)
    aspects.update({str(k): str(v) for k, v in
                    (raw.get("aspect_types") or {}).items()})
    return KcConfig(
        project=str(raw.get("project") or ""),
        location=str(raw.get("location") or "us"),
        glossary=str(raw.get("glossary") or ""),
        tables=[str(t).strip().lower() for t in (raw.get("tables") or [])
                if str(t).strip()],
        aspect_types=aspects,
        push_record_dir=str(raw.get("push_record_dir") or "runs/kc"),
        llm=bool(raw.get("llm", True)),
        prompt_version=str(raw.get("prompt_version") or PROMPT_VERSION),
        turn_tokens=int(budget.get("turn_tokens") or 120_000),
        turn_calls=int(budget.get("turn_calls") or 8),
        gate_halt_below=float(gate.get("halt_below", 0.60)),
        gate_review_below=float(gate.get("review_below", 0.80)),
        path=str(path))
