"""Read the extraction manifest (tables.yaml) — the one table list every
stage shares: BQ batch extraction, the MDM crawl, and graph compilation.
"""

from __future__ import annotations

from pathlib import Path


def read_tables_manifest(path: str | Path) -> list[dict]:
    """tables.yaml → [{name, bq_dataset?, bq_project?}, ...].

    Entries may be bare strings or dicts (same tolerance as
    bq_batch_extract.load_tables_config). Defaults are merged so each
    entry carries its effective dataset/project.
    """
    import yaml

    blob = yaml.safe_load(Path(path).expanduser().read_text(encoding="utf-8"))
    if not isinstance(blob, dict):
        return []
    defaults = blob.get("defaults") or {}
    out: list[dict] = []
    for entry in blob.get("tables") or []:
        if isinstance(entry, str):
            entry = {"name": entry}
        if not isinstance(entry, dict) or not entry.get("name"):
            continue
        merged = {**defaults, **entry}
        out.append({
            "name": str(merged["name"]),
            "bq_dataset": str(merged.get("bq_dataset") or ""),
            "bq_project": str(merged.get("bq_project") or ""),
        })
    return out
