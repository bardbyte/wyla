"""Prompt versions as Langfuse labels — registered, never fetched.

The system prompts stay assembled from code (byte-identical per
build, routing key pinned by tests). What Langfuse gets is the
template of each one, under its version string as a label, so the
dashboard can slice generations by prompt version. The dynamic
sections (the graph digest, skills, memory, the session block) are
``{{variables}}`` in the registered text; the static prose is the
prose the model runs under.

Registration writes a small local file mapping each version string
to the numeric version Langfuse assigned; the tracer reads that file
to link generations to the prompt. A version whose text changed
without its version string moving is reported as drift — that is
exactly the discipline the trajectory ritual asks for.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

ASSISTANT_PROMPT = "wyla-assistant-system"
LOOP_PROMPT = "wyla-loop-system"


def _section(tag: str, body: str) -> str:
    return f"<{tag}>\n{body.strip()}\n</{tag}>"


def assistant_template() -> str:
    from sahs.assistant.loop import CHAIN, IDENTITY
    return "\n\n".join([
        _section("identity", IDENTITY), _section("chain", CHAIN),
        _section("mode", "{{mode}}"), _section("graph", "{{graph}}"),
        _section("skills", "{{skills}}"), _section("memory", "{{memory}}"),
        _section("session", "{{session}}")])


def loop_template() -> str:
    from sahs.loop.prompt import (DOCTRINE, IDENTITY, PROTOCOL,
                                  STOP_CONDITIONS, TONE, TRACES)
    return "\n".join([IDENTITY, "", "{{digest}}", "", DOCTRINE, "",
                      "{{skills}}", "", STOP_CONDITIONS, "", TRACES, "",
                      TONE, "", PROTOCOL + "{{tools}}"])


def registry() -> list[dict[str, str]]:
    from sahs.assistant.loop import ASSISTANT_VERSION
    from sahs.loop.prompt import PROMPT_VERSION
    return [
        {"name": ASSISTANT_PROMPT, "version": ASSISTANT_VERSION,
         "prompt": assistant_template()},
        {"name": LOOP_PROMPT, "version": PROMPT_VERSION,
         "prompt": loop_template()},
    ]


def label_for(version: str) -> str:
    """A Langfuse label: lower case, [a-z0-9_.-] only."""
    return re.sub(r"[^a-z0-9_.-]+", "-", version.lower()).strip("-")


def git_sha(root: Path | None = None) -> str:
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                             cwd=str(root) if root else None,
                             capture_output=True, text=True, timeout=5)
        return out.stdout.strip() if out.returncode == 0 else ""
    except (OSError, subprocess.SubprocessError):
        return ""


def links_path(graph_root: Path) -> Path:
    return Path(graph_root) / "langfuse" / "prompts.json"


def load_links(path: Path) -> dict[str, dict[str, int]]:
    path = Path(path)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_links(path: Path, links: dict[str, dict[str, int]]) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(links, indent=1, sort_keys=True) + "\n",
                    encoding="utf-8")
    return path


def register_prompts(client: Any, out: Path,
                     root: Path | None = None) -> list[dict[str, Any]]:
    """Create each prompt version in Langfuse unless the same text is
    already under that label; record the numeric version locally."""
    sha = git_sha(root)
    links = load_links(out)
    rows: list[dict[str, Any]] = []
    for entry in registry():
        label = label_for(entry["version"])
        existing = None
        try:
            existing = client.api.prompts.get(entry["name"], label=label)
        except Exception:                             # noqa: BLE001
            existing = None                           # not registered yet
        drift = existing is not None and existing.prompt != entry["prompt"]
        if existing is not None and not drift:
            version, created = existing.version, False
        else:
            labels = [label] + ([f"git-{sha}"] if sha else [])
            made = client.create_prompt(
                name=entry["name"], prompt=entry["prompt"], labels=labels,
                type="text",
                config={"version": entry["version"], "git": sha},
                commit_message=(f"{entry['version']} @ {sha}" if sha
                                else entry["version"]))
            version, created = made.version, True
        links.setdefault(entry["name"], {})[entry["version"]] = int(version)
        rows.append({"name": entry["name"], "version": entry["version"],
                     "label": label, "langfuse_version": int(version),
                     "created": created, "drift": drift})
    save_links(out, links)
    return rows


class PromptLinks:
    """version string → (prompt name, Langfuse version), read from the
    links file and re-read when it changes. Unknown → None."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._stamp: float | None = None
        self._by_version: dict[str, tuple[str, int]] = {}

    def _refresh(self) -> None:
        try:
            stamp = self.path.stat().st_mtime
        except OSError:
            self._by_version, self._stamp = {}, None
            return
        if stamp == self._stamp:
            return
        self._by_version = {
            version: (name, int(number))
            for name, versions in load_links(self.path).items()
            for version, number in versions.items()}
        self._stamp = stamp

    def __call__(self, version: str) -> tuple[str, int] | None:
        self._refresh()
        return self._by_version.get(version)


__all__ = ["ASSISTANT_PROMPT", "LOOP_PROMPT", "PromptLinks",
           "assistant_template", "label_for", "links_path", "load_links",
           "loop_template", "register_prompts", "registry"]
