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
# the static parts the assembled prompt is made of, each its own
# registered prompt so a part can be diffed on its own; the assembled
# template above is what a generation links to
PART_PREFIX = "wyla-assistant"
PRODUCTION_LABEL = "production"


def _section(tag: str, body: str) -> str:
    return f"<{tag}>\n{body.strip()}\n</{tag}>"


def assistant_template() -> str:
    from sahs.assistant.loop import CHAIN, IDENTITY
    return "\n\n".join([
        _section("identity", IDENTITY), _section("chain", CHAIN),
        _section("mode", "{{mode}}"), _section("style", "{{style}}"),
        _section("graph", "{{graph}}"),
        _section("skills", "{{skills}}"), _section("memory", "{{memory}}"),
        _section("session", "{{session}}")])


def loop_template() -> str:
    from sahs.loop.prompt import (DOCTRINE, IDENTITY, PROTOCOL,
                                  STOP_CONDITIONS, TONE, TRACES)
    return "\n".join([IDENTITY, "", "{{digest}}", "", DOCTRINE, "",
                      "{{skills}}", "", STOP_CONDITIONS, "", TRACES, "",
                      TONE, "", PROTOCOL + "{{tools}}"])


def content_version(text: str) -> str:
    """A version string for a one-shot with no version constant of
    its own: the text's own hash, so a change is a new version and
    unchanged text is a no-op (drift cannot happen by construction)."""
    import hashlib
    return "text-" + hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]


def part_registry() -> list[dict[str, Any]]:
    """The static parts: identity, chain, one blurb per mode, one
    style section per engine family, the planner's one-shot, the
    judge's and the reviewer's. Labels: ``production`` on all, the
    engine family on a family's style."""
    from sahs.ask.verify import JUDGE_SYSTEM
    from sahs.assistant.loop import ASSISTANT_VERSION, CHAIN, IDENTITY, MODES
    from sahs.assistant.planner import PLAN_SYSTEM
    from sahs.assistant.reviews import REVIEW_SYSTEM
    from sahs.util.profiles import STYLES
    rows: list[dict[str, Any]] = [
        {"name": f"{PART_PREFIX}-identity", "version": ASSISTANT_VERSION,
         "prompt": IDENTITY, "labels": [PRODUCTION_LABEL]},
        {"name": f"{PART_PREFIX}-chain", "version": ASSISTANT_VERSION,
         "prompt": CHAIN, "labels": [PRODUCTION_LABEL]},
    ]
    for mode, blurb in MODES.items():
        rows.append({"name": f"{PART_PREFIX}-mode-{mode}",
                     "version": ASSISTANT_VERSION, "prompt": blurb,
                     "labels": [PRODUCTION_LABEL]})
    for family, style in STYLES.items():
        rows.append({"name": f"{PART_PREFIX}-style-{label_for(family)}",
                     "version": ASSISTANT_VERSION, "prompt": style,
                     "labels": [PRODUCTION_LABEL, label_for(family)]})
    for name, text in (("wyla-planner-system", PLAN_SYSTEM),
                       ("wyla-judge-system", JUDGE_SYSTEM),
                       ("wyla-review-system", REVIEW_SYSTEM)):
        rows.append({"name": name, "version": content_version(text),
                     "prompt": text, "labels": [PRODUCTION_LABEL]})
    return rows


def registry(parts: bool = True) -> list[dict[str, Any]]:
    from sahs.assistant.loop import ASSISTANT_VERSION
    from sahs.loop.prompt import PROMPT_VERSION
    rows: list[dict[str, Any]] = [
        {"name": ASSISTANT_PROMPT, "version": ASSISTANT_VERSION,
         "prompt": assistant_template(), "labels": [PRODUCTION_LABEL]},
        {"name": LOOP_PROMPT, "version": PROMPT_VERSION,
         "prompt": loop_template(), "labels": [PRODUCTION_LABEL]},
    ]
    return rows + (part_registry() if parts else [])


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
            labels = [label] + list(entry.get("labels") or []) \
                + ([f"git-{sha}"] if sha else [])
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


__all__ = ["ASSISTANT_PROMPT", "LOOP_PROMPT", "PART_PREFIX",
           "PRODUCTION_LABEL", "PromptLinks", "assistant_template",
           "content_version", "label_for", "links_path", "load_links",
           "loop_template", "part_registry", "register_prompts", "registry"]
