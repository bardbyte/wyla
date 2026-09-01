"""The assistant's tool registry (Synapse v2 §3–§5).

The Meridian toolkit is the §9.1 kit reused verbatim — descriptions
already written for a smart colleague, errors that already teach,
determinism already inside. This module adds what v2 brings: the
``python`` sandbox, and the artifact tools that put something in the
panel the user keeps.

The write surface grows by exactly the artifact tools; everything
else is still read-only against the compiled build, live execution
is still policy-gated inside run_sql's sandbox, and nothing here
touches truth (rule 3: the clerk is the only writer).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sahs.loop.tools import (LoopState, ToolSpec, render_tool_block,
                             toolkit as meridian_toolkit)
from sahs.tools.api import Build

from .artifacts import TYPES, validate_artifact
from .sandbox import run_python, save_rows
from .store import AssistantStore


@dataclass
class AssistantState(LoopState):
    """The loop state plus what an assistant turn accumulates: saved
    query results, artifacts touched, and (from §13.2 on) the check
    facts that let a composed number shed its watermark."""

    queries_saved: int = 0
    artifacts_touched: list[str] = field(default_factory=list)
    facts: set[str] = field(default_factory=set)
    facts_log: list[dict[str, Any]] = field(default_factory=list)
    skills_loaded: list[str] = field(default_factory=list)


def assistant_toolkit(build: Build, state: AssistantState, *,
                      store: AssistantStore, session_id: str,
                      turn_id: str, workspace: Path,
                      model: Any = None,
                      substrate: Any = None,
                      snapshot_runner: Any = None,
                      ledger_path: Path | None = None,
                      scout: Any = None,
                      graph_root: Path | None = None
                      ) -> dict[str, ToolSpec]:
    kit = meridian_toolkit(build, state, substrate=substrate,
                           snapshot_runner=snapshot_runner,
                           ledger_path=ledger_path, scout=scout)

    # ── run_sql learns to hand rows to the sandbox ───────────
    base_run_sql = kit["run_sql"]

    def run_sql(sql: str, mode: str = "dry_run",
                limit: int = 200) -> dict[str, Any]:
        result = base_run_sql.fn(sql, mode=mode, limit=limit)
        if isinstance(result, dict) and result.get("rows"):
            state.queries_saved += 1
            name = f"q{state.queries_saved}"
            save_rows(workspace, name, result["rows"])
            result["saved_as"] = name
            result["hint"] = (f"rows saved for python as "
                             f"meridian.rows({name!r})")
        return result

    kit["run_sql"] = ToolSpec(
        name="run_sql", signature=base_run_sql.signature,
        maps_to=base_run_sql.maps_to,
        description=base_run_sql.description
        + "\nRow results are saved to the workspace: the python tool "
          "reads them as meridian.rows(\"q<N>\").",
        fn=run_sql)

    # ── python: where the analysis happens ───────────────────
    def python(code: str) -> dict[str, Any]:
        return run_python(code, workspace)

    # ── artifacts: what the user keeps ───────────────────────
    def artifact(type: str, title: str,
                 spec: dict[str, Any]) -> dict[str, Any]:
        normalized, problems = validate_artifact(
            type, spec, build_id=build.version,
            facts=frozenset(state.facts), build=build)
        if problems:
            return {"error": "artifact refused",
                    "problems": problems,
                    "hint": "fix exactly these and call artifact "
                            "again; the renderer never shows an "
                            "undisclosed number"}
        if not (title or "").strip():
            return {"error": "an artifact needs a title",
                    "hint": "name it the way the user would look "
                            "for it later"}
        row = store.add_artifact(session_id, turn_id=turn_id,
                                 type=type, title=title.strip(),
                                 spec=normalized)
        state.artifacts_touched.append(row["artifact_id"])
        return {"ok": True, "artifact_id": row["artifact_id"],
                "version": 1, "type": type, "title": row["title"],
                "watermark": normalized.get("watermark", ""),
                "_artifact": row}

    def artifact_update(artifact_id: str, spec: dict[str, Any],
                        title: str = "") -> dict[str, Any]:
        current = store.get_artifact(artifact_id)
        if current is None or current["session_id"] != session_id:
            mine = [a["artifact_id"]
                    for a in store.list_artifacts(session_id)]
            return {"error": f"no artifact {artifact_id!r} in this "
                             "session",
                    "hint": "this session's artifacts: "
                            + (", ".join(mine) or "none yet — "
                               "create one with artifact()")}
        normalized, problems = validate_artifact(
            current["type"], spec, build_id=build.version,
            facts=frozenset(state.facts), build=build)
        if problems:
            return {"error": "artifact refused", "problems": problems,
                    "hint": "the previous version stands; fix these "
                            "and call artifact_update again"}
        row = store.update_artifact(artifact_id, turn_id=turn_id,
                                    spec=normalized,
                                    title=title.strip() or None)
        state.artifacts_touched.append(artifact_id)
        return {"ok": True, "artifact_id": artifact_id,
                "version": row["version"], "type": row["type"],
                "title": row["title"],
                "watermark": normalized.get("watermark", ""),
                "_artifact": row}

    def list_artifacts() -> dict[str, Any]:
        rows = store.list_artifacts(session_id)
        return {"artifacts": [
            {"artifact_id": r["artifact_id"], "type": r["type"],
             "title": r["title"], "version": r["version"]}
            for r in rows], "count": len(rows)}

    # ── skills: doctrine on demand (§7/§13.3) ────────────────
    from .skills_loader import all_skills, get_skill

    def list_skills() -> dict[str, Any]:
        packs = all_skills(graph_root)
        return {"skills": [{"name": p.name, "title": p.title,
                            "description": p.description,
                            "origin": p.origin} for p in packs],
                "count": len(packs)}

    def load_skill(name: str) -> dict[str, Any]:
        name = str(name).strip()
        if name in state.skills_loaded:
            return {"ok": True, "name": name,
                    "note": f"skill {name!r} is already loaded this "
                            "turn — its text is in your steps above"}
        pack = get_skill(graph_root, name)
        if pack is None:
            names = ", ".join(
                f"{p.name} ({p.origin})"
                for p in all_skills(graph_root)) or "none"
            return {"error": f"no skill named {name!r}",
                    "hint": f"available: {names}"}
        state.skills_loaded.append(name)
        return {"ok": True, "name": pack.name, "title": pack.title,
                "origin": pack.origin, "text": pack.text}

    from . import checks as _checks

    def subgraph(ids: list[str] | None = None) -> dict[str, Any]:
        return _checks.build_subgraph(build, state, ids)

    def check_reconcile(sql: str, metric: str) -> dict[str, Any]:
        return _checks.check_reconcile(state, build, sql=sql,
                                       metric=metric)

    def check_part_whole(breakdown: str, total: str, column: str = "",
                         ) -> dict[str, Any]:
        return _checks.check_part_whole(state, workspace,
                                        breakdown=breakdown,
                                        total=total, column=column)

    def check_crosscheck(a: str, b: str,
                         column: str = "") -> dict[str, Any]:
        return _checks.check_crosscheck(state, workspace, a=a, b=b,
                                        column=column)

    def check_coverage(result: str) -> dict[str, Any]:
        return _checks.check_coverage(state, workspace, result=result)

    def check_fanout(tables: list[str]) -> dict[str, Any]:
        return _checks.check_fanout(state, build, tables=tables)

    def verify_answer(sql: str, claim: str = "") -> dict[str, Any]:
        return _checks.verify_answer(state, build, model, sql=sql,
                                     claim=claim, substrate=substrate)

    kit.update({spec.name: spec for spec in (
        ToolSpec(
            name="subgraph",
            signature="subgraph(ids?)",
            maps_to="the receipts",
            description=(
                "The nodes and edges behind a set of facts — pass "
                "metric:/table:/concept: ids, or nothing for "
                "everything this turn touched.\n"
                "This is the citation record and the constellation's "
                "food: what the answer actually used, as data."),
            fn=subgraph),
        ToolSpec(
            name="check_reconcile",
            signature="check_reconcile(sql, metric)",
            maps_to="verification",
            description=(
                "Does composed SQL contain the certified expression "
                "of a governed metric, verbatim? Returns a citable "
                "FACT (structural method).\n"
                "For a NUMERIC reconcile, run both on the snapshot "
                "and check_crosscheck the saved results. A passing "
                "fact cited in provenance.facts lets a composed "
                "number shed its watermark."),
            fn=check_reconcile),
        ToolSpec(
            name="check_part_whole",
            signature="check_part_whole(breakdown, total, column?)",
            maps_to="verification",
            description=(
                "Do the slices add up to the total? Compares saved "
                "results (q<N>) within ±0.5%.\n"
                "The first check to run on any breakdown: a failure "
                "means double count, missing slice, or mismatched "
                "filters — say which before charting."),
            fn=check_part_whole),
        ToolSpec(
            name="check_crosscheck",
            signature="check_crosscheck(a, b, column?)",
            maps_to="verification",
            description=(
                "Do two independently-computed results agree? "
                "Compares saved results (q<N>) within ±0.5%.\n"
                "Two routes to the same number is the strongest "
                "check there is; a disagreement is a finding, not "
                "an embarrassment."),
            fn=check_crosscheck),
        ToolSpec(
            name="check_coverage",
            signature="check_coverage(result)",
            maps_to="verification",
            description=(
                "Did the query actually cover the ground: rows "
                "present, no null keys?\n"
                "Run it before trusting any aggregate — an empty or "
                "holed result charted confidently is the worst bug "
                "an analyst can ship."),
            fn=check_coverage),
        ToolSpec(
            name="check_fanout",
            signature="check_fanout(tables)",
            maps_to="verification",
            description=(
                "Is there a raw-safe join on record between these "
                "tables, so no row double-counts?\n"
                "Run before any join-based number; a candidate-tier "
                "edge is evidence of a relationship, not of safety."),
            fn=check_fanout),
        ToolSpec(
            name="verify_answer",
            signature="verify_answer(sql, claim?)",
            maps_to="the fresh-context verifier",
            description=(
                "The verifier as a tool: names are real, the query "
                "validates on the build, and (with a claim) a "
                "fresh-context judge confirms the words say only "
                "what the SQL supports.\n"
                "Call it before showing a governed number; the "
                "passing fact is citable in provenance.facts."),
            fn=verify_answer),
    )})

    kit.update({spec.name: spec for spec in (
        ToolSpec(
            name="python",
            signature="python(code)",
            maps_to="the analysis tool",
            description=(
                "Run Python in the session workspace: numpy and the "
                "meridian module (build indexes + saved query rows) "
                "are importable; files persist between calls.\n"
                "This is where decomposition, variance, cohorts, and "
                "checks actually happen. No credentials, no network "
                "promises, 30s cap. print() what matters — stdout is "
                "what you get back."),
            fn=python),
        ToolSpec(
            name="artifact",
            signature="artifact(type, title, spec)",
            maps_to="the panel",
            description=(
                "Create a versioned artifact the user keeps: "
                + " | ".join(TYPES) + ".\n"
                "chart: {kind: line|bar|scatter|area, series: "
                "[{name, points: [[x, y], …]}], unit?}. table: "
                "{columns: [{key, label}], rows: [{…}]}. document: "
                "{markdown}. Any spec that shows numbers MUST carry "
                "provenance {status, meridian_line} — the renderer "
                "refuses undisclosed numbers, and composed numbers "
                "keep an EXPLORATORY watermark until a check fact is "
                "cited."),
            fn=artifact, writes=True),
        ToolSpec(
            name="artifact_update",
            signature="artifact_update(artifact_id, spec, title?)",
            maps_to="the panel",
            description=(
                "Revise an existing artifact in place: a new VERSION "
                "is stored, the old one stays reachable.\n"
                "Send the complete new spec (same shape and same "
                "rules as artifact); use it to iterate — \"make the "
                "second chart a cohort view\" is an update, not a "
                "new artifact."),
            fn=artifact_update, writes=True),
        ToolSpec(
            name="list_artifacts",
            signature="list_artifacts()",
            maps_to="the panel",
            description=(
                "What this session has already produced: id, type, "
                "title, latest version. Update rather than duplicate "
                "when the user is iterating on the same thing."),
            fn=list_artifacts),
        ToolSpec(
            name="list_skills",
            signature="list_skills()",
            maps_to="the skill shelf",
            description=(
                "The doctrine packs on the shelf — built-in method "
                "notes and the analyst's own briefings — as names "
                "with one-line descriptions and origins.\n"
                "Cheap to call; load_skill pulls one in."),
            fn=list_skills),
        ToolSpec(
            name="load_skill",
            signature="load_skill(name)",
            maps_to="the skill shelf",
            description=(
                "Pull one pack's full text into this turn — the "
                "result IS the doctrine, follow it while it applies.\n"
                "Load when the task matches the description: a \"why "
                "did it change\" wants analysis-playbooks; writing "
                "SQL against unfamiliar ground wants meridian-sql. "
                "Packs steer where you look; they never add tables, "
                "metrics, or numbers to the world."),
            fn=load_skill),
    )})
    return kit


__all__ = ["AssistantState", "assistant_toolkit", "render_tool_block"]
