"""The v3 kit (Synapse v3 §2): eleven tools, sharp and non-overlapping,
declared in the native tool protocol — plus ``suggest_next``, the way
a model with no JSON wrapper offers follow-ups.

Every tool is a thin door over the deterministic implementations the
silo already has (the v1 kit, the checks, the artifact validator, the
sandbox, the skills loader, the store). The model reasons; the tools
never guess; the hooks (hooks.py) hold the must-haves.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sahs.loop.tools import ROW_CAP, ToolSpec, toolkit as v1_toolkit
from sahs.tools.api import Build
from sahs.tools.sandbox import DEFAULT_MAX_BYTES, execute_sandboxed
from sahs.tools.validate_sql import validate_sql

from . import checks as _checks
from .artifacts import TYPES, validate_artifact
from .hooks import literal_warnings
from .sandbox import run_python, save_rows
from .skills_loader import all_skills, get_skill
from .state import AssistantState
from .store import AssistantStore

RESULT_CAP = 20_000        # chars of one tool result the model sees


def _s(description: str, **extra: Any) -> dict[str, Any]:
    return {"type": "STRING", "description": description, **extra}


def _i(description: str) -> dict[str, Any]:
    return {"type": "INTEGER", "description": description}


def _arr(description: str, items: dict[str, Any] | None = None
         ) -> dict[str, Any]:
    return {"type": "ARRAY", "description": description,
            "items": items or {"type": "STRING"}}


def _obj(properties: dict[str, Any], required: list[str] | None = None
         ) -> dict[str, Any]:
    out: dict[str, Any] = {"type": "OBJECT", "properties": properties}
    if required:
        out["required"] = required
    return out


def build_kit(build: Build, state: AssistantState, *,
              store: AssistantStore, session_id: str, turn_id: str,
              workspace: Path, model: Any = None, substrate: Any = None,
              snapshot_runner: Any = None, runner: Any = None,
              graph_root: Path | None = None,
              project_id: str = "") -> dict[str, ToolSpec]:
    v1 = v1_toolkit(build, state, substrate=substrate,
                    snapshot_runner=snapshot_runner)
    base = {name: v1[name].fn for name in (
        "search_semantics", "grep_cards", "list_metrics", "read_card",
        "get_definition_line", "sample_values", "run_sql", "ask_user",
        "note")}

    # ── search: one door ─────────────────────────────────────
    def search(query: str, kind: str = "all") -> dict[str, Any]:
        if kind == "list":
            return base["list_metrics"](query)
        if kind == "exact":
            return base["grep_cards"](query)
        lane = kind if kind in ("metrics", "concepts", "joins",
                                "vocab", "values") else "all"
        result = base["search_semantics"](query, lane)
        if isinstance(result, dict) and not result.get("results"):
            exact = base["grep_cards"](query)
            hits = exact.get("hits") if isinstance(exact, dict) else None
            if hits:
                result["exact"] = hits[:8]
                result["hint"] = ("nothing ranked by meaning, but the "
                                  "token appears in these cards — "
                                  "read one")
        return result

    # ── read: a card whole, or the graph behind ids ──────────
    def read(id: str = "", section: str = "",
             graph_ids: list[str] | None = None) -> dict[str, Any]:
        if graph_ids is not None or not id:
            return _checks.build_subgraph(build, state,
                                          list(graph_ids or []) or None)
        result = base["read_card"](id, section)
        if (isinstance(result, dict) and not section
                and str(result.get("card", "")).startswith("metrics/")):
            line = base["get_definition_line"](
                result["card"].split("/", 1)[1])
            if isinstance(line, dict) and line.get("definition_line"):
                result["definition_line"] = line["definition_line"]
        return result

    def sample_values(table: str, column: str,
                      n: int = 20) -> dict[str, Any]:
        return base["sample_values"](table, column, n)

    # ── run_sql: gates before, literal check + rows after ────
    def _rows_as_dicts(data: dict[str, Any]) -> list[dict[str, Any]]:
        rows = data.get("rows") or []
        if rows and isinstance(rows[0], (list, tuple)):
            names = data.get("columns") or [
                c.get("name") for c in (data.get("schema") or [])
                if isinstance(c, dict)]
            rows = [dict(zip(names, r)) for r in rows]
        return rows

    def _run_live(sql: str, limit: int) -> dict[str, Any]:
        """Rows from the warehouse under two limits the model cannot
        lift: the scan ceiling (SAHS_LIVE_MAX_BYTES) and the row cap.
        Default-deny stays (SAHS_ALLOW_LIVE=1 on the laptop); every
        refusal comes back taught — cost is the model's to narrow,
        access is configuration to report."""
        import os
        from sahs.util.auth import load_dotenv
        load_dotenv()
        limit = max(1, min(int(limit or 200), ROW_CAP))
        verdict = validate_sql(build, sql)
        if not verdict["ok"]:
            return {"error": "sql_invalid",
                    "hint": "each violation names its correction; fix "
                            "and run again",
                    "violations": verdict["violations"],
                    "warnings": verdict["warnings"],
                    "kind": "sql", "yours_to_fix": True}
        sandboxed = execute_sandboxed(build, sql, mode="live",
                                      limit=limit, substrate=substrate,
                                      runner=runner)
        meta = sandboxed.get("meta") or {}
        if sandboxed["status"] != "ok":
            taught = meta.get("taught") or {}
            out = {"error": sandboxed.get("error") or "the sandbox "
                                                     "refused",
                   "hint": taught.get("hint")
                   or "the sandbox's reason stands: run never "
                      "bypasses the gates"}
            out.update({k: v for k, v in taught.items()
                        if k in ("kind", "yours_to_fix", "closest",
                                 "fix_env")})
            return out
        data = sandboxed["data"] or {}
        rows = _rows_as_dicts(data)[:limit]
        ceiling = int(os.environ.get("SAHS_LIVE_MAX_BYTES",
                                     DEFAULT_MAX_BYTES))
        scanned = int(meta.get("bytes_scanned") or 0)
        out = {"mode": "run", "rows": rows, "row_count": len(rows),
               "result_schema": data.get("schema"),
               "bytes_processed": scanned, "limit": limit,
               "capped": len(rows) >= limit,
               "scan_ceiling_bytes": ceiling,
               "warnings": verdict["warnings"],
               "note": f"{len(rows)} rows under LIMIT {limit}; scanned "
                       f"{scanned:,} bytes of a {ceiling:,}-byte "
                       "ceiling"}
        if meta.get("sql_sent"):
            out["sql_sent"] = meta["sql_sent"]
        return out

    def run_sql(sql: str, mode: str = "dry_run",
                limit: int = 200) -> dict[str, Any]:
        if mode == "run":
            result = _run_live(sql, limit)
        else:
            result = base["run_sql"](sql, mode=mode, limit=limit)
        if isinstance(result, dict):
            warnings = literal_warnings(build, sql)
            if warnings:
                result.setdefault("warnings", []).extend(warnings)
            if result.get("rows"):
                state.queries_saved += 1
                name = f"q{state.queries_saved}"
                save_rows(workspace, name, result["rows"])
                result["saved_as"] = name
                result["hint"] = (f"rows saved: python reads them as "
                                  f"meridian.rows({name!r}); check "
                                  f"compares saved results by name")
        return result

    def python(code: str) -> dict[str, Any]:
        return run_python(code, workspace)

    # ── check: one tool, six kinds, citable facts ────────────
    def check(kind: str, breakdown: str = "", total: str = "",
              a: str = "", b: str = "", column: str = "",
              result: str = "", tables: list[str] | None = None,
              sql: str = "", metric: str = "",
              claim: str = "") -> dict[str, Any]:
        kinds = {
            "part_whole": lambda: _checks.check_part_whole(
                state, workspace, breakdown=breakdown, total=total,
                column=column),
            "crosscheck": lambda: _checks.check_crosscheck(
                state, workspace, a=a, b=b, column=column),
            "coverage": lambda: _checks.check_coverage(
                state, workspace, result=result),
            "fanout": lambda: _checks.check_fanout(
                state, build, tables=list(tables or [])),
            "reconcile": lambda: _checks.check_reconcile(
                state, build, sql=sql, metric=metric),
            "answer": lambda: _checks.verify_answer(
                state, build, model, sql=sql, claim=claim,
                substrate=substrate),
        }
        if kind not in kinds:
            return {"error": f"unknown check kind {kind!r}",
                    "hint": "kinds: " + " | ".join(kinds)}
        return kinds[kind]()

    # ── artifact: create or version, one door ────────────────
    def artifact(type: str, title: str, spec_json: str,
                 artifact_id: str = "") -> dict[str, Any]:
        spec: Any = spec_json
        if isinstance(spec_json, str):
            try:
                spec = json.loads(spec_json)
            except ValueError as e:
                return {"error": f"spec_json is not valid JSON: {e}",
                        "hint": "one JSON object, the shape for this "
                                "type is in the tool description"}
        if (type == "diagram" and isinstance(spec, dict)
                and "from_subgraph" in spec):
            graph = _checks.build_subgraph(
                build, state, list(spec.get("from_subgraph") or [])
                or None)
            spec = {"kind": "graph", "nodes": graph.get("nodes", []),
                    "edges": graph.get("edges", []),
                    **({"caption": spec["caption"]}
                       if spec.get("caption") else {})}
        if artifact_id:
            current = store.get_artifact(artifact_id)
            if current is None or current["session_id"] != session_id:
                mine = [r["artifact_id"]
                        for r in store.list_artifacts(session_id)]
                return {"error": f"no artifact {artifact_id!r} in this "
                                 "session",
                        "hint": "this session's artifacts: "
                                + (", ".join(mine) or "none yet")}
            type = current["type"]
        normalized, problems = validate_artifact(
            type, spec, build_id=build.version,
            facts=frozenset(state.facts), build=build)
        if problems:
            return {"error": "artifact refused", "problems": problems,
                    "hint": "fix exactly these and call artifact again; "
                            "the renderer never shows an undisclosed "
                            "number"}
        if not artifact_id and not (title or "").strip():
            return {"error": "an artifact needs a title",
                    "hint": "name it the way the user would look for "
                            "it later"}
        if artifact_id:
            row = store.update_artifact(artifact_id, turn_id=turn_id,
                                        spec=normalized,
                                        title=(title or "").strip()
                                        or None)
        else:
            row = store.add_artifact(session_id, turn_id=turn_id,
                                     type=type, title=title.strip(),
                                     spec=normalized)
        state.artifacts_touched.append(row["artifact_id"])
        return {"ok": True, "artifact_id": row["artifact_id"],
                "version": row["version"], "type": row["type"],
                "title": row["title"],
                "watermark": normalized.get("watermark", ""),
                "_artifact": row}

    # ── ask, skills, memory, notes, next steps ───────────────
    def ask(question: str,
            options: list[Any] | None = None) -> dict[str, Any]:
        return base["ask_user"](question, list(options or []))

    def load_skill(name: str) -> dict[str, Any]:
        name = str(name).strip()
        if name in state.skills_loaded:
            return {"ok": True, "name": name,
                    "note": f"{name!r} is already loaded this turn"}
        pack = get_skill(graph_root, name)
        if pack is None:
            names = ", ".join(f"{p.name} ({p.origin})"
                              for p in all_skills(graph_root)) or "none"
            return {"error": f"no skill named {name!r}",
                    "hint": f"available: {names}"}
        state.skills_loaded.append(name)
        return {"ok": True, "name": pack.name, "title": pack.title,
                "origin": pack.origin, "text": pack.text}

    def remember(text: str, scope: str = "global") -> dict[str, Any]:
        text = str(text).strip()
        if not text:
            return {"error": "nothing to remember",
                    "hint": "one preference or disambiguation"}
        if scope == "project":
            if not project_id:
                return {"error": "this chat is in no project",
                        "hint": "use scope global"}
            scope = f"project:{project_id}"
        elif scope != "global":
            return {"error": f"unknown scope {scope!r}",
                    "hint": "global or project"}
        row = store.add_memory(text, scope=scope)
        return {"ok": True, "memory_id": row["id"], "scope": row["scope"],
                "text": row["text"],
                "note": "remembered — disclosed to the user inline with "
                        "an undo, in the memory panel, and in every "
                        "prompt it scopes to; they can retire it"}

    def note(text: str) -> dict[str, Any]:
        return base["note"](text)

    def suggest_next(options: list[str]) -> dict[str, Any]:
        chips = [str(o).strip()[:80] for o in (options or [])
                 if str(o).strip()][:3]
        state.chips = chips
        return {"ok": True, "chips": chips}

    specs = [
        ToolSpec(
            name="search", signature="search(query, kind?)",
            maps_to="the one search door",
            description=(
                "Find things in the governed graph by meaning: metrics, "
                "concepts, joins, vocabulary — and BUSINESS AREAS, which "
                "rank first when a query names one. kind=list gives "
                "the catalog for a business area, a status, or label "
                "words (\"all GMNS metrics\" → search(\"GMNS\", "
                "kind=\"list\")); kind=exact greps card text for a "
                "token; kind=vocab expands an acronym with its business "
                "unit and region (the same symbol can mean several "
                "things); kind=values turns a business phrase (\"KYC "
                "done\", \"Approved\") or a stored value written as "
                "stored (\"GB\", \"ACTIVE\") into the column, the code, "
                "its share of rows, and the predicate to filter with."),
            fn=search, schema=_obj({
                "query": _s("what you are looking for, in the user's "
                            "words or graph words"),
                "kind": _s("all (default) | metrics | concepts | joins "
                           "| vocab | values | list | exact")},
                ["query"])),
        ToolSpec(
            name="read", signature="read(id, section?, graph_ids?)",
            maps_to="the card reader",
            description=(
                "Read a card whole — metric:<id>, table:<name>, "
                "concept:<name> — before using anything on it. A "
                "metric card includes its definition line, the "
                "one-sentence disclosure every number carries; a table "
                "card includes its joins on record. With graph_ids "
                "(or nothing) it returns the subgraph behind those "
                "ids, or behind everything this turn touched."),
            fn=read, schema=_obj({
                "id": _s("the card id"),
                "section": _s("one section only, when the card is "
                              "long"),
                "graph_ids": _arr("metric:/table:/concept: ids to draw "
                                  "the subgraph for")})),
        ToolSpec(
            name="sample_values",
            signature="sample_values(table, column, n?)",
            maps_to="observed domain",
            description=(
                "The observed values of a column, never live. Call "
                "before writing any filter literal — a literal not "
                "among them comes back as a warning on run_sql."),
            fn=sample_values, schema=_obj({
                "table": _s("physical table"), "column": _s("column"),
                "n": _i("how many, default 20")}, ["table", "column"])),
        ToolSpec(
            name="run_sql", signature="run_sql(sql, mode?, limit?)",
            maps_to="the governed query",
            description=(
                "Validate the query, then either price it (mode "
                "dry_run, the default: shape and bytes, no rows) or "
                "run it for rows (mode run) under two limits you "
                "cannot lift: a scan ceiling in bytes and a row cap "
                "(limit, default 200, at most 1000). Rows save as q<N> "
                "for python and check. Name tables as the cards do "
                "(dw.table): the warehouse project and location are "
                "added for you. Refused for cost → narrow the scan (a "
                "partition filter on the table's date column, fewer "
                "columns) and run again; refused as disabled or "
                "restricted → that is configuration: say so and stop. "
                "Errors teach: an unknown column names the three "
                "closest real ones; an unobserved literal comes back "
                "as a warning."),
            fn=run_sql, writes=False, schema=_obj({
                "sql": _s("the query"),
                "mode": _s("dry_run (default) | run"),
                "limit": _i("row cap for run, default 200, at most "
                            "1000")}, ["sql"])),
        ToolSpec(
            name="python", signature="python(code)",
            maps_to="the analysis tool",
            description=(
                "Run Python in the session workspace: numpy and the "
                "meridian module (build indexes, saved query rows via "
                "meridian.rows('q1')) are importable; files persist. "
                "Decomposition, variance, cohorts, aligning two "
                "results side by side — this is where it happens. "
                "print() what matters."),
            fn=python, schema=_obj({"code": _s("the code")}, ["code"])),
        ToolSpec(
            name="check",
            signature="check(kind, …)", maps_to="verification",
            description=(
                "A citable FACT about the numbers. kind=part_whole "
                "(breakdown, total): do the slices add up. crosscheck "
                "(a, b): do two routes agree. coverage (result): rows "
                "present, no null keys. fanout (tables): is there a "
                "raw-safe join. reconcile (sql, metric): does the SQL "
                "contain the certified expression. answer (sql, "
                "claim): the fresh-context verifier. A passing "
                "fact_id cited in provenance.facts lets a composed "
                "number shed its watermark."),
            fn=check, schema=_obj({
                "kind": _s("part_whole | crosscheck | coverage | "
                           "fanout | reconcile | answer"),
                "breakdown": _s("saved result name, e.g. q1"),
                "total": _s("saved result name"),
                "a": _s("saved result name"), "b": _s("saved result name"),
                "column": _s("the numeric column to compare"),
                "result": _s("saved result name"),
                "tables": _arr("tables the join touches"),
                "sql": _s("the composed SQL"),
                "metric": _s("the governed metric id"),
                "claim": _s("the sentence the SQL is said to support")},
                ["kind"])),
        ToolSpec(
            name="artifact",
            signature="artifact(type, title, spec_json, artifact_id?)",
            maps_to="the panel",
            description=(
                "Put something in the panel the user keeps: "
                + " | ".join(TYPES) + ". spec_json is ONE JSON object. "
                "chart: {kind: line|bar|scatter|area, series: [{name, "
                "points: [[x, y], …]}], unit?}. table: {columns: "
                "[{key, label}], rows: [{…}]}. document: {markdown}. "
                "kpi: {value, unit?, label?, delta?}. dashboard: "
                "{panels: [{type, title?, spec}], filters?: [{slot, "
                "options}], notes?} — every numeric panel carries its "
                "OWN provenance. diagram: {kind: graph, nodes, edges} "
                "or {from_subgraph: [ids]} to draw what you used, or "
                "{kind: mermaid, source}. Any spec showing numbers "
                "MUST carry provenance {status, meridian_line, "
                "metric_id?, facts?}: the renderer refuses undisclosed "
                "numbers, and composed numbers keep an EXPLORATORY "
                "watermark until a passing check fact is cited. Pass "
                "artifact_id to publish a new version of an existing "
                "artifact instead of a duplicate."),
            fn=artifact, writes=True, schema=_obj({
                "type": _s(" | ".join(TYPES)),
                "title": _s("the name the user would look for later"),
                "spec_json": _s("the spec as a JSON object string"),
                "artifact_id": _s("an existing artifact to version")},
                ["type", "title", "spec_json"])),
        ToolSpec(
            name="ask", signature="ask(question, options)",
            maps_to="clarify", ends_turn=True, writes=True,
            description=(
                "Ask the user ONE question when the ask is markedly "
                "unclear and evidence cannot settle it — two to four "
                "named options with a reason each. The turn ends; the "
                "answer arrives as the next message. Do not ask what "
                "read or sample_values could tell you."),
            fn=ask, schema=_obj({
                "question": _s("the question, one sentence"),
                "options": _arr("the candidates", _obj({
                    "label": _s("what the user taps"),
                    "why": _s("the evidence for this option")},
                    ["label"]))}, ["question", "options"])),
        ToolSpec(
            name="load_skill", signature="load_skill(name)",
            maps_to="the skill shelf",
            description=(
                "Pull one doctrine pack into this turn — the result IS "
                "the doctrine. Loading is your job: when the task "
                "matches a pack in the shelf, load it before the work "
                "(\"why did it change\" → analysis-playbooks; unfamiliar "
                "SQL ground → lumi-data-connect; a deck or memo → "
                "executive-summary; tiles → dashboard-design)."),
            fn=load_skill, schema=_obj({"name": _s("pack name")},
                                       ["name"])),
        ToolSpec(
            name="remember", signature="remember(text, scope?)",
            maps_to="user memory", writes=True,
            description=(
                "Keep one preference or disambiguation the user "
                "settled, in their words (\"by spend they mean acquirer "
                "net spend\"). NEVER a metric definition or a number — "
                "those belong in the graph. Disclosed to the user, "
                "retirable by them."),
            fn=remember, schema=_obj({
                "text": _s("one sentence"),
                "scope": _s("global (default) | project")}, ["text"])),
        ToolSpec(
            name="note", signature="note(text)",
            maps_to="the notebook", writes=True,
            description=(
                "A working note that persists across turns: what you "
                "ruled out, what to compare next. Your later self reads "
                "it."),
            fn=note, schema=_obj({"text": _s("the note")}, ["text"])),
        ToolSpec(
            name="suggest_next", signature="suggest_next(options)",
            maps_to="follow-up chips",
            description=(
                "Offer up to three follow-ups the user might tap next, "
                "specific to what you just showed. Call it once, at the "
                "end, only when there is a natural next step."),
            fn=suggest_next, schema=_obj({
                "options": _arr("two or three short follow-ups")},
                ["options"])),
    ]
    return {spec.name: spec for spec in specs}


__all__ = ["build_kit", "RESULT_CAP", "AssistantState"]
