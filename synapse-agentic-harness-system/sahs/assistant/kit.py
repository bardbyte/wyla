"""The v3 kit (Synapse v3 §2): eleven tools, sharp and non-overlapping,
declared in the native tool protocol — plus ``suggest_next``, the way
a model with no JSON wrapper offers follow-ups, and, whenever a pack
over the whole-load ceiling is in reach, the three library tools
(``skill_toc``, ``skill_search``, ``skill_read``: the catalogue, the
lookup, the page — sahs.loop.skill_index).

Every tool is a thin door over the deterministic implementations the
silo already has (the v1 kit, the checks, the artifact validator, the
sandbox, the skills loader, the store). The model reasons; the tools
never guess; the hooks (hooks.py) hold the must-haves.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sahs.loop.skills import CHARS_VAR, library_reason
from sahs.loop.tools import ROW_CAP, ToolSpec, toolkit as v1_toolkit
from sahs.tools.api import Build
from sahs.tools.sandbox import (DEFAULT_MAX_BYTES, execute_sandboxed,
                                human_bytes, scan_ceiling)
from sahs.tools.validate_sql import validate_sql

from . import checks as _checks
from .artifacts import (CHART_KIND_ALIASES, CHART_KINDS, TYPES,
                        choose_for_series, validate_artifact)
from .hooks import literal_warnings
from .sandbox import run_python, save_rows
from .skills_loader import (TOC_TOOL_CAP, all_skills, get_skill,
                            preference_of, toc_lines,
                            whole_load_limit)
from .state import AssistantState
from .store import AssistantStore

RESULT_CAP = 20_000        # chars of one tool result the model sees
SKILL_READ_CHARS = 6000    # skill_read's default page
SKILL_SEARCH_K = 8         # skill_search's default hits


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
              project_id: str = "",
              owner: str = "",
              retriever: Any = None,
              searchable: list[str] | None = None,
              skill_limit: int | None = None,
              model_name: str = "") -> dict[str, ToolSpec]:
    """``retriever`` is the turn's SkillIndex (sahs.loop.skill_index),
    ``searchable`` the packs loaded as a library this turn, and
    ``skill_limit`` this turn's whole-load limit (default: the
    engine's, ``whole_load_limit(model_name)``); the three skill_*
    tools are declared when any pack the turn can reach is over the
    limit — every such pack is a library, whatever its frontmatter
    preferred."""
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

    # ── propose_sql: the handover — priced, disclosed, not run ─
    def propose_sql(sql: str, title: str, why: str = "",
                    metric_id: str = "") -> dict[str, Any]:
        """Hand a proved query to the person: priced by a dry run,
        carrying its status and meridian line. The turn ends and the
        card offers Run query / Run + dashboard; the rows come back
        as a table artifact and as q1 for the next step."""
        sql = str(sql or "").strip()
        title = str(title or "").strip()
        if not sql:
            return {"error": "propose_sql needs the SQL",
                    "hint": "the full SELECT you proved with a dry run"}
        if not title:
            return {"error": "a proposal needs a title",
                    "hint": "name it the way the user would look for "
                            "it later (\"Spend by day, last 30 days\")"}
        priced = base["run_sql"](sql, mode="dry_run", limit=200)
        if not isinstance(priced, dict) or priced.get("error"):
            out = dict(priced) if isinstance(priced, dict) \
                else {"error": str(priced)}
            out["hint"] = ((out.get("hint") or "").rstrip(". ")
                           + ". A query is handed over only once its "
                           "dry run passes: fix it and propose again"
                           ).lstrip(". ")
            return out
        warnings = list(priced.get("warnings") or [])
        warnings += [w for w in literal_warnings(build, sql)
                     if w not in warnings]
        # the ceiling is known now, not at Run: an over-ceiling query
        # comes back once to narrow (the partition filter the card's
        # grain line names); handed over the second time, the card
        # says Run will be refused unless it is narrowed or the
        # ceiling is raised
        from sahs.util.auth import load_dotenv
        load_dotenv()
        ceiling = scan_ceiling()
        scanned = int(priced.get("bytes_processed") or 0)
        over = scanned > ceiling
        if over and state.ceiling_refusals == 0:
            state.ceiling_refusals += 1
            return {"error": f"over_ceiling: this query would scan "
                             f"{human_bytes(scanned)}, over the "
                             f"{human_bytes(ceiling)} live ceiling: Run "
                             "would be refused for cost",
                    "hint": "narrow the scan before handing over: a "
                            "filter on the table's partition column (the "
                            "card's grain line names it), a tighter date "
                            "range, fewer columns; then propose again. If "
                            "it cannot be narrowed, propose it again as "
                            "is and the card will say so",
                    "kind": "cost", "yours_to_fix": True,
                    "bytes_processed": scanned,
                    "scan_ceiling_bytes": ceiling}
        status = "exploratory"
        line = ("An ad-hoc query, not on the meridian line: exploratory "
                "until a check stands behind it.")
        metric_id = str(metric_id or "").strip()
        if metric_id:
            got = base["get_definition_line"](metric_id)
            if isinstance(got, dict) and got.get("definition_line"):
                line = got["definition_line"]
                served = str(got.get("status_served")
                             or got.get("status") or "")
                status = "certified" if served == "certified" \
                    else "pending"
            else:
                metric_id = ""
        proposal = {
            "sql": priced.get("sql_sent") or sql, "sql_written": sql,
            "title": title, "why": str(why or "").strip()[:600],
            "bytes_processed": priced.get("bytes_processed"),
            "result_schema": priced.get("result_schema"),
            "warnings": warnings, "metric_id": metric_id,
            "status": status, "meridian_line": line,
            "scan_ceiling_bytes": ceiling, "over_ceiling": over}
        state.proposal = proposal
        note = ("handed over: the person runs it from the card (Run "
                "query, or Run + dashboard). Say in one or two sentences "
                "what it will show, then stop.")
        if over:
            note += (f" It would scan {human_bytes(scanned)}, over the "
                     f"{human_bytes(ceiling)} live ceiling: say so, and "
                     "that Run needs a narrower query or a higher "
                     "ceiling.")
        return {"ok": True, "ends_turn": True, "proposal": proposal,
                "note": note}

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
        # a chart with no kind: the heuristic picks one from the
        # series' shape and its one-sentence reason rides in the spec
        # (the card shows it); a kind the model names is kept, with
        # its own reason when it gave one
        if (type == "chart" and isinstance(spec, dict)
                and not str(spec.get("kind") or "").strip()):
            pick = choose_for_series(spec.get("series") or [],
                                     str(spec.get("intent") or title or ""))
            if pick["kind"] not in CHART_KINDS:
                return {"error": "artifact refused", "problems": [{
                    "code": "chart_kind",
                    "detail": "no kind, and these series do not read "
                              f"as a chart but as a {pick['kind']}",
                    "hint": pick["reason"] + " — build that instead, "
                            "or name a kind"}],
                    "hint": "the heuristic: time on x → line; one "
                            "category → bar; two measures → scatter; "
                            "parts → stacked bar; one number → kpi"}
            spec = {**spec, "kind": pick["kind"],
                    "reason": str(spec.get("reason") or pick["reason"]),
                    **({"sort": pick["sort"]}
                       if pick.get("sort") and not spec.get("sort")
                       else {})}
        elif type == "chart" and isinstance(spec, dict):
            raw = str(spec.get("kind") or "").lower().strip()
            spec = {**spec, "kind": CHART_KIND_ALIASES.get(raw, raw)}
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

    # ── the library: packs over the ceiling, read by the page ─
    library: list[str] = list(searchable or [])
    shelf_limit = skill_limit if skill_limit is not None \
        else whole_load_limit(model_name)

    def _library_pack(pack: Any) -> bool:
        """Over this turn's whole-load limit: a library, whatever its
        frontmatter preferred (the preference is disclosed, never a
        gate)."""
        return pack.chars > shelf_limit

    oversized = {p.name for p in all_skills(graph_root, owner)
                 if _library_pack(p)}
    index_box: list[Any] = [retriever]

    def _index() -> Any:
        if index_box[0] is None:
            from sahs.loop.skill_index import open_index
            index_box[0] = open_index(graph_root)
        return index_box[0]

    def _in_library(name: str) -> tuple[Any, dict[str, Any] | None]:
        """The pack, indexed, or the error the model reads."""
        name = str(name or "").strip()
        if not name:
            return None, {"error": "name the skill",
                          "hint": "the searchable packs: "
                                  + (", ".join(library) or "none loaded; "
                                     "load_skill(name) first")}
        pack = get_skill(graph_root, name, owner)
        if pack is None:
            return None, {"error": f"no skill named {name!r}",
                          "hint": "available: " + (", ".join(
                              f"{p.name} ({p.origin})"
                              for p in all_skills(graph_root, owner))
                              or "none")}
        if pack.error:
            return None, {"error": f"{name!r} cannot be read: {pack.error}",
                          "hint": "the file must be UTF-8 markdown; "
                                  "nothing of it loaded"}
        if pack.chars <= shelf_limit:
            return None, {"error": f"{name!r} is not a library pack: it "
                                   f"is {pack.chars:,} characters, under "
                                   f"the {shelf_limit:,} whole-load limit",
                          "hint": "load_skill(name) hands it over whole"}
        _index().ensure([pack])
        if name not in library:
            library.append(name)
        return pack, None

    def load_skill(name: str) -> dict[str, Any]:
        name = str(name).strip()
        if name in state.skills_loaded:
            return {"ok": True, "name": name,
                    "note": f"{name!r} is already loaded this turn"}
        pack = get_skill(graph_root, name, owner)
        if pack is None:
            names = ", ".join(f"{p.name} ({p.origin})"
                              for p in all_skills(graph_root, owner)) \
                or "none"
            return {"error": f"no skill named {name!r}",
                    "hint": f"available: {names}"}
        if pack.error:
            # broken input, the one refusal: by name, with the reason
            return {"error": f"{name!r} cannot be read: {pack.error}",
                    "hint": "the file must be UTF-8 markdown; nothing "
                            "of it loaded"}
        if pack.chars > shelf_limit:
            # on the shelf, over the limit: it loads as a library —
            # the contents now, the pages by skill_search / skill_read
            # — never as a cut of itself and never a refusal; a pack
            # whose frontmatter asked for the whole file says so here
            _pack, problem = _in_library(name)
            if problem:
                return problem
            state.skills_loaded.append(name)
            got = skill_toc(name)
            preferred, why = preference_of(pack)
            return {"ok": True, "name": pack.name, "title": pack.title,
                    "origin": pack.origin, "searchable": True,
                    "mode": "library", "preferred": preferred,
                    "reason": library_reason(pack, shelf_limit,
                                             model_name, why),
                    "chars": pack.chars, "sections": got.get("sections"),
                    "chunks": got.get("chunks"), "toc": got.get("toc"),
                    "toc_omitted": got.get("omitted", 0),
                    "note": f"{pack.chars:,} characters, over the "
                            f"{shelf_limit:,} whole-load ceiling "
                            f"({CHARS_VAR}): loaded as a library"
                            + (f" (its frontmatter preferred the whole "
                               f"file: {why}; it does not fit this "
                               "turn, so the library holds it whole by "
                               "section)" if preferred == "whole" else "")
                            + ". This is the table of contents; "
                            f"skill_search(query, \"{pack.name}\") ranks "
                            f"the passages and skill_read(\"{pack.name}\", "
                            "section) reads one — cite the breadcrumb"}
        state.skills_loaded.append(name)
        return {"ok": True, "name": pack.name, "title": pack.title,
                "origin": pack.origin, "text": pack.text}

    def skill_toc(name: str, under: str = "") -> dict[str, Any]:
        pack, problem = _in_library(name)
        if problem:
            return problem
        index = _index()
        info = index.overview(pack.name) or {}
        toc = index.toc(pack.name, under=under)
        if not toc and under:
            return {"error": f"no section of {pack.name!r} under "
                             f"{under!r}",
                    "hint": "skill_toc(name) alone lists the top level; "
                            "under takes a heading or a heading path"}
        lines, omitted = toc_lines(toc, TOC_TOOL_CAP)
        out = {"ok": True, "name": pack.name, "title": pack.title,
               "chars": pack.chars, "sections": info.get("sections"),
               "chunks": info.get("chunks"), "under": under,
               "toc": lines, "omitted": omitted}
        if omitted:
            out["hint"] = (f"{omitted:,} deeper sections folded: "
                           "skill_toc(name, under=\"<heading>\") opens "
                           "one branch; skill_search finds by words")
        return out

    def skill_search(query: str, skill: str = "",
                     k: int = SKILL_SEARCH_K) -> dict[str, Any]:
        query = str(query or "").strip()
        if not query:
            return {"error": "skill_search needs a query",
                    "hint": "the words the passage would use"}
        names: list[str]
        if str(skill or "").strip():
            _pack, problem = _in_library(str(skill))
            if problem:
                return problem
            names = [str(skill).strip()]
        else:
            names = list(library)
            if not names:
                return {"error": "no library pack is loaded this turn",
                        "hint": "name one: skill_search(query, skill), "
                                "or load_skill(name) first; the "
                                "library packs on the shelf: "
                                + (", ".join(sorted(oversized)) or "none")}
        k = max(1, min(int(k or SKILL_SEARCH_K), 20))
        hits = [h.as_row() for h in _index().search(query, skills=names,
                                                     k=k)]
        out = {"ok": True, "query": query, "skills": names,
               "count": len(hits), "hits": hits}
        if hits:
            out["hint"] = ("skill_read(skill, chunk_id) reads a passage "
                           "whole; skill_read(skill, section_id) the "
                           "section around it")
        else:
            out["hint"] = ("nothing matched: try the pack's own words "
                           "(skill_toc lists its headings)")
        return out

    def skill_read(name: str, section: str,
                   max_chars: int = SKILL_READ_CHARS,
                   offset: int = 0) -> dict[str, Any]:
        pack, problem = _in_library(name)
        if problem:
            return problem
        max_chars = max(200, min(int(max_chars or SKILL_READ_CHARS),
                                 RESULT_CAP - 2000))
        got = _index().read(pack.name, str(section or ""), max_chars,
                            int(offset or 0))
        if "error" in got:
            got.setdefault("hint", "a heading, a heading path, an s<N> "
                                   "from the contents, or a c<N> from "
                                   "skill_search")
            return got
        got["ok"] = True
        got["title"] = pack.title
        return got

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
            name="propose_sql",
            signature="propose_sql(sql, title, why?, metric_id?)",
            maps_to="the handover", ends_turn=True, writes=True,
            description=(
                "Hand a query to the person instead of running it: "
                "the SQL you proved with run_sql(mode=\"dry_run\"), a "
                "title, why it answers the ask, and the governed "
                "metric behind it (metric_id) so the card carries the "
                "status and the meridian line. The dry run prices it "
                "again; an invalid query comes back to fix. The turn "
                "ends: the card offers Run query and Run + dashboard, "
                "the rows arrive as a table artifact and as q1, and "
                "the next message can build from them."),
            fn=propose_sql, schema=_obj({
                "sql": _s("the full SELECT, proved by a dry run"),
                "title": _s("the name the user would look for later"),
                "why": _s("one or two sentences: what it answers and "
                          "the definition it uses"),
                "metric_id": _s("the governed metric behind it, when "
                                "there is one")}, ["sql", "title"])),
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
                "chart: {kind?: " + "|".join(CHART_KINDS) + ", series: "
                "[{name, points: [[x, y], …], dashed?, role?}], unit?, "
                "format?, x_label?, y_label?, reference?: {value, "
                "label}, sort?: x|-y, reason?} — every series shares "
                "ONE x axis of labels; y is null where a series has no "
                "value (a forecast is null over the actual periods and "
                "starts at the last actual, dashed: true). Leave kind "
                "out and the heuristic picks it from the data's shape "
                "(time on x → line; one category and a measure → bar, "
                "sorted, horizontal for long labels or > 8 categories; "
                "two measures → scatter; parts of a whole → stacked_bar "
                "or percent_bar, never a pie; a distribution → "
                "histogram; two categories and a measure → heatmap; "
                "> 12 series → small_multiples; actual vs plan → combo; "
                "a bridge → waterfall) and writes its one-sentence "
                "reason into the spec; when you pick the kind, say why "
                "in reason. heatmap: {kind: heatmap, x: [labels], y: "
                "[labels], values: [[row per y]]}; histogram: {kind: "
                "histogram, values: [numbers], bins?}; combo: role bar|"
                "line per series; waterfall: one series of steps, "
                "totals: [labels]. Numbers format themselves from the "
                "data (the decimals they carry, % and currency from the "
                "column name or unit, K/M/B past 100k); put the unit on "
                "the column or the spec, and format ({kind, decimals, "
                "unit} or a kind word) only to override. table: "
                "{columns: [{key, label, unit?, format?}], rows: [{…}]}. "
                "document: {markdown}. kpi: {value, unit?, label?, "
                "delta?, delta_format?, compare_label?, format?}. "
                "dashboard: {grid?: 1|2|3, panels: [{type, title?, "
                "span?, spec}], filters?: [{slot, options}], notes?} — "
                "every numeric panel carries its OWN provenance. "
                "diagram: {kind: graph, nodes, edges} "
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
                "SQL ground → synapse-data-connect; a deck or memo → "
                "executive-summary; tiles → dashboard-design)."),
            fn=load_skill, schema=_obj({"name": _s("pack name")},
                                       ["name"])),
    ]
    if library or oversized:
        specs += [
            ToolSpec(
                name="skill_toc", signature="skill_toc(name, under?)",
                maps_to="the card catalogue",
                description=(
                    "The table of contents of a pack loaded as a "
                    "library (one too large to hold whole): every "
                    "heading path with its chunk count and size, in "
                    "order. Read it before deciding a pack has nothing "
                    "on the ask. under=\"<heading>\" opens one branch "
                    "when the list folded to its top level."),
                fn=skill_toc, schema=_obj({
                    "name": _s("pack name"),
                    "under": _s("a heading or heading path to list "
                                "beneath")}, ["name"])),
            ToolSpec(
                name="skill_search",
                signature="skill_search(query, skill?, k?)",
                maps_to="the catalogue lookup",
                description=(
                    "Find the passages of a library pack that match "
                    "words: ranked chunks, each with its breadcrumb "
                    "(\"H1 > H2 > H3\"), character offsets, a score and "
                    "a snippet. Lexical, not semantic: use the words "
                    "the pack would use (its headings, its terms), "
                    "and try a second phrasing before concluding it "
                    "is silent. skill=name limits it to one pack; "
                    "without it, every pack loaded this turn."),
                fn=skill_search, schema=_obj({
                    "query": _s("the words the passage would use"),
                    "skill": _s("one pack name; default every library "
                                "pack loaded this turn"),
                    "k": _i("how many, default 8, at most 20")},
                    ["query"])),
            ToolSpec(
                name="skill_read",
                signature="skill_read(name, section, max_chars?, offset?)",
                maps_to="the page",
                description=(
                    "Read one section or passage of a library pack, "
                    "as written: a heading, a heading path, an s<N> "
                    "from the contents, or a c<N> from skill_search. "
                    "The result carries the breadcrumb and the "
                    "character offsets — cite them. A section longer "
                    "than max_chars (default 6000) comes back in pages: "
                    "call again with the offset the result names."),
                fn=skill_read, schema=_obj({
                    "name": _s("pack name"),
                    "section": _s("a heading, heading path, s<N> or "
                                  "c<N>"),
                    "max_chars": _i("characters per page, default 6000"),
                    "offset": _i("continue a long section from here")},
                    ["name", "section"])),
        ]
    specs += [
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
