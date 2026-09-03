"""The agent's tools (Agent Loop v1 §3): descriptions are the product.

Claude Code's Glob/Grep/Read/Bash/TodoWrite/Task map onto the graph
one-for-one. Each tool ships with EXACTLY the description the spec
wrote (a test pins the text), and each error message names the correct
next call — the error channel is a teaching channel, never a dead end.

Three pins live here:
  * **read-only except plan_set / note / ask_user.** Every other tool
    reads the compiled build; nothing in the loop writes truth (clerk
    only), nothing executes live (snapshot only), and `verify` is not
    a tool — the harness runs it on the final plan in fresh context.
  * **determinism relocated, not lost.** `resolve` is the same scored
    lexicographic binder; `plan_set` typechecks every call; the
    sandbox's parse → ACL → substrate order is unchanged. The model
    chooses WHEN to call, never how a tool decides.
  * **the trace is the sub-graph.** Cards read, resolves made, and
    bindings committed are recorded on the state automatically, so
    disclosure (`subgraph_used`) costs the model nothing.

`plan_set` takes a PATCH (several slots in one call) where E18 user
edits stay single-slot: that pin governs conversational mutations
("same for Canada" moves one slot, computed in code). A loop patch is
one tool call → one plan version → the full diff recorded, so the
defensibility the pin protects — a diff a steward can read — holds.
"""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Protocol

from sahs.ask.plan import Plan, diff as plan_diff
from sahs.tools.api import (Build, get_definition_line as
                            _definition_line, sample_values as
                            _sample_values, search_concepts,
                            search_metrics, _tokens)
from sahs.tools.resolver import resolve as _resolve
from sahs.tools.sandbox import execute_sandboxed
from sahs.tools.validate_sql import validate_sql

MAX_GREP_HITS = 40

_BINDING_TEMPLATE = re.compile(
    r"\bWHERE\s+(?:\w+\.)*(\w+)\s*(=|!=|>=|<=|>|<|IN)\s*(.+)$",
    re.IGNORECASE | re.DOTALL)


def binding_template(canonical_sql: str) -> dict[str, str] | None:
    """column + operator from a mined binding, the witnessed literal
    demoted to an EXAMPLE. The fix for the baked-literal defect: a
    binding teaches WHERE to filter, never WHAT value to use — the
    caller's own literal belongs in the query."""
    match = _BINDING_TEMPLATE.search(canonical_sql or "")
    if not match:
        return None
    return {"column": match.group(1), "op": match.group(2).upper(),
            "example_literal": match.group(3).strip()[:80]}


MAX_CHIPS = 4          # same ceiling as clarify chips: evidence, not a lineup
MAX_NOTES = 100
ROW_CAP = 1000


class SnapshotRunner(Protocol):
    """Rows from the frozen extract — the only row-returning engine a
    loop may hold. Attached by the exploratory lane (§9.5), absent by
    default; never a live connection."""

    name: str

    def run(self, sql: str, limit: int) -> dict[str, Any]: ...


@dataclass(frozen=True)
class ToolSpec:
    """One tool as the model sees it. ``signature`` and ``description``
    are the spec's text verbatim; ``maps_to`` names the Claude Code
    analogue the design borrowed."""

    name: str
    signature: str
    maps_to: str
    description: str
    fn: Callable[..., dict[str, Any]]
    writes: bool = False
    ends_turn: bool = False
    # v3: the OpenAPI-subset parameter schema the native tool
    # protocol declares (None = text-signature tools of the v1 loop)
    schema: dict[str, Any] | None = None


@dataclass
class LoopState:
    """What the loop owns between steps: the plan (the ONLY state the
    loop writes), the notebook, and the automatically-recorded
    sub-graph that becomes the answer's disclosure."""

    plan: Plan = field(default_factory=Plan)
    notes: list[str] = field(default_factory=list)
    subgraph: dict[str, list[Any]] = field(default_factory=lambda: {
        "cards_read": [], "resolves": [], "bindings_used": []})
    pending_question: dict[str, Any] | None = None


# ─── shared helpers ──────────────────────────────────────────


def _err(message: str, hint: str, **extra: Any) -> dict[str, Any]:
    out = {"error": message, "hint": hint}
    out.update(extra)
    return out


def _card_dirs(build: Build) -> dict[str, Path]:
    root = build.root / "cards"
    return {"tables": root / "tables", "metrics": root / "metrics",
            "concepts": root / "concepts"}


def _card_ids(build: Build, scope: str = "all") -> list[str]:
    ids = []
    for kind, path in _card_dirs(build).items():
        if scope not in ("all", kind) or not path.exists():
            continue
        ids += [f"{kind}/{p.stem}" for p in sorted(path.glob("*.md"))]
    return ids


def _card_path(build: Build, card_id: str) -> Path | None:
    """Resolve every address form a card answers to: the grep form
    (``tables/dw__x``), the id form (``table:dw.x`` / ``metric:<fp>``
    / ``concept:name``), or a bare unique stem."""
    dirs = _card_dirs(build)
    cid = (card_id or "").strip()
    if ":" in cid:
        kind, _, rest = cid.partition(":")
        kind = {"table": "tables", "metric": "metrics",
                "concept": "concepts"}.get(kind, kind)
        if kind == "tables":
            physical = build.physical_of(rest) or rest
            rest = physical.replace(".", "__")
        cid = f"{kind}/{rest}"
    if "/" in cid:
        kind, _, stem = cid.partition("/")
        path = dirs.get(kind, Path("/nonexistent")) / f"{stem}.md"
        return path if path.exists() else None
    hits = [i for i in _card_ids(build) if i.split("/", 1)[1] == cid]
    if len(hits) == 1:
        kind, _, stem = hits[0].partition("/")
        return dirs[kind] / f"{stem}.md"
    return None


def _sections(text: str) -> dict[str, str]:
    """A card's ``## section`` blocks; everything above the first is
    ``head`` (identity, provenance, the witness line)."""
    out: dict[str, list[str]] = {"head": []}
    current = "head"
    for line in text.splitlines():
        if line.startswith("## "):
            current = line[3:].strip().lower()
            out.setdefault(current, [])
            continue
        out[current].append(line)
    return {name: "\n".join(lines).strip() for name, lines in out.items()}


def _closest(name: str, candidates: list[str], n: int = 3) -> list[str]:
    return difflib.get_close_matches((name or "").lower(),
                                     candidates, n, cutoff=0.4)


def _metric_row(build: Build, ref: str) -> tuple[dict[str, Any] | None,
                                                 list[dict[str, Any]]]:
    """A metric by id, fingerprint, mgroup, or exact label →
    (row, other_matches). Ambiguity is returned, never argmaxed."""
    ref_l = (ref or "").strip().lower()
    hits = [m for m in build.metrics
            if m["id"].lower() == ref_l or m["fp"].lower() == ref_l
            or ref_l in [g.lower() for g in m.get("mgroups", [])]
            or (m.get("label") or "").lower() == ref_l]
    return (hits[0], hits[1:]) if hits else (None, [])


def _lob_hit(row: dict[str, Any], query: str) -> bool:
    """Does a query name this business area? Codes and short names
    match as tokens ('GMNS', '…all gmns metrics'); long names match
    on ≥2 shared tokens ('global merchant services'). Both sides go
    through _tokens so stemming stays consistent."""
    tokens = _tokens(query)
    raw = (query or "").strip().lower()
    names = {str(row.get("code", "")).lower(),
             str(row.get("lob", "")).lower()} - {""}
    if raw in names or raw == str(row.get("name", "")).lower():
        return True
    name_keys: set[str] = set()
    for name in names:
        name_keys |= _tokens(name)
    if tokens & name_keys:
        return True
    return len(tokens & _tokens(str(row.get("name", "")))) >= 2


def _metric_in_lob(metric: dict[str, Any],
                   lob_row: dict[str, Any]) -> bool:
    """A metric belongs to a business area by its recorded
    line_of_business (code, name, or a close spelling) or by the
    area's witness family attesting it."""
    code = str(lob_row.get("code", "")).lower()
    family = str(lob_row.get("lob", "")).lower()
    raw = str(metric.get("line_of_business", "")).strip().lower()
    if raw:
        if raw in (code, family, str(lob_row.get("name",
                                                 "")).lower()):
            return True
        if len(_tokens(raw)
               & _tokens(str(lob_row.get("name", "")))) >= 2:
            return True
    families = set(metric.get("support_by_witness") or {}) \
        | set(metric.get("seen_by_witness") or [])
    if family and family in families:
        return True
    # the laptop's truth: only ~1% of metric rows carry an area, but
    # the area's TABLES are mapped — so a metric on an area's table
    # is that area's metric (area → tables → metrics)
    return str(metric.get("table", "")) in set(lob_row.get("tables")
                                                  or [])


def _tier(row: dict[str, Any]) -> str:
    """A join edge's evidence tier. Declared constraints certify; a
    measured ON clause witnesses; togetherness (co-query) or an
    equality seen only between transformed CTEs (scoped_only) is a
    candidate — evidence the relationship exists, never that the raw
    tables join safely."""
    if row.get("source") == "constraints":
        return "certified"
    if row.get("on") and row.get("scope") != "scoped_only":
        return "witnessed"
    return "candidate"


# ─── the toolkit ─────────────────────────────────────────────


def toolkit(build: Build, state: LoopState, *,
            substrate: Any = None,
            snapshot_runner: SnapshotRunner | None = None,
            ledger_path: Path | None = None,
            scout: Callable[[str], dict[str, Any]] | None = None
            ) -> dict[str, ToolSpec]:
    """The tools of Agent Loop v1, closed over one build and one
    session's state. ``delegate_scout`` appears only when a scout
    runner is wired in (the loop wires one; a bare kit has none, and
    the scout's own kit never gets one — no worker spawns workers).
    ``verify``, truth writes, and live execution are not tools by
    design."""

    # ── list_tables ── Glob ──────────────────────────────────
    def list_tables(domain: str = "", lob: str = "") -> dict[str, Any]:
        want_domain = (domain or "").strip().lower()
        want_lob = (lob or "").strip().lower()
        by_table: dict[str, list[dict[str, Any]]] = {}
        for m in build.metrics:
            by_table.setdefault(m.get("table", ""), []).append(m)
        rows = []
        for physical in sorted(build.schema):
            metrics = by_table.get(physical, [])
            if want_domain and not any(
                    want_domain in (m.get("domain") or "").lower()
                    for m in metrics):
                continue
            if want_lob and not any(
                    want_lob in (m.get("line_of_business") or "").lower()
                    for m in metrics):
                continue
            facts = build.table_facts(physical)
            purpose, owner = "", ""
            path = _card_dirs(build)["tables"] / (
                physical.replace(".", "__") + ".md")
            if path.exists():
                for line in path.read_text(
                        encoding="utf-8").splitlines()[:8]:
                    if line.startswith("- purpose: "):
                        purpose = line[len("- purpose: "):].split(
                            " [prov:")[0].strip()
                    elif line.startswith("- owner: "):
                        owner = line[len("- owner: "):].split(
                            " · ")[0].strip()
            certified = sum(1 for m in metrics
                            if m.get("status") == "certified")
            total = facts.get("total_rows")
            rows.append({
                "table": physical,
                "purpose": purpose or "(no purpose on record)",
                # an absent row count is never a zero — pinned
                "rows": total if total not in (None, "") else "unknown",
                "readiness": f"{certified} certified of "
                             f"{len(metrics)} metrics"
                             + (f" · lifecycle {facts['lifecycle']}"
                                if facts.get("lifecycle") else ""),
                "owner": owner or "(unrecorded)",
            })
        hint = ""
        if not rows and (want_domain or want_lob):
            seen = sorted({(m.get("domain") or m.get("line_of_business")
                            or "").lower()
                           for m in build.metrics} - {""})
            hint = ("no table matches that domain/lob: this build "
                    "knows " + (", ".join(seen) if seen else "none")
                    + ". Call with no filter to see everything.")
        elif rows:
            hint = ("read_card(\"table:<name>\") before touching a "
                    "table's columns")
        return {"tables": rows, "count": len(rows), "hint": hint}

    # ── grep_cards ── Grep ───────────────────────────────────
    def grep_cards(pattern: str, scope: str = "all") -> dict[str, Any]:
        if scope not in ("all", "tables", "metrics", "concepts"):
            return _err(f"unknown scope {scope!r}",
                        "scope is one of: all | tables | metrics | "
                        "concepts")
        if not (pattern or "").strip():
            return _err("empty pattern",
                        "give a word, column, or code to find; "
                        "search_semantics ranks by meaning instead")
        literal = False
        try:
            rx = re.compile(pattern, re.IGNORECASE)
        except re.error:
            rx = re.compile(re.escape(pattern), re.IGNORECASE)
            literal = True
        hits, capped = [], False
        for card_id in _card_ids(build, scope):
            kind, _, stem = card_id.partition("/")
            path = _card_dirs(build)[kind] / f"{stem}.md"
            for number, line in enumerate(
                    path.read_text(encoding="utf-8").splitlines(), 1):
                if rx.search(line):
                    hits.append({"card": card_id, "line": number,
                                 "text": line.strip()})
                    if len(hits) >= MAX_GREP_HITS:
                        capped = True
                        break
            if capped:
                break
        hint = ""
        if capped:
            hint = (f"capped at {MAX_GREP_HITS}: narrow with scope= "
                    "or a longer pattern")
        elif not hits:
            hint = (f"no card line matches {pattern!r}: "
                    "search_semantics finds by meaning; list_tables "
                    "browses what exists")
        else:
            hint = "read_card(<card>) to see a hit in context"
        if literal:
            hint = ("pattern was not valid regex, searched literally. "
                    + hint)
        return {"hits": hits, "count": len(hits), "capped": capped,
                "hint": hint}

    # ── read_card ── Read ────────────────────────────────────
    def read_card(id: str, section: str = "") -> dict[str, Any]:
        path = _card_path(build, id)
        if path is None:
            universe = _card_ids(build)
            close = _closest((id or "").split(":")[-1].split("/")[-1],
                             [u.split("/", 1)[1] for u in universe])
            suggestions = [u for u in universe
                           if u.split("/", 1)[1] in close]
            return _err(
                f"unknown card {id!r}",
                "card ids come from grep_cards and list_tables; "
                "metric ids from search_semantics or resolve "
                "(metric:<fp>)",
                suggestions=suggestions[:3])
        text = path.read_text(encoding="utf-8")
        parts = _sections(text)
        card_id = f"{path.parent.name}/{path.stem}"
        if card_id not in state.subgraph["cards_read"]:
            state.subgraph["cards_read"].append(card_id)
        if section:
            want = section.strip().lower()
            if want not in parts:
                return _err(
                    f"card {card_id} has no section {section!r}",
                    "sections on this card: "
                    + ", ".join(parts) + ". Omit section to read "
                    "the whole card.")
            return {"card": card_id, "section": want,
                    "text": parts[want], "sections": sorted(parts),
                    "build": build.version}
        return {"card": card_id, "text": text,
                "sections": sorted(parts), "build": build.version}

    # ── search_semantics ── the index, not the text ──────────
    def search_semantics(query: str, kind: str = "all") -> dict[str, Any]:
        if kind not in ("all", "metrics", "concepts", "joins", "vocab",
                        "values"):
            return _err(f"unknown kind {kind!r}",
                        "kind is one of: all | metrics | concepts | "
                        "joins | vocab | values")
        results: list[dict[str, Any]] = []
        if kind == "all":
            # intent first: a business word resolves to the business
            # map before anything table-shaped gets a look-in
            for row in getattr(build, "lob", []) or []:
                if _lob_hit(row, query):
                    members = [m for m in build.metrics
                               if _metric_in_lob(m, row)]
                    results.append({
                        "kind": "line_of_business",
                        "code": row.get("code", ""),
                        "name": row.get("name", ""),
                        "domains": row.get("domains", []),
                        "metrics": len(members),
                        "tables": row.get("tables", []),
                        "hint": f"{row.get('code')} is a business "
                                "area, not a table: "
                                f"list_metrics({row.get('code')!r}) "
                                f"returns its {len(members)} "
                                "governed metrics"})
        if kind in ("all", "metrics"):
            for c in search_metrics(build, query,
                                    top_k=8)["candidates"]:
                row, _ = _metric_row(build, c["id"])
                results.append({
                    "kind": "metric", "id": c["id"],
                    "label": c["label"],
                    "status": c.get("status_served", c["status"]),
                    "table": c["table"], "grain": c.get("grain", ""),
                    "support": (row or {}).get("support", 0),
                    "agreement": (row or {}).get("witness_agreement", 0),
                })
        if kind in ("all", "concepts"):
            for b in search_concepts(build, query, top_k=8)["bindings"]:
                hit = {
                    "kind": "concept", "label": b["concept"],
                    "table": b["table"], "sql": b["sql"],
                    "support": b.get("support", 0),
                    "source": b.get("source", ""),
                }
                template = binding_template(b["sql"])
                if template:
                    hit["template"] = template
                results.append(hit)
        if kind in ("all", "vocab"):
            tokens = _tokens(query)
            raw = {(query or "").strip().lower()}
            # written as an acronym: REST, CARE — the guard list's
            # symbols expand only then (or when vocab is asked for)
            written_upper = {t.lower() for t in re.findall(
                r"[A-Za-z][A-Za-z0-9/\-]*", query or "") if t.isupper()}
            for v in build.vocab:
                symbol = (v.get("text") or "").lower()
                if symbol in raw or symbol in tokens or (
                        tokens & _tokens(v.get("definition", ""))):
                    common = bool(v.get("common_word"))
                    if (common and kind != "vocab"
                            and symbol not in written_upper
                            and symbol not in raw):
                        continue      # "rest" is a word, not REST
                    hit = {
                        "kind": "vocab", "text": v.get("text", ""),
                        "definition": v.get("definition", ""),
                        "bu": v.get("bu", ""),
                        "region": v.get("region", ""),
                        "common_word": common}
                    if common:
                        hit["guard"] = ("also an ordinary word: expand "
                                        "only when the ask writes it as "
                                        "an acronym or the scope fits")
                    results.append(hit)
                    if sum(1 for r in results
                           if r["kind"] == "vocab") >= 6:
                        break
        if kind in ("all", "values"):
            # a business phrase is a stored code somewhere: "KYC done"
            # → kyc_check_confirmed__c = '1'. Filter on the code,
            # never on the phrase.
            low = (query or "").strip().lower()
            tokens = _tokens(query)
            cap = 6 if kind == "values" else 3
            for m in getattr(build, "value_meanings", []) or []:
                synonym = str(m.get("synonym", "")).strip()
                syn_tokens = _tokens(synonym)
                if not synonym or not syn_tokens:
                    continue
                if synonym.lower() in low or (
                        len(syn_tokens) > 1 and syn_tokens <= tokens) \
                        or (kind == "values" and syn_tokens & tokens):
                    column = m.get("column", "")
                    results.append({
                        "kind": "value", "table": m.get("table", ""),
                        "column": column, "value": m.get("value", ""),
                        "synonym": synonym,
                        "predicate": f"{column} = '{m.get('value', '')}'",
                        "hint": "a stored code with this meaning: "
                                "filter with the predicate, never the "
                                "phrase; say the meaning in the answer"})
                    if sum(1 for r in results
                           if r["kind"] == "value") >= cap:
                        break
        if kind in ("all", "joins"):
            tokens = _tokens(query)
            for j in build.joins:
                shorts = {build.short_table(j["a"]),
                          build.short_table(j["b"])}
                if tokens & {t for s in shorts for t in _tokens(s)}:
                    results.append({
                        "kind": "join", "a": j["a"], "b": j["b"],
                        "tier": _tier(j),
                        "support": j.get("support", 0),
                        "on": j.get("on", "")})
                    if sum(1 for r in results
                           if r["kind"] == "join") >= 6:
                        break
        hint = ("" if results else
                f"nothing ranked for {query!r}: grep_cards finds "
                "exact tokens; list_tables browses what exists")
        return {"results": results[:16], "count": len(results[:16]),
                "hint": hint}

    # ── list_metrics ── the catalog, intent-shaped ───────────
    def list_metrics(filter: str = "") -> dict[str, Any]:
        raw = (filter or "").strip()
        rows = list(build.metrics)
        scope = ""
        if raw:
            lob_row = next((r for r in getattr(build, "lob", []) or []
                            if _lob_hit(r, raw)), None)
            lowered = raw.lower()
            if lob_row is not None:
                rows = [m for m in rows
                        if _metric_in_lob(m, lob_row)]
                scope = (f"{lob_row.get('code')} — "
                         f"{lob_row.get('name')}")
            elif lowered in ("certified", "pending", "composed",
                             "team_candidate"):
                rows = [m for m in rows
                        if (m.get("status_served")
                            or m.get("status")) == lowered]
                scope = f"status {lowered}"
            else:
                tokens = _tokens(raw)
                rows = [m for m in rows if tokens & (
                    _tokens(str(m.get("label", "")))
                    | _tokens(str(m.get("domain", "")))
                    | _tokens(str(m.get("line_of_business", ""))))]
                scope = f"matching {raw!r}"
            if not rows:
                areas = ", ".join(
                    f"{r.get('code')} ({r.get('name')})"
                    for r in getattr(build, "lob", []) or []) \
                    or "none on file"
                return _err(
                    f"no metrics match {raw!r}",
                    "filter by a business area, a status, or label "
                    f"words; business areas on file: {areas}")
        rows = sorted(rows, key=lambda m: (
            -(m.get("authority") or 0), -(m.get("support") or 0),
            m["id"]))[:40]
        return {"metrics": [{
            "id": m["id"], "label": m.get("label") or m["id"],
            "status": m.get("status_served") or m.get("status"),
            "table": m.get("table", ""),
            "lob": m.get("line_of_business", ""),
            "domain": m.get("domain", ""),
            "support": m.get("support", 0)} for m in rows],
            "count": len(rows),
            **({"scope": scope} if scope else {}),
            "hint": "read_card before using any of them; "
                    "get_definition_line writes the disclosure"}

    # ── resolve ── the deterministic binder, mid-loop ────────
    def resolve(text: str, table: str = "") -> dict[str, Any]:
        context: dict[str, Any] = {}
        if table:
            physical = build.physical_of(table)
            if physical is None:
                close = _closest(table,
                                 [build.short_table(t)
                                  for t in build.schema])
                return _err(f"unknown table {table!r}",
                            "list_tables shows what exists",
                            suggestions=close)
            context["tables_allowed"] = [physical]
        raw = _resolve(build, text, context)
        state.subgraph["resolves"].append(
            {"text": text, "table": table,
             "confidence": raw.get("confidence")})
        return raw

    # ── sample_values ────────────────────────────────────────
    def sample_values(table: str, column: str,
                      n: int = 20) -> dict[str, Any]:
        n = max(1, min(int(n or 20), 100))
        physical = build.physical_of(table)
        if physical is not None and (column or "").lower() not in \
                build.schema.get(physical, {}):
            close = _closest(column, list(build.schema[physical]))
            return _err(
                f"unknown column {column!r} on {physical}",
                "read_card(\"table:" + physical + "\", \"columns\") "
                "shows the real columns"
                + (": closest are " + ", ".join(close) if close
                   else ""))
        out = _sample_values(build, table, column)
        if "values" in out:
            out["values"] = out["values"][:n]
        return out

    # ── get_join_paths ───────────────────────────────────────
    def get_join_paths(tables: list[str]) -> dict[str, Any]:
        if not isinstance(tables, (list, tuple)) or len(tables) < 2:
            return _err(
                "get_join_paths takes a list of two or more tables",
                "pass every table the plan might touch, e.g. "
                "[\"gms_transaction\", \"wwcas_authorization\"]")
        physicals = []
        for name in tables:
            physical = build.physical_of(name)
            if physical is None:
                close = _closest(name, [build.short_table(t)
                                        for t in build.schema])
                return _err(f"unknown table {name!r}",
                            "list_tables shows what exists",
                            suggestions=close)
            physicals.append(physical)
        rank = {"certified": 0, "witnessed": 1, "candidate": 2}
        paths, missing = [], []
        for i in range(len(physicals)):
            for j in range(i + 1, len(physicals)):
                pair = {physicals[i], physicals[j]}
                rows = [r for r in build.joins
                        if {r["a"], r["b"]} == pair]
                evidence = sorted(
                    ({"tier": _tier(r), "on": r.get("on", ""),
                      "support": r.get("support", 0),
                      "source": r.get("source", ""),
                      **({"scope": r["scope"]} if r.get("scope")
                         else {})} for r in rows),
                    key=lambda e: (rank[e["tier"]], -e["support"]))
                tier = evidence[0]["tier"] if evidence else "none"
                if tier == "none":
                    missing.append(sorted(pair))
                paths.append({"tables": sorted(pair), "tier": tier,
                              "evidence": evidence})
        hint = ""
        if missing:
            hint = ("no join path on record for "
                    + "; ".join(" ↔ ".join(p) for p in missing)
                    + ": prefer answering from one table: a "
                    "hand-written join is ungoverned and the answer "
                    "must say so")
        return {"paths": paths, "hint": hint}

    # ── get_definition_line ──────────────────────────────────
    def get_definition_line(metric: str,
                            variant: str = "") -> dict[str, Any]:
        return _definition_line(build, metric, variant)

    # ── run_sql ── Bash, fenced ──────────────────────────────
    def run_sql(sql: str, mode: str = "dry_run",
                limit: int = 200) -> dict[str, Any]:
        if mode not in ("dry_run", "snapshot"):
            return _err(
                f"unknown mode {mode!r}",
                "modes: dry_run (shape and cost, no rows) | snapshot "
                "(rows from the frozen extract). Live execution is "
                "not a loop tool.")
        limit = max(1, min(int(limit or 200), ROW_CAP))
        verdict = validate_sql(build, sql,
                               metric_id=state.plan.metric_id or "")
        if not verdict["ok"]:
            return _err("sql_invalid",
                        "each violation names its correction; fix "
                        "and re-run",
                        violations=verdict["violations"],
                        warnings=verdict["warnings"])
        sandboxed = execute_sandboxed(
            build, sql, mode="snapshot", limit=limit,
            substrate=substrate, ledger_path=ledger_path)
        if sandboxed["status"] != "ok":
            taught = (sandboxed.get("meta") or {}).get("taught") or {}
            return _err(sandboxed.get("error") or "sandbox refused",
                        taught.get("hint")
                        or "the sandbox's reason stands: dry_run and "
                           "snapshot never bypass the ACL",
                        **{k: v for k, v in taught.items()
                           if k in ("kind", "yours_to_fix", "closest",
                                    "fix_env", "smoke", "tables")})
        shape = sandboxed["data"]
        sent = (sandboxed.get("meta") or {}).get("sql_sent")
        extra = {"sql_sent": sent} if sent else {}
        if mode == "dry_run":
            return {"mode": "dry_run", "valid": True,
                    "result_schema": shape.get("result_schema"),
                    "bytes_processed": shape.get("bytes_processed"),
                    "rows": None,
                    "warnings": verdict["warnings"], **extra,
                    "note": "dry_run: shape and cost, no rows"}
        if snapshot_runner is None:
            return _err(
                "no_snapshot",
                "no frozen snapshot is attached to this session: "
                "dry_run still checks shape and cost. The "
                "exploratory lane runs with snapshot on.")
        ran = snapshot_runner.run(sql, limit)
        rows = ran.get("rows") or []
        return {"mode": "snapshot", "rows": rows[:limit],
                "row_count": len(rows[:limit]),
                "result_schema": ran.get("schema")
                or shape.get("result_schema"),
                "bytes_processed": shape.get("bytes_processed"),
                "warnings": verdict["warnings"], **extra,
                "source": getattr(snapshot_runner, "name", "snapshot")}

    # ── plan_set ── TodoWrite: the ONLY state the loop writes ─
    def plan_set(patch: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(patch, dict) or not patch:
            return _err(
                "plan_set takes a patch of named slots",
                "e.g. plan_set({\"metric\": \"approval_rate\", "
                "\"grain\": \"transaction\", \"filters\": "
                "{\"country\": \"CA\"}})")
        aliases = {"dimensions": "dims", "time_window": "time"}
        allowed = ("metric", "table", "filters", "grain", "dims",
                   "time", "checks")
        problems: list[dict[str, str]] = []
        warnings: list[dict[str, str]] = []
        fields: dict[str, Any] = {}
        provenance = dict(state.plan.provenance)

        for key, value in patch.items():
            slot = aliases.get(key, key)
            if slot not in allowed:
                problems.append({
                    "code": "unknown_slot",
                    "detail": f"unknown slot {key!r}",
                    "hint": "plans mutate by named slot: "
                            + ", ".join(allowed)})
                continue
            if slot == "metric":
                row, others = _metric_row(build, str(value))
                if row is None:
                    close = _closest(str(value),
                                     [m.get("label", "").lower()
                                      for m in build.metrics])
                    problems.append({
                        "code": "unknown_metric",
                        "detail": f"no metric matches {value!r}",
                        "hint": "resolve() or search_semantics() "
                                "first"
                                + (": closest labels are "
                                   + ", ".join(close) if close
                                   else "")})
                    continue
                if others:
                    problems.append({
                        "code": "ambiguous_metric",
                        "detail": f"{value!r} matches "
                                  f"{1 + len(others)} metrics",
                        "hint": "name one id: "
                                + ", ".join(
                                    f"{m['id']} ({m['label']} on "
                                    f"{m['table']})"
                                    for m in [row] + others[:2])})
                    continue
                fields.update(metric_id=row["id"], metric_fp=row["fp"],
                              metric_label=row.get("label", ""),
                              metric_sql=row.get("canonical_sql", ""))
                if "table" not in patch and not state.plan.table:
                    fields["table"] = row.get("table", "")
                    provenance["table"] = "typecheck"
                provenance["metric"] = "model"
            elif slot == "table":
                physical = build.physical_of(str(value))
                if physical is None:
                    close = _closest(str(value),
                                     [build.short_table(t)
                                      for t in build.schema])
                    problems.append({
                        "code": "unknown_table",
                        "detail": f"unknown table {value!r}",
                        "hint": "list_tables shows what exists"
                                + (": closest are " + ", ".join(close)
                                   if close else "")})
                    continue
                fields["table"] = physical
                provenance["table"] = "model"
            elif slot == "filters":
                if isinstance(value, dict):
                    filters = {str(k): str(v)
                               for k, v in value.items()}
                elif isinstance(value, (list, tuple)) and all(
                        isinstance(v, str) and "=" in v
                        for v in value):
                    filters = dict(v.split("=", 1) for v in value)
                    filters = {k.strip(): v.strip()
                               for k, v in filters.items()}
                else:
                    problems.append({
                        "code": "bad_filters",
                        "detail": "filters must be a dict",
                        "hint": "e.g. {\"country\": \"CA\"}; the "
                                "name binds via the build, the value "
                                "is the literal"})
                    continue
                merged = dict(state.plan.filters)
                for name, literal in filters.items():
                    if literal in ("", None):
                        merged.pop(name, None)
                    else:
                        merged[name] = literal
                    provenance[f"filters.{name}"] = "model"
                fields["filters"] = merged
            elif slot in ("dims", "checks"):
                items = ([value] if isinstance(value, str)
                         else list(value or ()))
                target = "dimensions" if slot == "dims" else "checks"
                fields[target] = tuple(str(v) for v in items)
                provenance[target] = "model"
            elif slot == "time":
                fields["time_window"] = str(value or "")
                provenance["time_window"] = "model"
            elif slot == "grain":
                fields["grain"] = str(value or "")
                provenance["grain"] = "model"

        if problems:
            # errors return the plan UNCHANGED: the model fixes its
            # own patch from the teaching, Claude-Code style
            return {"ok": False, "plan": state.plan.to_dict(),
                    "changes": [], "problems": problems,
                    "warnings": warnings}

        candidate = replace(state.plan, version=state.plan.version + 1,
                            parent=state.plan.version,
                            provenance=provenance, **fields)

        # ── typecheck: warnings travel, they do not block ────
        if candidate.metric_id and candidate.table:
            row, _ = _metric_row(build, candidate.metric_id)
            home = (row or {}).get("table", "")
            if home and home != candidate.table:
                warnings.append({
                    "code": "metric_home_mismatch",
                    "detail": f"{candidate.metric_label or candidate.metric_id} "
                              f"is compiled on {home}, the plan says "
                              f"{candidate.table}",
                    "hint": "move the plan to the metric's table, or "
                            "pick a metric on yours: "
                            "search_semantics(kind=\"metrics\")"})
        if not candidate.grain:
            warnings.append({
                "code": "grain_missing",
                "detail": "no grain, no answer: the contract will "
                          "refuse this plan",
                "hint": "read_card the metric, or ask_user what one "
                        "row should mean"})
        if candidate.table:
            columns = build.schema.get(candidate.table, {})
            for name, literal in candidate.filters.items():
                sample = _sample_values(build, candidate.table, name) \
                    if name.lower() in columns else {}
                values = sample.get("values")
                if values and literal not in values:
                    close = _closest(literal, [str(v) for v in values])
                    warnings.append({
                        "code": "literal_off_domain",
                        "detail": f"{literal!r} is not an observed "
                                  f"value of {candidate.table}.{name}",
                        "hint": "sample_values disagrees"
                                + (": closest observed are "
                                   + ", ".join(close) if close
                                   else "")})
                elif name.lower() not in columns and not search_concepts(
                        build, name, table=candidate.table,
                        top_k=1)["bindings"]:
                    warnings.append({
                        "code": "unbound_filter",
                        "detail": f"nothing on {candidate.table} "
                                  f"binds {name!r} yet",
                        "hint": "resolve() can route it; "
                                "sample_values checks literals "
                                "before they reach a WHERE"})

        changes = plan_diff(state.plan, candidate)
        state.plan = candidate
        if candidate.metric_id and (
                not state.subgraph["bindings_used"]
                or state.subgraph["bindings_used"][-1].get("metric")
                != candidate.metric_id):
            state.subgraph["bindings_used"].append(
                {"metric": candidate.metric_id,
                 "table": candidate.table,
                 "version": candidate.version})
        return {"ok": True, "plan": candidate.to_dict(),
                "changes": changes, "problems": [],
                "warnings": warnings}

    # ── note ─────────────────────────────────────────────────
    def note(text: str) -> dict[str, Any]:
        if not (text or "").strip():
            return _err("an empty note teaches nothing",
                        "write what you ruled out and why: that is "
                        "the note your later self needs")
        if len(state.notes) >= MAX_NOTES:
            return _err("the notebook is full",
                        "finish, ask_user, or stop honestly: more "
                        "notes will not replace a decision")
        state.notes.append(text.strip())
        return {"ok": True, "notes": len(state.notes)}

    # ── ask_user ── ends the turn ────────────────────────────
    def ask_user(question: str,
                 options: list[Any]) -> dict[str, Any]:
        if not (question or "").strip():
            return _err("a question needs words",
                        "one question, named options, evidence on "
                        "each chip")
        raw = list(options or ())
        if not raw:
            return _err("ask_user needs options",
                        "name the candidates with their evidence; "
                        "a bare question makes the user do the "
                        "navigation you were asked to do")
        if len(raw) > MAX_CHIPS:
            return _err(
                f"{len(raw)} options is a lineup, not a question",
                f"at most {MAX_CHIPS} chips: settle the rest by "
                "evidence first (grep_cards, read_card, "
                "sample_values)")
        chips = []
        for opt in raw:
            if isinstance(opt, str):
                chips.append({"value": opt, "label": opt,
                              "why": "", "evidence": ""})
            elif isinstance(opt, dict) and (opt.get("label")
                                            or opt.get("value")):
                label = str(opt.get("label") or opt.get("value"))
                chips.append({
                    "value": str(opt.get("value") or label),
                    "label": label,
                    "why": str(opt.get("why", "")),
                    "evidence": str(opt.get("evidence", ""))})
            else:
                return _err(
                    "each option needs at least a label",
                    "options are strings or "
                    "{value, label, why, evidence} dicts")
        clarify = {"slot": "agent", "question": question.strip(),
                   "options": chips}
        state.pending_question = clarify
        return {"ok": True, "ends_turn": True, "clarify": clarify}

    # ── delegate_scout ── Task (present only when wired) ─────
    def delegate_scout(question: str) -> dict[str, Any]:
        if not (question or "").strip():
            return _err("a scout needs a question",
                        "one concrete errand, e.g. \"which table "
                        "holds merchant country and what are its "
                        "join paths?\"")
        result = scout(question) if scout else {}
        # the scout's reads join the parent's sub-graph: disclosure
        # stays complete no matter who did the looking
        for card in result.get("cards_read", []):
            if card not in state.subgraph["cards_read"]:
                state.subgraph["cards_read"].append(card)
        return {"summary": result.get("summary", ""),
                "steps": result.get("steps", 0),
                "cards_read": result.get("cards_read", [])}

    # ── the registry: signatures and descriptions VERBATIM ───
    scouts = () if scout is None else (
        ToolSpec(
            name="delegate_scout",
            signature="delegate_scout(question)",
            maps_to="Task",
            description=(
                "A read-only scout explores one question and returns "
                "a summary of at most 400 tokens.\n"
                "Hard cap on its looks; it cannot write the plan, ask "
                "the user, or delegate further."),
            fn=delegate_scout),
    )
    return {spec.name: spec for spec in (
        ToolSpec(
            name="list_tables",
            signature="list_tables(domain?, lob?)",
            maps_to="Glob",
            description=(
                "Lists governed tables with one-line purpose, row "
                "count, readiness, owner.\n"
                "Use first when you don't know which table holds "
                "the concept."),
            fn=list_tables),
        ToolSpec(
            name="grep_cards",
            signature="grep_cards(pattern, "
                      "scope=all|tables|metrics|concepts)",
            maps_to="Grep",
            description=(
                "Literal/regex search across every compiled card "
                "line. Returns card id, line, [prov].\n"
                "Fast and exact. Use to find where a word, column, "
                "or code appears before reading."),
            fn=grep_cards),
        ToolSpec(
            name="read_card",
            signature="read_card(id, section?)",
            maps_to="Read",
            description=(
                "Returns a card (or one section). Every line "
                "carries its witness tag.\n"
                "Read a table card before touching its columns; "
                "read a metric card before using it."),
            fn=read_card),
        ToolSpec(
            name="search_semantics",
            signature="search_semantics(query, kind?)",
            maps_to="ranked, fuzzy — the index, not the text",
            description=(
                "Ranked metrics/concepts/joins/vocab/values with "
                "status, support, agreement, aliases — and business "
                "areas: a query naming a line of business comes back "
                "as the area itself, not its furniture.\n"
                "Use for meaning (\"spend\", \"SMB\"); use "
                "grep_cards for exact tokens."),
            fn=search_semantics),
        ToolSpec(
            name="list_metrics",
            signature="list_metrics(filter?)",
            maps_to="the governed catalog",
            description=(
                "Every governed metric — id, label, status, table, "
                "business area, domain, support — filterable by a "
                "business area (\"GMNS\"), a status (\"certified\"), "
                "or label words.\n"
                "\"Give me all X metrics\" starts HERE, not at "
                "tables: business words name areas of the map, not "
                "furniture."),
            fn=list_metrics),
        ToolSpec(
            name="resolve",
            signature="resolve(text, table?)",
            maps_to="the deterministic binder, callable mid-loop",
            description=(
                "Binds words to governed metrics/concepts with "
                "confidence + candidates. Never guesses;\n"
                "returns ambiguities you may settle by evidence or "
                "by asking."),
            fn=resolve),
        ToolSpec(
            name="sample_values",
            signature="sample_values(table, column, n=20)",
            maps_to="observed domain",
            description=(
                "Observed low-cardinality domain, never live. Call "
                "before writing any filter literal."),
            fn=sample_values),
        ToolSpec(
            name="get_join_paths",
            signature="get_join_paths(tables[])",
            maps_to="join topology",
            description=(
                "Tiered: certified / witnessed / candidate / none. "
                "Call before any join."),
            fn=get_join_paths),
        ToolSpec(
            name="get_definition_line",
            signature="get_definition_line(metric, variant?)",
            maps_to="disclosure",
            description=(
                "The one-sentence disclosure every answer must "
                "carry: which definition, whose authority, on the "
                "meridian or off it."),
            fn=get_definition_line),
        ToolSpec(
            name="run_sql",
            signature="run_sql(sql, mode=dry_run|snapshot, limit=200)",
            maps_to="Bash, fenced",
            description=(
                "Validates, then dry-runs or runs on the frozen "
                "snapshot under cost gates and ACL.\n"
                "Returns schema, rows, bytes. Errors teach: unknown "
                "column → the 3 closest real ones."),
            fn=run_sql),
        ToolSpec(
            name="plan_set",
            signature="plan_set(patch)",
            maps_to="TodoWrite — the ONLY state the loop writes",
            description=(
                "Update the session's semantic plan (metric, table, "
                "filters, grain, dims, time, checks).\n"
                "The plan is what gets verified and disclosed; keep "
                "it current as you learn."),
            fn=plan_set, writes=True),
        ToolSpec(
            name="note",
            signature="note(text)",
            maps_to="scratchpad",
            description=(
                "Scratchpad artifact, never in prompt after "
                "compaction. Write what you ruled out and why."),
            fn=note, writes=True),
        ToolSpec(
            name="ask_user",
            signature="ask_user(question, options[])",
            maps_to="clarify",
            description=(
                "One question, named options with evidence; ends "
                "the turn."),
            fn=ask_user, writes=True, ends_turn=True),
    ) + scouts}


def render_tool_block(kit: dict[str, ToolSpec]) -> str:
    """The tools as the system prompt shows them (§3): signature,
    description, nothing else. Identical every turn by construction,
    which is what makes it prompt-cacheable."""
    blocks = []
    for spec in kit.values():
        body = "\n".join("  " + line
                         for line in spec.description.splitlines())
        blocks.append(f"{spec.signature}\n{body}")
    return "\n\n".join(blocks)
