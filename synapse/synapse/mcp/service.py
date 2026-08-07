"""GraphService — the one implementation behind every tool surface.

Task-shaped, read-only queries over a compiled GraphStore snapshot
(MCP_SERVER_SPEC.md §1). MCP (`server.py`) and Google-ADK
(`adk_tools.py`) are thin adapters over this class, so every consumer —
Claude Desktop/Code over MCP, the in-house Gemini analyst over ADK, the
Streamlit UI in-process — sees identical answers with identical
provenance envelopes.

Design rules enforced here:
  * every fact carries `confidence_tier` + `sources` (no bare facts)
  * errors are structured (`ErrorDetail` + suggestions), never raised
  * results are bounded (`top_k` / `limit` everywhere)
  * the graph substrate is hidden — tools speak table/column/metric names,
    not canonical URIs (URIs only appear in `explain_confidence`)
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any

from synapse.graph.inspector import inspect_table as _inspect_impl
from synapse.graph.store import (
    Edge, GraphStore, Node, canonical_uri, normalize_table_name,
)
from synapse.mcp.cache import TTLCache
from synapse.mcp.envelope import ErrorDetail, ResponseMeta, SynapseResponse

_TOOL_VERSION = "1.0.0"

_SEVERITY_ORDER = {"info": 0, "warning": 1, "error": 2}


class GraphService:
    def __init__(
        self,
        store: GraphStore,
        *,
        tenant_id: str = "default",
        cache: TTLCache | None = None,
        skills: "Any | None" = None,
        llm_client: "Any | None" = None,
        overlay: "Any | None" = None,
    ) -> None:
        self.store = store
        self.tenant_id = tenant_id
        self.cache = cache or TTLCache()
        # The skills registry (business logic + guardrails) is a SEPARATE
        # source from the graph. When present, get_skill and guardrail
        # enforcement read it instead of graph nodes — so the data graph
        # carries no skill-derived nodes. Absent → fall back to graph nodes
        # (transition/back-compat).
        self.skills = skills
        # On-demand enrichment (the lazy layer): explain_column fills a
        # column with no grounded meaning using llm_client, persisting into
        # overlay. Both optional — absent, explain_column serves the
        # grounded profile and read-through cache without an LLM.
        self.llm_client = llm_client
        self.overlay = overlay

    # ─── envelope helpers ────────────────────────────────────

    def _meta(self, tool: str, started: float, *, cached: bool = False,
              warnings: list[str] | None = None) -> ResponseMeta:
        return ResponseMeta(
            tool_name=tool,
            tool_version=_TOOL_VERSION,
            snapshot_version=self.store.snapshot_version,
            latency_ms=int((time.monotonic() - started) * 1000),
            cached=cached,
            tenant_id=self.tenant_id,
            warnings=warnings or [],
        )

    def _ok(self, tool: str, started: float, data: Any, *,
            cached: bool = False, warnings: list[str] | None = None,
            partial: bool = False) -> dict[str, Any]:
        return SynapseResponse(
            status="partial" if partial else "ok", data=data,
            meta=self._meta(tool, started, cached=cached, warnings=warnings),
        ).model_dump()

    def _err(self, tool: str, started: float, code: str, message: str,
             suggestions: list[str] | None = None) -> dict[str, Any]:
        return SynapseResponse(
            status="error", data=None,
            error=ErrorDetail(code=code, message=message,  # type: ignore[arg-type]
                              suggestions=suggestions or []),
            meta=self._meta(tool, started),
        ).model_dump()

    @staticmethod
    def _prov(item: Node | Edge) -> dict[str, Any]:
        p = item.provenance
        return {
            "confidence_tier": p.confidence_tier,
            "confidence_score": round(p.confidence_score, 3),
            "sources": sorted(set(p.sources)),
        }

    # ─── name resolution ─────────────────────────────────────

    def _resolve_table(self, name: str) -> Node | None:
        """Exact URI → exact table_name → unique suffix match."""
        node = self.store.get(canonical_uri("table", name))
        if node is not None:
            return node
        lowered = name.lower()
        tables = self.store.nodes_by_type("Table")
        exact = [t for t in tables
                 if str(t.properties.get("table_name", "")).lower() == lowered]
        if len(exact) == 1:
            return exact[0]
        suffix = [
            t for t in tables
            if str(t.properties.get("table_name", "")).lower()
            .endswith(lowered)
        ]
        return suffix[0] if len(suffix) == 1 else None

    _STOPWORDS = frozenset({
        "the", "a", "an", "of", "and", "or", "not", "no", "do", "does",
        "is", "are", "in", "for", "to", "on", "by", "with", "per", "this",
        "that", "what", "which", "how",
    })

    @classmethod
    def _tokens(cls, text: str) -> set[str]:
        return {cls._stem(t) for t in
                "".join(c if c.isalnum() else " " for c in text.lower()).split()
                if len(t) > 1 and t not in cls._STOPWORDS}

    @staticmethod
    def _stem(token: str) -> str:
        """Light plural fold so 'merchants' meets 'merchant'. Applied to
        BOTH query and haystack tokens — consistency, not linguistics,
        is what makes bag-of-words equality survive the transform."""
        if len(token) > 4 and token.endswith("ies"):
            return token[:-3] + "y"
        if len(token) > 3 and token.endswith("s") and not token.endswith("ss"):
            return token[:-1]
        return token

    def _score_node(self, node: Node, query_tokens: set[str]) -> float:
        """Deterministic lexical scoring — BM25-lite over name-ish fields.

        Beyond names: the behavioral vocabulary the catalogs carry — the
        curated question a DMP metric answers, the group-by/filter
        patterns analysts actually used, a business unit's derived
        vocabulary — because that is how askers phrase things, which
        names alone never cover."""
        props = node.properties
        haystacks = [
            (str(props.get(k, "")), w) for k, w in (
                ("table_name", 3.0), ("business_name", 2.5),
                ("skill_id", 3.0), ("surface_form", 3.0),
                ("name", 3.0), ("canonical_entity", 2.0), ("rule", 2.0),
                ("column", 2.0), ("business_unit", 2.0),
                ("question_answered", 1.5),
                ("description", 1.0), ("domain", 1.0),
                ("sub_domain", 1.0), ("data_domain", 1.0),
            )
        ]
        name_tail = node.canonical_uri.rsplit("/", 1)[-1]
        haystacks.append((name_tail.replace("_", " "), 2.5))
        for syn in props.get("synonyms", []) or []:
            haystacks.append((str(syn), 2.5))
        for key, weight in (("group_by_patterns", 1.0),
                            ("common_filters", 1.0), ("vocabulary", 1.0),
                            ("member_tables", 1.0), ("top_metrics", 1.0),
                            ("question_bank", 1.5)):
            values = props.get(key) or []
            if values:
                haystacks.append(
                    (" ".join(str(v) for v in values), weight))
        score = 0.0
        for text, weight in haystacks:
            if not text:
                continue
            tokens = self._tokens(text)
            overlap = query_tokens & tokens
            if overlap:
                score += weight * len(overlap) / max(len(query_tokens), 1)
            if query_tokens and text.lower().replace("_", " ").strip() == " ".join(
                    sorted(query_tokens)):
                score += 1.0
        # small provenance boost so corroborated facts outrank one-source ones
        score *= 1.0 + 0.1 * min(len(node.provenance.sources), 4)
        return score

    # ─── 1. discovery & search ───────────────────────────────

    def search_entities(self, query: str, top_k: int = 10,
                        node_kinds: list[str] | None = None,
                        business_unit: str = "") -> dict[str, Any]:
        """Resolve a business term to graph objects (tables, columns,
        metrics, synonyms, skills, guardrails, business units). First
        call for any term whose binding isn't obvious. ``business_unit``
        narrows to one segment: only nodes carrying that unit (or the
        unit node itself, or columns of its tables) are scored."""
        started = time.monotonic()
        if not query or not query.strip():
            return self._err("search_entities", started, "invalid_input",
                             "query must be a non-empty string")
        kinds = set(node_kinds) if node_kinds else None
        query_tokens = self._tokens(query)
        bu_filter = business_unit.strip().lower()
        bu_tables: set[str] | None = None
        if bu_filter:
            bu_tables = {
                str(t.properties.get("table_name", "")).lower()
                for t in self.store.nodes_by_type("Table")
                if bu_filter in str(
                    t.properties.get("business_unit", "")).lower()
            }
        hits: list[tuple[float, Node]] = []
        for node in self.store.nodes.values():
            if kinds and node.node_type not in kinds:
                continue
            if bu_filter and not self._in_business_unit(
                    node, bu_filter, bu_tables or set()):
                continue
            score = self._score_node(node, query_tokens)
            # A business unit aggregates its members' vocabulary, so it
            # matches almost anything about them — in FLAT search the
            # precise fact must outrank its container. Dampen units here
            # only; route_question scores them undamped (routing is
            # exactly the container question). Exception: the query IS
            # the unit's name — asked for the container, get the
            # container.
            if node.node_type == "BusinessUnit" and query_tokens \
                    != self._tokens(str(node.properties.get("name", ""))):
                score *= 0.5
            if score >= 0.2:  # floor: a single weak token hit is noise
                hits.append((score, node))
        hits.sort(key=lambda pair: (-pair[0], pair[1].canonical_uri))
        top = [{
            "kind": n.node_type,
            "name": n.properties.get("table_name")
            or n.properties.get("skill_id")
            or n.properties.get("surface_form")
            or n.properties.get("rule")
            or n.properties.get("name")
            or n.canonical_uri.rsplit("/", 1)[-1],
            "business_name": n.properties.get("business_name", ""),
            "business_unit": str(n.properties.get("business_unit", "")),
            "uri": n.canonical_uri,
            "summary": (str(n.properties.get("description")
                            or n.properties.get("rule")
                            or n.properties.get("formula") or ""))[:160],
            "score": round(score, 3),
            **self._prov(n),
        } for score, n in hits[:max(1, min(top_k, 50))]]
        if not top:
            return self._err(
                "search_entities", started, "not_found",
                f"nothing in the graph matches {query!r}"
                + (f" within business unit {business_unit!r}" if bu_filter
                   else ""),
                suggestions=["try a shorter term",
                             "route_question() to find the right segment",
                             "list_tables_for_domain() to browse"],
            )
        return self._ok("search_entities", started, {"hits": top})

    @staticmethod
    def _in_business_unit(node: Node, bu_lower: str,
                          bu_table_names: set[str]) -> bool:
        """Does a node belong to the filtered business unit? Tables and
        metrics carry the label; the unit node matches by name; columns
        belong through their table. Nodes with no BU affiliation at all
        (entities, synonyms…) are excluded — that is what a filter means."""
        props = node.properties
        own = str(props.get("business_unit", "")).lower()
        if own and bu_lower in own:
            return True
        if node.node_type == "BusinessUnit":
            return bu_lower in str(props.get("name", "")).lower()
        table = str(props.get("table_name", "")
                    or props.get("sourced_from_table", "")).lower()
        return bool(table) and table in bu_table_names

    def list_tables_for_domain(self, data_domain: str = "",
                               company_domain: str = "",
                               tag: str = "",
                               business_unit: str = "") -> dict[str, Any]:
        """Browse tables by MDM domain taxonomy, tag, or business unit.
        Not a free-text search — use search_entities for that."""
        started = time.monotonic()
        rows = []
        for t in self.store.nodes_by_type("Table"):
            props = t.properties
            if data_domain and data_domain.lower() not in str(
                    props.get("data_domain", "")).lower():
                continue
            if company_domain and company_domain.lower() not in str(
                    props.get("company_domain", "")).lower():
                continue
            if tag and tag not in (props.get("tags") or []):
                continue
            if business_unit and business_unit.lower() not in str(
                    props.get("business_unit", "")).lower():
                continue
            rows.append({
                "table": props.get("table_name", ""),
                "fqn": props.get("fqn", ""),
                "business_name": props.get("business_name", ""),
                "business_unit": str(props.get("business_unit", "")),
                "asset_kind": props.get("asset_kind", "Table"),
                "n_columns": len(self.store.outgoing(t.canonical_uri, "CONTAINS")),
                **self._prov(t),
            })
        rows.sort(key=lambda r: r["table"])
        return self._ok("list_tables_for_domain", started, {"tables": rows})

    def route_question(self, question: str,
                       top_units: int = 3) -> dict[str, Any]:
        """Segment-first routing: which business unit is this question
        about, and what lives there? Ranks BusinessUnit nodes against the
        question, then scores tables/metrics WITHIN the winning units and
        attaches the skill playbooks that cover them. Call FIRST for any
        broad or ambiguous question; then search_entities with the
        ``business_unit`` filter, or the unit's tables directly.

        Falls back to flat search hits when the snapshot has no
        business-unit rollup — honest about which mode answered."""
        started = time.monotonic()
        if not question or not question.strip():
            return self._err("route_question", started, "invalid_input",
                             "question must be a non-empty string")
        tokens = self._tokens(question)
        units = self.store.nodes_by_type("BusinessUnit")
        if not units:
            flat = self.search_entities(question, top_k=5)
            return self._ok("route_question", started, {
                "mode": "flat_search",
                "note": ("no business-unit rollup in this snapshot — "
                         "returning flat search hits; run the pipeline "
                         "rollup stage to enable segment routing"),
                "hits": (flat.get("data") or {}).get("hits", []),
            })
        scored = sorted(
            ((self._score_node(u, tokens), u) for u in units),
            key=lambda pair: (-pair[0], pair[1].canonical_uri))
        scored = [(s, u) for s, u in scored if s > 0]
        if not scored:
            # zero lexical signal on every unit — routing would be a
            # coin toss dressed as an answer. Fall back honestly.
            flat = self.search_entities(question, top_k=5)
            return self._ok("route_question", started, {
                "mode": "flat_search",
                "note": ("no business unit matches this phrasing — "
                         "returning flat search hits instead of a "
                         "guessed segment"),
                "hits": (flat.get("data") or {}).get("hits", []),
            })
        routed = []
        for score, unit in scored[:max(1, min(top_units, 10))]:
            name = str(unit.properties.get("name", ""))
            members = [
                self.store.get(e.to_uri)
                for e in self.store.outgoing(unit.canonical_uri, "CONTAINS")
            ]
            members = [m for m in members if m is not None]
            top_tables = sorted(
                members,
                key=lambda t: (-self._score_node(t, tokens),
                               str(t.properties.get("table_name"))))[:5]
            member_names = {
                normalize_table_name(str(t.properties.get(
                    "table_name", ""))) for t in members}
            metrics = [
                m for m in self.store.nodes_by_type("Metric")
                if normalize_table_name(str(m.properties.get(
                    "sourced_from_table", ""))) in member_names]
            top_metrics = sorted(
                metrics,
                key=lambda m: (-self._score_node(m, tokens),
                               -int(m.properties.get("execution_count")
                                    or 0), m.canonical_uri))[:5]
            routed.append({
                "business_unit": name,
                "uri": unit.canonical_uri,
                "score": round(score, 3),
                "description": str(unit.properties.get("description", "")),
                **self._prov(unit),
                "top_tables": [{
                    "table": str(t.properties.get("table_name", "")),
                    "business_name": str(
                        t.properties.get("business_name", "")),
                    "match": round(self._score_node(t, tokens), 3),
                    "confidence_tier": t.provenance.confidence_tier,
                } for t in top_tables],
                "top_metrics": [{
                    "metric": str(m.properties.get("business_name")
                                  or m.canonical_uri.rsplit("/", 1)[-1]),
                    "table": str(
                        m.properties.get("sourced_from_table", "")),
                    "match": round(self._score_node(m, tokens), 3),
                } for m in top_metrics],
                "skills": self._skills_for_unit(name, member_names),
            })
        return self._ok("route_question", started, {
            "mode": "segment_routing",
            "units": routed,
            "next": ("inspect the winning unit's tables, or "
                     "search_entities(query, business_unit=<name>) "
                     "to stay inside it"),
        })

    def _skills_for_unit(self, unit_name: str,
                         member_tables: set[str]) -> list[dict[str, Any]]:
        """Skill playbooks covering a business unit — matched by an
        explicit company_domain on the bundle, or by the bundle's
        tables_used landing inside the unit. Registry (files) preferred;
        graph Skill nodes with APPLIES_TO edges are the fallback."""
        unit_l = unit_name.lower()
        out: list[dict[str, Any]] = []
        if self.skills is not None:
            for b in getattr(self.skills, "skills", []):
                declared = str(b.get("company_domain", "")).lower()
                tables = {normalize_table_name(str(t))
                          for t in (b.get("tables_used") or [])}
                if ((declared and (declared in unit_l
                                   or unit_l in declared))
                        or tables & member_tables):
                    out.append({
                        "skill_id": b.get("skill_id"),
                        "domain": b.get("domain", ""),
                        "description": str(b.get("description", ""))[:160],
                    })
            return out
        for s in self.store.nodes_by_type("Skill"):
            applies = set()
            for e in self.store.outgoing(s.canonical_uri, "APPLIES_TO"):
                target = self.store.get(e.to_uri)
                if target is not None:
                    applies.add(normalize_table_name(str(
                        target.properties.get("table_name", ""))))
            if applies & member_tables:
                out.append({
                    "skill_id": s.properties.get("skill_id")
                    or s.canonical_uri.rsplit("/", 1)[-1],
                    "domain": s.properties.get("domain", ""),
                    "description": str(
                        s.properties.get("description", ""))[:160],
                })
        return out

    # ─── 2. table-centric inspection ─────────────────────────

    def inspect_table(self, table: str, include: list[str] | None = None,
                      column_limit: int = 50) -> dict[str, Any]:
        """Full structured view of one table (identity, columns,
        governance by default; metrics/related/lineage/dq/per_source
        opt-in via `include`). Resolve the name via search_entities first."""
        started = time.monotonic()
        node = self._resolve_table(table)
        if node is None:
            return self._err(
                "inspect_table", started, "not_found",
                f"table {table!r} not in graph",
                suggestions=[f"search_entities(query={table!r}, "
                             "node_kinds=['Table'])"],
            )
        table_name = str(node.properties.get("table_name") or table)
        cache_key = ("inspect_table", table_name,
                     tuple(sorted(include or [])), column_limit)
        cached = self.cache.get(cache_key)
        if cached is not None:
            return self._ok("inspect_table", started, cached, cached=True)
        raw = _inspect_impl(self.store, table_name)
        if "error" in raw:
            return self._err("inspect_table", started, "not_found",
                             f"table {table!r} not in graph")
        sections = set(include or ["identity", "columns", "governance"])
        sections.add("fused_view")
        trimmed = {k: v for k, v in raw.items()
                   if k in sections or k == "table"}
        if isinstance(trimmed.get("columns"), list):
            trimmed["columns"] = trimmed["columns"][:max(1, column_limit)]
        # guardrails that constrain this table always ride along — an agent
        # must not have to remember to ask. Sourced from the registry (files)
        # when wired, else the graph nodes (fallback).
        if self.skills is not None:
            trimmed["guardrails"] = self.skills.guardrails_for(table_name)
        else:
            trimmed["guardrails"] = self._guardrails_for_uris(
                {node.canonical_uri},
                prefixes={canonical_uri("column", table_name) + "/"},
            )
        self.cache.set(cache_key, trimmed, ttl_seconds=300)
        return self._ok("inspect_table", started, trimmed)

    def get_filter_values(self, table: str, column: str,
                          limit: int = 20) -> dict[str, Any]:
        """Observed values for a column (corpus + profiling) — call before
        emitting any WHERE col='X' predicate."""
        started = time.monotonic()
        node = self._resolve_table(table)
        if node is None:
            return self._err("get_filter_values", started, "not_found",
                             f"table {table!r} not in graph")
        table_name = str(node.properties.get("table_name") or table)
        values = []
        prefix = canonical_uri("filtervalue", table_name, column) + "/"
        for fv in self.store.nodes_by_type("FilterValue"):
            if not fv.canonical_uri.startswith(prefix):
                continue
            values.append({
                "raw_value": fv.properties.get("value", ""),
                "observation_count": fv.properties.get("count_obs", 0),
                "is_structural": bool(fv.properties.get("is_structural")),
                **self._prov(fv),
            })
        values.sort(key=lambda v: (-v["observation_count"], v["raw_value"]))
        col_node = self.store.get(canonical_uri("column", table_name, column))
        sample = (col_node.properties.get("distinct_sample") or []
                  if col_node else [])
        return self._ok("get_filter_values", started, {
            "table": table_name, "column": column,
            "values": values[:max(1, limit)],
            "profiled_sample": sample[:max(1, limit)],
            "cardinality_bucket": (col_node.properties.get(
                "cardinality_bucket", "unknown") if col_node else "unknown"),
        })

    def resolve_code(self, column: str, raw_value: str,
                     table_hint: str = "") -> dict[str, Any]:
        """Decode a coded value ('005' ↔ 'Platinum') via CodeMapping
        nodes mined from CASE WHENs and lookup tables."""
        started = time.monotonic()
        matches = []
        want_value = raw_value.strip().lower()
        for cm in self.store.nodes_by_type("CodeMapping"):
            props = cm.properties
            if str(props.get("column", "")).lower() != column.lower():
                continue
            raw = str(props.get("raw_value", ""))
            meaning = str(props.get("human_meaning", ""))
            if want_value in (raw.lower(), meaning.lower()):
                matches.append({
                    "raw_value": raw, "human_meaning": meaning,
                    "mapping_source": props.get("source", ""),
                    **self._prov(cm),
                })
        if not matches:
            return self._err(
                "resolve_code", started, "not_found",
                f"no code mapping for {column}={raw_value!r}",
                suggestions=[f"get_filter_values(table=..., column={column!r})"],
            )
        return self._ok("resolve_code", started,
                        {"resolved": matches[0], "alternates": matches[1:5]})

    # ─── 3. relationships & lineage ──────────────────────────

    def _join_index(self) -> dict[str, list[dict[str, Any]]]:
        index: dict[str, list[dict[str, Any]]] = {}
        for edge in self.store.edges.values():
            if edge.edge_type != "EQUIVALENT_TO":
                continue
            l_node, r_node = (self.store.get(edge.from_uri),
                              self.store.get(edge.to_uri))
            if l_node is None or r_node is None:
                continue
            l_table = str(l_node.properties.get("table_name", ""))
            r_table = str(r_node.properties.get("table_name", ""))
            if not l_table or not r_table or l_table == r_table:
                continue
            hop = {
                "left_table": l_table,
                "left_column": edge.from_uri.rsplit("/", 1)[-1],
                "right_table": r_table,
                "right_column": edge.to_uri.rsplit("/", 1)[-1],
                "join_type": edge.properties.get("join_type", "INNER"),
                "n_observations": sum(
                    edge.provenance.evidence_count_by_source.values()),
                **self._prov(edge),
            }
            index.setdefault(l_table.lower(), []).append(hop)
            index.setdefault(r_table.lower(), []).append({
                **hop,
                "left_table": r_table, "left_column": hop["right_column"],
                "right_table": l_table, "right_column": hop["left_column"],
            })
        return index

    def get_join_path(self, from_table: str, to_table: str,
                      max_hops: int = 3) -> dict[str, Any]:
        """Ranked join paths between two tables from OBSERVED joins
        (EQUIVALENT_TO evidence), most-corroborated first."""
        started = time.monotonic()
        src, dst = self._resolve_table(from_table), self._resolve_table(to_table)
        if src is None or dst is None:
            missing = from_table if src is None else to_table
            return self._err("get_join_path", started, "not_found",
                             f"table {missing!r} not in graph")
        src_name = str(src.properties.get("table_name")).lower()
        dst_name = str(dst.properties.get("table_name")).lower()
        index = self._join_index()
        paths: list[list[dict[str, Any]]] = []
        queue: deque[tuple[str, list[dict[str, Any]]]] = deque([(src_name, [])])
        while queue and len(paths) < 5:
            current, hops = queue.popleft()
            if len(hops) >= max_hops:
                continue
            for hop in index.get(current, []):
                nxt = hop["right_table"].lower()
                if any(h["left_table"].lower() == nxt
                       or h["right_table"].lower() == nxt
                       for h in hops) or nxt == src_name:
                    continue
                chain = hops + [hop]
                if nxt == dst_name:
                    paths.append(chain)
                else:
                    queue.append((nxt, chain))
        if not paths:
            return self._err(
                "get_join_path", started, "not_found",
                f"no observed join path {from_table} → {to_table} "
                f"within {max_hops} hops",
                suggestions=["get_lineage() to check shared upstreams",
                             "surface this to the user — do NOT invent a join"],
            )
        ranked = sorted(paths, key=lambda p: (
            len(p), -min(h["n_observations"] for h in p)))
        return self._ok("get_join_path", started, {"paths": [{
            "hops": p,
            "total_observations": min(h["n_observations"] for h in p),
        } for p in ranked]})

    def get_lineage(self, table: str, direction: str = "both",
                    depth: int = 2) -> dict[str, Any]:
        """Declared lineage (UPSTREAM_OF edges from MDM/BQ), distinct
        from observed join paths."""
        started = time.monotonic()
        node = self._resolve_table(table)
        if node is None:
            return self._err("get_lineage", started, "not_found",
                             f"table {table!r} not in graph")

        def walk(start: str, upstream: bool) -> list[dict[str, Any]]:
            seen, out = {start}, []
            frontier = [(start, 0)]
            while frontier:
                uri, dist = frontier.pop()
                if dist >= depth:
                    continue
                edges = (self.store.incoming(uri, "UPSTREAM_OF") if upstream
                         else self.store.outgoing(uri, "UPSTREAM_OF"))
                for edge in edges:
                    other = edge.from_uri if upstream else edge.to_uri
                    if other in seen:
                        continue
                    seen.add(other)
                    other_node = self.store.get(other)
                    out.append({
                        "table": (other_node.properties.get("table_name")
                                  if other_node else other.rsplit("/", 1)[-1]),
                        "hops_from_origin": dist + 1,
                        **self._prov(edge),
                    })
                    frontier.append((other, dist + 1))
            return out

        data: dict[str, Any] = {}
        if direction in ("upstream", "both"):
            data["upstream"] = walk(node.canonical_uri, upstream=True)
        if direction in ("downstream", "both"):
            data["downstream"] = walk(node.canonical_uri, upstream=False)
        return self._ok("get_lineage", started, data)

    # ─── 4. metrics & skills ─────────────────────────────────

    def get_metric(self, name_or_synonym: str) -> dict[str, Any]:
        """Canonical formula + grain for a named metric ('approval rate',
        'C-30'). Resolves synonyms; never invent a formula the graph
        doesn't have."""
        started = time.monotonic()
        want = name_or_synonym.strip().lower()
        want_tokens = self._tokens(name_or_synonym)
        scored: list[tuple[float, Node]] = []
        for m in self.store.nodes_by_type("Metric"):
            name_tail = m.canonical_uri.rsplit("/", 1)[-1]
            names = {name_tail.lower(),
                     str(m.properties.get("business_name", "")).lower()}
            names |= {str(s).lower() for s in m.properties.get("synonyms") or []}
            if want in names:
                scored.append((10.0, m))
                continue
            score = self._score_node(m, want_tokens)
            if score > 0:
                scored.append((score, m))
        if not scored:
            return self._err(
                "get_metric", started, "not_found",
                f"no metric matches {name_or_synonym!r}",
                suggestions=["search_entities(query=..., node_kinds=['Metric'])"],
            )
        scored.sort(key=lambda pair: (-pair[0], pair[1].canonical_uri))

        def render(m: Node) -> dict[str, Any]:
            skill_edges = self.store.outgoing(m.canonical_uri, "DEFINED_BY")
            return {
                "technical_name": m.canonical_uri.rsplit("/", 1)[-1],
                "business_name": m.properties.get("business_name", ""),
                "formula": m.properties.get("formula", ""),
                "grain": m.properties.get("grain", ""),
                "sourced_from_table": m.properties.get("sourced_from_table", ""),
                "synonyms": m.properties.get("synonyms", []),
                "defined_by_skill": [
                    e.to_uri.rsplit("/", 1)[-1] for e in skill_edges],
                **self._prov(m),
            }

        best_score = scored[0][0]
        ambiguous = [m for s, m in scored[1:4] if s >= best_score * 0.8]
        return self._ok("get_metric", started, {
            "metric": render(scored[0][1]),
            "candidates_if_ambiguous": [render(m) for m in ambiguous],
        })

    def get_skill(self, topic: str) -> dict[str, Any]:
        """Fetch the curated skill package covering a topic/table/metric —
        the business-logic playbook for HOW to answer a class of question.
        Sourced from the skills registry (files), never the graph."""
        started = time.monotonic()
        if self.skills is not None:
            bundle = self.skills.find_skill(topic)
            if bundle is None:
                return self._err(
                    "get_skill", started, "not_found",
                    f"no skill covers {topic!r}")
            others = [b for b in self.skills.skills if b is not bundle]
            return self._ok("get_skill", started, {
                "skill": bundle,
                "guardrails": [g for g in self.skills.guardrails
                               if g.get("skill_id") == bundle.get("skill_id")],
                "alternates": [
                    {"skill_id": b.get("skill_id", ""),
                     "description": b.get("description", "")}
                    for b in others[:2]],
            })
        # ── graph-node fallback (no registry wired) ──
        want_tokens = self._tokens(topic)
        scored = []
        for s in self.store.nodes_by_type("Skill"):
            extra = " ".join([str(s.properties.get("domain", "")),
                              " ".join(s.properties.get("tables_used") or []),
                              " ".join(s.properties.get("metrics_defined") or [])])
            score = self._score_node(s, want_tokens)
            score += 1.5 * len(want_tokens & self._tokens(extra)) / max(
                len(want_tokens), 1)
            if score > 0:
                scored.append((score, s))
        if not scored:
            return self._err(
                "get_skill", started, "not_found",
                f"no skill covers {topic!r}",
                suggestions=["search_entities(query=..., node_kinds=['Skill'])"],
            )
        scored.sort(key=lambda pair: (-pair[0], pair[1].canonical_uri))
        skill = scored[0][1]
        skill_id = str(skill.properties.get("skill_id", ""))
        guardrails = [
            {**g.properties, **self._prov(g)}
            for g in self.store.nodes_by_type("Guardrail")
            if str(g.properties.get("skill_id")) == skill_id
        ]
        return self._ok("get_skill", started, {
            "skill": {**skill.properties, **self._prov(skill)},
            "guardrails": guardrails,
            "alternates": [
                {"skill_id": s.properties.get("skill_id", ""),
                 "description": s.properties.get("description", "")}
                for _, s in scored[1:3]
            ],
        })

    # ─── 5. trust, quality, guardrails ───────────────────────

    def _guardrails_for_uris(
        self, uris: set[str], prefixes: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Guardrails whose CONSTRAINS edge hits any URI (or URI prefix —
        catches column targets that never got a CONTAINS edge)."""
        out = []
        for g in self.store.nodes_by_type("Guardrail"):
            for edge in self.store.outgoing(g.canonical_uri, "CONSTRAINS"):
                if edge.to_uri in uris or any(
                        edge.to_uri.startswith(p) for p in (prefixes or ())):
                    out.append({**g.properties, **self._prov(g)})
                    break
        return out

    def get_guardrails(self, target: str) -> dict[str, Any]:
        """Every guardrail constraining a table/column/metric. Call before
        generating SQL that touches the target. Sourced from the skills
        registry (files) when wired — enforcement never depends on skill
        nodes being in the data graph."""
        started = time.monotonic()
        if self.skills is not None:
            rails = self.skills.guardrails_for(target)
            rails.sort(key=lambda r: (-_SEVERITY_ORDER.get(
                str(r.get("severity", "warning")), 1), str(r.get("rule", ""))))
            # ok even when empty — a table with no guardrails is not an error,
            # and validate_sql_plan must still iterate the other tables.
            return self._ok("get_guardrails", started,
                            {"target": target, "guardrails": rails})
        # ── graph-node fallback (no registry wired) ──
        uris: set[str] = set()
        prefixes: set[str] = set()
        node = self._resolve_table(target)
        table_name = target
        if node is not None:
            table_name = str(node.properties.get("table_name"))
            uris.add(node.canonical_uri)
            # columns of the table + metrics computed from it
            for edge in self.store.outgoing(node.canonical_uri, "CONTAINS"):
                uris.add(edge.to_uri)
            for edge in self.store.incoming(node.canonical_uri, "COMPUTED_FROM"):
                uris.add(edge.from_uri)
        # Match guardrails by the NAME-derived canonical URI so enforcement
        # never depends on the target Table node existing in the graph — a
        # guardrail-only table (no data witness) must still be protected.
        # Canonical URIs are computed from names at ingest time, so a
        # CONSTRAINS edge to synapse://table/<name> (or a column under it)
        # is found whether or not the node was minted.
        uris.add(canonical_uri("table", table_name))
        prefixes.add(canonical_uri("column", table_name) + "/")
        if "." in target:
            head, _, col = target.rpartition(".")
            uris.add(canonical_uri("column", head, col))
        for m in self.store.nodes_by_type("Metric"):
            if m.canonical_uri.rsplit("/", 1)[-1] == target.lower():
                uris.add(m.canonical_uri)
        rails = self._guardrails_for_uris(uris, prefixes)
        if not rails and node is None:
            return self._err("get_guardrails", started, "not_found",
                             f"{target!r} did not resolve to any graph object")
        rails.sort(key=lambda r: (-_SEVERITY_ORDER.get(
            str(r.get("severity", "warning")), 1), str(r.get("rule", ""))))
        return self._ok("get_guardrails", started,
                        {"target": target, "guardrails": rails})

    def get_dq_status(self, table: str,
                      min_severity: str = "warning") -> dict[str, Any]:
        """Data-quality rules attached to a table; check before trusting
        an aggregate."""
        started = time.monotonic()
        node = self._resolve_table(table)
        if node is None:
            return self._err("get_dq_status", started, "not_found",
                             f"table {table!r} not in graph")
        floor = _SEVERITY_ORDER.get(min_severity, 1)
        rules = []
        for edge in self.store.outgoing(node.canonical_uri, "VALIDATED_BY"):
            rule = self.store.get(edge.to_uri)
            if rule is None:
                continue
            sev = str(rule.properties.get("severity", "warning"))
            if _SEVERITY_ORDER.get(sev, 1) < floor:
                continue
            rules.append({**rule.properties, **self._prov(rule)})
        summary = {
            "n_rules": len(rules),
            "n_fail": sum(1 for r in rules
                          if r.get("last_run_status") == "fail"),
            "n_pass": sum(1 for r in rules
                          if r.get("last_run_status") == "pass"),
            "n_unknown": sum(1 for r in rules if r.get("last_run_status")
                             not in ("pass", "fail", "warning")),
        }
        return self._ok("get_dq_status", started,
                        {"rules": rules, "summary": summary})

    def explain_confidence(self, name_or_uri: str) -> dict[str, Any]:
        """WHY a fact has its tier: contributing sources, evidence counts,
        conflicts, and what would raise it."""
        started = time.monotonic()
        node = self.store.get(name_or_uri)
        if node is None:
            resolved = self._resolve_table(name_or_uri)
            node = resolved if resolved is not None else None
        if node is None:
            for candidate in self.store.nodes.values():
                if candidate.canonical_uri.rsplit("/", 1)[-1] == \
                        name_or_uri.lower():
                    node = candidate
                    break
        if node is None:
            return self._err("explain_confidence", started, "not_found",
                             f"{name_or_uri!r} not in graph")
        p = node.provenance
        distinct = len(set(p.sources))
        raise_paths = []
        if "human_approval" not in p.sources:
            raise_paths.append("steward approval → human_asserted")
        if distinct < 4:
            raise_paths.append(
                f"corroboration from {4 - distinct} more independent "
                "source(s) → grounded")
        return self._ok("explain_confidence", started, {
            "canonical_uri": node.canonical_uri,
            "tier": p.confidence_tier,
            "score": round(p.confidence_score, 3),
            "contributing_sources": [
                {"source": s, "evidence_count":
                 p.evidence_count_by_source.get(s, 1)}
                for s in sorted(set(p.sources))
            ],
            "first_observed_at": p.first_observed_at,
            "last_observed_at": p.last_observed_at,
            "conflicts": p.conflicts,
            "what_would_raise_it": raise_paths,
        })

    def disambiguate_term(self, term: str,
                          context_query: str = "") -> dict[str, Any]:
        """Pick between competing meanings using the full question as
        context. If this can't decide, STOP and ask the user — do not loop."""
        started = time.monotonic()
        base = self.search_entities(term, top_k=8)
        if base["status"] == "error":
            return self._err("disambiguate_term", started, "not_found",
                             f"{term!r} matches nothing",
                             suggestions=["ask the user to rephrase"])
        hits = base["data"]["hits"]
        context_tokens = self._tokens(context_query) - self._tokens(term)
        rescored = []
        for hit in hits:
            bonus = 0.0
            hay = self._tokens(" ".join([
                str(hit.get("summary", "")), str(hit.get("business_name", "")),
                str(hit.get("name", "")),
            ]))
            if context_tokens:
                bonus = len(context_tokens & hay) / len(context_tokens)
            rescored.append({**hit, "score": round(hit["score"] + bonus, 3)})
        rescored.sort(key=lambda h: -h["score"])
        chosen, ambiguity = rescored[0], None
        if len(rescored) > 1 and rescored[1]["score"] >= chosen["score"] * 0.9:
            ambiguity = (
                f"{rescored[0]['name']} and {rescored[1]['name']} both "
                "plausibly match — surface the choice to the user"
            )
        return self._ok("disambiguate_term", started, {
            "chosen": None if ambiguity else chosen,
            "alternatives": rescored[1:4],
            "ambiguity_reason": ambiguity,
        })

    # ─── 6. SQL plan validation ──────────────────────────────

    def validate_sql_plan(self, sql: str,
                          dialect: str = "bigquery") -> dict[str, Any]:
        """Static pre-flight for generated SQL: parse check + enforcement
        of machine-checkable guardrails + the full must-respect list for
        every table touched. Advisory (static analysis, not proof) — but a
        `violations` entry means DO NOT run the query as written."""
        started = time.monotonic()
        try:
            import sqlglot
            from sqlglot import expressions as exp
        except ImportError:
            return self._err("validate_sql_plan", started, "internal_error",
                             "sqlglot not installed on the server")
        try:
            statements = sqlglot.parse(sql, read=dialect)
        except Exception as exc:
            return self._ok("validate_sql_plan", started, {
                "parse_ok": False, "parse_error": str(exc)[:400],
                "violations": [], "must_respect": [],
            }, partial=True)

        tables: set[str] = set()
        columns: set[str] = set()
        has_lag = False
        count_without_distinct = False
        for stmt in statements:
            if stmt is None:
                continue
            for t in stmt.find_all(exp.Table):
                if t.name:
                    parts = [p.name for p in (t.args.get("catalog"),
                                              t.args.get("db")) if p is not None]
                    tables.add(".".join([*parts, t.name]) if parts else t.name)
            for c in stmt.find_all(exp.Column):
                if c.name:
                    columns.add(c.name.lower())
            for fn in stmt.find_all(exp.Anonymous):
                if str(fn.this).lower() == "lag":
                    has_lag = True
            for w in stmt.find_all(exp.Window):
                if isinstance(w.this, exp.Lag) or (
                        isinstance(w.this, exp.Anonymous)
                        and str(w.this.this).lower() == "lag"):
                    has_lag = True
            for cnt in stmt.find_all(exp.Count):
                if not cnt.find(exp.Distinct):
                    count_without_distinct = True

        rails: list[dict[str, Any]] = []
        seen_rules: set[str] = set()
        for table in tables:
            res = self.get_guardrails(table)
            if res["status"] != "ok":
                continue
            for rail in res["data"]["guardrails"]:
                key = rail.get("rule", "")
                if key not in seen_rules:
                    seen_rules.add(key)
                    rails.append(rail)

        violations = []
        for rail in rails:
            if not rail.get("machine_checkable"):
                continue
            rule_text = str(rail.get("rule", "")).lower()
            targets = [str(t).lower() for t in rail.get("applies_to") or []]
            if rail.get("category") == "privacy":
                banned = {t.rsplit(".", 1)[-1] for t in targets} or {"never"}
                exposed = sorted(banned & columns)
                if exposed:
                    violations.append({
                        "rule": rail["rule"], "severity": rail["severity"],
                        "reason": f"forbidden column(s) referenced: {exposed}",
                    })
            elif "lag(" in rule_text and has_lag:
                violations.append({
                    "rule": rail["rule"], "severity": rail["severity"],
                    "reason": "SQL applies LAG() but the table's lags are "
                              "pre-materialized",
                })
            elif "count(distinct" in rule_text and count_without_distinct:
                violations.append({
                    "rule": rail["rule"], "severity": rail["severity"],
                    "reason": "COUNT() without DISTINCT found; this metric "
                              "requires COUNT(DISTINCT …)",
                })

        return self._ok("validate_sql_plan", started, {
            "parse_ok": True,
            "tables_referenced": sorted(tables),
            "violations": violations,
            "must_respect": [
                {"rule": r.get("rule"), "severity": r.get("severity"),
                 "category": r.get("category"),
                 "machine_checkable": r.get("machine_checkable", False)}
                for r in rails
            ],
        })

    # ─── 7. concept → columns (spec §1.1) ────────────────────

    def find_columns_for_concept(self, concept: str, table_hint: str = "",
                                 max_results: int = 25) -> dict[str, Any]:
        """Physical columns materializing a business concept ('spend',
        'delinquency bucket'). Not a synonym resolver and not a metric
        lookup — see search_entities / get_metric."""
        started = time.monotonic()
        tokens = self._tokens(concept)
        rows = []
        for col in self.store.nodes_by_type("Column"):
            table_name = str(col.properties.get("table_name", ""))
            if table_hint and table_hint.lower() not in table_name.lower():
                continue
            score = self._score_node(col, tokens)
            if score <= 0:
                continue
            props = col.properties
            role = ("identifier" if props.get("is_primary")
                    else "measure" if props.get("is_aggregated")
                    else "filter" if props.get("is_filter")
                    else "dimension")
            rows.append((score, {
                "table": table_name,
                "column": col.canonical_uri.rsplit("/", 1)[-1],
                "role": role,
                "data_type": props.get("data_type", ""),
                "description": str(props.get("description", ""))[:160],
                "why": f"lexical match on {concept!r}"
                       + (" + corpus usage" if props.get(
                           "referenced_in_corpus") else ""),
                **self._prov(col),
            }))
        rows.sort(key=lambda pair: (-pair[0], pair[1]["table"],
                                    pair[1]["column"]))
        result = [r for _, r in rows[:max(1, max_results)]]
        if not result:
            return self._err(
                "find_columns_for_concept", started, "not_found",
                f"no column matches {concept!r}",
                suggestions=["search_entities first; the concept may be a "
                             "metric or synonym"],
            )
        return self._ok("find_columns_for_concept", started,
                        {"columns": result})

    def get_entity(self, name: str) -> dict[str, Any]:
        """One business entity (Account, Card Product…): its steward-
        approved definition, which physical columns identify it, and the
        tables those columns live in. Entities are minted only by human
        approval — this is the strongest-tier object in the graph."""
        started = time.monotonic()
        entities = self.store.nodes_by_type("Entity")
        if not entities:
            return self._err(
                "get_entity", started, "not_found",
                "no entities are minted in this snapshot yet — proposals "
                "are approved via scripts/entities.py (propose → review "
                "→ apply)",
                suggestions=["search_entities(query=...)"],
            )
        want = self._tokens(name)
        best, best_score = None, 0.0
        for e in entities:
            e_name = str(e.properties.get(
                "name", e.canonical_uri.rsplit("/", 1)[-1]))
            if "".join(sorted(self._tokens(e_name))) == \
                    "".join(sorted(want)):
                best, best_score = e, 10.0
                break
            score = self._score_node(e, want)
            if score > best_score:
                best, best_score = e, score
        if best is None:
            return self._err(
                "get_entity", started, "not_found",
                f"no entity matches {name!r}; known: "
                + ", ".join(sorted(str(e.properties.get('name', ''))
                                   for e in entities)[:10]),
            )
        identified_by = []
        for edge in self.store.incoming(best.canonical_uri, "IDENTIFIES"):
            col = self.store.get(edge.from_uri)
            if col is None:
                continue
            identified_by.append({
                "column": edge.from_uri.rsplit("/", 1)[-1],
                "table": col.properties.get(
                    "table_name", edge.from_uri.rsplit("/", 2)[-2]),
                "uri": edge.from_uri,
                "provenance": self._prov(edge),
            })
        return self._ok("get_entity", started, {
            "name": best.properties.get(
                "name", best.canonical_uri.rsplit("/", 1)[-1]),
            "uri": best.canonical_uri,
            "description": best.properties.get("description", ""),
            "properties": {k: v for k, v in best.properties.items()
                           if k not in ("name", "description")},
            "provenance": self._prov(best),
            "identified_by": identified_by,
            "n_supporting_tables": len({c["table"]
                                        for c in identified_by}),
        })

    def get_steward_review_queue(self, limit: int = 20) -> dict[str, Any]:
        """The facts most in need of a human: lowest-confidence,
        fewest-witness assertions, ranked weakest first. Use when asked
        what needs review/curation, or to qualify how settled an area of
        the graph is."""
        started = time.monotonic()
        queue: list[tuple[float, dict[str, Any]]] = []
        for node in self.store.nodes.values():
            prov = node.provenance
            if prov.confidence_tier not in ("guessed", "deprecated") \
                    and set(prov.sources) != {"llm"}:
                continue
            reason = ("deprecated — superseded or retired"
                      if prov.confidence_tier == "deprecated" else
                      "single witness: llm enrichment only"
                      if set(prov.sources) == {"llm"} else
                      "unverified — no corroborating source")
            queue.append((prov.confidence_score, {
                "name": str(node.properties.get(
                    "name", node.properties.get(
                        "table_name",
                        node.canonical_uri.rsplit("/", 1)[-1]))),
                "kind": node.node_type,
                "uri": node.canonical_uri,
                "tier": prov.confidence_tier,
                "score": round(prov.confidence_score, 3),
                "sources": list(prov.sources),
                "reason": reason,
            }))
        queue.sort(key=lambda pair: (pair[0], pair[1]["uri"]))
        items = [item for _, item in queue[:max(1, min(limit, 100))]]
        return self._ok("get_steward_review_queue", started, {
            "total_in_queue": len(queue),
            "showing": len(items),
            "items": items,
        })

    def explain_column(self, table: str, column: str,
                       question: str | None = None) -> dict[str, Any]:
        """Explain what a column means. Read-through: returns the graph's
        grounded description when it has one; otherwise, if on-demand
        enrichment is available, fills it with ONE gated LLM call at capped
        (inferred) provenance and persists it so the next caller gets it
        free. No evidence → an honest 'not enough to define this', never
        invention. Use when a column's meaning isn't already clear from
        inspect_table. Always returns the grounded profile (type, range,
        nulls) even when no description can be grounded."""
        started = time.monotonic()
        from synapse.enrichment.on_demand import explain_column as _fill
        res = _fill(self.store, table, column,
                    llm_client=self.llm_client, overlay=self.overlay,
                    question=question)
        if res.get("status") == "error":
            return self._err(
                "explain_column", started, "not_found",
                res.get("reason", "column not found"),
                suggestions=[f"check the column name via "
                             f"inspect_table('{table}')"])
        return self._ok("explain_column", started, res,
                        partial=(res.get("status") == "partial"))

    def check_data_trust(self, table: str) -> dict[str, Any]:
        """Should the user be warned before relying on a number from this
        table? Bundles the governance/lifecycle/DQ facts into red flags — a
        recent breaking change, a passed recertification, deprecated columns,
        failing data-quality rules — plus PII context. Call it before
        committing a relied-on figure; surface a warning only if one fires."""
        started = time.monotonic()
        from synapse.graph.trust import assess_trust
        res = assess_trust(self.store, table)
        if res.get("status") == "error":
            return self._err(
                "check_data_trust", started, "not_found",
                res.get("reason", "table not found"),
                suggestions=[f"resolve the table via inspect_table('{table}')"])
        return self._ok("check_data_trust", started, res)

    def capture_knowledge(self, subject_type: str, subject_ref: str,
                          statement: str, actor: str = "analyst"
                          ) -> dict[str, Any]:
        """Record a trusted human's definition/correction as authoritative
        (human_asserted, credited to them) — it outranks the machine's guess
        for everyone, immediately. Use when the user asserts what a table,
        column, or entity MEANS in their world. subject_type is
        table|column|entity; for a column, subject_ref is 'table.column'."""
        started = time.monotonic()
        from synapse.graph.capture import capture_assertion
        res = capture_assertion(
            self.store, subject_type=subject_type, subject_ref=subject_ref,
            statement=statement, actor=actor, overlay=self.overlay)
        if res.get("status") == "error":
            return self._err(
                "capture_knowledge", started, "invalid_subject",
                res.get("reason", "could not record the assertion"),
                suggestions=["subject_type is table|column|entity; "
                             "for a column use 'table.column'"])
        return self._ok("capture_knowledge", started, res)


# Ordered registry — single source of truth for both transports.
TOOL_NAMES: tuple[str, ...] = (
    "search_entities",
    "list_tables_for_domain",
    "inspect_table",
    "find_columns_for_concept",
    "get_filter_values",
    "resolve_code",
    "get_join_path",
    "get_lineage",
    "get_metric",
    "get_skill",
    "get_guardrails",
    "get_dq_status",
    "explain_confidence",
    "disambiguate_term",
    "validate_sql_plan",
    "get_entity",
    "get_steward_review_queue",
    "explain_column",
    "check_data_trust",
    "capture_knowledge",
)
