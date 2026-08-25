"""c(sql) — the one function every SQL-shaped thing passes through.

Meridian's identity story rests on this module: two expressions that mean
the same thing must produce the same canonical text, and therefore the
same fingerprint, regardless of who wrote them or how they formatted them.
Everything downstream — equivalence classes, the authority lattice, the
census, metric identity, the E2 verdict lattice — keys on its output.

Pinned operation order (the plan's contract; changing it bumps
CANON_RULESET and remints every fingerprint deliberately):

    1. parse (dialect=bigquery; failure → CanonError with category)
    2. strip comments (generation-time)
    3. normalize identifiers (lowercase unquoted; quoted case preserved)
    4. qualify columns from the extracted schema
       (degrades to alias-resolution only; flags qualified="partial")
    5. de-alias tables + strict single-table CTE inlining
       (pure `SELECT * FROM t` passthroughs only; self-join-safe:
       full de-alias when each alias maps to a distinct once-used table,
       canonical t1..tn renaming otherwise)
    6. constant folding (literals KEPT — predicates are literal-bearing)
    7. sort commutative operands (AND/OR sets, IN lists, col-before-literal)
    8. canonical literal rendering (numerics via parsed-value repr)
    9. generate with fixed settings (no pretty, functions lowercased)

NO CNF expansion — explosion risk on the 35K snippet corpus.

Two fingerprints per expression:
    fp_expr      identity of the exact expression, literals included
    fp_template  literals → typed placeholders (?s/?n/?d, IN-lists → ?*)
                 BEFORE operand sorting — pattern identity, arity-blind

Both embed dialect and canon_version, and the canonical text is stored
beside every fingerprint so a future sqlglot upgrade is a deliberate
remint migration, never silent drift.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.simplify import simplify

from sahs import CANON_RULESET
from sahs.canon.fingerprint import fingerprint

CANON_VERSION = f"{CANON_RULESET}:{sqlglot.__version__}"

_QUARANTINE_CATEGORIES = ("parse_error", "fragment", "dialect", "transform")


class CanonError(ValueError):
    """Canonicalization failure — carries the quarantine category."""

    def __init__(self, category: str, message: str) -> None:
        assert category in _QUARANTINE_CATEGORIES
        self.category = category
        super().__init__(message)


@dataclass
class CanonResult:
    canonical_sql: str
    fp_expr: str
    fp_template: str
    tables: list[str]
    kind: str                      # select | union | ...
    qualified: str                 # full | partial | none
    canon_version: str = CANON_VERSION
    ast: Any = field(default=None, repr=False)


# ─── helpers ─────────────────────────────────────────────────


def _arg(node: exp.Expression, key: str) -> Any:
    """sqlglot escapes python-keyword arg names (`with_`, `from_`);
    accept both spellings so the ruleset survives minor API drift."""
    return node.args.get(key, node.args.get(key + "_"))


def _set_arg(node: exp.Expression, key: str, value: Any) -> None:
    node.set(key + "_" if key + "_" in node.args else key, value)


def _table_key(t: exp.Table) -> str:
    parts = [p.name for p in (t.args.get("catalog"), t.args.get("db"))
             if p is not None]
    parts.append(t.name)
    return ".".join(parts)


def _is_pure_passthrough(cte_this: exp.Expression) -> exp.Table | None:
    """`SELECT * FROM one_real_table` and nothing else → that table."""
    select = cte_this
    if not isinstance(select, exp.Select):
        return None
    for key in ("group", "where", "joins", "limit", "qualify", "having",
                "distinct", "windows", "order"):
        if _arg(select, key):
            return None
    if len(select.expressions) != 1 \
            or not isinstance(select.expressions[0], exp.Star):
        return None
    source = _arg(select, "from")
    if source is None or not isinstance(source.this, exp.Table):
        return None
    return source.this


def _inline_passthrough_ctes(tree: exp.Expression) -> exp.Expression:
    with_ = _arg(tree, "with")
    if not isinstance(with_, exp.With):
        return tree
    replaced: dict[str, exp.Table] = {}
    kept = []
    for cte in with_.expressions:
        target = _is_pure_passthrough(cte.this)
        if target is not None:
            replaced[cte.alias_or_name.lower()] = target
        else:
            kept.append(cte)
    if not replaced:
        return tree

    def _swap(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Table) and not node.args.get("db"):
            hit = replaced.get(node.name.lower())
            if hit is not None:
                new = hit.copy()
                if node.alias:
                    new.set("alias", node.args["alias"])
                return new
        if isinstance(node, exp.Column) and node.table:
            hit = replaced.get(node.table.lower())
            if hit is not None:
                node.set("table", exp.to_identifier(hit.name))
        return node

    tree = tree.transform(_swap)
    with_ = _arg(tree, "with")
    if isinstance(with_, exp.With):
        if kept:
            with_.set("expressions", kept)
        else:
            _set_arg(tree, "with", None)
    return tree


def _dealias_tables(tree: exp.Expression) -> exp.Expression:
    """Self-join-safe de-aliasing. Each scope-free pass over the whole
    statement: if every aliased table is distinct and appears once, drop
    aliases and rewrite qualifiers to real table names; otherwise rename
    aliases canonically t1..tn in order of appearance."""
    cte_names = {c.alias_or_name.lower()
                 for w in tree.find_all(exp.With) for c in w.expressions}
    tables = [t for t in tree.find_all(exp.Table)]
    aliased = [t for t in tables if t.alias]
    if not aliased:
        return tree
    keys = [_table_key(t).lower() for t in tables
            if t.name.lower() not in cte_names]
    safe = len(keys) == len(set(keys))

    mapping: dict[str, str] = {}
    if safe:
        for t in aliased:
            mapping[t.alias.lower()] = _table_key(t).lower()
            t.set("alias", None)
    else:
        for i, t in enumerate(aliased, start=1):
            new_alias = f"t{i}"
            mapping[t.alias.lower()] = new_alias
            t.set("alias", exp.TableAlias(this=exp.to_identifier(new_alias)))

    def _requalify(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column) and node.table:
            hit = mapping.get(node.table.lower())
            if hit is not None:
                parts = hit.split(".")
                node.set("table", exp.to_identifier(parts[-1]))
                if len(parts) > 1:
                    node.set("db", exp.to_identifier(parts[-2]))
                if len(parts) > 2:
                    node.set("catalog", exp.to_identifier(parts[-3]))
        return node

    return tree.transform(_requalify)


def _flip_literal_first_comparisons(tree: exp.Expression) -> exp.Expression:
    """`5 = x` → `x = 5` (col-before-literal, pinned; EQ/NEQ only —
    ordered comparisons are not commutative)."""

    def _flip(node: exp.Expression) -> exp.Expression:
        if isinstance(node, (exp.EQ, exp.NEQ)) \
                and isinstance(node.this, exp.Literal) \
                and not isinstance(node.expression, exp.Literal):
            cls = type(node)
            return cls(this=node.expression.copy(),
                       expression=node.this.copy())
        return node

    return tree.transform(_flip)


def _sort_commutative(tree: exp.Expression) -> exp.Expression:
    """Deterministic operand order for AND/OR chains and IN lists."""

    def _sort(node: exp.Expression) -> exp.Expression:
        if isinstance(node, (exp.And, exp.Or)):
            parent = node.parent
            if isinstance(parent, (exp.And, exp.Or)) \
                    and type(parent) is type(node):
                return node          # only rebuild at the chain root
            operands = list(node.flatten())
            keyed = sorted(operands, key=lambda e: e.sql(dialect="bigquery"))
            if keyed == operands:
                return node
            builder = exp.and_ if isinstance(node, exp.And) else exp.or_
            rebuilt = keyed[0]
            for nxt in keyed[1:]:
                rebuilt = builder(rebuilt, nxt, copy=False)
            return rebuilt
        if isinstance(node, exp.In) and node.expressions:
            if len(node.expressions) == 1 and not node.args.get("query"):
                # x IN ('B')  ≡  x = 'B' — collapse so the census doesn't
                # count a phantom second class (found by fixture census)
                return exp.EQ(this=node.this.copy(),
                              expression=node.expressions[0].copy())
            node.set("expressions", sorted(
                node.expressions,
                key=lambda e: e.sql(dialect="bigquery")))
        return node

    return tree.transform(_sort)


def _canonical_numbers(tree: exp.Expression) -> exp.Expression:
    def _norm(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Literal) and node.is_number:
            raw = node.name
            try:
                value = float(raw)
                text = (str(int(value))
                        if value == int(value) and "e" not in raw.lower()
                        else repr(value))
                if text != raw:
                    return exp.Literal.number(text)
            except (ValueError, OverflowError):
                pass
        return node

    return tree.transform(_norm)


_DATE_TYPES = {"DATE", "DATETIME", "TIMESTAMP", "TIME", "INTERVAL"}


def _templatize(tree: exp.Expression) -> exp.Expression:
    """Literals → typed placeholders; IN lists collapse to one ?*."""

    def _ph(name: str) -> exp.Expression:
        return exp.Var(this=name)

    def _swap(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.In) and node.expressions:
            if all(isinstance(e, exp.Literal) for e in node.expressions):
                node.set("expressions", [_ph("?*")])
            return node
        if isinstance(node, exp.Cast) \
                and isinstance(node.this, exp.Literal) \
                and node.to.this.name.upper() in _DATE_TYPES:
            return _ph("?d")
        if isinstance(node, exp.Literal):
            parent = node.parent
            if isinstance(parent, (exp.DataType, exp.Interval)):
                return node
            return _ph("?s" if node.is_string else "?n")
        return node

    return tree.transform(_swap)


# ─── the function ────────────────────────────────────────────


def c(sql: str, *, schema: dict[str, dict[str, str]] | None = None,
      dialect: str = "bigquery") -> CanonResult:
    """Canonicalize one SQL statement. Raises CanonError on failure —
    callers quarantine with the error's category, never crash a run."""
    text = (sql or "").strip().rstrip(";")
    if not text:
        raise CanonError("fragment", "empty statement")
    try:
        tree = sqlglot.parse_one(text, read=dialect)
    except sqlglot.errors.ParseError as e:
        raise CanonError("parse_error", str(e)) from e
    if tree is None:
        raise CanonError("parse_error", "parser returned nothing")

    try:
        tree = normalize_identifiers(tree, dialect=dialect)

        qualified = "none"
        if schema:
            try:
                tree = qualify(tree, schema=schema, dialect=dialect,
                               validate_qualify_columns=False,
                               identify=False)
                qualified = "full"
            except Exception:
                qualified = "partial"

        tree = _inline_passthrough_ctes(tree)
        tree = _dealias_tables(tree)
        try:
            tree = simplify(tree, dialect=dialect)
        except Exception:
            pass                      # folding is best-effort, never fatal

        # template branch forks BEFORE sorting (pinned)
        template_tree = _templatize(tree.copy())
        template_tree = _flip_literal_first_comparisons(template_tree)
        template_tree = _sort_commutative(template_tree)

        tree = _flip_literal_first_comparisons(tree)
        tree = _sort_commutative(tree)
        tree = _canonical_numbers(tree)

        gen = dict(dialect=dialect, pretty=False, comments=False,
                   normalize_functions="lower")
        canonical_sql = tree.sql(**gen)
        template_sql = template_tree.sql(**gen)
    except CanonError:
        raise
    except Exception as e:                      # any transform blow-up
        raise CanonError("transform", f"{type(e).__name__}: {e}") from e

    cte_names = {cte.alias_or_name.lower()
                 for w in tree.find_all(exp.With) for cte in w.expressions}
    tables = sorted({
        _table_key(t).lower() for t in tree.find_all(exp.Table)
        if t.name.lower() not in cte_names})

    return CanonResult(
        canonical_sql=canonical_sql,
        fp_expr=fingerprint(canonical_sql, dialect, CANON_VERSION),
        fp_template=fingerprint(template_sql, dialect, CANON_VERSION),
        tables=tables,
        kind=type(tree).__name__.lower(),
        qualified=qualified,
        ast=tree,
    )


def try_canon(sql: str, **kwargs: Any) -> tuple[CanonResult | None,
                                                CanonError | None]:
    """The quarantine-friendly wrapper adapters use."""
    try:
        return c(sql, **kwargs), None
    except CanonError as e:
        return None, e


def wrap_predicate(fragment: str, table: str) -> str:
    """Blue-insights WHERE fragments enter the one pipeline as
    `SELECT 1 FROM <t> WHERE <pred>`."""
    return f"SELECT 1 FROM {table} WHERE {fragment}"


def wrap_case(fragment: str, table: str) -> str:
    return f"SELECT {fragment} FROM {table}"
