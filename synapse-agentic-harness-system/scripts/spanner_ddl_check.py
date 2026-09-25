"""The Spanner DDL, checked without a Spanner: the files under
db/spanner/ are read as statements, applied in file order to one
table model, and held to what the database will hold them to, and to
the Python registries the graph and chat halves must match.

    python scripts/spanner_ddl_check.py          # exit 1 on any finding

Checks:
  * every CREATE TABLE names a PRIMARY KEY whose columns exist;
  * an INTERLEAVE parent is a table defined earlier, and the child's
    key starts with the parent's key;
  * a FOREIGN KEY references a table and column defined earlier;
  * an index (plain, unique, null-filtered, search) names an existing
    table and existing columns; a search index's columns are TOKENLIST;
  * a ROW DELETION POLICY names a TIMESTAMP column of its own table;
  * no column is named with a GoogleSQL reserved word;
  * the property graph's node and edge tables exist and the keys are
    their primary keys;
  * an ALTER TABLE (the three forms a later file uses: DROP CONSTRAINT,
    ALTER COLUMN, ADD CONSTRAINT) names a table defined earlier, a
    constraint or column that exists, and is applied to the model, so
    every check below sees the schema as the database ends up with it;
    any other ALTER form is a finding, never silently skipped;
  * the CHECK lists for node kinds, edge relations and witnesses are
    exactly the Python registries (sahs.graph.quads, sahs.graph.ids);
  * the chat half matches its code: ChatArtifacts' type list is
    sahs.assistant.artifacts.TYPES, and ChatSessions.Model is as wide
    as the store writes (sahs.assistant.spanner_store) with no plane
    CHECK left on it — the catalog, not the schema, says which choices
    exist.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILO))
DDL_DIR = SILO / "db" / "spanner"

# GoogleSQL reserved keywords (the ones a column is likely to be named)
RESERVED = {
    "ALL", "AND", "ANY", "ARRAY", "AS", "ASC", "AT", "BETWEEN", "BY", "CASE",
    "CAST", "COLLATE", "CONTAINS", "CREATE", "CROSS", "CUBE", "CURRENT",
    "DEFAULT", "DEFINE", "DESC", "DISTINCT", "ELSE", "END", "ENUM", "ESCAPE",
    "EXCEPT", "EXCLUDE", "EXISTS", "EXTRACT", "FALSE", "FETCH", "FOLLOWING",
    "FOR", "FROM", "FULL", "GROUP", "GROUPING", "GROUPS", "HASH", "HAVING",
    "IF", "IGNORE", "IN", "INNER", "INTERSECT", "INTERVAL", "INTO", "IS",
    "JOIN", "LATERAL", "LEFT", "LIKE", "LIMIT", "LOOKUP", "MERGE", "NATURAL",
    "NEW", "NO", "NOT", "NULL", "NULLS", "OF", "ON", "OR", "ORDER", "OUTER",
    "OVER", "PARTITION", "PRECEDING", "PROTO", "RANGE", "RECURSIVE",
    "RESPECT", "RIGHT", "ROLLUP", "ROWS", "SELECT", "SET", "SOME", "STRUCT",
    "TABLESAMPLE", "THEN", "TO", "TREAT", "TRUE", "UNBOUNDED", "UNION",
    "UNNEST", "USING", "WHEN", "WHERE", "WINDOW", "WITH", "WITHIN",
}


def statements(text: str) -> list[str]:
    out, buf = [], []
    for line in text.splitlines():
        stripped = line.split("--", 1)[0] if not line.lstrip().startswith(
            "--") else ""
        if not stripped.strip():
            continue
        buf.append(stripped)
        if stripped.rstrip().endswith(";"):
            out.append("\n".join(buf).strip().rstrip(";"))
            buf = []
    if buf:
        out.append("\n".join(buf).strip())
    return out


class Table:
    def __init__(self, name: str) -> None:
        self.name = name
        self.columns: dict[str, str] = {}
        # named constraints: ck_/fk_ name → the text after the name
        # ("CHECK (...)", "FOREIGN KEY (...) REFERENCES ..."), kept
        # current through the ALTERs a later file applies
        self.constraints: dict[str, str] = {}
        self.pk: list[str] = []
        self.parent: str | None = None
        self.tail = ""
        self.body = ""


COLUMN_RE = re.compile(
    r"^\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s+(?P<type>ARRAY<[^>]+>|[A-Z]+(?:\([^)]*\))?)",
    re.M)
CONSTRAINT_RE = re.compile(r"^\s*CONSTRAINT\s+(?P<name>\w+)\s+(?P<rest>.+)$",
                           re.S | re.I)

# the ALTER TABLE forms the migration files use
ALTER_DROP_RE = re.compile(
    r"^ALTER TABLE\s+(?P<table>\w+)\s+DROP CONSTRAINT\s+(?P<name>\w+)\s*$",
    re.S)
ALTER_COLUMN_RE = re.compile(
    r"^ALTER TABLE\s+(?P<table>\w+)\s+ALTER COLUMN\s+(?P<column>\w+)\s+"
    r"(?P<spec>.+)$", re.S)
ALTER_ADD_RE = re.compile(
    r"^ALTER TABLE\s+(?P<table>\w+)\s+ADD CONSTRAINT\s+(?P<name>\w+)\s+"
    r"(?P<rest>.+)$", re.S)


def _split_top_level(text: str) -> list[str]:
    """The column list split on top-level commas."""
    parts, depth, cur = [], 0, []
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        parts.append("".join(cur))
    return parts


def parse_table(stmt: str) -> Table:
    head, body = stmt.split("(", 1)
    name = head.split()[-1]
    table = Table(name)
    depth, cols_text, i = 1, [], 0
    for i, ch in enumerate(body):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                break
        cols_text.append(ch)
    columns = "".join(cols_text)
    tail = body[i + 1:]
    for part in _split_top_level(columns):
        part = part.strip()
        if not part:
            continue
        c = CONSTRAINT_RE.match(part)
        if c:
            table.constraints[c.group("name")] = " ".join(
                c.group("rest").split())
            continue
        m = COLUMN_RE.match(part)
        if m:
            table.columns[m.group("name")] = m.group("type")
    pk = re.search(r"PRIMARY KEY\s*\(([^)]*)\)", tail)
    table.pk = [c.strip().split()[0] for c in pk.group(1).split(",")] \
        if pk else []
    parent = re.search(r"INTERLEAVE IN PARENT\s+(\w+)", tail)
    table.parent = parent.group(1) if parent else None
    table.tail = tail
    table.body = columns
    return table


def check_list_in(table: Table, constraint: str) -> set[str] | None:
    """The quoted values of a ``CHECK (Col IN (...))`` constraint by
    name, as the model holds it now; None when the table has no such
    constraint."""
    text = table.constraints.get(constraint)
    if text is None:
        return None
    m = re.search(r"IN\s*\(([^)]*)\)", text, re.S)
    if not m:
        return None
    return {x.strip().strip("'") for x in m.group(1).split(",") if x.strip()}


def _check_foreign_keys(where: str, t: Table, text: str,
                        tables: dict[str, Table], findings: list[str]) -> None:
    for m in re.finditer(r"FOREIGN KEY\s*\(([^)]*)\)\s*REFERENCES\s+"
                         r"(\w+)\s*\(([^)]*)\)", text):
        own, ref, refcols = m.group(1), m.group(2), m.group(3)
        for col in own.split(","):
            if col.strip() not in t.columns:
                findings.append(f"{where}: FK column {col.strip()} "
                                "is not a column")
        target = tables.get(ref)
        if target is None:
            findings.append(f"{where}: FK references {ref}, not "
                            "defined before it")
        else:
            for col in refcols.split(","):
                if col.strip() not in target.columns:
                    findings.append(f"{where}: FK references "
                                    f"{ref}.{col.strip()}, no such "
                                    "column")


def _apply_alter(where: str, stmt: str, tables: dict[str, Table],
                 findings: list[str]) -> None:
    """One ALTER TABLE statement onto the model: the three forms the
    migration files use; any other form is a finding."""
    flat = " ".join(stmt.split())
    m = ALTER_DROP_RE.match(flat)
    if m:
        t = tables.get(m.group("table"))
        if t is None:
            findings.append(f"{where}: ALTER on unknown table "
                            f"{m.group('table')}")
            return
        if m.group("name") not in t.constraints:
            findings.append(f"{where}: DROP CONSTRAINT {m.group('name')}: "
                            f"{t.name} has no such constraint")
            return
        del t.constraints[m.group("name")]
        return
    m = ALTER_COLUMN_RE.match(flat)
    if m:
        t = tables.get(m.group("table"))
        if t is None:
            findings.append(f"{where}: ALTER on unknown table "
                            f"{m.group('table')}")
            return
        column = m.group("column")
        if column not in t.columns:
            findings.append(f"{where}: ALTER COLUMN {column}: {t.name} has "
                            "no such column")
            return
        typed = COLUMN_RE.match(f"{column} {m.group('spec')}")
        if not typed:
            findings.append(f"{where}: ALTER COLUMN {column}: unreadable "
                            f"type in {m.group('spec')!r}")
            return
        t.columns[column] = typed.group("type")
        return
    m = ALTER_ADD_RE.match(flat)
    if m:
        t = tables.get(m.group("table"))
        if t is None:
            findings.append(f"{where}: ALTER on unknown table "
                            f"{m.group('table')}")
            return
        name = m.group("name")
        if name in t.constraints:
            findings.append(f"{where}: ADD CONSTRAINT {name}: {t.name} "
                            "already has one by that name (DROP it first)")
            return
        rest = " ".join(m.group("rest").split())
        t.constraints[name] = rest
        _check_foreign_keys(where, t, rest, tables, findings)
        return
    findings.append(f"{where}: an ALTER form this check does not read "
                    "(it knows DROP CONSTRAINT, ALTER COLUMN, ADD "
                    "CONSTRAINT)")


def load(files: list[Path]) -> tuple[dict[str, Table], list[str]]:
    """The table model after every statement of ``files``, in order,
    with the structural findings on the way."""
    findings: list[str] = []
    tables: dict[str, Table] = {}
    all_stmts: list[tuple[str, str]] = []
    for path in files:
        for stmt in statements(path.read_text(encoding="utf-8")):
            all_stmts.append((path.name, stmt))
    for fname, stmt in all_stmts:
        kind = stmt.split(None, 3)
        where = f"{fname}: {' '.join(kind[:3])}"
        if stmt.startswith("CREATE TABLE"):
            t = parse_table(stmt)
            if t.name in tables:
                findings.append(f"{where}: defined twice")
            for col in t.columns:
                if col.upper() in RESERVED:
                    findings.append(f"{where}: column {col} is a reserved word")
            if not t.pk:
                findings.append(f"{where}: no PRIMARY KEY")
            for col in t.pk:
                if col not in t.columns:
                    findings.append(f"{where}: key column {col} is not a column")
            if t.parent:
                parent = tables.get(t.parent)
                if parent is None:
                    findings.append(f"{where}: parent {t.parent} not defined "
                                    "before it")
                elif t.pk[:len(parent.pk)] != parent.pk:
                    findings.append(f"{where}: key must start with the "
                                    f"parent's key {parent.pk}")
            _check_foreign_keys(where, t, t.body, tables, findings)
            m = re.search(r"ROW DELETION POLICY\s*\(\s*OLDER_THAN\s*\(\s*(\w+)",
                          t.tail)
            if m:
                col = m.group(1)
                if not t.columns.get(col, "").startswith("TIMESTAMP"):
                    findings.append(f"{where}: row deletion policy on {col}, "
                                    "not a TIMESTAMP column")
            tables[t.name] = t
        elif re.match(r"CREATE (UNIQUE |NULL_FILTERED |SEARCH )?INDEX", stmt):
            m = re.search(r"INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]*)\)", stmt)
            if not m:
                findings.append(f"{where}: unreadable index")
                continue
            iname, tname, cols = m.groups()
            t = tables.get(tname)
            if t is None:
                findings.append(f"{where}: index {iname} on unknown table "
                                f"{tname}")
                continue
            for col in cols.split(","):
                col = col.strip().split()[0]
                if col not in t.columns:
                    findings.append(f"{where}: index {iname} names {col}, "
                                    f"not a column of {tname}")
                elif stmt.startswith("CREATE SEARCH INDEX") and \
                        t.columns[col] != "TOKENLIST":
                    findings.append(f"{where}: search index {iname} on {col}, "
                                    "not a TOKENLIST column")
            im = re.search(r"INTERLEAVE IN\s+(\w+)", stmt)
            if im and im.group(1) != t.parent:
                findings.append(f"{where}: index {iname} interleaved in "
                                f"{im.group(1)}, but {tname}'s parent is "
                                f"{t.parent}")
        elif stmt.startswith("CREATE CHANGE STREAM"):
            m = re.search(r"FOR\s+(\w+)", stmt)
            if m and m.group(1) not in tables:
                findings.append(f"{where}: change stream on unknown table "
                                f"{m.group(1)}")
        elif stmt.startswith("CREATE PROPERTY GRAPH"):
            for m in re.finditer(r"(\w+)\s+KEY\s*\(([^)]*)\)", stmt):
                tname, cols = m.group(1), [c.strip() for c in
                                           m.group(2).split(",")]
                if tname in ("SOURCE", "DESTINATION"):
                    continue                # an edge's endpoint keys
                t = tables.get(tname)
                if t is None:
                    findings.append(f"{where}: graph table {tname} unknown")
                elif cols != t.pk:
                    findings.append(f"{where}: {tname} KEY {cols} is not its "
                                    f"primary key {t.pk}")
            for m in re.finditer(r"REFERENCES\s+(\w+)\s*\(([^)]*)\)", stmt):
                t = tables.get(m.group(1))
                if t is None or [c.strip() for c in
                                 m.group(2).split(",")] != t.pk:
                    findings.append(f"{where}: {m.group(1)} reference key is "
                                    "not its primary key")
        elif stmt.startswith("ALTER TABLE"):
            _apply_alter(where, stmt, tables, findings)
    return tables, findings


def ddl_files() -> list[Path]:
    return sorted(DDL_DIR.glob("*.sql"))


def check() -> list[str]:
    files = ddl_files()
    if not files:
        return [f"no DDL under {DDL_DIR}"]
    tables, findings = load(files)

    def check_list(table: Table, constraint: str, expected: set[str],
                   what: str) -> None:
        got = check_list_in(table, constraint)
        if got is None:
            findings.append(f"{table.name}: no {constraint}")
            return
        if got != expected:
            findings.append(f"{table.name}.{constraint}: {what} differ "
                            f"from the registry: missing "
                            f"{sorted(expected - got)}, extra "
                            f"{sorted(got - expected)}")

    # ── the graph half must match the Python registries ──
    from sahs.graph.ids import ID_PATTERNS
    from sahs.graph.quads import RELATIONS, WITNESSES
    graph = tables.get("GraphEdges"), tables.get("GraphNodes")
    if all(graph):
        edges, nodes = graph
        check_list(edges, "ck_edges_relation", set(RELATIONS), "relations")
        check_list(edges, "ck_edges_witness", set(WITNESSES), "witnesses")
        check_list(nodes, "ck_nodes_kind", set(ID_PATTERNS), "node kinds")
    # ── the chat half must match its code (after every ALTER) ──
    from sahs.assistant.artifacts import TYPES
    from sahs.assistant.spanner_store import MODEL_CHOICE_CHARS
    artifacts = tables.get("ChatArtifacts")
    if artifacts is not None:
        check_list(artifacts, "ck_artifacts_type", set(TYPES),
                   "artifact types")
    sessions = tables.get("ChatSessions")
    if sessions is not None:
        wanted = f"STRING({MODEL_CHOICE_CHARS})"
        got = sessions.columns.get("Model", "")
        if got != wanted:
            findings.append(f"ChatSessions.Model is {got or 'missing'}; the "
                            f"store writes up to {MODEL_CHOICE_CHARS} "
                            f"characters ({wanted})")
        for name, text in sessions.constraints.items():
            if re.search(r"\bModel\s+IN\b", text):
                findings.append(f"ChatSessions.{name} still lists the model "
                                "choices; the catalog owns them, the "
                                "schema holds the width")
    return findings


def main() -> int:
    findings = check()
    if findings:
        print("\n".join(findings))
        print(f"\n{len(findings)} finding(s)")
        return 1
    files = ddl_files()
    tables, _ = load(files)
    print(f"ok: {len(tables)} tables across {len(files)} files; keys, "
          "interleaves, foreign keys, indexes, policies, the property "
          "graph, the ALTERs and the registries agree")
    return 0


if __name__ == "__main__":
    sys.exit(main())
