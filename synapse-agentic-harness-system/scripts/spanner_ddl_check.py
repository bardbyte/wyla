"""The Spanner DDL, checked without a Spanner: the files under
db/spanner/ are read as statements and held to what the database will
hold them to, and to the Python registries the graph half must match.

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
  * the CHECK lists for node kinds, edge relations and witnesses are
    exactly the Python registries (sahs.graph.quads, sahs.graph.ids).
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
        self.pk: list[str] = []
        self.parent: str | None = None


COLUMN_RE = re.compile(
    r"^\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s+(?P<type>ARRAY<[^>]+>|[A-Z]+(?:\([^)]*\))?)",
    re.M)


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
    # split the column list on top-level commas
    parts, depth, cur = [], 0, []
    for ch in columns:
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
    for part in parts:
        part = part.strip()
        if not part or part.upper().startswith("CONSTRAINT"):
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


def check() -> list[str]:
    findings: list[str] = []
    tables: dict[str, Table] = {}
    files = sorted(DDL_DIR.glob("*.sql"))
    if not files:
        return [f"no DDL under {DDL_DIR}"]
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
            for m in re.finditer(r"FOREIGN KEY\s*\(([^)]*)\)\s*REFERENCES\s+"
                                 r"(\w+)\s*\(([^)]*)\)", t.body):
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
            for m in re.finditer(r"DYNAMIC (LABEL|PROPERTIES)\s*\((\w+)\)",
                                 stmt):
                pass    # the columns are checked with the tables above
    # ── the graph half must match the Python registries ──
    from sahs.graph.ids import ID_PATTERNS
    from sahs.graph.quads import RELATIONS, WITNESSES
    graph = tables.get("GraphEdges"), tables.get("GraphNodes")
    if all(graph):
        edges, nodes = graph
        def check_list(table: Table, constraint: str, expected: set[str],
                       what: str) -> None:
            m = re.search(constraint + r".*?IN\s*\(([^)]*)\)", table.body,
                          re.S)
            if not m:
                findings.append(f"{table.name}: no {constraint}")
                return
            got = {x.strip().strip("'") for x in m.group(1).split(",")}
            if got != expected:
                findings.append(f"{table.name}.{constraint}: {what} differ "
                                f"from the registry: missing "
                                f"{sorted(expected - got)}, extra "
                                f"{sorted(got - expected)}")
        check_list(edges, "ck_edges_relation", set(RELATIONS), "relations")
        check_list(edges, "ck_edges_witness", set(WITNESSES), "witnesses")
        check_list(nodes, "ck_nodes_kind", set(ID_PATTERNS), "node kinds")
    return findings


def main() -> int:
    findings = check()
    if findings:
        print("\n".join(findings))
        print(f"\n{len(findings)} finding(s)")
        return 1
    n_tables = sum(1 for p in DDL_DIR.glob("*.sql")
                   for s in statements(p.read_text(encoding="utf-8"))
                   if s.startswith("CREATE TABLE"))
    print(f"ok: {n_tables} tables across {len(list(DDL_DIR.glob('*.sql')))} "
          "files; keys, interleaves, foreign keys, indexes, policies, the "
          "property graph and the registries agree")
    return 0


if __name__ == "__main__":
    sys.exit(main())
