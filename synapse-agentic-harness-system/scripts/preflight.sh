#!/usr/bin/env bash
# Meridian preflight — inventory the meridian-data folder in a
# structured way, verify every loader expectation, benchmark
# canonicalization ON THIS MACHINE with the real 30-day queries, and
# print a measured per-phase time estimate for the P0→P3 run.
#
#     bash scripts/preflight.sh [~/meridian-data]
#
# Read-only: never modifies data, never prints secrets (env vars are
# reported set/unset only), never touches the network. Ends with a
# "PASTE THIS BACK" JSON block — send that block to Claude to confirm
# readiness and lock the run-time estimate.

set -u
ROOT="${1:-$HOME/meridian-data}"
SILO="$(cd "$(dirname "$0")/.." && pwd)"
PY=python3

ok()   { printf '  \033[32m✓\033[0m %s\n' "$1"; }
bad()  { printf '  \033[31m✗\033[0m %s\n' "$1"; PROBLEMS=$((PROBLEMS+1)); }
warn() { printf '  \033[33m⚠\033[0m %s\n' "$1"; WARNINGS=$((WARNINGS+1)); }
hdr()  { printf '\n\033[1m── %s ──\033[0m\n' "$1"; }
PROBLEMS=0; WARNINGS=0

[ -d "$ROOT" ] || { echo "no such data root: $ROOT"; exit 3; }
echo "Meridian preflight · data root: $ROOT"

# ── locate the three input roots (names may vary, e.g. *_patched) ──
BQ=$(find "$ROOT" -maxdepth 1 -type d -name 'real_extractions*' | head -1)
MDM=$(find "$ROOT" -maxdepth 1 -type d -name 'mdm_*' | head -1)
SRC="$ROOT/sources"

hdr "roots"
[ -n "$BQ" ]  && ok "bq archive     $BQ"      || bad "bq archive (real_extractions*) not found"
[ -n "$MDM" ] && ok "mdm archive    $MDM"     || bad "mdm archive (mdm_*) not found"
[ -d "$SRC" ] && ok "sources        $SRC"     || bad "sources/ not found"

# ── environment + toolchain ──
hdr "environment (P1+ needs these; P0 needs none)"
for v in LUMI_BQ_SA_KEY GOOGLE_APPLICATION_CREDENTIALS BQ_PROJECT_ID \
         LUMI_BQ_PROJECT GOOGLE_CLOUD_PROJECT BIGQUERY_API_BASE_URL \
         BQ_LOCATION; do
  if [ -n "${!v:-}" ]; then ok "$v is set"; else echo "    · $v unset"; fi
done
[ -n "${LUMI_BQ_SA_KEY:-}${GOOGLE_APPLICATION_CREDENTIALS:-}" ] \
  || warn "no BQ key env — P1 dry-run will exit 3 until set"
$PY --version 2>/dev/null | grep -qE '3\.(1[1-9]|[2-9][0-9])' \
  && ok "$($PY --version)" || bad "python >= 3.11 required"
SQLGLOT_OK=0
$PY - <<'EOF' >/dev/null 2>&1 && SQLGLOT_OK=1
import sqlglot
assert sqlglot.__version__.startswith("30.15")
EOF
[ "$SQLGLOT_OK" = 1 ] && ok "sqlglot 30.15.* importable" \
  || warn "sqlglot 30.15.* missing — pip install -e '.[sql]' (benchmark will be skipped)"

# ── bq archive ──
hdr "bq archive"
BLUE=0 GOLD=0 MEASURES=0 DMPN=0 GMNS=0 GLOSS=0 TERMS=0 STDTECH=0 JOBS_TOTAL=0 N_TABLES=0
if [ -n "$BQ" ]; then
  [ -f "$BQ/_batch_summary.csv" ] \
    && ok "_batch_summary.csv ($(($(wc -l < "$BQ/_batch_summary.csv")-1)) rows) — the --registry file" \
    || bad "_batch_summary.csv MISSING (needed for --registry)"
  N_TABLES=$(find "$BQ" -maxdepth 1 -type d ! -name '_*' ! -path "$BQ" | wc -l | tr -d ' ')
  ok "table directories: $N_TABLES"
  MISSING_ART=""
  for d in "$BQ"/*/; do
    t=$(basename "$d"); [ "${t#_}" != "$t" ] && continue
    for a in 00_logical_table_resource.json 02_logical_columns.csv \
             13_table_metrics.csv 16_row_access_policies.json; do
      [ -e "$d$a" ] || MISSING_ART="$MISSING_ART $t/$a"
    done
    gz="$d/17_queries_30d/jobs_30d.jsonl.gz"
    if [ -f "$gz" ]; then
      n=$(gunzip -c "$gz" 2>/dev/null | wc -l | tr -d ' ')
      JOBS_TOTAL=$((JOBS_TOTAL + n))
    fi
  done
  [ -z "$MISSING_ART" ] && ok "core artifacts (00/02/13/16) present in every table dir" \
    || warn "missing artifacts:$(echo "$MISSING_ART" | head -c 300)"
  N_JOBS_FILES=$(find "$BQ" -name 'jobs_30d.jsonl.gz' | wc -l | tr -d ' ')
  ok "jobs_30d.jsonl.gz: $N_JOBS_FILES of $N_TABLES tables · $JOBS_TOTAL jobs total (the E12 witness corpus)"
  [ "$N_JOBS_FILES" = 0 ] && warn "no raw job history — E12 mining will no-op"
  N_FAILED=$(find "$BQ" -name 'jobs_failed_queries.json' | wc -l | tr -d ' ')
  echo "    · jobs_failed_queries.json in $N_FAILED tables"
fi

# ── mdm archive ──
hdr "mdm archive"
if [ -n "$MDM" ]; then
  for f in coverage.json table_summaries.json; do
    [ -f "$MDM/$f" ] && ok "$f" || bad "$f MISSING"
  done
  M_TABLES=$(find "$MDM/tables" -maxdepth 1 -type d ! -path "$MDM/tables" 2>/dev/null | wc -l | tr -d ' ')
  ok "tables/: $M_TABLES table dirs"
  N_RESP=$(find "$MDM/tables" -name '*.json' -path '*responses*' 2>/dev/null | wc -l | tr -d ' ')
  echo "    · response files: $N_RESP"
  [ "$M_TABLES" != "$N_TABLES" ] && warn "bq tables ($N_TABLES) ≠ mdm tables ($M_TABLES)"
fi

# ── semantic sources (exact discovery names) ──
hdr "semantic sources"
count_json() { $PY - "$1" <<'EOF' 2>/dev/null || echo 0
import json, sys
d = json.load(open(sys.argv[1]))
if isinstance(d, list): print(len(d))
elif isinstance(d, dict):
    lists = [v for v in d.values() if isinstance(v, list)]
    print(max((len(v) for v in lists), default=1))
else: print(1)
EOF
}
csv_rows() { echo $(( $(wc -l < "$1") - 1 )); }
src_check() {  # name  human-label  →  count in $CNT
  CNT=0
  if [ -f "$SRC/$1" ]; then
    case "$1" in
      *.csv)  CNT=$(csv_rows "$SRC/$1");;
      *.json) CNT=$(count_json "$SRC/$1");;
    esac
    ok "$(printf '%-34s' "$1") $CNT $2"
  else
    bad "$1 MISSING ($2)"
  fi
}
src_check blue_business_insights.csv "mined snippets";        BLUE=$CNT
src_check extracted_gold_queries.json "gold pairs";           GOLD=$CNT
src_check measures_catalog.json "mined measures";             MEASURES=$CNT
src_check metrics_dmp.json "DMP certified";                   DMPN=$CNT
src_check extended_gmns_semantics.json "GMNS pending";        GMNS=$CNT
src_check data_cleaned.csv "glossary rows";                   GLOSS=$CNT
src_check business_terms.csv "atlas terms";                   TERMS=$CNT
count_std_tech() { $PY - "$@" <<'EOF' 2>/dev/null || echo 0
import glob, json, sys
def walk(node, found):
    if isinstance(node, dict):
        if "dataset" in node and ("pde" in node
                                  or "datasetAttribute" in node):
            found.append(node)
            return
        if "dataset" in node and isinstance(
                node.get("tech_metadata_list"), list):
            found.extend(i for i in node["tech_metadata_list"]
                         if isinstance(i, dict)
                         and ("pde" in i or "datasetAttribute" in i))
            return
        for v in node.values():
            walk(v, found)
    elif isinstance(node, list):
        for item in node:
            walk(item, found)
found = []
for path in sys.argv[1:]:
    try:
        walk(json.load(open(path)), found)
    except Exception:
        pass
print(len(found))
EOF
}
if [ -f "$SRC/std_tech_metadata_all.json" ]; then
  STDTECH=$(count_std_tech "$SRC/std_tech_metadata_all.json")
  ok "std_tech_metadata_all.json         $STDTECH entries (combined form — wins over the dir)"
  [ -d "$SRC/std_tech_metadata" ] \
    && warn "per-table std_tech_metadata/ is SHADOWED by the combined file — delete the stale one"
elif [ -d "$SRC/std_tech_metadata" ]; then
  STDTECH=$(count_std_tech "$SRC/std_tech_metadata"/*.json)
  ok "std_tech_metadata/                 $STDTECH entries across $(find "$SRC/std_tech_metadata" -name '*.json' | wc -l | tr -d ' ') per-table files"
else
  bad "std_tech (neither _all.json nor directory)"
fi
N_PACKS=$(find "$SRC/skills" -name skill.yaml 2>/dev/null | wc -l | tr -d ' ')
[ "$N_PACKS" -gt 0 ] && ok "skills/: $N_PACKS packs (dirs holding skill.yaml)" \
  || warn "skills/: no skill.yaml found — skill-contract tier will be empty"
[ "$STDTECH" -gt 0 ] && [ "$STDTECH" -lt "$N_TABLES" ] \
  && warn "std_tech entries ($STDTECH) < bq tables ($N_TABLES)"

# ── crosswalk (the P2 blocking human step) ──
hdr "crosswalk (E1 — required before build-graph)"
XW="$SILO/graph/identity/crosswalk.jsonl"
if [ -f "$XW" ]; then
  ok "crosswalk.jsonl: $(wc -l < "$XW" | tr -d ' ') rows (need $N_TABLES)"
else
  warn "graph/identity/crosswalk.jsonl not yet authored — P0/P1 can run; P2 build-graph will refuse"
fi

# ── benchmark: canonicalization rate on THIS machine, THEIR queries ──
hdr "canon benchmark (drives the estimate)"
RATE=0
if [ "$SQLGLOT_OK" = 1 ] && [ -n "$BQ" ] && [ "$JOBS_TOTAL" -gt 0 ]; then
  RATE=$($PY - "$BQ" "$SILO" <<'EOF' 2>/dev/null || echo 0
import glob, gzip, json, sys, time
sys.path.insert(0, sys.argv[2])          # make `sahs` importable
queries = []
for path in sorted(glob.glob(
        sys.argv[1] + "/*/17_queries_30d/jobs_30d.jsonl.gz")):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            if len(queries) >= 300:
                break
            if line.strip():
                q = json.loads(line).get("query") or ""
                if q:
                    queries.append(q)
    if len(queries) >= 300:
        break
if not queries:
    print(0)
    raise SystemExit
try:
    from sahs.canon.canonical import try_canon       # full pipeline
    fn = try_canon
except Exception:
    import sqlglot                                   # parse-only proxy
    def fn(s):
        try:
            sqlglot.parse_one(s, read="bigquery")
        except Exception:
            pass
start = time.perf_counter()
for q in queries:
    fn(q)
dt = time.perf_counter() - start
print(int(len(queries) / dt) if dt > 0 else 0)
EOF
)
fi
if [ "$RATE" -gt 0 ] 2>/dev/null; then
  ok "measured: $RATE statements/second (sample of your real jobs, up to 300)"
else
  RATE=300
  warn "benchmark unavailable — assuming a conservative $RATE stmts/s"
fi

# ── the estimate ──
hdr "run-time estimate (machine time; formulas shown)"
est() { $PY -c "import math; print(max(1, math.ceil($1)))"; }
SEM=$((BLUE + MEASURES + DMPN + GMNS + GOLD))
T_P0=$(est "$SEM / $RATE / 60 + 1")
T_BG=$(est "($SEM + $JOBS_TOTAL) / $RATE / 60 + 3")
T_P1=$(est "$GOLD * 1.2 / 60 + 1")
T_CMP=3
T_FLOOR=1
T_TOTAL=$((T_P0 + T_BG + T_P1 + T_CMP + T_FLOOR))
printf '  %-28s ~%3s min   (%s stmts / %s per s)\n' "P0 census + make-tasks" "$T_P0" "$SEM" "$RATE"
printf '  %-28s ~%3s min   (semantic re-canon + %s jobs + archive fold)\n' "P2 build-graph" "$T_BG" "$JOBS_TOTAL"
printf '  %-28s ~%3s min   (%s gold × ~1.2s dry-run RTT — network-bound)\n' "P1 oracle dry-run" "$T_P1" "$GOLD"
printf '  %-28s ~%3s min   (fold + reconcile + cards + indexes)\n' "P2 compile" "$T_CMP"
printf '  %-28s ~%3s min   (resolver floor, no network)\n' "P3 floor" "$T_FLOOR"
printf '  \033[1m%-28s ~%3s min machine time\033[0m\n' "TOTAL" "$T_TOTAL"
echo "  human steps on top: empty-SQL triage ~20m · crosswalk ($N_TABLES rows) 30–60m · DIFF review ~20m · floor triage 0–60m"

# ── verdict + paste-back block ──
hdr "verdict"
[ "$PROBLEMS" = 0 ] && ok "READY ($WARNINGS warning(s))" \
  || bad "$PROBLEMS blocking problem(s), $WARNINGS warning(s) — fix ✗ items first"

echo
echo "=== PASTE THIS BACK TO CLAUDE ==="
$PY - <<EOF
import json
print(json.dumps({
  "bq_tables": $N_TABLES, "mdm_tables": ${M_TABLES:-0},
  "jobs_total": $JOBS_TOTAL, "jobs_files": ${N_JOBS_FILES:-0},
  "blue": $BLUE, "gold": $GOLD, "measures": $MEASURES,
  "dmp": $DMPN, "gmns": $GMNS, "glossary": $GLOSS,
  "terms": $TERMS, "std_tech": $STDTECH, "skill_packs": ${N_PACKS:-0},
  "canon_rate_per_s": $RATE, "sqlglot_ok": $SQLGLOT_OK,
  "crosswalk_rows": $( [ -f "$XW" ] && wc -l < "$XW" | tr -d ' ' || echo 0 ),
  "est_minutes": {"p0": $T_P0, "build_graph": $T_BG, "p1": $T_P1,
                  "compile": $T_CMP, "floor": $T_FLOOR, "total": $T_TOTAL},
  "problems": $PROBLEMS, "warnings": $WARNINGS}, indent=1))
EOF
echo "================================="
exit $([ "$PROBLEMS" = 0 ] && echo 0 || echo 1)
