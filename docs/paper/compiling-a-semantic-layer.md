# The graph is compiled, never edited

### Building a provenance-typed semantic layer so an agent can answer data questions without making things up

*A paper about a build system that happens to output a knowledge graph.*

---

## Abstract

We describe the construction of the semantic layer underneath `wyla`: a
provenance-typed graph compiled from the artifacts a data team already
owns — a catalog export, warehouse metadata, thirty days of raw query
history, 158 human-verified prompt→SQL pairs, ~35.7K tribal SQL
fragments, ~12.3K acronyms, ten analytical skill packs — and served to a
language-model agent as ~2K-token markdown cards.

The system has one architectural commitment, and everything else falls
out of it: **the graph is compiled, never edited.** Sources are read into
append-only JSONL quads that carry provenance on every single statement.
A reconciler folds agreeing witnesses and keeps disagreeing ones side by
side. A compiler emits an immutable, content-addressed build. The agent
reads only the promoted build, and never the graph.

That commitment buys three properties that turn out to matter more than
any modeling trick: *same graph → byte-identical build*, so a diff means
what it appears to mean; *every served line carries a `[prov:…]` ref*, so
"where did this number come from" is a lookup rather than an
investigation; and *disagreement is a first-class output*, so the
platform's honest difficulty is a number in `census.json` instead of a
thing you discover in production.

We report on the mechanisms (canonicalization, 15 witness families, a
5-tier authority lattice, the D1–D5 structural handlers, the 46→45
reconciliation gate), the two guards we would not ship without (the gold
contamination guard and the max-not-sum support combiner), and the places
where the design is still a bet rather than a result.

---

## 1 · The problem is not SQL

Let me start with the thing that took me longest to accept.

Text-to-SQL is presented as a translation problem, and translation
problems are the kind of thing large models are extremely good at. Point
a modern model at a schema, ask it for "new accounts approval rate last
quarter," and you will get syntactically perfect BigQuery back. It will
run. It will return a number. The number will be wrong, and — this is the
part that matters — nothing about it will look wrong.

The failure is not in the SQL. It is upstream, in four places:

1. **Which definition.** Three catalogs in this warehouse define
   "approval rate," and they do not agree on the denominator. The model
   picks one. Nothing tells it there were three.
2. **What one row means.** A join against a table with no declared
   primary key silently doubles the numerator. The SQL is valid. The
   grain changed.
3. **What the literal is.** `WHERE status = 'Approved'` returns zero
   rows, because the warehouse stores `'A'`. The query succeeds. The
   answer is 0.
4. **Whose authority.** A mined pattern with 4,000 executions and a
   certified KPI with 3 look identical to a ranker that scores on usage.
   Popularity is not governance.

Every one of those is a *meaning* problem, and meaning does not live in
the schema. It lives scattered across a catalog nobody fully trusts, a
metadata service that returns `DENIED` a third of the time, a CSV of
35,700 SQL fragments people pasted into a wiki over six years, and the
query history, which is the only source that knows what is *actually*
run.

So: don't build a better translator. Build a substrate that makes the
four questions above answerable, and hand the translator a world it
cannot lie about.

Here is the analogy I keep coming back to. A compiler does not ask you to
trust it. It takes source text, applies a fixed sequence of passes, and
emits an artifact you can diff, hash, and ship. Two runs over the same
source produce the same binary; if they do not, that is a bug in the
compiler, not a mystery about the program. What we wanted was that, for
meaning: a build system whose source is "everything the team knows,
messily," and whose output is a world an agent can stand on.

---

## 2 · The shape of the thing

Four stages, one rule.

```
sources ──► quads ──► reconcile ──► build ──► CURRENT
 (L0/L1)     (L2)        (E1)        (L3)      (L4)
```

- **L0/L1 — read and canonicalize.** One loader per source *shape*. Each
  emits typed records and nothing else. Anything SQL-shaped passes
  through `c(sql)`.
- **L2 — the quad store.** Append-only JSONL in git. Nodes and edges,
  each with a `prov` block. Nobody edits history; current state is a
  fold.
- **E1/L3 — reconcile and compile.** A pure function of the graph: fold →
  reconcile → ACL → indexes → cards → census → manifest → diff → gates →
  atomic cutover.
- **L4 — serve.** `Build.open()` and a handful of read functions. The
  agent's world ends here.

The rule: **information flows one way, and the only human write path is a
signed governance transition.** There is a `clerk` module and it is the
only thing that appends a human decision. Nothing in a chat writes to the
graph. Ever. That is enforced by *absence* — the chat toolkit simply
contains no writer — which is the only kind of enforcement that survives
a refactor.

The numbers, so you have scale in your head: 45,002 lines of Python
across 182 files, 476 tests, all of them running offline on 500KB of
checked-in fixtures. 25 registered relations. 20 node kinds. 15 witness
families. The reference warehouse is 46 tables.

---

## 3 · `c(sql)`: identity by canonical form

Everything rests on one function. Two expressions that mean the same
thing must produce the same text, and therefore the same 12-hex
fingerprint, regardless of who wrote them or how they formatted them.

`sahs/canon/canonical.py` is 395 lines and it is the most load-bearing
file in the repository. Nine passes, in a pinned order:

```
1. parse (dialect=bigquery; failure → CanonError with a category)
2. strip comments
3. normalize identifiers (lowercase unquoted; quoted case preserved)
4. qualify columns from the extracted schema
5. de-alias tables + strict single-table CTE inlining
6. constant folding (literals KEPT — predicates are literal-bearing)
7. sort commutative operands (AND/OR sets, IN lists, col-before-literal)
8. canonical literal rendering
9. generate with fixed settings (no pretty, functions lowercased)
```

Two fingerprints come out, not one:

```python
fp_expr      # identity of the exact expression, literals included
fp_template  # literals → typed placeholders (?s/?n/?d, IN-lists → ?*)
             # forked BEFORE operand sorting — pattern identity, arity-blind
```

And the fingerprint embeds its own provenance:

```python
fp = sha256(canonical_text ⊕ NUL ⊕ dialect ⊕ NUL ⊕ canon_version)[:12]
```

`canon_version` is `f"{CANON_RULESET}:{sqlglot.__version__}"`. Read that
again, because it is a design decision disguised as a string: **upgrading
the SQL parser changes every fingerprint in the system.** That sounds
insane until you consider the alternative. If fingerprints did not embed
the ruleset, a parser upgrade would silently re-fingerprint *some*
expressions — the ones whose canonical form happened to change — and the
graph would fork identities with no signal. Two nodes for one metric, no
error, nobody notices for a month.

By embedding the version, an upgrade is loud: `tests/test_canon.py` goes
red, and the remint is a deliberate migration (`SAHS_REGEN_GOLDENS=1`)
that re-derives every id from the *stored canonical text*. We store the
canonical text next to every fingerprint precisely so this migration is
mechanical.

This is the single strongest lesson from the whole project. **Make your
derived identities carry the version of the deriver.** Silent drift in an
identity function is unrecoverable; loud drift is a Tuesday.

### The ones that came from the data

Two passes exist only because the corpus taught us:

```python
# ruleset 2: COUNT(*) ≡ COUNT(1) — BigQuery treats them identically;
# two fingerprints for one function is a lie the jobs witness exposed
# (catalog said COUNT(1), jobs said COUNT(*), and the same metric
# refused to fuse)
```

```python
if len(node.expressions) == 1 and not node.args.get("query"):
    # x IN ('B')  ≡  x = 'B' — collapse so the census doesn't count a
    # phantom second class (found by fixture census)
```

Neither is clever. Both were found by looking at a conflict count that
was too high and asking why. Which is the actual method: the census is
not a report, it is an instrument.

### And the failures are typed

`c(sql)` raises `CanonError` with one of four categories —
`parse_error`, `fragment`, `dialect`, `transform` — and there is a
wrapper, `try_canon`, with a documented guarantee: it never raises. One
junk row in a 35,700-row corpus must not kill a run. So quarantine is
categorized, counted, and reported, and the *rate* becomes a gate.

That matters because the quarantine ledger is where honesty lives. A
fragment whose table is outside this run's registry is `out_of_scope` —
nothing is missing, the row belongs to a table we do not carry. A row
with prose in the SQL column ("Cheque Cashing") is `not_sql`, caught
*before* the parser, so the canonicalization rate measures the pipeline
on rows that claim to be SQL rather than being dragged down by data-entry
noise. Some export rows arrive with the two columns swapped; detection is
deterministic (this side fails to parse, that side parses) and the row is
recovered with a flag. A row broken on both sides still reaches the
parser and fails there honestly, where the gate can see it.

None of that is glamorous. All of it is the difference between a number
you can act on and a number you can argue about.

---

## 4 · IDs are pure functions

```python
table:<dataset>.<table>            # project lives in prov, not the id
col:<dataset>.<table>.<column>
pred:<fp12>   tmpl:<fp12>          # canonical-AST fingerprints
metric:<fp12>                      # fp(expr + grain + entity)
mgroup:<key>                       # dmp/gmns id, else label@entity
concept:<label_norm>@table:<ds>.<t>
term:atlas:<id>
acr:<symbol>@<bu>@<region>         # missing scope → all
```

Never auto-increment, never UUID. An id is a pure function of the entity,
so two runs producing the same fact produce the same line, and a diff
means what it appears to mean. This is the same instinct as
content-addressing in a build cache, applied to meaning.

Where an id must embed a source-provided string, the slug happens at
*exactly one mint site*, and the verbatim value stays in `props`:

```python
def concept_id(label_norm: str, physical_table: str) -> str:
    """The node id is a DERIVATION, not the label: E9 keeps the concept
    label verbatim on the record, while the id slugs every character the
    grammar forbids to `_` (real tribal labels carry `/ . ::`)."""
    safe = _ID_SAFE.sub("_", label_norm)
    return f"concept:{safe}@{table_id(physical_table)}"
```

There is a comment in `lob_id` that is worth quoting whole, because it
shows the discipline at its sharpest:

```python
"""LOB identity is the slug of its CODE — "GMNS" (a dmp lineOfBusiness
value) and "gmns" (a steward lob_map code) are the same node, so the two
declarations corroborate instead of forking. A display name used as a
code slugs to a DIFFERENT node — divergence stays visible in the graph
rather than being guessed away."""
```

"Divergence stays visible rather than being guessed away" is the whole
paper in eleven words.

---

## 5 · The quad store: append-only, folded, witnessed

`graph/` is JSONL files in git. That is the entire database.

```
graph/
  nodes/<kind>.jsonl        {"id", "props", "prov"}
  edges/<relation>.jsonl    {"s", "r", "o", "props"?, "prov"}
  runs/<run_id>/manifest.json
  identity/crosswalk.jsonl
```

Discipline replaces the database: single writer, append-only, current
state = a fold where the last line wins per identity. The relation
registry types every edge's endpoints; a 14-check validator is the
foreign-key story. Twenty-five relations, each with declared subject and
object kinds:

```python
RELATIONS: dict[str, tuple[set[str], set[str]]] = {
    "has_column":      ({"table"}, {"col"}),
    "bound_to":        ({"concept"}, {"pred"}),
    "measured_on":     ({"metric"}, {"table"}),
    "certified_as":    ({"metric", "pred", "mgroup"}, {"status"}),
    "joins_via":       ({"table"}, {"table"}),
    "co_queried_with": ({"table"}, {"table"}),
    "fk_references":   ({"col"}, {"col"}),
    ...
}
```

### Provenance is not metadata, it is the payload

Every node and every edge carries:

```python
class Prov(BaseModel):
    source: str
    run: str
    retrieved: str = ""
    valid_for: list[str] = ["unversioned"]
    status: str = "active"          # active | superseded | retracted
    support: int | None = None
    evidence: str = ""              # back to the exact source location
    actor: str | None = None        # REQUIRED when source == "clerk"
    witness: str = ""               # ∈ WITNESSES
```

`evidence` points back to the exact source location — a file, a line, a
row id. The bottom of the provenance chain is a byte offset in somebody's
CSV export. That is what lets a card print `[prov:…]` on every line and
mean it.

### The witness idea

This is the piece I would keep if I could keep one thing.

A **witness** is *who saw it* — the independent evidence family behind an
assertion. Fifteen families:

```python
WITNESSES = (
    "catalog_mined",  "jobs_30d",     "audit_30d",   "dmp",
    "gmns",           "skill_contract","snippet",     "atlas",
    "lumi",           "bq",            "steward",     "user_variant",
    "llm_enriched",   "gold_attested", "studio",
)
```

And the fold key for edges is not `(s, r, o)`. It is
`(s, r, o, witness)`:

```python
def fold_edges(self) -> dict[tuple[str, str, str, str], Quad]:
    """Current state per (s, r, o, WITNESS) — each witness family
    testifies independently, and a retraction by one witness never
    erases another's testimony. Aggregation across witnesses (support
    arrays, max-combiner) is compiler output, never fold state."""
```

Independent testimony never collapses at the store. That is the load-bearing
sentence. If you fold on `(s, r, o)`, the last writer wins and you have
destroyed the fact that three sources agreed — which is the single most
useful signal in the entire system. Agreement is not a nice-to-have; it is
the feature the resolver ranks on (`witness_agreement`), and you cannot
compute it after you have thrown it away.

The general lesson: **aggregate at read time, store at witness
granularity.** Folding early feels tidy and costs you the evidence.

### Crash hygiene, briefly

A run killed mid-append can leave a torn final line. Before the first
append to a file, the tail is checked; torn bytes move to a `.torn`
sidecar (evidence, never deleted) and the file truncates to its last
complete line. Corruption anywhere *else* is refused loudly with
file and line named, and the error message tells you the honest recovery:

```
graph store corrupt at nodes/metric.jsonl:8814 (Expecting value) —
the append-only store is derivable: remove graph/nodes and graph/edges
(KEEP graph/identity and graph/runs) and re-run build-graph
```

The store is derivable from the archives. So the honest recovery is
rebuild, never silent skipping. Errors that tell you what to do are worth
more than errors that tell you what happened.

One more small thing that cost us real time: `json.dumps` passes
U+2028/U+2029/NEL through raw, and every line-based tool — including
`str.splitlines()` — treats them as line breaks. Real catalog
descriptions carry them. So the writer escapes them explicitly, and the
reader splits on `"\n"` only, never `splitlines()`. One record is always
one physical line. If your store is JSONL, this bug is waiting for you.

---

## 6 · The loaders: one per source shape, and they only emit

Nine loaders. Each reads one documented export shape and emits quads.
They contain no policy — arbitration happens later, in one place.

| loader | source | scale | what it contributes |
|---|---|---|---|
| `archives/bq_extraction.py` | warehouse extraction, 00–17 per table | 46 tables | columns, types, partitions, profiles, row policies |
| `archives/mdm46.py` | metadata service responses | 46 tables | ownership, lineage, lifecycle, PII flags |
| `archives/jobs_30d.py` | raw 30-day job history | per table | true recency, measured joins, cost priors |
| `sources/catalogs.py` | three metric catalogs | 35 + 14 + 6,223 | certified KPIs, pending specs, mined patterns |
| `sources/blue_insights.py` | tribal SQL fragments CSV | ~35.7K | predicates and CASE expressions |
| `sources/vocab.py` | acronyms, terms, catalog entries | ~12.3K + ~4.4K + 46 | vocabulary, column↔term links |
| `sources/skills.py` | ten analytical packs | 10 | exact contract expressions |
| `sources/gold_queries.py` | human-verified pairs | 158 | the answer key |
| `sources/studio_csv.py` | filtered catalog re-export | — | observed metric SQL with CTEs |

Three behaviours are worth pulling out.

**Identity errors block; everything else quarantines.** Every
archive-derived quad's table subject must resolve through a
human-verified crosswalk (46 rows, one per table, signed and dated). An
unresolvable archive record does not quarantine — it *stops the build*.
Semantic sources are different: they legitimately mention out-of-scope
tables, so a skip with a count is honest there. The rule reads: identity
confusion is the one error class that corrupts everything downstream, so
it fails at the door.

**`DENIED` is not absence.** The metadata service returns HTTP 503 and
`DENIED` regularly. A denied row-access-policy listing becomes
`has_policy → policy:unknown_denied`, and the ACL builder fails *closed*
on it — the table is marked restricted. A present-but-empty policy file
is *confirmed none* and emits nothing. Those two states look the same in
a naive loader and mean opposite things.

**The witness we hired for recency, not for volume.** There is a comment
in `jobs_30d.py` that was written to stop a future engineer from
"fixing" the design:

> **the catalog winning the max is the design working — the jobs witness
> was never hired to out-count a longer-horizon miner; it was hired for
> true recency, corroboration, and discovery of what the catalog missed.**

The jobs miner is deliberately a *top-level-only* extractor: aggregates
from the outermost select list, conjuncts from the outermost `WHERE`.
Anything nested — correlated subqueries, `EXISTS`, derived tables — goes
to a `nested` quarantine category, counted, never silently skipped. A
fragment lifted out of nested context changes meaning, and *a wrong
fragment with support 200 is worse than none*. The gate: ≥90% of a
table's jobs canonicalized-or-understood, where nested counts as
understood and parse breakage does not.

And support is the count of distinct `job_id`s, not rows — repeats become
a separate `run_count` prop. The distinction between "200 people ran
this" and "one dashboard ran it 200 times" is the distinction between a
consensus and a cron job.

### The utilization ledger

One small mechanism I did not expect to love. "Are we actually using
everything we're getting?" stops being a question you answer from memory:
every file under every input root is checksummed and marked

```
consumed              a loader actually read it this run
deferred(reason)      deliberately unread, with the pinned reason
inventoried           present, unread, NOT deliberately deferred —
                      the honest "we have this and do nothing yet"
```

The third category is the interesting one. `inventoried` is a
machine-maintained list of your own blind spots, and a CI test guarantees
it only ever contains files you already know about. Read that list after
every run; anything you do not recognize is a finding.

---

## 7 · Reconciliation: five ways to disagree, five handlers

Three sources describe the same column: the catalog, the metadata
service, and the warehouse itself. They disagree constantly. `E1`
reconciliation runs inside compile, before cards, and produces one
consensus record per column with per-field `{value, source}` and an
`agreement_count`.

Disagreements route through five pinned handlers:

| code | disagreement | handler | ticket |
|---|---|---|---|
| **D1** | documented in catalog, absent in BigQuery | **omit from cards entirely** | `catalog_stale` |
| **D2** | in BigQuery, undocumented | keep, flag `ungoverned`, bind only with disclosure | `coverage_gap` |
| **D3** | type/DDL mismatch | **BigQuery wins**, everywhere execution-facing | `catalog_mismatch` |
| **D4** | description divergence | keep both with attribution, catalog display-first | none |
| **D5** | sensitivity flags disagree | **most-restrictive applies immediately** | `sensitivity_conflict` |

D1 is the one people argue about, and it is the one I am most confident
in: *the agent must never see a column the runtime cannot serve.* A
documented column that does not exist is not a gap in the graph, it is a
trap. Omit it, ticket it, move on.

D3 encodes a hierarchy of trust that is really a hierarchy of
consequence: for anything that will end up in a query, the warehouse is
the authority, because the warehouse is what executes. The catalog can be
right about *meaning* and wrong about *type*, and only one of those
breaks at runtime.

D5 is the pattern to copy elsewhere. When two witnesses disagree about
risk, apply the restrictive reading *immediately* and open the ticket. Do
not wait for resolution. The restrictive answer holds while the ticket is
open. Fail-closed by default, and let governance open it up later, with a
signature.

The counts land in `census.json` under `structural`, per table and in
total, which means "did the catalogs and the warehouse drift closer or
further apart this month" is a diffable number.

### The 46→45 rule

A table can drop out of a build silently: reconciliation iterates
`has_column` edges, so a table node with zero columns never reaches
consensus. That is exactly the kind of quiet loss that erodes trust in a
pipeline.

So: every crosswalk row either **compiles** or is **explained**. A table
that does not compile must appear in `identity/exclusions.jsonl` with a
steward reason, or the gate blocks promotion. The manifest's
`table_reconciliation` block accounts for every row either way, with the
diagnosis attached:

```python
reason = (
    "table node exists but ZERO columns on record: no has_column edge "
    "from any plane (archive 02, atlas std_tech)"
    if f"table:{phys}" in nodes else
    "no table node in the graph: never attested by any archive or "
    "catalog quad")
```

45 of 46 tables compiled on the reference warehouse. The 46th has a
signed reason. That is the whole point: the gap is not a mystery, it is a
row.

---

## 8 · The compiler

`compile_build(graph_root, builds_root)` is 878 lines and one promise:

```python
"""compile(graph, crosswalk) → builds/b_<graph_hash12>/ — deterministic.

The compiler is a pure function of the truth graph: same graph, byte-
identical build (no timestamps inside build artifacts — run metadata
lives in the event stream)."""
```

No wall clock anywhere in the output. The build id *is* the graph hash:

```python
def graph_hash(graph_root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(glob("nodes/*.jsonl")) + sorted(glob("edges/*.jsonl")):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()[:12]
```

Run it twice, get `b_2cd603279061` twice. The runbook actually tells you
to do this — "run it twice if you want to see determinism with your own
eyes" — which is good advice for any pipeline claiming to be
reproducible.

### What comes out

```
builds/b_<hash12>/
  cards/tables/*.md         ≤2K tokens each, the agent's world
  cards/metrics/*.md
  cards/concepts/*.md
  indexes/metrics.jsonl     + sqlite twin with precomputed rank columns
  indexes/bindings.jsonl
  indexes/vocab.jsonl       + FTS5 where available
  indexes/joins.jsonl       five witness families, each named
  indexes/domains.jsonl     observed column values
  indexes/value_meanings.jsonl   stored code → business phrase
  indexes/tables.jsonl      row counts + declared primary keys
  indexes/cost_priors.json  p50/p95 bytes per job, per table
  indexes/graph_map.json    3-D positions, baked at compile
  indexes/sources.json      the display-plane sources shelf
  census.json               conflict counts + D1–D5 structural
  acl.json                  fail-closed access
  schema.json               only columns BigQuery can serve
  columns.json              the same columns with their meaning
  tickets.jsonl             the governance queue
  manifest.json             counts, resolver constants, reconciliation
  DIFF_vs_prev.md           what a human reads before promoting
```

JSONL is the diffable source of truth; the sqlite index is a derived
artifact rebuilt from the same rows. That ordering is deliberate: the
thing you can review in a pull request is authoritative, and the thing
that is fast is disposable.

### Promotion is atomic, and gated

```python
failures = list(recon_failures)
for table in consensus.values():
    if not card_path(table).exists():
        failures.append(f"missing table card: {table.physical}")
for row in metric_rows:
    if row["status"] == "certified" and not row["table"]:
        failures.append(f"certified metric without table: {row['id']}")
if not failures:
    tmp = current_path.with_suffix(".tmp")
    tmp.write_text(build_id + "\n")
    os.replace(tmp, current_path)      # atomic
```

`builds/CURRENT` is one line of text containing a build id, replaced with
`os.replace`. A reader either sees the old build or the new one, never a
half state. A build that fails a gate still exists on disk with its
census and its diff — you can go read why it failed — it simply never
becomes `CURRENT`.

"A certified metric without a table is a lie" is a gate, not a lint. I
like gates that are stated as sentences about the world.

### Where witness aggregation finally happens

Remember that folding kept every witness separate. Here is where they
combine, and the combiner is the interesting part:

```python
def _finish_witness_features(row):
    """support_effective = max over RANKING witnesses (never a sum —
    the catalog was mined from a superset of the same history);
    agreement = ranking families attesting; recency from the jobs
    witness alone when present (the only true timestamps)."""
    ranking = {f: v for f, v in row["support_by_witness"].items()
               if f in RANKING_WITNESSES}
    row["support"] = max(ranking.values(), default=0)
    row["witness_agreement"] = len(ranking)
```

**Max, not sum.** The catalog was mined from a superset of the same query
history the jobs witness reads. Summing would double-count the same
events and reward the sources with the most overlap. Max asks "what is
the strongest single piece of evidence," and `witness_agreement` asks
"how many independent families saw it," and those are two different
questions that a sum smears together.

Recency comes from the jobs witness *alone* when present, because job
creation times are the only true timestamps in the corpus. Everything
else is a catalog-provided date, and when we fall back to one we flag it:
`recency_source: "catalog"`.

### The gold contamination guard

This one is short and it is an assertion at import time:

```python
# Gold contamination guard (pinned): the gold pairs are the eval answer
# key — a full graph citizen (census, cards, evidence) that must never
# feed a feature the resolver ranks on. audit_30d corroborates, never
# votes. The SUT must not contain its own test set.
assert "gold_attested" not in RANKING_WITNESSES
assert "audit_30d" not in RANKING_WITNESSES
```

The 158 gold prompt→SQL pairs are genuinely useful graph content: they
attest expressions, they show up as evidence on cards, a steward can read
them. They are also the evaluation answer key. So they are full citizens
everywhere *except* the three features the ranker sees
(`support_effective`, `witness_agreement`, recency).

`audit_30d` is excluded for a different reason: it corroborates the same
events `jobs_30d` already witnessed. Two witnesses of the same events do
not count twice.

If you build a system that both learns from and is evaluated on the same
corpus, write this assert. Put it at module import. Let it fail loudly
the day somebody adds a witness family to the wrong tuple.

---

## 9 · Ranking: the lattice, and why support cannot win

```
certified(dmp) ≻ pending(gmns) ≻ skill-contract ≻ mined(support, recency) ≻ snippet
```

Five tiers, one `IntEnum`, defined once in `canon/authority.py` and
imported everywhere — including by the census, which is why it lives in
the canonicalization package and not in the compiler.

The resolver sorts **lexicographically on `(authority_tier, score_rest)`**:

```python
score_rest = 0.4·log1p(support)/log1p(max_support)
           + 0.3·recency_decay(last_seen, half-life 90d)
           + 0.3·context_fit
```

Because the sort is lexicographic and not a weighted sum, support can
never outvote certification. A mined pattern with 6,000 executions loses
to a certified KPI with 3, always, by construction. That is the lattice
made literal, and it is why it is a sort key rather than a feature with a
large coefficient — a coefficient is a thing someone tunes, and one day
somebody tunes it wrong.

Two more choices worth naming.

**Recency decay is data-relative, not wall-clock:**

```python
"""DATA-relative decay: age is measured against the build's own newest
last_seen, not the wall clock — deterministic forever, and a build never
'goes stale' by merely being re-scored later."""
```

A build re-scored six months later produces the same ranking. Wall-clock
decay would mean the same build answers differently on different days,
which quietly breaks every reproducibility claim in the paper.

**Disambiguation is a success state.** The margin between the top two
candidates is computed on `score_rest` only when they tie on tier. If the
margin is below threshold, *or* if the top candidate was only reached via
fuzzy matching, there is **no bind** — the turn stops and asks one
question with named options, each carrying its evidence.

```python
"m < margin_threshold OR a top candidate reached only via fuzzy ⇒ NO
BIND — a structured ambiguity with named options instead (never argmax;
disambiguation is a success state)."
```

Never argmax. An agent that asks "did you mean the certified definition
or the one your team uses?" is doing its job. An agent that picks is
gambling with your quarter-end.

And every constant is versioned:

```python
RESOLVER_CONSTANTS = {
    "version": "rc1",
    "weights": {"support": 0.4, "recency": 0.3, "context_fit": 0.3},
    "recency_half_life_days": 90,
    "margin_threshold": 0.15,
    "tier_ceiling": {"5": 0.95, "4": 0.80, "3": 0.70, "2": 0.55, "1": 0.40},
    "fuzzy_reach_forces_ask": True,
}
```

That block is embedded in every build manifest and cited in every
`resolve()` response. These are uncalibrated bets — we say so in the
docstring — and the versioning is how a wrong bind stays traceable to the
constants that produced it.

---

## 10 · What the agent actually sees

The agent never touches the graph. It reads cards: markdown pages,
budgeted to ~2,000 tokens for a table, with a **fixed drop order** and a
list of things that are never dropped.

```python
"""Cards — the agent's entire world, one token-budgeted page at a time.

Three templates (pinned sections; every line carries a [prov:…] ref;
conflicts are ALWAYS printed — a card that hides ambiguity is lying to
the agent). Table cards budget to ≤2K tokens with a fixed drop order —
column long-tail → history/usage → join detail — and NEVER drop grain,
conflicts, or access."""
```

The budgeter is where a lot of systems quietly go wrong. Truncation is
inevitable at some scale; *what* you truncate is a design decision, and
most implementations make it accidentally, by ordering. Here it is
explicit: the twelfth column can go, the conflict cannot. And the drops
are counted into the manifest's `budget` block, so a diff can tell you
"this table started dropping columns this build."

The complete column list still exists, in `columns.json`, for the
surfaces that can afford it. The card is the *budgeted* view, not the
only view.

Cards also print disagreement in the served vocabulary rather than the
store's:

```python
_STATUS_SERVED = {"mined": "unreviewed", "pending": "pending_certification", …}
_EVIDENCE_ORIGIN = {"measures_catalog": "usage_mining", "jobs_30d": "query_history", …}
```

Governance status and evidence origin are different axes, and the store's
word "mined" conflates them for a reader. So the agent-facing surface
says "unreviewed (evidence: usage_mining)" while the store state and the
governance lattice stay untouched. Display vocabularies should be
translations, never renames.

---

## 11 · Results

**Determinism.** Same graph → same 12-hex build id → byte-identical
artifacts. Verified by re-running compile in CI on fixtures, and by hand
on the real warehouse.

**Coverage on the reference warehouse.** Crosswalk 46/46 resolved. 45
tables compiled with cards; the 46th carries a signed exclusion reason.
Validator: 0 errors on the real graph. Every certified metric has a card
and an active binding — the gate would have blocked promotion otherwise.

**Canonicalization.** The `jobs_canon_rate` gate requires ≥90% of every
table's 30-day jobs canonicalized-or-understood, and it passes per table;
the accounting is committed in the run manifest, per table, so the number
is auditable rather than asserted.

**Capability baseline.** The reconstructed capability suite (11 tiers)
scores the built lanes at full marks on the fixture build
`b_2cd603279061` — vocabulary 4/4, certified bind 2/2, clarification 4/4,
mutation 1/1, contract 2/2, join preview 3/3, receipts 1/1, abstention
3/3, conversation 9/9 — with two tiers (composition, exploratory) marked
**absent — not built, not scored**, which is the honest way to report a
matrix with holes in it.

The three-configuration ablation on the margin threshold (0.05 / 0.15 /
0.30) produces **identical** two-number lines on the fixture build, and
we report that as a finding about the fixture rather than about the knob:
none of those tasks sits near the margin boundary at this scale. On the
full graph, with 3,000+ mined classes, the knob is expected to
differentiate; a flat line *there* would be a finding about the knob.

**Test suite.** 476 tests, offline, on 500KB of checked-in fixtures.
Tests that would need a warehouse or a model stub the plane and assert on
what would have been sent. Fixture CI runs every pipeline subcommand
end-to-end, which is the guard against runbook drift — the failure mode
where the documentation describes a command nobody has run in a month.

---

## 12 · What I would tell you to steal

In rough order of how much they earned:

1. **Embed the deriver's version in every derived identity.** Silent
   drift in an identity function is unrecoverable.
2. **Fold at witness granularity; aggregate at read time.** Agreement is
   your best feature and folding early destroys it.
3. **Max, not sum, when sources overlap.** Then report agreement
   separately.
4. **Make the ranking a lexicographic sort on a governance tier.** Not a
   weighted feature. Nobody can tune governance away.
5. **Assert your evaluation set out of your ranking features at import
   time.**
6. **`DENIED` is not absence.** Give unknowns their own node and fail
   closed on them.
7. **Categorize your quarantine, and gate on the rate.** `out_of_scope`
   and `parse_error` are different facts about your pipeline.
8. **Ledger every input file as consumed / deferred(reason) /
   inventoried.** The third bucket is a machine-maintained list of your
   blind spots.
9. **Truncate on purpose, with a named drop order and a never-drop
   list.**
10. **Make errors say what to do.** "The store is derivable: remove
    these two directories and re-run" beats a stack trace every time.

---

## 13 · What is still a bet

I would rather write this section than have someone else write it for me.

**The authority lattice is asserted, not learned.** Five tiers, ordered
by where a definition came from. It is obviously right that a certified
KPI outranks a wiki fragment. It is not obvious that a skill-pack
contract outranks a mined pattern with 4,000 executions in every case,
and we have no measurement that says it does.

**The resolver constants are uncalibrated.** Weights 0.4/0.3/0.3, a
90-day half-life, a 0.15 margin. They are versioned, cited, and honestly
labeled as bets. The ablation could not distinguish them at fixture
scale.

**`metric_conflicts` measures less than its name suggests,** and the
census says so out loud in a `meta` block: it counts same-*identity*
drift only — two expression classes under one catalog id. Same-intent,
different-formula across *different names* is invisible to that counter.
Mined labels are near-unique by construction, so the real duplicate-intent
problem surfaces through concept conflicts, alias work, and enrichment
review, not through this number. A census field whose docstring explains
what it cannot see is worth more than one that quietly overclaims.

**Label normalization is lower/trim/collapse-whitespace only.** `ALIF`
and "active locations in force" count separately. That is deliberate — we
would rather over-count conflicts than silently merge two meanings — but
it means the census total is not a deduplicated count, and the meta block
says so.

**No CNF expansion in canonicalization.** Explosion risk on a 35K-fragment
corpus. So two logically equivalent predicates written with different
boolean structure will not fuse. We sort commutative operands and stop
there.

**Single writer, single machine.** The store's correctness rests on
"nobody else is appending right now." That is a discipline, not a lock.
It held for a laptop with warehouse access, which was the deployment
target throughout. It would not hold for a team, and the honest fix is a
real transaction boundary, not a stricter README.

**The loaders are shaped to specific exports.** Nine documented export
layouts under `docs/contracts/`. Pointing this at another warehouse is
real work, and the README says so rather than implying a `pip install`
will do it.

---

## 14 · Related work, briefly

Semantic layers (dbt's metric layer, LookML, Cube) solve the *definition*
half: one place to write a metric, one place to change it. They assume
the definitions arrive already agreed. This work starts one step earlier,
where three catalogs disagree and nobody has authority to pick, and it
treats the disagreement as the primary output rather than as an input
error.

Provenance systems (PROV-O, W3C RDF named graphs, `nanopublications`) got
there long before us on the modeling: a quad with a graph label is the
same idea as a quad with a `prov` block. What we add is narrow and
practical: the witness *family* as a fold key, a max-not-sum combiner
over overlapping families, and a compiler that treats the whole store as
build input.

Text-to-SQL benchmarks (Spider, BIRD, and the enterprise papers that
followed) measure execution accuracy against a fixed schema, which is
exactly the part that was never the bottleneck here. The interesting
metric on a real warehouse is not "does the SQL run" but "did it use the
definition the business would have used, and does the answer say which
one it used."

Retrieval-augmented generation is the closest neighbour, and the
difference is one word: *compiled*. RAG retrieves from a corpus at query
time. Here the corpus is compiled into an immutable artifact, gated, and
promoted, and the agent reads only what was promoted. The cost is a build
step. The benefit is that "what did the agent see last Tuesday" is
answerable with a build id.

---

## 15 · Closing

The thing I did not expect: almost none of the engineering that made this
work was about the model. It was about making the environment truthful
enough that intelligence had something to stand on.

There is a principle in the repo's own `harness-discipline.md` that says
it better than I can:

> **Make the environment truthful before making the model clever.**
> Intelligence on a lying environment is confident nonsense; intelligence
> on a truthful one compounds.

A compiled graph with provenance on every statement, a lattice that
cannot be out-voted by popularity, gates that block promotion rather than
warn, quarantine categories that tell you *why* a row sat out, and a
`census.json` that reports the honest difficulty of your own data — none
of that is intelligent. All of it is what intelligence needs in order to
be trustworthy.

The graph is compiled, never edited. Everything else is downstream of
that sentence.

---

## Appendix A · Reading order

```
scripts/pipeline.py              the CLI: census, ground, build-graph, compile, promote, enrich
sahs/canon/canonical.py          c(sql): the canonical form every fingerprint hangs off
sahs/canon/fingerprint.py        12-hex identity, dialect and ruleset embedded
sahs/graph/ids.py                the id grammar; every mint site
sahs/graph/quads.py              the store, the witness vocabulary, the fold
sahs/graph/validate.py           the 14 checks that block a build
sahs/loaders/                    one module per source shape; each emits quads, nothing else
sahs/compiler/reconcile.py       where witnesses agree, disagree, and how that is recorded
sahs/compiler/compile.py         quads → build directory
sahs/compiler/cards.py           the budgeted page the agent reads
sahs/tools/api.py                Build.open(): the read API everything downstream uses
sahs/tools/resolver.py           the lexicographic binder and its margin
```

## Appendix B · The gates, in one table

| gate | stage | blocks | says |
|---|---|---|---|
| `crosswalk_resolution` | build-graph | yes | every archive table resolved to a signed crosswalk row |
| `jobs_canon_rate` | build-graph | yes | ≥90% of each table's jobs canonicalized-or-understood |
| `graph_valid` | build-graph | yes | 14 checks: grammar, endpoints, relations, lattice, witnesses |
| `table_reconciliation` | compile | yes | every crosswalk row compiled or explained (46→45) |
| card existence | compile | yes | every reconciled table has a card |
| certified-has-home | compile | yes | no certified metric without a table |
| `blind name recovery` | enrich | yes (halts run) | <60% recovery writes nothing; iterate the prompt, not the graph |
| bytes-scanned band | eval | no (warning) | partition pruning makes it flaky |
