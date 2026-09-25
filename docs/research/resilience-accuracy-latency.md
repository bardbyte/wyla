# Resilient, accurate, fast: what to add, what not to, and why

Status: decision paper, September 2026. Written after the production-ready
integration (PR #150) and before any of the work below starts. Every
recommendation names what exists in the repo today, what it would change,
and the evidence behind it.

The one-sentence version: the harness already has the three things that
matter most (governed knowledge, a store every pod shares, an event record
of every turn); what it lacks are a few well-placed caches with honest keys,
fallbacks on every dependency, and a measurement loop. None of that needs a
new database.

## 1. The three questions, answered up front

**No refusals.** Every verbatim of every skill is always usable. A skill that
fits the engine's window loads whole; one that does not loads as a library
(a complete table of contents plus the passages that match the ask, and
tools to read any section whole). Nothing is truncated in silence, and
nothing is refused. This is being changed in the harness now
(`docs/skill-retrieval.md`).

**Redis for memory: not yet, and not for memory.** Short-term memory (the
transcript, the working notes, the handoff) and long-term memory (the
person's remembered preferences) are already durable rows in Spanner
(`ChatMessages`, `ChatSessions.Notes/Handoff`, `ChatMemories`), shared by
every pod, and the model reads them from the prompt. Redis would add a
second copy of the same facts with a second failure mode. Where a shared
in-memory tier does earn its keep is *hot, disposable* state: the session
cookie cache, the running-turn registry, rate limits, and a cross-pod answer
cache. Adopt Memorystore for those when the deployment runs more than one
pod per environment, not before, and behind an interface whose fallback is
the process memory we have today.

**A cache for repeated questions: yes, with governed keys, never as a
shortcut past the checks.** Two caches pay for themselves: the model's own
context cache over the stable prompt prefix (skills included), which cuts
input cost by 90% on cache hits and most of the prefill latency; and an
*answer* cache keyed on the normalized question, the promoted build id, the
skill versions, the person's permission scope and the model, serving a
previous answer as a draft with its "as of" stamp, or better, serving the
validated SQL plan and re-executing it. What must never be cached across
people is a number computed under one person's BigQuery credentials.

## 2. What exists today

| concern | mechanism in the repo | gap |
|---|---|---|
| knowledge | 8 governed skills; whole-load under the engine budget, library over it; FTS5 index keyed by content hash (`sahs/loop/skill_index.py`) | index is per pod (fine: derived); no embedding rerank (seam exists) |
| short-term memory | `ChatMessages`, `ChatSessions.Notes`, `Handoff`, the files on the chat | none for correctness; every turn re-reads the transcript from the store |
| long-term memory | `ChatMemories` per person, edited as `memory.md` | no decay or dedupe; no cross-session summary |
| shared state | Spanner via one `Database` object; per-person runtimes in process memory | runtimes never evicted; a turn is visible only on the pod that runs it (replay from `ChatEvents` covers restarts) |
| caches | 5-second session cache; `.env` parsed once; KC fold and build caches | no cross-pod cache; no answer cache; no model context cache configured |
| resilience | store write failures on the event sink are logged, never raised; identity failures map to 503 | no retry/backoff policy on model calls; no circuit breaker on BigQuery; no index fallback (being added) |
| accuracy | the E19 suite on scripted transports; checks and hooks inside every turn; the loader record per turn | no routing-accuracy eval; no SQL-precedent eval; no production sampling of transcripts by outcome |
| latency | parallel task waves; SSE streaming; the KC fold once per graph state | prefill of a whole 125K-token skill every turn; a cold skill index on a fresh pod |

## 3. Latency: where the time goes, and the order to attack it

1. **Prefill of the skills block.** A 502K-character skill is roughly 125K
   tokens. On Vertex, implicit context caching is on by default for Gemini
   2.5 and later and gives a 90% discount on cached input tokens with no
   storage cost; explicit caching needs a minimum of 1,024 to 2,048 tokens
   depending on the model and a TTL that defaults to 60 minutes and can be
   extended ([overview](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/context-cache/context-cache-overview?authuser=0),
   [Gemini API caching](https://ai.google.dev/gemini-api/docs/caching)).
   The harness already keeps the prompt prefix byte-identical before the
   skills section; putting the whole-loaded skills inside that stable prefix
   is what makes the implicit cache hit. Action: pin the prefix through the
   skills block (a test), then measure cache hit rate from the usage
   metadata the client already receives. On the gateway plane, caching
   support is unproven; `gateway_check.py` should send the same prefix twice
   and read the cached-token count before anyone assumes it.
2. **The library path when a skill does not fit.** Search is 3 to 5 ms; the
   cost is the passages' tokens, bounded by the engine's library budget. No
   work needed beyond the accuracy loop in §5.
3. **Cold pods.** The skill index builds in well under a second for 2.5 MB;
   the build bundle unpack is the real cold cost. Action: warm both in the
   readiness probe, not on the first request.
4. **Store round trips per turn.** Each turn reads the transcript, memories
   and project once; the session cache saves the identity read. This is
   already close to the floor; do not add a cache here until a trace shows
   it matters (Langfuse spans give the number).
5. **Repeated questions across people.** See §4.

## 4. Caches, with their keys

A cache without a complete key is a wrong answer waiting to be served. The
literature on semantic caching agrees on three failure modes: staleness
(the cache becomes "a museum of old truths"), permission leakage across
users, and poisoning through crafted near-collisions
([semantic caching safety](https://pyimagesearch.com/2026/05/04/semantic-caching-for-llms-ttls-confidence-and-cache-safety/),
[Redis semantic cache](https://redis.io/docs/latest/develop/use-cases/semantic-cache/),
[GPTCache](https://github.com/zilliztech/GPTCache)). Each cache below names
its key so all three are closed by construction.

| cache | what it stores | key | TTL / invalidation | where |
|---|---|---|---|---|
| model context cache | the stable prompt prefix (system, style, whole skills, shelf) | the provider's own hash of the prefix | 60 min default, refreshed by use | Vertex (on by default); gateway to be proven |
| skill index | chunks, TOC, routing rows | (skill name, content sha256, chunker version) | rebuilt only when the skill changes | per pod, sqlite; already built |
| session cache | the person behind a cookie | token hash | 5 s; sign-out evicts | process today; Memorystore when multi-pod |
| **plan cache** (new) | the validated SQL plan for a normalized question: metric, population, grain, dates, source, joins | (normalized question, promoted build id, skill content hashes, model id, plane) | invalidated by a new build or a skill change; no TTL needed | Spanner table `AnswerPlans`; hot copy in Memorystore later |
| **answer draft cache** (new, optional) | the final prose and artifact spec with its "as of" stamp | plan key plus the person's permission scope hash | short TTL (hours); never across scopes | same |
| skill rerank cache (later) | embeddings per chunk | (chunk id, embedder version) | with the index | same file as the index |

Design rules for the two new caches:

- **Cache the plan, re-run the query.** The expensive part of a repeated
  analytics question is getting to the right governed SQL, not running it.
  A plan hit skips the model's reasoning and the precedent search, then
  executes under the same checks, limits and credentials as a fresh turn.
  The number is always fresh and always the person's own.
- **Serve a draft only with its stamp.** If an answer draft is served, the
  card says "as of build X, computed at T" and the person can re-run.
- **Normalize before matching, lexically first.** Lowercase, strip
  punctuation, expand the aliases the skills declare (the routing index
  already holds them), then exact match. Add embedding similarity only with
  a high threshold and a confidence check, and log every semantic hit for
  the weekly read; a wrong hit costs trust faster than a miss costs tokens.
- **Namespace by everything that changes the answer**: build id, skill
  hashes, model, plane, permission scope. A cache that ignores one of these
  is the museum.
- **Measure before enabling by default**: hit rate, tokens saved, and a
  sampled correctness check against a fresh run. The `skills_loaded` record
  and Langfuse spans already carry the tokens; add a `plan_cache` field to
  `turn_done`.

## 5. Accuracy: the loop that makes the numbers move

Long-context models do not make retrieval optional. Needle-in-a-haystack
scores are near perfect on Gemini 1.5 and later regardless of position, but
multi-hop and aggregation accuracy drops sharply between 32K and 128K
tokens on most models, and questions that score 95% at 4K can score 60 to
70% at 256K ([long-context RAG performance](https://arxiv.org/pdf/2411.03538),
[RAG vs long context, hybrid](https://arxiv.org/pdf/2407.16833),
[BABILong](https://arxiv.org/pdf/2406.10149)). The governed skills are
exactly the multi-hop case (a KPI's definition, its segment, its field, its
lag and its SQL precedent live in different sections). So:

1. **Keep whole-load for skills that fit, and measure it.** The evaluation
   from the analyst question-to-SQL precedents scores two things
   separately: did the turn pick the right skill (the `skills_loaded`
   record and the routing hint make this observable), and was the SQL
   semantically right (metric, population, grain, dates, joins). Run it per
   engine at each depth. This is the missing loss function; everything else
   in this paper is tuned against it.
2. **Prefer the library path when it wins.** If the eval shows a 125K-token
   whole load loses to TOC plus passages on multi-hop questions for some
   engine, mark that skill `sectioned` for that engine. The policy is data,
   not doctrine.
3. **Section anchors in the skills.** The `METRIC:` / `RULE:` / `SOURCE:` /
   `PRECEDENT:` / `CONFLICT:` headings the knowledge design prescribes are
   what the chunker keys on; they raise both whole-load attention and
   library precision. Worth doing before the eval, cheap.
4. **Precedent before generation.** The plan cache in §4 is the mechanized
   form of "reuse a validated precedent after checking the semantic frame".
   A hit should record which precedent it came from, so the eval can grade
   precedent reuse on its own.
5. **Weekly transcript reads by outcome.** Langfuse holds the traces; sample
   twenty by status (refused checks, low-confidence chips, cache hits) and
   read what the model saw. This is the harness-discipline rule the repo
   already states; the new records make it cheaper.

## 6. Resilience: every dependency gets a fallback and a signal

| dependency | failure | today | to add |
|---|---|---|---|
| Spanner | transient abort / unavailable | 503 on identity; event sink logs and continues | retry with backoff on reads through the `Database` wrapper (the SDK retries aborts inside `run_in_transaction` already); a per-pod read-through of the last transcript so a turn can finish when the store blinks |
| model plane | 429 / 5xx / timeout | the turn ends with an error card | bounded retry with jitter on idempotent calls; on repeated failure, the other plane if configured (the model picker already knows both); the task board marks the task failed, never the turn |
| BigQuery | quota, auth, timeout | the sandbox refuses live runs by gate | a circuit breaker per person (N failures in M minutes opens it; the card says why); dry-run stays available |
| skill index | read-only disk, corrupt file, chunker error | (being added) in-memory index; single-chunk fallback | none further |
| gateway token service | expired / unreachable | refused call | cache the token to its expiry minus a margin; refresh ahead of time |
| a pod restart mid-turn | lost bus | replay from `ChatEvents`; the page shows the interrupted turn | resume a task run from `plan_made` for the tasks not yet done |
| Memorystore (when adopted) | unreachable | n/a | every use falls back to process memory; the cache is never a system of record |

Two principles hold across the table: a cache or an optimization must never
change an answer, only its cost; and every fallback must be visible on the
card or in the record, never silent.

## 7. The memory design, in one place

- **Working memory (a turn):** the prompt, the tool results, the sandbox
  scratch. Lives for the turn.
- **Short-term memory (a chat):** the transcript, the notes the loop keeps
  (last eight), the handoff summary written at each turn's end, the files.
  All in Spanner; every pod sees them; the next turn reads them.
- **Long-term memory (a person):** `ChatMemories`, scoped global or per
  project, retired never deleted, editable as `memory.md`. In Spanner.
- **Shared memory (a deployment):** the governed skills, the knowledge
  files, the review board, the plan cache. In Spanner (skills soon inside
  the build bundle).
- **Hot memory (a pod, then a cluster):** session cache, running turns,
  rate limits, cache hot copies. Process memory now; Memorystore when there
  is more than one pod and the numbers say the store round trip matters
  ([Memorystore with GKE](https://oneuptime.com/blog/post/2026-02-17-how-to-connect-to-memorystore-redis-from-a-gke-cluster/view)).

What is deliberately not added: a vector store for memories (the memory list
is small and disclosed whole in the prompt), and a summarizer that rewrites
old transcripts (the handoff already carries the summary; lossy rewriting of
governed conversations is the thing this system exists to avoid).

## 8. Where the skills live in production

Today the skills tree is a directory each pod reads. The build bundle
already ships as a checksummed, chunked artifact in Spanner
(`BuildBundles`, `BuildBundleChunks`). Shipping the eight skills inside that
bundle gives one promotion, identical pods, and a content hash per skill
for free (the index key, the cache key). Spanner's own full-text search
(`TOKENIZE_FULLTEXT`, `SEARCH`, `SCORE`, already used on `ChatMessages`)
could hold the chunk index too, so a fresh pod never builds one
([Spanner full-text search](https://docs.cloud.google.com/spanner/docs/full-text-search),
[ranked search](https://docs.cloud.google.com/spanner/docs/full-text-search/ranked-search)).
Recommended order: bundle first (correctness and identity), Spanner-side
index later only if cold-start time on a pod becomes the bottleneck the
readiness probe cannot hide.

## 9. The plan, in order, with the measurement each step needs

| step | change | proves itself by |
|---|---|---|
| 1 | no-refusal skill loading; index fallbacks | both suites; the loader record shows `mode` per skill |
| 2 | frontmatter on the eight skills (`runtime_loading`, `aliases`); section anchors | `scripts/skill_index_check.py` on the real packs: right section first for the precedent questions |
| 3 | the routing and SQL eval from the precedents, per engine and depth | a number per engine; the policy per skill follows it |
| 4 | prefix pin through the skills block; cached-token count read from usage; gateway caching probe | cache hit rate per plane in Langfuse |
| 5 | plan cache in Spanner (`AnswerPlans`), lexical keys, `plan_cache` on `turn_done` | hit rate, tokens saved, sampled correctness against fresh runs |
| 6 | retries, the BigQuery breaker, the plane fallback | fault-injection tests in the harness suite |
| 7 | skills inside the build bundle; warm-up in readiness | identical `skills_loaded` hashes across pods |
| 8 | Memorystore behind an interface for hot state, when multi-pod | the store round-trip number before and after |
| 9 | answer-draft cache with stamps; embedding rerank behind the seam | the eval from step 3, unchanged or better |

Sources consulted, beyond the repository:
[Vertex AI context caching overview](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/context-cache/context-cache-overview?authuser=0) ·
[Gemini API context caching](https://ai.google.dev/gemini-api/docs/caching) ·
[Semantic caching: TTLs, confidence, safety](https://pyimagesearch.com/2026/05/04/semantic-caching-for-llms-ttls-confidence-and-cache-safety/) ·
[Redis semantic cache](https://redis.io/docs/latest/develop/use-cases/semantic-cache/) ·
[GPTCache](https://github.com/zilliztech/GPTCache) ·
[Long-context RAG performance of LLMs](https://arxiv.org/pdf/2411.03538) ·
[RAG or long-context LLMs? A study and hybrid approach](https://arxiv.org/pdf/2407.16833) ·
[BABILong](https://arxiv.org/pdf/2406.10149) ·
[Spanner full-text search](https://docs.cloud.google.com/spanner/docs/full-text-search) ·
[Spanner ranked search](https://docs.cloud.google.com/spanner/docs/full-text-search/ranked-search) ·
[Memorystore from GKE](https://oneuptime.com/blog/post/2026-02-17-how-to-connect-to-memorystore-redis-from-a-gke-cluster/view)
