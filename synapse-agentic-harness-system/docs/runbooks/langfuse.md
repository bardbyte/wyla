# Langfuse: the mirror of the record

Langfuse is a read-mostly analytics and review layer over material
Wyla already writes. Every object in it is derived from a local
artifact by a one-way sync:

| Wyla writes | Langfuse shows |
|---|---|
| the assistant's event stream, one turn | one trace: generations, tool spans, artifact events, a status score |
| `tests/tasks/*/*.jsonl` | one dataset per file, items keyed by task id, the whole task in metadata |
| `run_suite`'s verdicts | one dataset run per eval, the verdict as a score on each trial's trace |

Nothing in the product or the suite reads Langfuse back. Off (the
default) or unreachable, a turn is a turn and a score is a score. The
translator (`sahs/observe/tracer.py`) is a pure function of event
records, so replaying an events file gives the trace the live turn gave.

## Switch it on

In the silo `.env`:

```
SAHS_LANGFUSE=1
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...
LANGFUSE_BASE_URL=http://localhost:3000     # LANGFUSE_HOST on older SDKs
```

`pip install langfuse` (SDK 4.x; the adapter was verified on 4.15).
Then prove the keys:

```
python scripts/langfuse_sync.py check
```

Tests run with the switch off: no import, no thread, no network.

## The four coordinates

Every trace carries `build_id`, `version` (the assistant's prompt
version), the plane and the model label in its metadata, and every
eval run is named `<sut>·tasks@<hash>·canon<version>·<time>`. Two
lines are comparable only when all four match. Filter on them; never
compare across them.

## Step one: live traces

Start the app with the switch on and ask one question. Open the trace
in Langfuse and put it beside the "what the model saw" panel for the
same turn. They should agree step for step: same calls, same tools,
same summaries. Usage per generation is the delta between the budget
ticks around it, from the client's own counters.

Tool results carry real warehouse rows. Spans keep the compact summary
unless `SAHS_LANGFUSE_FULL_RESULTS=1`. Tag an environment with
`SAHS_LANGFUSE_ENV=laptop` so laptop and CI traces separate.

## Step two: backfill

```
python scripts/langfuse_sync.py backfill graph/runs/chat/events
```

Same translator, same trace ids (a hash of session and turn), so a
re-run overwrites rather than duplicates. Timing is import time; the
record's own timestamps stay in the events file.

## Step three: datasets and the calibration lines

```
python scripts/langfuse_sync.py datasets
python scripts/run_evals.py --tasks tests/tasks/curated/curated.jsonl --sut oracle --langfuse
python scripts/run_evals.py --tasks tests/tasks/curated/curated.jsonl --sut null --langfuse
```

The dataset-run view must show the oracle at 100 percent pass and the
null SUT at exactly the abstain share before any real SUT is worth a
look. If either is off, the sync is broken, not the model. The grader
never moved: it is `sahs/evals/grading.py`, and Langfuse displays what
it decided.

## What is deliberately not here

- **No prompt fetching at runtime.** The system prompt is assembled
  from code, byte identical per build, with a pinned routing key.
  Prompt versions are metadata on the trace, not state in Langfuse.
- **No LLM judge on SQL.** Fingerprint equality, arity, dry run and
  schema decide. A judge belongs only where fingerprints cannot reach.
- **No writes back.** Annotation-queue resolutions land in the task
  files through git, when that step is built, never through the API
  into a running system.

## Shutdown

If Langfuse is down, the SDK retries a score batch a few times with
backoff and drops it. Expect a few seconds' delay on process exit, not
a hang.
