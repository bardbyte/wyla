# E18 Ask — the agentic loop (Stage A)

One stateful loop carrying the versioned semantic plan; stateless
workers around it. It runs **in-process** with the Synapse app, not as
a subprocess, so a turn is a thread inside the server and the events
it emits are the objects the browser receives.

```
classify → apply → delta-resolve → validate → generate → verify → render
```

## Start it

```bash
# from the repo root
uvicorn apps.lumi.backend.app:app --port 8400
```

Ask mounts at `/api/sessions*`. Paths and credentials come from the
silo `.env` (see `.env.example`); nothing is passed on the command
line and no key ever reaches the browser.

| variable | what it does |
|---|---|
| `MERIDIAN_BUILDS_DIR` / `MERIDIAN_GRAPH_DIR` | which build Ask answers from, where sessions and events are written (`graph/runs/ask/`) |
| `LUMI_VERTEX_SA_KEY` | the Vertex service-account key (a DIFFERENT project than BQ) |
| `VERTEX_PROJECT_ID` · `VERTEX_LOCATION` · `VERTEX_MODEL` | the model contract; location defaults to `global`, model to `gemini-3.1-pro-preview` |
| `SAHS_ALLOW_LIVE=1` + `ASK_EXECUTE=live` | BOTH required before Ask executes for real; otherwise every query is a dry run that returns zero rows by design |
| `SYNAPSE_COST_IN` / `SYNAPSE_COST_OUT` | dollars per million tokens. Unset → the budget meter reports tokens and says the rate is not configured, rather than inventing a price |

## Drive it without any UI

```bash
python scripts/ask_demo.py "acquirer net spend by day"
python scripts/ask_demo.py "acquirer net spend" --pick 1 \
       --then "same for Canada"
```

A real run against the fixture build looks like this — note the
timings, which are the whole UX bet:

```
> acquirer net spend            [analyst · build b_2cd603279061]
  classify: new_question (deterministic) first turn of the session
  resolved in 0.6ms: Acquirer Net Spend
? What should one row mean?
    1. transaction              declared by another metric on this table
    2. card member              declared by another metric on this table
    3. card member x day        declared by another metric on this table
    4. one row for the whole window   no grain on record for this metric
> transaction
  classify: mutate (deterministic) the analyst picked transaction
  plan v2: grain '' → 'transaction'
  resolved in 0.0ms: (nothing new)
  contract (all false until proven):
    ✗ the query runs against the promoted build
    ✗ every table, column and metric it names is real
    ✗ one row means: transaction
    ✗ the scan stays inside the cost gates
    ✗ the written answer says only what the query supports
```

## Or with curl

```bash
S=$(curl -s -XPOST localhost:8400/api/sessions \
      -H 'content-type: application/json' -d '{"kind":"analyst"}' \
      | python -c 'import sys,json;print(json.load(sys.stdin)["session"]["id"])')

curl -N "localhost:8400/api/sessions/$S/stream" &      # watch it live

curl -s -XPOST "localhost:8400/api/sessions/$S/messages" \
     -H 'content-type: application/json' \
     -d '{"text":"acquirer net spend by day"}'

# answer a clarify by posting the chip back as a structured choice
curl -s -XPOST "localhost:8400/api/sessions/$S/messages" \
     -H 'content-type: application/json' \
     -d '{"text":"transaction","choice":{"slot":"grain","value":"transaction"}}'

curl -s -XPOST "localhost:8400/api/sessions/$S/stop"    # stop means stop
```

`GET /api/sessions/{id}/stream?once=1` closes at `turn_done` (handy in
scripts); `?after=<seq>` or a `Last-Event-ID` header resumes exactly
where a dropped connection left off.

## The event family (`meridian.event/1`)

`turn_started · classify_result · plan_delta · resolve_started ·
resolve_result · clarify_request · contract_ready · generate_token ·
verify_progress · verify_verdict · answer_payload · notebook_artifact ·
budget_tick · budget_grace · turn_done · error`

The stream is the single source of UI truth and the events file is the
record: replay is re-consuming `graph/runs/ask/events/<session>.jsonl`.

## What is pinned

- **Deterministic until you can't.** The first turn of a session and
  every chip answer classify without a model call. Resolution is the
  resolver, not a prompt, and is measured in the tests: under 300ms.
- **One clarifying question per turn**, ranked metric → grain →
  filter binding, chips carrying real evidence. Below-margin is a
  success state; the resolver never argmaxes.
- **Grain is required.** No grain, no contract, no answer. The
  serializer raises rather than rendering an ungoverned payload.
- **Single-slot mutation.** "Same for Canada" moves one slot,
  computed in code, and only that slot is re-resolved.
- **Default-FAIL contract.** Every criterion starts false; UNKNOWN is
  a failure (a cost gate that cannot read a byte estimate does not
  wave the query through). The verifier is fresh-context, read-only,
  and never sees the generator's reasoning.
- **Budgets and breakers in code.** Session and turn caps, one turn
  per session, oversized results truncated with the withheld count
  carried. The stop button fires the same abort the breaker fires.
- **The deterministic half needs no model.** The model is built at
  first use, so a machine with no Vertex still resolves, still asks
  its clarifying question, and reports the missing contract as an
  honest error card with next actions.

## Where an unreachable warehouse lands

A dry run that cannot reach BigQuery does not kill the turn: the
`executes` and `cost_gate` criteria fail on the evidence, and the
answer renders with those failures named in its limits. An answer
whose verdict is `fail` is still shown — with the failure visible.
That is the difference between honest and polished.

## Stage status

Stage A (this runbook) is landed and tested. Stages B–F — the chat
surface, the plan panel, the search constellation, the exploratory
notebook lane, sessions/budgets/flywheel, and the TQSR loss function —
build on this stream without changing it.
