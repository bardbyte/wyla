# The model playbook: one harness, several engines

The harness is one chassis: one loop, one system prompt, one tool kit,
one depth dial with five stops. The models are engines, and each has
its own engine map. This page says what each map holds, where the map
lives in the code, how to prove it against the gateway, and how to
tune one engine without touching the others.

The table is `sahs/util/profiles.py`. Everything the harness does per
model reads from it; nothing else in the code names a model.

## The engines

| Model | Plane | Thinking | Levels it accepts | Where it fits |
|---|---|---|---|---|
| Gemini 3.1 Pro (Preview) | Vertex, streams | `thinkingLevel` | low · medium · high | Deep and Extra deep; the multi-step SQL and python turns. The laptop's only engine. |
| Gemini 3.7 Flash | gateway, whole calls | `thinkingLevel` | low · medium · high (minimal refused) | Everyday chat at Standard; Quick autopilot at high. The gateway's default. |
| Gemini 3.5 Flash | gateway | `thinkingLevel` | medium · high | The alternate workhorse when 3.7 Flash is not served. |
| Gemini 3.1 Flash Lite | gateway | `thinkingLevel` | minimal · low · medium · high | The one-shot JSON calls (judge, title, memory, reviews, suggestions) and Minimal depth. |
| Gemini 2.5 Pro | gateway | `thinkingBudget` | — | Retiring. Not listed by default; an `.env` that still names it gets the budget dialect and nothing more. |

The level lists come from Google's model pages (September 2026) and
the laptop's `gateway_check.py --all-models` run. The `--levels` probe
below asks the gateway itself; when it disagrees with the table, paste
the line it prints and the `.env` wins.

## How the dial folds onto an engine

The dial has five stops for every model: Minimal, Quick, Standard,
Deep, Extra deep, spelled `minimal / low / medium / high / max` to the
API. An engine that does not know a stop gets the nearest level it
does, the deeper one on a tie. So:

| Stop | 3.1 Pro | 3.7 Flash | 3.5 Flash | 3.1 Flash Lite |
|---|---|---|---|---|
| Minimal | low | low | medium | minimal |
| Quick | low | low | medium | low |
| Standard | medium | medium | medium | medium |
| Deep | high | high | high | high |
| Extra deep | high | high | high | high |
| the JSON one-shots | low | low | medium | minimal |

Before the table existed there was one global fold, and it sent `low`
to 3.5 Flash (which lists nothing below medium) and `json` as a
literal level to every 3.x model on the one-shot calls. Both are gone.

## Proving an engine against the gateway

Run these on the laptop, with the silo `.env` in place. Never from a
container, never with pasted credentials.

```bash
python scripts/gateway_check.py --all-models            # token · path · generate · tools · thoughts flag
python scripts/gateway_check.py --all-models --levels   # …plus one tiny call per thinkingLevel
python scripts/gateway_check.py --model gemini-3.5-flash --levels --only token,generate,levels
```

The `levels` row reads, per model:

```
✓ levels    accepts low, medium, high · refuses minimal, max · as the profile says (docs)
✗ levels    accepts medium, high · refuses minimal, low, max · the profile expected low, medium, high — paste GATEWAY_MODEL_LEVELS=gemini-3.5-flash:medium|high into the .env
```

Paste the line when asked. The profile is the default; the `.env` is
the deployment's word.

## The knobs, all in the `.env`

| Variable | What it does | Example |
|---|---|---|
| `GATEWAY_MODEL` | the gateway's default engine (the composer's first row) | `gemini-3.7-flash` |
| `GATEWAY_MODELS` | every model the gateway serves, default first | `gemini-3.7-flash gemini-3.5-flash gemini-3.1-flash-lite` |
| `GATEWAY_MODEL_LEVELS` | the levels a model accepts, from the probe; either plane, keyed by model | `gemini-3.5-flash:medium\|high` |
| `GATEWAY_MODEL_THINKING` | force a thinking style on one model | `gemini-3.1-flash-lite:none` |
| `GATEWAY_MODEL_CAPS` | an output ceiling per model (default 65536) | `gemini-3.1-flash-lite:8192` |
| `GATEWAY_THINKING_LEVELS` | the dial's words for every model, applied last | `minimal:low,max:high` |
| `VERTEX_THINKING_LEVELS` | the same, for the Vertex model | `max:high` |
| `GATEWAY_JSON_MODEL` | route the one-shot JSON calls to a lighter engine of the plane | `gemini-3.1-flash-lite` |
| `SAHS_TEMPERATURE_POLICY` | `default` leaves Gemini 3's temperature alone; `explicit` sends the caller's | `default` |
| `SAHS_MAX_SKILL_CHARS` | the longest skill that loads whole; over it a pack loads as a searchable library under the engine's `skill_budget` ([skill retrieval](skill-retrieval.md)) | `4000` |

## What Gemini 3 asked for, and where the harness answers

- **Temperature at the model's default.** Google's guidance for Gemini
  3 is to keep 1.0; lowering it on a thinking model invites loops and
  weaker reasoning. The chat turns always left it alone. The one-shot
  JSON calls were tuned at 0.0–0.3 for 2.5; on a Gemini 3 engine the
  agents now leave the field out (`temperature_for` in the profiles
  module), and `SAHS_TEMPERATURE_POLICY=explicit` restores the number
  for a side-by-side on the evals.
- **`thinkingLevel`, never a budget, on 3.x.** The two are mutually
  exclusive in the API; the profile picks one per engine.
- **Thought signatures back verbatim.** Both planes already echo the
  model's parts unchanged, and every functionResponse rides one user
  turn. Unchanged here.
- **A direct brief.** Gemini 3 is terse by default and answers a direct
  brief best; "think step by step" adds nothing to a model that already
  thinks. The system prompt gains one `<style>` section for the family
  (lead with the answer, match the length to the ask, independent tool
  calls in one turn, no narrated plan). It sits after `<mode>` and
  before the graph digest so the cached prefix holds. A 2.5 engine gets
  no such section; the shared prompt already speaks its dialect.
- **Prompt caching.** The gateway showed no cached tokens on any model,
  so nothing leans on a cache there. Vertex does cache the stable
  prefix; the section order keeps it stable.
- **No stream on the gateway.** Every gateway engine lands whole. The
  UI already shows the thinking pause and the tool rows between calls.

## Per-engine how-to

**Gemini 3.1 Pro on Vertex.** Leave the temperature at default, use
Deep or Extra deep (both fold to high), keep the system prompt prefix
stable so the implicit cache pays. Give it the multi-step SQL and
python work. It is the only engine on the laptop; everything below is
production.

**Gemini 3.7 Flash.** The everyday engine. Standard is medium; Minimal
and Quick both fold to low, since the model refuses minimal. Use high
for a Quick autopilot run that must finish in one go.

**Gemini 3.5 Flash.** Medium is its floor: Minimal, Quick and Standard
all fold to medium, so it is never the cheap choice. Keep it in the
list as the alternate when 3.7 Flash is not served; expect little gain
over 3.7 otherwise.

**Gemini 3.1 Flash Lite.** The only engine that takes minimal, so
Minimal depth reaches the model as minimal. Route the JSON one-shots
here with `GATEWAY_JSON_MODEL=gemini-3.1-flash-lite`: they are short,
structured, and many. Measure the judge's agreement before trusting it
for reviews.

**Gemini 2.5 Pro.** Retiring. Not in the default list, not in the
scopes, not in the docs beyond this line. An `.env` that still names
it gets the budget dialect (`THINKING_BUDGET`, `GATEWAY_THINKING_BUDGETS`).

## Adding an engine

1. Add a row to `PROFILES` in `sahs/util/profiles.py`: stem, family,
   thinking style, the levels it accepts (shallow to deep), the cap,
   one line on where it fits.
2. Name it in `GATEWAY_MODELS`; the scope derives from the name.
3. Run `gateway_check.py --model <name> --levels`; paste the
   `GATEWAY_MODEL_LEVELS` line if the probe disagrees with the row.
4. Run the E19 capability matrix on it before it becomes a default.

## What is deliberately not here yet

- **Tool descriptions.** Thirteen tools carry about 5,400 characters of
  description. Tightening them to roughly 3,500 and removing the
  overlap between `propose_sql` and `run_sql` is the next prompt change,
  and it wants an E19 delta of its own.
- **Role routing by depth.** Escalating Deep and Extra deep to 3.1 Pro
  when a chat is on a Flash engine is a one-table change once the evals
  say the Flash engines slip there.
- **Embedding rerank for skills.** A pack over the whole-load ceiling
  now loads as a searchable library (see [Skill retrieval](skill-retrieval.md):
  the per-engine `skill_budget` next to the engine maps, the depth
  fold, the three `skill_*` tools). The ranking is lexical BM25; the
  `Embedder` seam in `sahs/loop/skill_index.py` is where an
  embedding-based reranker plugs in once the transcripts say the
  lexical hits miss.
