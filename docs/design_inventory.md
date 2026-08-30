# Synapse by Lumi — design inventory (draft, staged for docs/design_inventory.md at go-ahead)

> Sweep of apps/console/design/** on 2026-08-30 (branch tip e0067f4). Two visual systems
> run through the set; **Lumi is canonical**, Synapse-branded twins are superseded.

## Format
All 26 artboards are Claude Design canvas files: `<x-dc>` template + `DCLogic` component with
`renderVals()`; verbs `sc-for` / `sc-if` / `x-import` / `{{ }}` / `style-hover`. All styling
inline (no classes) except `Synapse Wireframes.dc.html`. `support.js` (1,911 L) is the
VENDORED canvas runtime ("GENERATED — do not edit"; expects window.React) — black box.
Artboards are design references; production pages build in the React SPA.

## Token systems
- **Synapse** (earlier): Libre Franklin + IBM Plex Mono; #f4f6f9/#1a2332/#71809b/#d8dee8;
  accent #006fcf; tiers green #0e7a55, amber #9a6407, gray #b9c2d0, crimson #be3a48.
  = EXACTLY frontend/src/styles/tokens.css (that file rendered).
- **Lumi** (rebrand, canonical): Plus Jakarta Sans + IBM Plex Mono; #f5f8fc/#00175a/#5b6b8f/
  #d7e2f2; deep #00175A, bright #006FCF, ice #EAF2FB, on-navy #c6dcf5; tiers green
  #0e7a55+#eaf6f0, amber #b07908+#fdf4e0, gray #7c8bab/#a9b8d4, crimson #b3282d+#fdf0f0.
  Recorded in github.md as intentional; NOT yet in tokens.css → sync task.
- Shared: cards #fff/1px line/r14-16/shadow 0 1px 3px rgba(0,23,90,.07); chips r999;
  meridian line = border-top:2px solid #006fcf; tiers ● ◆ ◐ ○ ⊘ (shape+color, never color
  alone); fadeUp/barIn/slotPulse/ripple/blink/dashFlow + prefers-reduced-motion kill switch.
- tokens.css doctrine: brand blue = interactive only ("If it isn't clickable, it isn't
  blue"); content colors always MEAN something; one reserved data hue (#3b6fe0); dark theme
  first-class (accent #57A9F2). Wireframes are light-only → dark port is our job.
  KNOWN AA FAILURE: --ink-3 #71809b at 3.41:1 (ux-research-notes) — still unfixed.

## Artboards (file → screen → notes)
- Synapse Wireframes.dc.html — exploration sheet (brand options 2a-2f, screens ×3 layouts);
  only file with CSS classes; sketch layer in Patrick Hand.
- Lumi Home.dc.html — landing: navy hero + 3 CTAs; WHAT IT DOES (6 promises) / LIVE PROOF
  (46 tables·3,074 metrics·15,288 vocab·joins 2◆·readiness 73%·TQSR 0.71·reviews 14);
  SINCE LAST BUILD strip; 5-group screen index (HOME/UNDERSTAND/GOVERN/OPERATE/WORK·E16);
  tier legend.
- Semantics Explorer.dc.html — tabs metrics/concepts/joins/vocab; filter chips
  (status/witness/LOB/ungoverned); 7-col table (sel/metric+expr/status/witnesses/uses/
  reach/lifecycle); bulk Promote→steward queue; "writes go through the clerk" line.
- Metric Profile.dc.html — header+status chip+owner+LOB; mono SQL+fp+evidence_origin;
  MEANING (question prov:dmp, grain prov:studio·observed, amber llm_enriched unreviewed);
  WITNESSES bars+agreement; FAMILY tree (proposed/child/quiet ○) + crimson competing-
  definition banner; BINDINGS; WHO USES (GMNS 1302/CFR 1089); Rename/Deprecate.
- Lumi Table Profile.dc.html (canonical; Table Profile.dc.html = Synapse twin) — 73%
  readiness donut; columns grid w/ per-column tier+PII ⊘; measures 3-up; witness ledger
  (human ×10/MDM ×8/BigQuery ×6/glossary ×5/mined ×2); usage sparkline; join topology;
  "what we don't know yet" dashed card; 360px "Who says so" drawer; viewMode analyst|VP.
- Concept Profile.dc.html — alias/anti-alias chips; bindings-by-table grid w/ conflict
  banner; REMEMBERED CHOICES (scoped memory) card.
- Graph Cosmos.dc.html / F6 Cosmos.dc.html (Lumi canonical) — search + edge-filter chips;
  <synapse-cosmos> 560px; right rail docked node profile/legend/RECENT CHANGES feed;
  "size = usage · glow = trust tier · gold = multi-domain".
- Agent Theater.dc.html — 5-stage replayable run player (Understand/Route/Load skills/
  Descend/Draft SQL); routing scores; guardrails ⊘; SQL trace annotated; refusal branch.
- Metric Flywheel.dc.html / F3 Variation Flywheel.dc.html (Lumi canonical) — answer card
  DERIVED FROM strip (meridian labeled); Propose-as-governed/Save-as-child/Just-this-once;
  family tree; steward promotion card.
- F1 Analyst Conversation.dc.html — 100vh chat + 330px Semantic plan rail; 6-step scripted
  convo (US TBB→Canada→by market→avg ticket); plan slots metrics/source/grain*(REQ)/
  filters/compare; v‹4› stepper; slotPulse.
- F2 Executive View.dc.html — persona-switchable answer chrome (exec 52px hero vs analyst
  38px + bindings/build line).
- F4 Steward Cockpit.dc.html — 3-col: ranked queue + inbox (D1 768, divergence, enricher
  suggestions); ONE DECISION card; ripple feed + undo; approve-rate guard ">95% flags
  rubber-stamping".
- F5 Discovery.dc.html — search + lifecycle rows (owner of next action, SLA); dashed
  "nothing here means…" → 4-step request lifecycle stepper.
- F7 Complex Analysis.dc.html — exploratory notebook (EXPLORATORY watermark, amber banner);
  governed-SDK script; contract-gate checklist (bytes/certified expr/grain/explanation);
  promoted meridian footer.
- F8 Session Continuity.dc.html — 4-frame storyboard: resume/ask/diff (geo Canada←changed)/
  answer.
- F9 Budget Grace.dc.html — 4-frame budget degradation: meter 58→86%, graceful wrap,
  handoff w/ verified partials.
- Enrichment Runs.dc.html — run cards (b1_r3 prompt b1.2); blind gate 28/34=82%→BATCH
  (80/60 tiers); leakage 0/34; enriched 500/500; collisions 7; invalid_json 2; margins
  histogram; thought tokens 1.2M; cost $18.40. → feeds directly from enrich_report.json +
  blind_results.jsonl.
- Builds and Diffs.dc.html — CURRENT card (b_228309551bae, gates ✓); "tables 45 (+1
  excluded, 46 vs 45 reconciliation)"; diff grid (+1 studio metric, joins 0→2 ◆,
  agreement=2 0→14, D1 768/D3 37/D5 901, warnings→0); promote/rollback.
- Evals Dashboard.dc.html — TQSR pass³ 0.71 stacked bar; clarification quality; suite table
  (curated/sheet-derived/adversarial/exploratory w/ failure classes).
- Assumptions.dc.html — A-register rows (A1..A8 w/ status incl. BLOCKING) + PINNED
  CONSTANTS + MODELS & ABLATION LOG. → docs/assumptions.md rendered.
- Sessions.dc.html — session list (mine/all); plan-version chain v1→v5; budget $/cap;
  handoff preview. Flagged "ships with E16".
- Lumi Components.dc.html — component library spec, 13 specimens: 1 Answer Card · 2 Plan
  Panel · 3 Disambiguation Chips · 4 Join & Grain Preview · 5 Tap-out Card · 9 Lifecycle
  Tracker · 10 Provenance Popover · 11 Agent Theater Strip · 12 Verifier Verdict Card ·
  13 Analysis Notebook · 14 Budget Meter · 15 Plan Version Stepper · 16 Session Handoff
  Card (numbers 6-8 absent). Grain REQ crimson *; evidence-bearing disambiguation options.
- Lumi Principles.dc.html — 8 principles (thresholds; one component everywhere; uncertainty
  is a rendered surface; recorded as you·reversible; flywheel shown; verification is a
  surface; exploratory carries a passport; the stream is the interface). "crimson =
  definition conflict, only, ever."
- github.md — sync manifest → apps/console types.ts/tokens.css/events.py; Lumi rebrand
  recorded.

## cosmos.js (151 L, near-complete)
<synapse-cosmos> custom element; three.js 0.160.0 ESM from jsdelivr; NOT force-directed —
deterministic seeded positions (mulberry(42)) around hard-coded DOMAIN wells; node
{id,name,domain,domains[],tier,usage,kind,pos,star?}; edge {a,b,kind joins|membership|
computed-from}; scale 0.3+sqrt(usage)/90; TIER colors ha/gr/in/gu/de; star = gold halo;
edge groups toggleable; drag-orbit/wheel-zoom/raycast pick → CustomEvent 'cosmos-pick';
theme paper|navy; ResizeObserver; reduced-motion. Missing: real data feed (makeGraph is
synthetic), 2D fallback, edge-kind colors, label collision.

## Earlier mocks (apps/console/design/)
- console-mock.html (v1) — chat-transcript-first + trace rail (model later abandoned);
  own palette; real dark theme; self-documenting spec.
- radix-workspace-mock.html (v2) — "The Brief is the product"; introduces the Bright Blue
  token set (#006FCF/#00175A/#0E7A55/#9A6407/#BE3A48/#3B6FE0) + dark theme; tchip c0-c3
  fill-levels → later ● ◆ ◐ ○; witness drawer; hold ceremony.
- radix-app-mock-v3.html — full IA in one SPA-shaped doc; five tabs → the five groups and
  the shipped App.tsx tabs; same tokens as v2.
- Companion verdicts: ux-research-notes (approve direction, block on 6 items; no in-flight
  state was fatal; AA failure measured) · greenfield-vision (not a chat app; the durable
  Brief; never stream the answer — stream the work; NO numeric confidence ever; success
  includes declining weak claims) · synthesis-vision (settled: narrate the wait, refusal
  as designed beat, authorization as ceremony, evidence chip on every claim; D1 divergence
  transcript-vs-Brief) · critique-panel (projection architecture strong; "nothing
  accumulates" fatal; celebrating-a-delinquency-spike bomb; "Composing"-while-stopped bomb)
  · feature-notes-tabs (nav = Inquiries/Data Products/Metrics/Knowledge Graph; labeled
  previews, never unlabeled fakes).

## App state (apps/console) — real, running
Vite+React18+TS SPA (radix-console 0.1.0; built dist/ checked in; ~2,900 L src: App.tsx
tab shell, Ask 679 L, Graph 634 L, Products, Knowledge, WitnessDrawer, api/sse/types/copy/
nav/theme/useLexicon). FastAPI backend (app.py 361 L "Radix Console" 0.3.0 + data.py 1,013 L
+ runner 488 L + events 228 L + pins 292 L + evaluator + verbs): /health, /api/config,
POST /chat SSE, POST /approve, read API (/api/products{,/by-unit}, /api/metrics{,/viability},
/api/terms/resolve, /api/graph/{summary,map,insights,thread}, /api/questions{,/starters},
/api/witness?ref=, /api/lexicon, /api/evals/recent, /api/agent/selftest, /api/pins CRUD);
serves the SPA via StaticFiles after API routes. Runner: SYNAPSE_CONSOLE_RUNNER scripted
(default, offline goldens) | adk (Gemini on Vertex). Pytest suite present. CORS open (dev)
— lock before non-localhost. data.py binds to the OLD semantic-graph → re-point at
sahs.tools.api.Build.

## Deltas this inventory forces into the build plan
1. tokens.css: sync to Lumi palette (light + dark per doctrine) + fix --ink-3 AA failure.
2. Backend: re-point existing read endpoints at Build.open (don't invent parallel routes).
3. Enrichment Runs + Builds & Diffs pages are near-free (reports already shaped).
4. Builds artboard already renders "45 (+1 excluded)" — honesty decision pre-made by design.
5. Lumi Components is the component contract; new components register there + here.
6. Carry doctrines: no numeric confidence in UI; labeled previews never unlabeled fakes;
   accumulation surfaces (pins/reviews/feedback); crimson = conflict only ever.
