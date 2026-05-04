# LUMI morning testing plan

Step-by-step run order for tomorrow morning. Each step has one goal, one
exact command, the success signal to look for, and a one-line remediation
when it fails. Walk top to bottom — do not skip ahead.

All commands assume:
- cwd is `lookml-enrichment-pipeline/lumi_final/`
- `.venv` is activated (`source ../.venv/bin/activate`)
- you are on `feat/sessions-2-through-7-pipeline`

---

## 1. Pull branch

**Goal:** start from latest pushed state.

```bash
git fetch origin && git checkout feat/sessions-2-through-7-pipeline && git pull --ff-only
```

Success: working tree clean, log shows the new probe commits.
If fail: `git status` to inspect divergence; do NOT force push.

## 2. Source Vertex env

**Goal:** export the four `GOOGLE_*` env vars before any `google.*` import.

```bash
source agent_test/setup_vertex_env.sh ~/Downloads/key.json
```

Success: `echo $GOOGLE_CLOUD_PROJECT` prints `prj-d-ea-poc`.
If fail: re-download SA JSON (svc-d-lumigct-hyd@prj-d-ea-poc); `chmod 600`.

## 3. Vertex preflight

**Goal:** confirm Gemini 3.1 Pro is reachable end-to-end before spending tokens.

```bash
python scripts/check_vertex_gemini.py --insecure
```

Success: prints a final answer to "what is 2+2?" using `gemini-3.1-pro-preview`.
If fail: TLS cert error → `pip install truststore`; 404 model → confirm location=`global`.

## 4. Run Session 1 on prod data

**Goal:** parse + discover all gold queries; produce `data/session1_output.json`.

```bash
python scripts/run_session1.py
```

Success: stdout shows ~30 unique tables; ≥120 SQLs parsed; guardrail status `pass` or `warn`.
If fail: missing inputs error → run `excel_to_queries.py` and `import_lookml_local.py` per the printed hints.

## 5. Hydrate MDM cache

**Goal:** prefetch MDM payloads so subsequent runs are offline-fast.

```bash
python scripts/probe_mdm.py --save data/mdm_cache/
```

Success: one `<table>.json` per discovered table under `data/mdm_cache/`.
If fail: VPN dropped → reconnect; 404s → MDM uses `?tableName=`, not path.

## 6. Re-run Session 1 with hydrated MDM

**Goal:** confirm MDM coverage lifts now that the cache is warm.

```bash
python scripts/run_session1.py
```

Success: per-table summary shows MDM% > 0 for most tables; "MDM cache miss" warning is gone.
If fail: cache dir wrong → check `LumiConfig.mdm_cache_dir` matches the `--save` target.

## 7. Phase 1 dry run (no LLM)

**Goal:** sanity-check the planner against real contexts; populate `review_queue/`.

```bash
python scripts/probe_pipeline_dry_run.py
```

Success: per-stage progress prints; `review_queue/<table>.plan.md` written for every discovered table.
If fail: planner error on a specific table → re-run with `--single-table NAME` to isolate.

## 8. Phase 1 with real Gemini

**Goal:** get LLM-authored "understanding" + "assessment" sections on each plan.

```bash
python -m lumi plan --input data/gold_queries/
```

Success: same plan files updated; status command shows `● Plan: N plans written, 0 approved`.
If fail: `NotImplementedError` from pipeline → Session 5 wiring incomplete; fall back to step 7.

## 9. Review queue triage

**Goal:** see who needs your attention; tick approval boxes in your editor.

```bash
python scripts/probe_review_queue.py
```

Success: prints `table | approved? | approver | feedback` table; auto-approved rows show `auto_low_risk`.
If fail: every row shows `pending` → open `review_queue/*.plan.md`, tick `[x] APPROVED` lines.
Then re-run; exit code should be 0.

## 10. Single-table enrich dry run

**Goal:** prove the enrichment prompt assembles for one approved table without spending tokens.

```bash
python scripts/probe_enrich.py --table cornerstone_metrics --dry-run
```

Success: prints prompt size + the planned EnrichedOutput contract; no Gemini call made.
If fail: this probe doesn't exist yet — substitute `pytest tests/test_enrich.py -k cornerstone -v`.

## 11. Single-table enrich with real Gemini

**Goal:** burn one table's worth of tokens to validate the live prompt.

```bash
python scripts/probe_enrich.py --table cornerstone_metrics
```

Success: writes `data/enriched/cornerstone_metrics.json`; gate `enrichment` status `pass`.
If fail: 429 quota → wait 60s, retry; output_schema mismatch → check fixture vs live response shape.

## 12. All-tables enrich

**Goal:** full Phase 2 enrichment for every approved plan.

```bash
python -m lumi execute
```

Success: `data/enriched/*.json` populated for every approved table; status command shows `✓ Enrich`.
If fail: `NotImplementedError` → Session 5 wiring; loop step 11 per table instead.

## 13. Validate enriched outputs

**Goal:** run coverage + SQL reconstruction; save the report.

```bash
python scripts/probe_validate.py --save data/coverage_report.json
```

Success: prints coverage ≥ 90%; SQL reconstruction status `pass`.
If fail: top_gaps section names the offending dim/measure → fix prompt + re-enrich that table.

## 14. Publish to dryrun output

**Goal:** materialise everything to disk WITHOUT touching `output/`.

```bash
python scripts/probe_publish.py
```

Success: every `output_dryrun/views/*.view.lkml` round-trips through `lkml.load`; pre-publish gate `pass`.
If fail: lint error names the bad view → diff against baseline; usually a missing `;;` on a SQL line.

## 15. Manual verification + apply

**Goal:** eyeball one enriched view against `.claude/skills/lookml/SKILL.md` and apply if good.

```bash
diff -u data/baseline_views/cornerstone_metrics.view.lkml output_dryrun/views/cornerstone_metrics.view.lkml | less
```

Then once happy:

```bash
python scripts/probe_publish.py --apply
```

Success: `output/views/`, `output/models/`, and the four catalog JSONs exist and lint.
If fail: revert with `git restore output/`; re-run dryrun until satisfied.

## 16 (optional). Open PR

**Goal:** ship the enriched LookML to GitHub Enterprise.

```bash
gh pr create --title "lumi: enrich N views" --body-file - <<'EOF'
Summary: enriched output of LUMI Phase 2 against approved review queue.

Test plan:
- [ ] coverage report ≥ 90% on all approved tables
- [ ] every emitted .view.lkml lints clean
- [ ] manual diff review on top-3 priority tables
EOF
```

Success: PR URL printed.
If fail: SSO not authorized for amex-eng → re-tick "SAML SSO" on PAT settings.

---

## Known gotchas tonight

- **407 corporate proxy.** Some Vertex calls intermittently fail with HTTP 407 from the SquidGuard gateway. Symptom: `urllib3.exceptions.ProxyError`. Fix: `unset HTTPS_PROXY HTTP_PROXY` or run via `scripts/no_proxy_shell.sh` for that terminal. The diag helper is `scripts/diag_network.sh`.
- **BigQuery probe deferred.** The `INFORMATION_SCHEMA` + `SELECT DISTINCT` step (originally planned to enrich allowed_values) is not implemented yet — `data/enriched/*.json` will have empty `known_values` lists in the filter catalog. That is acceptable for tonight; backfill in a follow-up sprint.
- **Tuning prompts.** If coverage stalls below 90%, the lever is `lumi/prompts/enrich_view.md`. Look at top_gaps in step 13 to see which fields are missing, then tighten the SCOPE block of the prompt to require them. Keep the change small (one rule per iteration) and re-run `pytest tests/test_enrich.py` between edits.
- **Don't touch `lumi/enrich.py` while the looker-expert agent is iterating.** That file plus `lumi/prompts/enrich_view.md`, `tests/test_enrich.py`, and `tests/fixtures/llm_responses/enrich_*.json` are owned by the parallel session. Coordinate before editing.
- **`adk web` paperclip is broken for Excel.** When iterating against the curator app, paste the path in chat — never use the file-attach button.
- **MDM is array-shaped.** If a probe crashes on `data["schema"]`, you forgot to peel `data[0]` first. Always.
- **`gemini-3-pro-preview` is gone.** Pin `gemini-3.1-pro-preview` everywhere.
