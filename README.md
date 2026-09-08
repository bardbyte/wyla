# wyla — Synapse by Lumi

The repo has two halves: the product, and the archive of everything that
came before it.

```
synapse-agentic-harness-system/   SAHS — the Meridian silo: census → ground →
                                  truth graph → compiled builds → serving tools,
                                  the agent loop, evals, the laptop scripts
apps/synapse_admin/                        Synapse by Lumi — FastAPI read plane + the
                                  hand-authored ES-module frontend (one process,
                                  zero build); apps/synapse_admin/design/ holds the Lumi
                                  wireframes and mocks the product was built from
apps/synapse/                     Synapse Semantic Intelligence — the second
                                  frontend, served by the same Lumi server at /synapse/
docs/design_inventory.md          the design-file sweep the UI skill keeps current
archive/                          the pre-SAHS platform, kept whole for reference
```

## Run

```bash
pip install -e "synapse-agentic-harness-system[sql,dev]" fastapi uvicorn
cp synapse-agentic-harness-system/.env.example synapse-agentic-harness-system/.env  # fill in paths
uvicorn apps.synapse_admin.backend.app:app --port 8400        # from the repo root
python -m pytest synapse-agentic-harness-system/tests apps/synapse_admin/tests -q
```

The silo's runbooks are the operating manual:
`synapse-agentic-harness-system/docs/runbooks/laptop_end_to_end.md` first,
then `e18_ask.md` for the conversational surface. Meridian data (sources,
archives, `graph/`, `builds/`) lives outside git at the `MERIDIAN_*` paths in
the silo `.env`; only the test fixtures are checked in.

## The archive

Everything under `archive/` predates the SAHS scaffold (commit `9f9ef7a`,
2026-08-25) and nothing in the product imports from it: the `synapse/`
platform, `lumi_final/`, `semantic-graph/`, the legacy `apps/console` shell
and the ADK agents it hosted, the root probe scripts, and the old
blueprint, audit, and session docs. `archive/README.md` is the README that
described that world. It is kept so it can be mined or deleted deliberately,
not because anything runs from it.
