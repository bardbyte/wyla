# One flag for local / dev / prod: ENV picks the .env file the loader
# reads (SAHS_ENV_FILE), and everything else follows from that file.
# docs/deploy.md is the mental model; synapse-agentic-harness-system/env/
# holds one placeholder example per profile.
#
#   make run ENV=local      copy env/local.env.example to env/local.env first
#   make check ENV=e1       the readiness table for that profile
#   make test               both suites
#   make ddl-check          the DDL under db/spanner, without a Spanner

ENV ?= local
SILO := synapse-agentic-harness-system
ENV_FILE ?= $(SILO)/env/$(ENV).env
PY ?= python
PORT ?= 8810

.PHONY: run test check ddl-check env-file

env-file:
	@test -f "$(ENV_FILE)" || { \
	  echo "no $(ENV_FILE): cp $(SILO)/env/$(ENV).env.example $(ENV_FILE) and fill it in"; \
	  exit 2; }

run: env-file
	SAHS_ENV_FILE="$(abspath $(ENV_FILE))" PYTHONPATH="$(SILO)" \
	  $(PY) -m uvicorn apps.synapse_admin.backend.app:app --host 0.0.0.0 --port $(PORT)

test:
	PYTHONPATH="$(SILO)" $(PY) -m pytest -q apps/synapse_admin/tests -p no:cacheprovider
	cd $(SILO) && $(PY) -m pytest -q tests -p no:cacheprovider

check: env-file
	SAHS_ENV_FILE="$(abspath $(ENV_FILE))" \
	  $(PY) $(SILO)/scripts/readiness.py --env $(ENV)

ddl-check:
	cd $(SILO) && $(PY) scripts/spanner_ddl_check.py
