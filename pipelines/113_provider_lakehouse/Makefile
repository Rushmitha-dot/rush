SHELL := /bin/bash
VENV  := .venv
PY    := $(VENV)/bin/python
RUN   := set -a; [ -f .env ] && source .env; set +a; $(PY) -m provider_lake.runner
export PYTHONPATH := src

.PHONY: setup refs run load partd gold backfill rebuild-parity api-check freshness report test lint ci clean

setup:
	python3.11 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements-dev.txt
	cp -n .env.example .env || true

refs:  ## NUCC taxonomy code list, used by the taxonomy gate and gold
	$(RUN) refs

run:  ## what the schedule runs, loads any NPPES file the ledger hasn't seen, then gold
	$(RUN) run

load:  ## make load FILES="data/downloads/NPPES_...zip ..."
	$(RUN) load $(FILES)

partd:  ## streams ~4 GB per year from data.cms.gov, keeps scope states only
	$(RUN) partd 2023 2024

gold:
	$(RUN) gold

backfill:  ## make backfill SINCE=2026-11-08, or no SINCE to rebuild from nothing
	$(RUN) backfill $(if $(SINCE),--since $(SINCE),)

rebuild-parity:  ## rebuild into a scratch lake from the same landed files, then compare
	rm -rf data/rebuild && mkdir -p data/rebuild/downloads
	cp data/downloads/nucc_taxonomy_261.csv data/rebuild/downloads/  # same reference file, no second download
	LAKE_HOME=data/rebuild LANDING_DIR=data/landing $(PY) -m provider_lake.runner refs
	LAKE_HOME=data/rebuild LANDING_DIR=data/landing $(PY) -m provider_lake.runner backfill
	LAKE_HOME=data/rebuild LANDING_DIR=data/landing $(PY) -m provider_lake.runner partd 2023 2024
	LAKE_HOME=data/rebuild LANDING_DIR=data/landing $(PY) -m provider_lake.runner gold
	$(RUN) parity rebuild data/rebuild

api-check:
	$(RUN) api-check --sample 300

freshness:
	$(RUN) freshness

report:
	$(RUN) report

test:
	$(PY) -m pytest -q

lint:
	$(VENV)/bin/ruff check src tests

ci: lint test

clean:
	rm -rf data/lake data/metastore_db data/rebuild data/derby.log
