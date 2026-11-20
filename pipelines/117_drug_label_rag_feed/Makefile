SHELL := /bin/bash
VENV  := .venv
PY    := $(VENV)/bin/python
export PYTHONPATH := src

FIRST ?= 2026-11-02
LAST  ?= 2026-11-19

.PHONY: setup up down fetch bootstrap init run load backfill parity freshness status crosscheck eval \
        test test-fast lint ci clean

setup:
	python3.11 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements-dev.txt

up:  ## Postgres 17 with pgvector, capped at 640 MB
	docker compose up -d --wait

down:
	docker compose down

fetch:  ## every daily and weekly file on the DailyMed page from FIRST on, slimmed to Rx XML
	$(PY) -m labelfeed.runner fetch

bootstrap:  ## the subset's labels as they were the day before FIRST
	$(PY) -m labelfeed.runner bootstrap

init:
	$(PY) -m labelfeed.runner init

run:  ## what cron runs. fetch what's new, apply it in order, check freshness
	$(PY) -m labelfeed.runner run

load:  ## make load FILE=dm_spl_daily_update_11192026.zip [FORCE=1]
	$(PY) -m labelfeed.runner load $(FILE) $(if $(FORCE),--force)

backfill:  ## the range as one batch, net of intermediate versions
	$(PY) -m labelfeed.runner backfill $(FIRST) $(LAST)

parity:  ## backfill into a scratch schema and compare with the day by day index
	$(PY) -m labelfeed.parity $(FIRST) $(LAST)

freshness:
	$(PY) -m labelfeed.runner freshness

status:
	$(PY) -m labelfeed.runner status

crosscheck:  ## sample against openFDA, tombstones against DailyMed's current list
	$(PY) -m labelfeed.crosscheck

eval:
	$(PY) -m labelfeed.evaluate

test:
	$(PY) -m pytest -q

test-fast:  ## no database
	$(PY) -m pytest -q -m "not db"

lint:
	$(VENV)/bin/ruff check src tests

ci: lint test

clean:  ## raw downloads and the bootstrap cache. data/updates is kept, it's 67 MB and slow to rebuild
	rm -rf data/incoming data/bootstrap_cache
