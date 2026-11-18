SHELL := /bin/bash
VENV  := .venv
BIN   := $(VENV)/bin
DATA  ?= $(CURDIR)/data
export PYTHONPATH := src
export LATENIGHT_DATA_DIR := $(DATA)
export LATENIGHT_WAREHOUSE := $(DATA)/warehouse/latenight.duckdb
export DAGSTER_HOME := $(DATA)/dagster_home
export DBT_PROFILES_DIR := $(CURDIR)/dbt

# fall 2025 semester, first night of classes to the last night of finals
FIRST ?= 2025-09-02
LAST  ?= 2025-12-19
NIGHT ?= 2025-10-17
DAGSTER := $(BIN)/dagster asset materialize -m latenight.definitions

.PHONY: setup manifest gtfs calendar reference run backfill report dev test test-fast lint ci clean

setup:
	python3.11 -m venv $(VENV)
	$(BIN)/pip install -q -r requirements-dev.txt
	cp -n .env.example .env || true
	$(MAKE) manifest

$(DAGSTER_HOME):
	mkdir -p $(DAGSTER_HOME)

manifest:
	cd dbt && ../$(BIN)/dbt parse -q

gtfs:  ## re-derive the MBTA seeds from the current feed
	$(BIN)/python -m latenight.gtfs

calendar:
	$(BIN)/python -m latenight.calendar

reference: manifest $(DAGSTER_HOME)  ## dbt seeds + GBFS stations
	$(DAGSTER) --select 'group:reference'

run: reference  ## one night: make run NIGHT=2025-10-17
	$(DAGSTER) --select 'group:nightly' --partition $(NIGHT)
	$(DAGSTER) --select night_report

backfill: reference  ## the whole semester in one Dagster run
	$(DAGSTER) --select 'group:nightly' --partition-range $(FIRST)...$(LAST)
	$(DAGSTER) --select night_report

report:
	$(BIN)/python -c "from latenight import report; from latenight.settings import load_settings as s; \
	print(report.build(s().warehouse, s().site_dir))"

dev: manifest $(DAGSTER_HOME)  ## Dagster UI on http://localhost:3114
	$(BIN)/dagster dev -m latenight.definitions -p 3114

test:
	$(BIN)/python -m pytest -q

test-fast:
	$(BIN)/python -m pytest -q -m "not dbt"

lint:
	$(BIN)/ruff check src tests

ci: lint test

clean:
	rm -rf data/bluebikes data/warehouse data/dagster_home site dbt/target
