SHELL := /bin/bash
VENV  := .venv
PY    := $(VENV)/bin/python
export PYTHONPATH := src

YEAR  ?= 2024
RUNS  ?= 3
FIRST ?= 2023
LAST  ?= 2024

.PHONY: setup run land backfill parity freshness status job-baseline \
        experiments exp-skew exp-joins exp-small-files exp-udf exp-cache exp-job results \
        test lint ci clean

setup:
	python3.11 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements-dev.txt

run:  ## what the schedule runs, streams any year CMS changed, rebuilds what's stale
	$(PY) -m claims_perf.runner run

land:  ## make land YEAR=2024, ~3.2 GB streamed per year, ~370 MB of Parquet kept
	$(PY) -m claims_perf.runner land $(YEAR)

backfill:
	$(PY) -m claims_perf.runner backfill $(FIRST) $(LAST)

parity:  ## second data home from the same landed files, backfilled, then compared table by table
	rm -rf data/scratch/parity && mkdir -p data/scratch/parity
	ln -s $(CURDIR)/data/landing data/scratch/parity/landing  # read only, saves 700 MB of copying
	$(PY) -c "from claims_perf import land, ops; from claims_perf.settings import Settings; from pathlib import Path; \
	o = Settings(home=Path('data/scratch/parity')); \
	[ops.record(o, 'land', y, m['url'] + '|' + m['last_modified'], 'copied', 'ok', m, 0, 0) \
	 for y in range($(FIRST), $(LAST) + 1) for m in [land.read_manifest(o, y)]]"
	CLAIMS_HOME=data/scratch/parity $(PY) -m claims_perf.runner backfill $(FIRST) $(LAST)
	$(PY) -m claims_perf.runner parity data/scratch/parity $(FIRST) $(LAST)
	rm -rf data/scratch/parity

freshness:
	$(PY) -m claims_perf.runner freshness --discover

status:
	$(PY) -m claims_perf.runner status

job-baseline:  ## the first version of the nightly job, for the Spark UI on :8118
	$(PY) -m claims_perf.runner summarize $(YEAR) --baseline --force

exp-skew:
	$(PY) -m claims_perf.experiments.skew --year $(YEAR) --runs $(RUNS)
exp-joins:
	$(PY) -m claims_perf.experiments.joins --year $(YEAR) --runs $(RUNS)
exp-small-files:
	$(PY) -m claims_perf.experiments.small_files --year $(YEAR) --runs $(RUNS)
exp-udf:
	$(PY) -m claims_perf.experiments.udf --year $(YEAR) --runs $(RUNS)
exp-cache:
	$(PY) -m claims_perf.experiments.cache --year $(YEAR) --runs $(RUNS)
exp-job:
	$(PY) -m claims_perf.experiments.job --year $(YEAR) --runs $(RUNS)

experiments: exp-skew exp-joins exp-small-files exp-udf exp-cache exp-job

results:  ## markdown tables from results/*.json
	$(PY) -m claims_perf.experiments.report

test:
	$(PY) -m pytest -q

lint:
	$(VENV)/bin/ruff check src tests

ci: lint test

clean:
	rm -rf data/lake data/marts data/scratch data/eventlogs data/ops.db
