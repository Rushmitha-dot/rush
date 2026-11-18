SHELL := /bin/bash
VENV  := .venv
PY    := $(VENV)/bin/python
export PYTHONPATH := src

SRC   := https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw
FIRST ?= 2022-12-31
LAST  ?= 2023-09-09
AIRFLOW_HOME := $(CURDIR)/airflow_home

.PHONY: setup airflow-setup data init run backfill parity score-v2 activate freshness status eval chat airflow \
        test lint ci tf-validate clean

setup:
	python3.11 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements-dev.txt

airflow-setup:  ## own venv, Airflow's pins and dbt's pins don't get along
	python3.11 -m venv .venv-airflow
	.venv-airflow/bin/pip install -q -r requirements-airflow.txt \
	  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt

data:  ## ~134 MB download once, then one drop per review day (history before 2023 is one drop)
	mkdir -p data/source
	[ -f data/source/All_Beauty.jsonl.gz ] || curl -fsSL -o data/source/All_Beauty.jsonl.gz \
	  $(SRC)/review_categories/All_Beauty.jsonl.gz
	[ -f data/source/meta_All_Beauty.jsonl.gz ] || curl -fsSL -o data/source/meta_All_Beauty.jsonl.gz \
	  $(SRC)/meta_categories/meta_All_Beauty.jsonl.gz
	$(PY) -m finder.replay --source data/source/All_Beauty.jsonl.gz --out data/drops --start 2023-01-01

init:
	$(PY) -m finder.runner init

run:  ## make run DATE=2023-01-05 [FORCE=1]
	$(PY) -m finder.runner run $(DATE) $(if $(FORCE),--force)

backfill:
	$(PY) -m finder.runner backfill $(FIRST) $(LAST) --keep-going

parity:  ## one dbt build at the end vs one per day, same marts expected
	$(PY) -m finder.parity

score-v2:
	$(PY) -m finder.runner score-version rules-v2

activate:  ## make activate VERSION=rules-v2 (also how you roll back)
	$(PY) -m finder.runner activate $(VERSION)

freshness:
	$(PY) -m finder.runner freshness --expect $(LAST)

status:
	$(PY) -m finder.runner status

eval:
	$(PY) -m finder.label evaluate rules-v1 --split test
	$(PY) -m finder.label evaluate rules-v2 --split test

chat:
	$(VENV)/bin/streamlit run app/chatbot.py --server.port 8215 --server.headless true

airflow:  ## UI on :8115. standalone starts its parts by calling `airflow` from PATH, so the venv goes first
	PATH=$(CURDIR)/.venv-airflow/bin:$$PATH AIRFLOW_HOME=$(AIRFLOW_HOME) AIRFLOW__CORE__DAGS_FOLDER=$(CURDIR)/dags AIRFLOW__CORE__LOAD_EXAMPLES=False \
	  AIRFLOW__API__PORT=8115 .venv-airflow/bin/airflow standalone

test:
	$(PY) -m pytest -q

lint:
	$(VENV)/bin/ruff check src tests app dags

ci: lint test

tf-validate:
	docker run --rm -v $(CURDIR)/infra/terraform:/w -w /w hashicorp/terraform:1.9.8 init -backend=false -input=false >/dev/null
	docker run --rm -v $(CURDIR)/infra/terraform:/w -w /w hashicorp/terraform:1.9.8 fmt -check -diff
	docker run --rm -v $(CURDIR)/infra/terraform:/w -w /w hashicorp/terraform:1.9.8 validate

clean:
	rm -rf data/warehouse data/serving dbt/target dbt/logs
