SHELL := /bin/bash
VENV  := .venv
PY    := $(VENV)/bin/python
CLI   := $(PY) -m violcdc.cli
export PYTHONPATH := src

COMPOSE := docker compose -f docker/compose.yml
KAFKA   := data/tools/kafka_2.13-3.8.1
DBZ_URL := https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.7.3.Final/debezium-connector-postgres-2.7.3.Final-plugin.tar.gz
RUN     := data/run

FROM  ?= 2026-06-01
TO    ?= 2026-07-01
UNTIL ?=

.PHONY: setup tools up stack-up connect-up register down init seed poll poll-loop consume follow publish \
        backfill backfill-apply parity rebuild replay-check drift-drill freshness status gold \
        test test-fast lint ci clean

setup:
	python3.11 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements-dev.txt

tools:  ## Kafka Connect and the Debezium Postgres plugin for the host JVM, about 130 MB
	mkdir -p data/tools/plugins
	[ -d $(KAFKA) ] || (curl -fsSL https://archive.apache.org/dist/kafka/3.8.1/kafka_2.13-3.8.1.tgz | tar xz -C data/tools)
	[ -d data/tools/plugins/debezium-connector-postgres ] || (curl -fsSL $(DBZ_URL) | tar xz -C data/tools/plugins)

up: stack-up init connect-up register

stack-up:  ## Redpanda preallocates segment files, the defaults filled the small Docker disk on my laptop
	$(COMPOSE) up -d --wait
	$(COMPOSE) exec -T redpanda rpk cluster config set segment_fallocation_step 4096 >/dev/null
	$(COMPOSE) exec -T redpanda rpk cluster config set log_segment_size 16777216 >/dev/null
	# Connect refuses to start unless its topics are compacted, and Redpanda's auto create made them with delete
	$(COMPOSE) exec -T redpanda rpk topic create _connect_configs _connect_offsets _connect_status \
	  -p 1 -c cleanup.policy=compact >/dev/null 2>&1 || true

connect-up: tools  ## Connect on the host JVM with a 384 MB heap. Needs Java 17+
	mkdir -p $(RUN)
	sed "s#PLUGIN_PATH#$(CURDIR)/data/tools/plugins#" docker/connect-distributed.properties > $(RUN)/connect.properties
	if [ -f $(RUN)/connect.pid ] && kill -0 $$(cat $(RUN)/connect.pid) 2>/dev/null; then echo "connect already running"; \
	elif lsof -nP -iTCP:18116 -sTCP:LISTEN >/dev/null; then echo "port 18116 is held by another process, stop that worker first"; exit 1; \
	else KAFKA_HEAP_OPTS="-Xms128m -Xmx384m" LOG_DIR=$(CURDIR)/$(RUN)/connect-logs \
	  nohup $(KAFKA)/bin/connect-distributed.sh $(RUN)/connect.properties > $(RUN)/connect.log 2>&1 & echo $$! > $(RUN)/connect.pid; fi
	for i in $$(seq 1 60); do curl -sf localhost:18116/connectors >/dev/null && exit 0; sleep 2; done; exit 1

register:
	$(CLI) register

down:
	-[ -f $(RUN)/connect.pid ] && kill $$(cat $(RUN)/connect.pid) && rm -f $(RUN)/connect.pid
	$(COMPOSE) down

init:
	$(CLI) init

seed:  ## first load, one inspection month at a time. UNTIL=2026-11-14T00:00:00 holds back later changes
	$(CLI) seed $(if $(UNTIL),--until $(UNTIL))

poll:
	$(CLI) poll

poll-loop:  ## poll every 5 minutes
	$(CLI) poll-loop --every 300

consume:  ## drain the topic into Iceberg and stop when idle
	$(CLI) consume

follow:  ## keep consuming, publish gold every 5 minutes
	$(CLI) consume --follow

publish:
	$(CLI) publish

backfill:  ## Debezium incremental snapshot of an inspection date range: make backfill FROM=2026-06-01 TO=2026-07-01
	$(CLI) backfill $(FROM) $(TO)

backfill-apply:  ## the snapshot events alone into data/backfill, for the parity check
	rm -rf data/backfill
	$(CLI) consume --lake backfill --group hpd-backfill-$$(date +%s) --since $(SINCE) --snapshots-only

parity:
	$(CLI) parity $(FROM) $(TO)

rebuild:  ## replay the whole topic into data/rebuild and compare every table with live
	rm -rf data/rebuild
	$(CLI) rebuild

replay-check:  ## replay the whole topic into live, nothing may change
	$(CLI) replay-check

drift-drill:  ## start pulling rentimpairing, a source column the mirror didn't have
	HPD_EXTRA_COLUMNS=rentimpairing $(CLI) refetch $(FROM) $(TO)
	$(CLI) consume

freshness:
	$(CLI) freshness

status:
	$(CLI) status

gold:
	$(CLI) gold

test:
	$(PY) -m pytest -q

test-fast:
	$(PY) -m pytest -q -m "not stack"

lint:
	$(VENV)/bin/ruff check src tests

ci: lint test-fast

clean:
	rm -rf data/live data/backfill data/rebuild
