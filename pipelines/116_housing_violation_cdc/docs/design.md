# Design notes

## Problem and consumer

A housing team wants two answers from HPD's housing code violations. How long do violations take to close, by class and community district, and which buildings keep getting class C (immediately hazardous) violations. They also want the history of every status change, not just the row as it looks today. The open data API only serves the latest version of each violation, so if nobody records the changes, they are gone.

The consumer is an analyst who queries the gold tables with DuckDB or pyiceberg, and anyone who needs to ask "what was the status of violation X on date D".

## Data

[NYC HPD Housing Maintenance Code Violations](https://data.cityofnewyork.us/Housing-Development/Housing-Maintenance-Code-Violations/wvxf-dwi5), Socrata dataset `wvxf-dwi5`, read through the SODA API without an app token. The task I was given named `wvxf-yeqz`, which returns `dataset.missing`. `wvxf-dwi5` is the violations dataset.

* Scope is the Bronx, violations inspected on or after 2026-04-01. That's 214,235 rows. The whole dataset is about 11.4M rows across five boroughs.
* `:updated_at` is a Socrata system field. HPD refreshes once a day, around 16:00 UTC, and only rows that changed get a new `:updated_at` (41,497 Bronx rows on Nov 20, 9,487 on Nov 18, none on Nov 19).
* Pages are 5,000 rows with a 1 second pause between them. A page is about 6 MB of JSON.

Nothing here is made up. Tests use three real API records and one real Debezium event saved under `tests/fixtures`, plus small hand-written rows for the merge logic.

## Flow

1. **Poller to mirror.** `soda.py` pages with a keyset on `(:updated_at, violationid)` after the stored watermark, minus a 60 minute overlap. Each page is COPYed into a temp table and upserted. The update only fires when the row's content changed (jsonb of the row minus the meta columns, compared old against new). Page rows and the new watermark commit together.
2. **Debezium.** The Postgres connector with `pgoutput` reads the WAL of `mirror.violations` and writes JSON change events, keyed by violation id, to `hpd.mirror.violations` on Redpanda. A trigger keeps `row_hash` (md5 of the row's jsonb, minus meta) on every row, so the hash travels inside the event.
3. **Consumer to Iceberg.** Batches of up to 20,000 messages. For each violation, events at or before the log position already applied are dropped (replays), and so are events whose row hash equals the previous one. Then three writes, in this order. Append to `violation_changes`, rebuild `violation_status_history` for the touched ids, overwrite `violations_current` for the touched ids. Kafka offsets are committed last.
4. **Gold.** `gold.py` runs the checks and, only if no ERROR check fails, overwrites `gold_time_to_close` and `gold_repeat_class_c`. The follow mode publishes every 5 minutes.

### Why each step is safe to repeat

* changes log, an anti join on `(violationid, lsn, snap)` skips events already appended
* history, events at or before the highest lsn recorded in the violation's history rows are skipped
* current, the lsn comparison above

So a crash between any two Iceberg commits is fixed by replaying the batch. The replay test is in `tests/test_merge.py`, and on the real run a replay of the whole topic into the live tables kept 0 events and left every snapshot id unchanged.

## Options considered

| Choice | Picked | Why |
|---|---|---|
| Broker | Redpanda, one node, `--smp 1 --memory 512M` | Kafka API, no ZooKeeper or KRaft quorum, used about 175 MB |
| Iceberg writer | pyiceberg with a sqlite SQL catalog | the data fits in memory many times over. Spark would add a JVM and about 1 GB of RAM for nothing at 214k rows |
| Query engine | DuckDB over pyiceberg Arrow scans | DuckDB's iceberg extension wants a REST catalog or a metadata path. Arrow scans were simpler and fast enough |
| Merge strategy | copy on write overwrite of touched ids | pyiceberg doesn't write delete files. At this size each batch rewrites a few small files |
| Backfill | Debezium incremental snapshot by inspection date range | it interleaves with the stream safely (watermark windows), so it can run while changes flow. A plain replay from the mirror has a race against in-flight transactions |
| Connect | Kafka Connect 3.8.1 on the host JVM, heap 384 MB | see below |

### Connect runs on the host

The compose file has a `connect` service under a `docker-connect` profile. I couldn't use it. When I built this the Docker disk on my laptop (an 8 GB VM shared with two other builds) had about 1.3 GB free, and pulling `quay.io/debezium/connect:2.7.3.Final` failed with `no space left on device`. So `make connect-up` downloads Apache Kafka 3.8.1 and the Debezium Postgres plugin (about 130 MB) and runs `connect-distributed.sh` on the host with the same worker settings. Docker then holds only Postgres (limit 320 MB) and Redpanda (limit 640 MB).

## SCD2 history

`violation_status_history` has one row per status interval, `valid_from` to `valid_to`, with `is_current`. A new row starts when `violationstatus`, `currentstatusid` or `currentstatus` changes. `valid_from` is the source's `currentstatusdate` when it's present and not before the open row, otherwise the commit day.

The first time a violation is seen we only know its current status. If its status date is after its issue date, I add a row marked `derived` that covers issue date to status date as Open with an unknown status. Without it every violation would seem to start life with its current status.

Honest limit. During this build HPD did not publish a new refresh, so every status interval in the history came from first images. The observed-change path (a second version of a row arriving) is covered by tests, including one on the running stack, and by the drift drill, but not by a real HPD status change. Running the poller for a few days would fill that in.

## Data quality

Poller, before the mirror. Rows go to `mirror.quarantine` with a reason for a missing violation id, unknown class (A, B, C, I), unknown status (Open, Close), bad numbers or dates, missing inspection date, wrong borough.

Consumer, before Iceberg. Events go to the Iceberg `quarantine` table with a reason for bad JSON, unknown op, no position (lsn), no row image, unknown class or status. More than 5% of a batch of 100 or more quarantined sends an alert.

Gold gate, ERROR blocks the publish and alerts, WARN only records.

| Check | Severity |
|---|---|
| violation id unique in current | error |
| class known | error |
| every current row has exactly one open history row | error |
| no history row ends before it starts | error |
| history rows of a violation are contiguous | error |
| open history row matches current status | error |
| current row is the last logged change | error |
| over 5% of change events quarantined in 24 hours (100+ events) | error |
| closed before issued | warn |
| closed without a status date | warn |

Every publish writes its check results to `publish_runs` in `ops.sqlite` next to the catalog.

## Schema drift

The poller selects an explicit column list. `HPD_EXTRA_COLUMNS=rentimpairing` adds a real source column the mirror didn't have. The poller runs `ALTER TABLE ADD COLUMN`, Debezium starts sending the field, and the consumer sees a field it doesn't know and calls `update_schema().union_by_name(...)`. Iceberg tracks columns by id, so existing data files aren't rewritten and read the new column as null. The row hash trigger is built from jsonb, so it covers the new column without a change.

## Failure modes

| What breaks | What happens |
|---|---|
| SODA timeout or 429 | five retries with backoff, then the poll fails, watermark stays at the last committed page |
| Socrata touches `:updated_at` without a content change | the upsert's WHERE skips it, no change event |
| consumer dies mid batch | offsets not committed, replay is dropped by lsn and hash |
| Connect down | WAL piles up behind the slot. `make freshness` alerts on a stopped connector and on rows older than the SLA that aren't in Iceberg |
| bad event | quarantine with reason, alert over 5% |
| broken history or duplicate ids | gold publish blocked, last good gold stays |
| new source column | schema evolves, no rewrite |

Not handled. Deletes in the source (the poller only upserts, so a row removed from Socrata stays in the mirror). Multi-table atomicity in Iceberg, which pyiceberg doesn't offer; the write order and idempotent steps stand in for it.

## SLA and cost

Target, a source change visible in Iceberg within 15 minutes of Socrata publishing it. The end to end time is poll interval + poll time + pipeline lag. Measured numbers are in the README. HPD itself publishes once a day, so the time from a real world status change to Socrata is up to a day and outside this pipeline.

Everything runs locally for free. On AWS the same shape would be RDS Postgres, MSK Serverless or a small Redpanda node, Connect on ECS and Iceberg on S3 with Glue as the catalog. I haven't priced or run that.
