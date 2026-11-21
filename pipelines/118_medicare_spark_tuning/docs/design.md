# Design notes

This is the reasoning behind the pipeline and each Spark fix. Every fix has a short "in plain words" part, which is how I'd explain it in an interview.

## Problem and consumer

A payer analytics team runs a nightly job that summarizes Medicare payments to providers by service code (HCPCS) and specialty. The first version took long enough that people noticed, and the Spark UI showed a few very uneven tasks. The consumers are analysts who read three tables per data year

* `code_specialty`, payments per HCPCS code and specialty, with the prior year next to it
* `specialty_state`, payments per specialty and provider state
* `code_outliers`, provider lines billing far more services per beneficiary than peers on the same code and place of service (over 3x the median and over the 99th percentile, at least 30 peers)

## Data

CMS [Medicare Physician & Other Practitioners - by Provider and Service](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service), data years 2023 and 2024. One row per NPI, HCPCS code and place of service, with beneficiary and service counts and average submitted, allowed and paid amounts. Provider columns (name, credentials, city, state, specialty) are repeated on every line.

| | 2023 | 2024 |
|---|---|---|
| CSV size streamed | 3.06 GB | 3.25 GB |
| Lines | 9,660,647 | 9,781,673 |
| NPIs | 1,175,281 | 1,207,473 |
| Medicare payment (avg paid x services) | $93.72B | $97.29B |
| Landed Parquet | ~340 MB | ~363 MB |

The CSV is never stored. `land.py` streams it over HTTP into pyarrow, hashes the bytes on the way, and writes zstd Parquet in 1M row files with every column still a string. Street address, middle initial and the RUCA description are dropped at landing since nothing downstream uses them and they were most of the bytes. I left NPPES out on purpose. Project 113 already joins NPPES, and the provider columns in this file are enough for specialty and state.

The data is clean. Both years had zero rows failing a row rule. What it does have

* 21,895 (2023) and 20,669 (2024) lines with a non-state code, territories, military addresses (AA, AE, AP) and the CMS unknown or foreign codes XX and ZZ. Kept, but not counted toward the "whole file" gate.
* bene-day services above services on 151 and 72 lines, and beneficiaries above services on 67 and 34. Fractional services on drug and lab codes explain most. Counted as warnings, kept.
* 13 and 18 lines with an allowed amount of zero.

## Layout and reruns

```
data/landing/year=YYYY/        string Parquet + _manifest.json (written last)
data/lake/claims/year=YYYY/state=XX/   typed fact
data/lake/providers/year=YYYY/         one row per NPI
data/lake/hcpcs/year=YYYY/             one row per code
data/lake/quarantine/year=YYYY/        failed rows with a reason column
data/marts/<mart>/year=YYYY/
data/ops.db                    ledger, runs, check results (SQLite)
```

Every step takes the year as a parameter and replaces that year's folder in one rename (`publish.py`). I didn't use Spark's dynamic partition overwrite, because it only replaces partitions that appear in the new data. If a state disappeared on a rerun its old files would stay. A test drops Florida from a rerun and checks the folder is gone. Writes go to `_staging-year=...` first, and Spark skips folders that start with an underscore, so readers never see half a year.

The ledger stores what each step read

* land, the URL plus the Last-Modified header (CMS moves a republished year to a new URL)
* stage, the land run id
* summarize, the stage run id of the year and the summarize run id of the year before, because `code_specialty` carries the prior year's payment

So a rerun of the same years does nothing, and republishing 2023 makes 2024's marts stale. `make freshness` reports that, and `make run` rebuilds only 2024's marts. `make backfill FIRST=2023 LAST=2024` forces stage and summarize for the range. `make parity` builds a second data home from the same landed files with a backfill and compares row counts plus a sum of row hashes per table and year (doubles rounded to the cent).

## Gates

Row rules, each failure goes to quarantine with a reason: bad NPI or failed NPI check digit, missing code, place of service not F or O, missing specialty, a metric that isn't a number, services not positive, beneficiaries under 11 (CMS suppresses anything under 11, so a lower number means the source changed), negative amounts, paid over allowed, duplicate key.

Batch gates, an ERROR blocks the publish and keeps the last good year in place

| Gate | Severity |
|---|---|
| rows match the landing manifest | error |
| column contract (header checked again at landing) | error |
| good + quarantined = landed | error |
| quarantine share over 0.1% | error |
| fewer than 51 states plus DC present | error (catches a truncated download) |
| one provider row per NPI | error |
| rows or payment vs the prior year over 50% | error |
| rows or payment vs the prior year over 15% | warn |
| the soft counts above | warn, recorded |

A blocked year exits non-zero and alerts (log line, plus a webhook if `ALERT_WEBHOOK` is set). That's the alert path. There is no pager here.

### The gate that blocked both years

The first real run blocked both 2023 and 2024 on `one_provider_row_per_npi` (1,175,803 provider rows for 1,175,281 NPIs in 2023). I had profiled specialty, state, ZIP and name per NPI and they were constant, so I put every `Rndrng_Prvdr_*` column in the provider dimension. The one I hadn't checked was the Medicare participation indicator. It varies across lines for 522 NPIs in 2023 and 513 in 2024. It belongs on the claim line, so it moved to the fact. Without the gate, the claims to provider join would have double counted those NPIs' lines.

## The fixes

All numbers are from data year 2024 (9,781,673 lines), median of 3 timed runs after a warm-up round, variants run round robin. See the README for the machine and why local numbers are only a guide.

### 1. Skew on a few HCPCS codes

**What happened.** The outlier mart joins every claim line to its code's peer stats. The stats side is an aggregate, and without table statistics Spark estimates an aggregate at about the size of its input, so the first version planned a sort merge join and shuffled all 9.8M lines on (code, place of service). 99214 alone is 5.35% of lines, 99213 4.84%, 99204 2.44%. In the join stage the biggest of the 200 partitions read 536,058 rows against a median of 32,369, 16.6x. Its task took 865 ms against a 108 ms median.

**What I tried.**

| | join stage, slowest / median task | rows in the biggest partition | shuffle written |
|---|---|---|---|
| sort merge join, AQE off | 865 / 108 ms | 536,058 | 203 MB |
| salted (12 codes over 1% of lines, 16 salts) | 156 / 26 ms | 170,201 | 217 MB |
| AQE skew join, default thresholds | 7,960 / 7,023 ms (4 tasks) | 3,005,664 | 203 MB, 165 MB spilled |
| AQE skew join, threshold 8 MB | 1,163 / 875 ms (31 tasks) | 536,058 | 203 MB |
| broadcast the stats | no shuffle of the fact | | 49 MB |

**What I learned.** Salting worked as advertised, the hot partition shrank 3x. AQE's skew join never split anything. With the defaults a partition has to be over 256 MB and 5x the median, and nothing here is 256 MB. With the threshold lowered, AQE first coalesced the small partitions toward 8 MB, which raised the median so much that the hot partition was no longer 5x it. So AQE skew handling is tuned for cluster sized partitions, and its coalescing and its skew rule pull against each other on small data.

Whole run time didn't move (32.1 to 33.7 s for every variant). With 3 cores the join stage is short and the scan and percentile stages dominate. On a cluster with 200 cores, the stage takes as long as its slowest task, so the 865 vs 156 ms is the number that would show up there. I'd say that plainly rather than claim a local speedup.

**In plain words.** One key had far more rows than the others, so one task did a lot of the work. The first question is whether the other side is small enough to broadcast. Here it was 9k rows, so broadcasting it removed the shuffle and the skew together. Salting is for when both sides are big, you split the hot key into N buckets on the big side and copy the matching rows N times on the small side. AQE can do that split for you in joins, but only once partitions are big enough to cross its thresholds.

### 2. Broadcast vs sort merge join for the provider dimension

**What happened.** I expected the provider dimension (1.2M NPIs, 22.5 MB of Parquet) to be over the 10 MB `spark.sql.autoBroadcastJoinThreshold` and get a sort merge join. It didn't. Spark scales the file size by the columns read, and after pruning to four columns it estimated 7,984,165 bytes, under 10 MB, so it broadcast on its own. Forcing a sort merge join (threshold -1) cost 4.5 s and 139 MB of shuffle against 2.0 to 2.3 s and almost no shuffle for every broadcast variant.

**In plain words.** A broadcast join sends the small table to every executor so the big one never moves. Spark does it automatically under 10 MB, using its own estimate, and the estimate is from file size and column widths, not real memory. It can be wrong both ways. Selecting fewer columns can tip a table under the line. A hint (`broadcast()`) makes it explicit so a schema change doesn't silently turn it back into a shuffle, which is why the job keeps the hint. The limit is driver and executor memory, the table is collected to the driver and copied to every executor.

### 3. Small files

**What happened.** A first version of the stage job dropped duplicate keys with `dropDuplicates`, which shuffles into 200 partitions, then wrote `partitionBy("state")`. Every task writes a file for every state it holds.

| | write s | files | avg file | downstream read query |
|---|---|---|---|---|
| 200 shuffle partitions, AQE off | 126.2 | 11,688 | 54 KB | 14.7 s |
| AQE on (coalesces toward 64 MB) | 56.2 | 1,025 | 528 KB | 3.0 s |
| repartition("state") + maxRecordsPerFile 500k | 52.5 | 66 | 7.4 MB | 1.5 s |
| compact the 11,688 file output afterwards | 35.2 | 66 | 7.5 MB | 1.1 s |

The read query aggregates the whole year by state and code. 11,688 small files made it 10x slower than 66 files, almost all of it per file overhead (opening, footers, task scheduling).

**In plain words.** Files per partition folder = tasks that hold rows for that partition. Fix it at write time by repartitioning on the partition column, and cap the file size with `maxRecordsPerFile` so the biggest state doesn't become one huge file. AQE helps by merging small shuffle partitions but doesn't group by state. If the small files already exist, a compaction job rewrites each partition. Delta and Iceberg have OPTIMIZE for this.

The pipeline itself doesn't dedupe with a shuffle any more. It counts the key with an aggregate, which shuffles only the key columns, and broadcasts the duplicate keys back (there were none in either year).

### 4. Python UDF vs native functions

| | median s |
|---|---|
| Python UDF | 4.3 |
| pandas UDF | not measured, see below |
| native `when` / `substring` / `rlike` | 3.6 |

Smaller gap than I expected, 16%. The query reads one column and groups to 20 families, so the scan is cheap and the UDF is a simple branch. The native version also keeps the plan visible to the optimizer and needs no Python workers, which matters more on a cluster where each executor would start its own. The tests check the three versions give the same family for every case.

The pandas UDF can't run on this machine. Spark 3.5.3 ships Arrow 12 and Netty 4.1.96, which look for a `DirectByteBuffer(long, int)` constructor that JDK 21 changed, and every Arrow batch fails with `sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long, int) not available`. Setting `-Dio.netty.tryReflectionSetAccessible=true` and the Arrow unsafe allocator didn't help. Spark 3.5 supports Java 17, and CI runs the pandas version there for correctness. Locally the experiment detects this and skips the variant.

**In plain words.** A Python UDF sends every row from the JVM to a Python process and back. A pandas UDF sends Arrow batches, much cheaper per row. Native functions never leave the JVM and Spark can compile them into the stage. Use native when the logic can be written with column functions, pandas UDFs when it really needs Python libraries.

### 5. Caching the enriched fact

All three marts read the same claims joined to providers and HCPCS.

| | median s | cached size |
|---|---|---|
| no cache | 23.9 | |
| persist MEMORY_AND_DISK, 12 columns | 32.2 | 324 MB in memory |
| persist DISK_ONLY | 27.5 | 324 MB on disk |

Caching made it slower. Recomputing the input is a Parquet scan plus two broadcast joins, which takes a few seconds, and building the cache (encoding 9.8M rows into columnar batches) costs more than the two rescans it saves. 324 MB is also a big share of a 2 GB driver's storage memory.

Separately, the first stage job persisted the wide flagged rows (every landed column plus the reasons) with MEMORY_AND_DISK and ran out of heap. Blocks that didn't fit in memory went to disk, and reading a block back deserialized a whole 2.4M row partition at once. It now writes those rows once to a Parquet work folder and reads them back for its four outputs.

**In plain words.** Cache when the input is expensive to recompute (a big shuffle, a slow source, an ML loop) and it's read several times. Don't cache when the input is a cheap scan, and cache only the columns you need. Writing an intermediate to Parquet is often the safer cache, it survives a crash and doesn't fight execution memory.

### The whole job

`summarize --baseline` vs the tuned job, all three marts written, 2024

| | median s | shuffle written | spill |
|---|---|---|---|
| first version (AQE off, no hints, Python UDF, no cache, 200 files per mart) | 74.9 | 1,281 MB | 376 MB |
| tuned (AQE on, broadcast hints, native family, cache, 1 file per mart) | 49.8 | 650 MB | 690 MB |

34% faster, half the shuffle. Most of it is the outlier join no longer shuffling the fact. The tuned job still caches, which experiment 5 says is a loss here, and the cache is also where the extra spill comes from. I left it on so the comparison matches what ran, and it's the next thing I'd turn off.

### percentile_approx isn't stable across plans

Every experiment checks that its variants give the same answer. The skew, cache and job checks failed at first. Flagged outlier lines were 36,419 to 36,448 depending on the plan. `percentile_approx` merges partial sketches in whatever order the plan produces, so p50 and p99 move slightly and a few lines sitting right at the threshold flip. Those checks now require the other marts to match exactly and outliers within 0.5%. The real backfill parity run hit it too, with the same plan. 13 of 14 tables were identical, and 2024 `code_outliers` had 36,451 lines against 36,444 in the incremental run. A production version should use an exact percentile for a threshold that decides who gets flagged, or round the threshold.

## Failure modes

| What breaks | What happens |
|---|---|
| download cut off | no manifest, stage refuses; or the state count gate blocks |
| CMS changes the header | landing raises before writing anything |
| CMS republishes a year | new URL or Last-Modified, the year re-lands, later years go stale and rebuild |
| a column assumed per provider varies | one_provider_row_per_npi blocks, as it did |
| crash between the two renames | `clean_leftovers` puts the old folder back on the next run |
| stage blocked | last good year stays, exit 1, alert |

## SLA and cost

CMS publishes a data year once, around 18 months after it ends, and sometimes republishes. The schedule is a nightly `make run` that costs a HEAD request per year when nothing changed. A new or changed year lands, stages and summarizes in about 5 minutes on my laptop (54 to 70 s landing, 150 s staging, 83 to 116 s summarizing). On a cluster I'd expect the job to run on a small number of workers for a few minutes. I haven't run it on one, so I don't have a cost number.

## Local mode vs a cluster

local[3] means one JVM, three task threads, no network shuffle, and one 2 GB heap for everything. Shuffle is a local disk write and read. Broadcast is free because the driver is the executor. So shuffle heavy variants look better here than they would on a cluster, and skew shows up in task numbers rather than run time. The relative ordering of the fixes is what I'd trust, not the seconds.
