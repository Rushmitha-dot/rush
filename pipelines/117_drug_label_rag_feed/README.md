# 117 Drug label change feed for a retrieval index

Keeps a Postgres retrieval index of FDA drug labels current, so a clinical Q&A assistant answers from the latest version of each label and stops citing withdrawn ones, without re-embedding everything every day.

Data is real DailyMed SPL update files (human prescription labels, Nov 2 to Nov 19, 2026) plus the DailyMed web services and openFDA for checks. It runs locally with Postgres 17 + pgvector in Docker and a small sentence-transformers model on CPU.

```mermaid
flowchart LR
    A[DailyMed daily / weekly zip] -->|md5 vs listed md5| B[Slim to Rx XML<br/>+ manifest last]
    V[History API +<br/>version archive] --> BS[Bootstrap file<br/>pre-window versions]
    B & BS --> C[Load one file<br/>ledger, parse SPL,<br/>label checks]
    C -->|bad label| Q[(quarantine)]
    C --> D[Chunk by LOINC section<br/>+ content hash]
    D -->|new hashes only| E[MiniLM embed<br/>+ embedding cache]
    D --> F[(labels, chunks,<br/>chunk_vectors)]
    C -->|deleted, not re-added| T[(tombstones)<br/>delete vectors]
    F --> G{publish gates}
    G -->|fail| R[rollback + alert]
    F --> H[dense / full text / hybrid RRF]
    C -.-> L[OpenLineage JSON]
    C -.-> S[freshness check]
```

## What it does

1. **Fetch.** Downloads each update zip, checks its MD5 against the one the DailyMed page lists, keeps only the human Rx XML plus the added and deleted lists, writes a manifest last and deletes the original. 1.72 GB of downloads became 67 MB.
2. **Bootstrap.** The full Rx release is about 10 GB, so I didn't load it. For every Rx label a window file replaces or removes, it fetches the version that was current before Nov 2 from DailyMed's version archive. That starting state goes through the loader like any other file.
3. **Load.** One file per transaction. The ledger is keyed by file name and checksum, so a rerun is a no-op. Each label is parsed with lxml into the sections an assistant answers from (boxed warning, indications, dosage, contraindications, warnings and precautions, adverse reactions), chunked inside each subsection, and hashed. Only hashes without a vector get embedded.
4. **Supersede and tombstone.** A new version replaces the old one's chunks. A label whose old file is deleted and not replaced gets a tombstone, and its chunks and unshared vectors are deleted in the same transaction.
5. **Gates.** Bad labels go to `quarantine` with a reason. If too many fail, or the index ends up inconsistent (chunks of a removed label, chunks from an old version, orphan vectors), the whole file rolls back and an alert fires.
6. **Retrieve.** Dense (cosine on pgvector), Postgres full text (`ts_rank_cd`, BM25-like but not BM25), and hybrid with reciprocal rank fusion.

Decisions, the check list, failure modes, SLA and cost are in [docs/design.md](docs/design.md).

## Results

Run on my laptop (M2, 8 GB), shared with two other builds.

| | |
|---|---|
| Update files | 2 weekly + 4 daily (Nov 2 to Nov 19, 2026), plus the bootstrap file |
| Downloaded / kept on disk | 1.72 GB / 67 MB (Rx XML only), every MD5 matched the one DailyMed lists |
| Bootstrap | 1,347 set ids wanted, 1,322 fetched (96 only as the archived last copy), 25 had no older version |
| Label versions in the window | 1,694 |
| Index after the run | 1,488 active labels, 123 removed with tombstones, 49,475 chunks, 38,857 vectors |
| Labels with a new version since the bootstrap | 1,039 (13 of them got 3 or more versions in 18 days) |
| Quarantined | 349 (348 with none of the kept sections, mostly medical gases, 1 effective date in the future) |
| First full build | 28 min 27 s, of which 954 s was embedding 30,940 bootstrap texts on CPU |
| Daily files | 71 to 111 s each. 17,787 chunks in the updated labels, 13,057 (73%) unchanged, 3,803 texts embedded |
| Rerun of all 7 files | 0.9 s, every file skipped by the ledger |
| Forced reload of Nov 16 and Nov 19 | hashes of labels, chunks, vectors and tombstones identical before and after |
| Backfill parity | whole range as one batch in 81 s, labels, chunks, tombstones and vectors identical to day by day |
| openFDA cross-check | 100 random active labels, all 100 on the same version in openFDA |
| Tombstones vs DailyMed | 123 checked, none still in DailyMed's current list |
| Freshness | passes on the run. With the clock set to Nov 26 it fails with exit 2 and an alert line |
| Tests | 36 pytest (unit, plus Postgres integration with a hashing embedder), ruff clean |
| Postgres | 339 MB on disk, about 175 MB RAM in use under a 640 MB cap |

Retrieval on a reviewed question set of 54 questions I wrote from the loaded labels (30 lookup, 12 needing two sections, 12 about text that changed in a new version). A hit means a chunk from the right section of a label for that drug containing the expected phrase.

| | dense | full text | hybrid (RRF) |
|---|---|---|---|
| hit@1 | 0.35 | 0.06 | 0.26 |
| hit@5 | 0.46 | 0.15 | 0.39 |
| MRR@10 | 0.42 | 0.09 | 0.32 |
| stale hits (old version text in top 10) | 0 | 0 | 0 |

| hit@5 by type | dense | full text | hybrid |
|---|---|---|---|
| lookup (30) | 0.53 | 0.20 | 0.43 |
| version change (12) | 0.75 | 0.17 | 0.67 |
| two sections (12) | 0.00 | 0.00 | 0.00 |

Hybrid lost to dense here, which I didn't expect. It was worse on 18 questions and missed 27 in the top 10. The full text side is weak on this corpus. An OR of lexemes ranked by `ts_rank_cd` rewards chunks that repeat the drug name and common label words ("patients", "dose"), and with 5 to 14 labels per common generic, those chunks crowd out the one that answers. RRF then pulls those into the fused list. The misses are mostly the right drug but the wrong section (the Warnings 5.x subsection instead of Contraindications) or a different drug with similar wording. No question needing two sections got both into the top 10 by any method, because near duplicate labels of the same drug fill the list. The freshness part held: no old version text came back for any version change question.

## Run it

Needs Python 3.11 and Docker.

```bash
make setup          # venv with lxml, psycopg, sentence-transformers (CPU torch)
make up             # Postgres 17 + pgvector, capped at 640 MB
make fetch          # every daily/weekly file on the DailyMed page from Nov 2, slimmed to Rx XML
make bootstrap      # pre-window versions of the labels those files touch
make init run       # apply every file in order, then the freshness check
make load FILE=dm_spl_daily_update_11192026.zip FORCE=1   # forced rerun, changes nothing
make backfill       # the whole range as one batch
make parity         # backfill into a scratch schema and compare with day by day
make eval           # dense vs full text vs hybrid on eval/questions.jsonl
make crosscheck     # openFDA sample and tombstones vs DailyMed's current list
make ci             # ruff + pytest (needs make up for the db tests)
make down
```

DailyMed only lists the last few days of update files, so a fresh clone will fetch a later window than mine. Raw zips go to `data/incoming/` and are deleted after slimming. Everything under `data/` is gitignored.

Cron, once a day:

```
0 6 * * * cd /srv/labelfeed && make run >> data/cron.log 2>&1
```

## Layout

```
src/labelfeed/
  dailymed.py    listing, download, md5 check, slim, history and version archive
  bootstrap.py   pre-window versions for the subset
  spl.py         SPL parsing, LOINC sections, chunking, content hash
  load.py        one file per transaction, supersede, tombstone, backfill netting, embedding cache
  checks.py      label rules and publish gates
  embed.py       MiniLM on CPU, and a hashing embedder for tests
  retrieve.py    dense, full text, hybrid RRF
  evaluate.py    hit rate and MRR per method and question type
  freshness.py   SLA check
  crosscheck.py  openFDA and DailyMed current list
  parity.py      backfill vs day by day
  lineage.py     OpenLineage events to a JSON lines file
  runner.py      CLI
eval/            questions, results, crosscheck output
docs/design.md
```

## Tradeoffs

* **Plain CLI and cron, not Dagster.** One input file a day, applied strictly in order. The ledger, exit codes and alert log cover what I'd want from an orchestrator here.
* **Vectors keyed by content hash.** Generic and repackager labels share paragraphs word for word, so identical text is embedded once. It also makes "unchanged chunk" free to detect.
* **Exact vector scan.** About 40k vectors is a few milliseconds. HNSW would come with the full label set.
* **Full text is ts_rank_cd, not BM25.** Postgres has no corpus IDF. I say hybrid "BM25-style" and mean that.
* **Subset by change window.** Only labels touched between Nov 2 and Nov 19 are in the index, about 1,500 active labels, not all of DailyMed.

## Failure stories

**A duplicate chunk id threw away 21 minutes of embedding.** The first full run died at the insert of the bootstrap with a primary key violation. A fluoxetine label has two boxed warning sections, one titled and one not, and my subsection counter restarted per section element, so both got `/34066-1/0/0`. The transaction rolled back, which was right, but all 30,940 vectors went with it. I fixed the counter (it now runs per section code) and added a test with that shape. I also added `embedding_cache`, written on its own autocommit connection, so a rollback no longer loses embedding work. The next run hit "No space left on device" in the Docker VM, which three builds share. The cache already held 26,000 vectors from that attempt, so the retry only embedded the rest.

**The quarantine gate blocked the bootstrap on real data.** 162 of 1,322 labels had none of the kept sections, 12%, over my 10% limit. They were medical gases (oxygen, nitrogen, carbon dioxide, air), which are Rx labels with only a package label. They now count against a separate 30% limit, so a parser that stops finding sections still trips the gate.

**Parity caught a backfill bug.** The first parity run differed on 8 labels. The backfill kept the highest version per set id before checking it, so a label whose newest version had an effective date three days in the future was quarantined in the backfill and never got its previous version, while day by day had served v14 all along. The same mistake recorded removals for medical gas labels that were never indexed. Netting now only keeps versions that pass the label checks, and the next parity run matched exactly.

## What I'd change

* Re-queue labels quarantined for a future effective date once the date passes. Right now they wait for the next version.
* Strip zero-width characters (`﻿`) in the text cleaner. A few older labels have them. Changing it changes hashes, so it's one planned re-embed.
* Chunk on the model's tokenizer instead of words.
* Load the full Rx release once as the bootstrap, and add an HNSW index.
* Try a drug name filter before ranking, since most questions name the drug and most misses were near duplicate labels crowding the list. I haven't measured it.
* Try a section prior or section aware query routing for questions that need two sections. All three methods scored 0 there.
