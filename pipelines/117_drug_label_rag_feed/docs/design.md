# Design notes

The decisions behind the drug label feed, and the places I'd want a reviewer to push.

## Problem and consumer

A clinical Q&A assistant answers questions like "how long before surgery should dapagliflozin be held" from FDA drug labels. Labels change all the time. Between Nov 2 and Nov 19, 2026 DailyMed published 1,694 new human prescription label versions, and 123 of the labels I index were withdrawn. If the assistant's index lags, it quotes a dosing threshold or an age limit that the manufacturer has already changed. If a withdrawn label stays in the index, it keeps citing a label that no longer exists.

The consumer is the retrieval layer of that assistant. It needs, at all times, exactly one version of each label (the current one), split into the sections people ask about, with no chunks left over from older versions or withdrawn labels. There's no LLM in this project. The output is the index and a measured retrieval quality.

## Data

DailyMed (NLM) publishes Structured Product Labels (SPL, an HL7 XML format) and update files.

* **Daily update zips** on the downloads page, `dm_spl_daily_update_MMDDYYYY.zip`, 57 to 244 MB each. Only the last four days are listed at a time, plus two weekly files and the monthly ones. Each zip has one inner zip per label (the XML and package photos) under `prescription/`, `otc/`, `animal/` and so on, plus `files_added.txt` and `files_deleted.txt`.
* **The downloads page** lists an MD5 for each update file. The loader checks against it.
* **Web services** `/services/v2/spls/{setid}/history.json` for a label's versions, `getFile.cfm?setid=&version=` for an old version, `spls.json?setid=` for whether a label is in the current list.
* **openFDA** `drug/label.json` as an outside cross-check. It's rebuilt weekly, so it lags.

### The subset

The full human Rx release is six zips of about 1.6 GB each. I didn't download it. The window is every update file DailyMed offered on Nov 20, 2026, which is two weekly files (Nov 2 to 6 and Nov 9 to 13) and four dailies (Nov 16 to 19). Together they cover Nov 2 to Nov 19 without a gap.

The starting state is built the same way a real system would start from the full release, but only for the labels the window touches. For every Rx set id that a window file deletes an older file for, `bootstrap.py` asks the history API which version was current before Nov 2 and fetches that version. That's 1,347 set ids. 1,226 came back as the exact older version. 96 came back only as the latest copy, because DailyMed doesn't serve old versions of labels it has archived, and those are the withdrawn ones the window later removes. 25 had no version older than the window in the history API and were skipped. The bootstrap is written as one more update file (`bootstrap_20261101.zip`) so it goes through the same loader, checks and ledger.

Downloads were 1.72 GB in total. Each file is slimmed on arrival to Rx XML only (images and other categories dropped) and the original deleted, so what stays on disk is 67 MB in `data/updates/`.

## Parsing and chunking

`spl.py` reads the XML with lxml (entities off, no network). It keeps eight LOINC section codes.

| Code | Section |
|---|---|
| 34066-1 | Boxed warning |
| 34067-9 | Indications and usage |
| 34068-7 | Dosage and administration |
| 34070-3 | Contraindications |
| 43685-7 | Warnings and precautions |
| 34071-1 | Warnings (older non-PLR labels) |
| 42232-9 | Precautions (older non-PLR labels) |
| 34084-4 | Adverse reactions |

The highlights block (`excerpt`) is skipped because it repeats the full text. Paragraphs, list items and table rows are read in order, a table row becomes `cell | cell`, and block level tags get a space around them (plain `itertext()` turned "Myalgia" followed by a nested list item "severe" into "Myalgiasevere").

Chunks are packed inside each subsection, up to 160 words, splitting on sentences. MiniLM truncates at 256 word pieces, and label text runs about 1.3 pieces per word. Packing per subsection is what makes the incremental part work. An edit in 5.3 changes 5.3's chunks and nothing else.

Every chunk carries set id, version, section code, effective date and a content hash. The hash is sha256 of exactly the text that gets embedded, which is `drug (generic). section. heading path. body`. Putting the drug name in the embedded text matters, since questions almost always name the drug and the body often says only "this drug".

## Incremental loading

One update file is one transaction. Readers see the index before the file or after it.

1. **Ledger.** `load_ledger` is keyed by (file name, source md5). The same file again is a no-op. The same name with a different md5 means DailyMed republished it, and the loader refuses without `--force`.
2. **Manifest.** The slim step writes a manifest last, with the source md5 (checked against the md5 DailyMed lists) and the slim file's md5. A file without a manifest, or whose bytes don't match it, is blocked before anything is read.
3. **Per label.** Parse, run the label checks, compare with the indexed version. Older than indexed goes to quarantine. Same version and document id is skipped. Otherwise it's planned.
4. **Embed only new text.** Vectors live in `chunk_vectors` keyed by content hash. Only hashes with no vector yet are embedded, once per file even when several labels share them.
5. **Swap.** Delete the label's chunks, insert the new ones, upsert `labels`, close the old row in `label_versions`.
6. **Tombstones.** A set id whose old file is in `files_deleted.txt` and that has no new file in the same update was withdrawn. Its chunks are deleted, the label is marked removed, a tombstone row records the version and how many chunks and vectors went, and vectors no other chunk uses are deleted.
7. **Publish gates** run inside the transaction. Any failure rolls the whole file back.

Keying vectors by hash rather than by chunk did more than I expected. Generic labels from different labelers (and repackagers like Bryant Ranch or REMEDYREPACK) share whole paragraphs word for word, so the 38,432 chunks of the bootstrap needed only 30,940 vectors.

### Backfill

`make backfill FIRST LAST` applies a range of files as one batch, keeping only the last event per set id. A label that got three versions in the range is parsed three times but embedded once. A label added and then removed inside the range still gets its removed row and a tombstone, so the end state is the same as day by day.

`make parity` checks that claim by backfilling the whole window into a scratch schema and comparing labels, chunks, tombstones and vectors with the day by day index. Vectors come from the embedding cache, since the model gives the same vector for the same text and the check is about which chunks end up served.

The first parity run found a real bug. Netting kept the highest version per set id before checking it. One label's newest version had an effective date in the future, so the backfill quarantined it and never served the version before, which day by day had served all along. The same mistake wrote removals for labels that were never indexed (medical gases quarantined for having no sections). Netting now keeps only versions that pass the label checks. After the fix the whole window backfilled in 81 seconds and matched day by day on 1,611 labels, 49,475 chunks, 123 tombstones and 38,857 vectors.

### Embedding cache

`embedding_cache` holds every vector computed, keyed by model and content hash, written on its own autocommit connection. It exists because of a real failure. The first full load rolled back on a duplicate chunk id (a fluoxetine label with two boxed warning sections) and the 30,940 vectors computed inside the transaction were lost, 21 minutes of CPU. The retry then hit a full disk in the shared Docker VM, and by then the cache already held 26,000 vectors. Retrieval never reads it, and it can be truncated at any time.

## Data quality

Label rules quarantine one label, with a reason, into `quarantine`.

| Reason | Meaning |
|---|---|
| xml_parse_error, bad_id, bad_version, bad_effective_date | the document can't be trusted |
| set_id_mismatch | file name and XML disagree |
| not_human_rx | wrong document type in the Rx folder |
| future_effective_date, implausible_effective_date | |
| no_product_name | |
| no_target_sections | none of the kept sections has text |
| older_than_indexed | a version lower than the one already served |
| missing_xml | listed in files_added but not in the zip |

Publish gates block the whole file.

| Gate | Why |
|---|---|
| more than 10% of labels quarantined (at least 5), not counting no_target_sections | a broken file or a parser regression |
| more than 30% with no_target_sections | a parser that stopped finding sections. Medical gas labels (oxygen, nitrogen, carbon dioxide) have only a package label and were 162 of the 1,322 bootstrap labels, which tripped the 10% gate on the first real run, so they get this looser limit |
| an active label with no chunks | |
| chunks of a removed label | a tombstone that didn't delete |
| a chunk whose version isn't its label's | a half applied new version |
| vectors from a different model | mixed embedding spaces |
| orphan vectors | garbage collection missed something |

## Retrieval

Three methods over the same chunks (`retrieve.py`).

* **Dense.** Cosine distance on the MiniLM vectors, exact scan. 38,857 vectors is small enough that an HNSW index isn't needed yet.
* **Full text.** A stored `tsvector` (English config) over heading and body, a GIN index, an OR of the question's lexemes, ranked by `ts_rank_cd` with length normalisation. It's BM25 in spirit, not BM25. Postgres doesn't have IDF across the corpus.
* **Hybrid.** Reciprocal rank fusion of the top 50 from each, k = 60. Only ranks are used, so the two scores don't need calibrating.

### Results

On 54 reviewed questions, hit@5 was 0.46 dense, 0.15 full text and 0.39 hybrid, MRR@10 0.42, 0.09 and 0.32. Hybrid lost to dense. Full text over an OR of lexemes favours chunks that repeat the drug name and common words, and with 5 to 14 near duplicate labels per common generic those chunks crowd out the answer, then RRF carries them into the fused list. No method got both sections of a two section question into the top 10. No version change question returned old version text. Per question ranks are in `eval/results.json`.

Postgres needed `max_parallel_workers_per_gather=0` and a 128 MB `shm_size`. The default 64 MB of shared memory in the container ran out on the first parallel scan of the vectors.

## Orchestration

A plain CLI and one cron line. The daily job is one file, strictly in order, and every step depends on the one before. Dagster would add a daemon and a UI to run what is a single command, and its partitions would fight the "files must apply in order" rule. What I needed from an orchestrator (retries, history, alerting) is covered by the ledger, the exit codes and the alert log. If this grew to several sources with their own schedules, I'd move it to Dagster or Airflow and keep the CLI as the task body.

```
# crontab, 06:00 UTC daily. DailyMed posts the day's file the evening before (US Eastern)
0 6 * * * cd /srv/labelfeed && make run >> data/cron.log 2>&1
```

`make run` exits 1 if a file was blocked, 2 if freshness failed.

## Freshness SLA

A label version in the update file for day D is searchable by 00:00 UTC on D+2. DailyMed posts the file for D in the evening, Eastern time, so that's a little over 24 hours from publication. Two checks, both alert and exit 2.

1. A daily file the downloads page lists is past its deadline and not in the ledger.
2. Nothing newer than the last loaded file for 4 days past its deadline. This one doesn't need the listing, so it still fires if the page is down or changes format.

The alert path is a stderr line, a line in `data/alerts.log`, the exit code, and a JSON post to `LABELFEED_ALERT_WEBHOOK` if it's set. I haven't connected that to a real channel.

## Lineage

Each load and backfill appends OpenLineage START and COMPLETE (or FAIL) events to `data/lineage/events.jsonl`, with the update file as input and the tables as outputs. No backend. It's there so a Marquez or DataHub collector could pick the same events up later, and it cost about 40 lines.

## Failure modes

| What breaks | What happens |
|---|---|
| download truncated or corrupted | md5 differs from the listed one, file renamed `.bad`, nothing slimmed |
| slim step killed halfway | no manifest, the loader blocks the file |
| DailyMed republishes a file | same name, new md5, refused without `--force` |
| a bad batch of labels | quarantined with reasons. Over the limit, the file is rolled back and later files wait |
| loader bug or crash mid file | transaction rolled back, ledger row `failed`, alert, embeddings kept in the cache |
| version arrives out of order | older_than_indexed in quarantine, served version unchanged |
| label withdrawn | tombstone, chunks and unshared vectors deleted in the same transaction |
| DailyMed listing down | fetch alert, silence check still runs |
| model changed | the mixed model gate blocks every file until the index is rebuilt |

## Cost and resources

Everything is local and free. Postgres 17 with pgvector 0.8.1 in Docker, capped at 640 MB with `shared_buffers=128MB` and `max_wal_size=256MB`. MiniLM runs on CPU. No paid APIs.

## Limits

* The subset is labels that changed in an 18 day window. Labels that didn't change aren't in the index at all, so retrieval here is over 1,488 active labels, a small slice of DailyMed.
* For withdrawn labels the bootstrap has the archived last copy, not the version that was live before Nov 2.
* Chunks are cut on words, not on the model's tokenizer. A chunk with long chemical names can still pass 256 word pieces and get truncated.
* Full text is ts_rank_cd, not BM25.
* The question set was written by reading labels, by the same person who built the retrieval. It's a reviewed question set, not an independent benchmark, and 54 questions gives wide error bars.
* A label quarantined for a future effective date isn't retried when the date passes. It waits for the next version.
