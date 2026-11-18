# Design notes

These are the decisions behind the product finder and the places I'd want a reviewer to push.

## Problem and consumer

Someone shopping for skincare or hair care usually starts from a problem ("my hair frizzes", "this kind of cleanser breaks me out") and a type ("curly", "sensitive skin"). Star ratings don't answer that. A five star review might say the cream broke the reviewer out but smells nice. The consumer here is a menu driven assistant that asks concern, then type, then shows the products reviewers with that concern and type were happiest with, plus the sentences that back the score.

So the pipeline's job is turning free review text into a tested table of (product, concern, type, score, evidence), refreshed daily, where a change to the text model never silently rewrites what the assistant serves.

## Data

[Amazon Reviews 2023](https://amazon-reviews-2023.github.io/) from the McAuley lab, category All_Beauty. 701,528 reviews of 112,565 products from Nov 2000 to Sep 2023, plus 112,590 rows of product metadata. Download is 94 MB of reviews and 40 MB of metadata, gzipped.

* The links on the Hugging Face dataset card point at `datarepo.eng.ucsd.edu`, which returned 404 when I built this. The same files are served from `mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/`, which is what the Makefile uses.
* The dataset card has no license field. The project page asks for a citation and describes it as research data. I download it in the pipeline and don't commit the raw files. The only review text in the repo is the 150 short reviews in `eval/sample.jsonl`, kept so the labels can be checked against what they label.
* Beauty_and_Personal_Care would be the bigger category (tens of millions of reviews). All_Beauty fits the laptop budget I gave myself (under 3 GB of disk, under 1.5 GB of RAM) and still has skin, hair and body products.

Everything in the pipeline is real reviews. Nothing is generated. The "daily feed" is the same records split by their own timestamps, see below. Unit tests use a handful of hand written rows.

## Replay as a daily feed

The source is a single static dump, so `replay.py` turns it into drops. Every review before 2023-01-01 goes into one history drop (687,123 reviews, the initial load), then one drop per UTC day from 2023-01-01 to 2023-09-09 (250 drops, 14,405 reviews). Sep 5 and Sep 7 have no reviews at all, so there's no drop for them. Volume falls off hard through 2023, from about 124 reviews a day in January to single digits in September, because the crawl ended in September 2023.

Each drop is the source lines untouched in a gzipped JSONL file plus a `_manifest.json` written last (rows, bytes, md5, the time window it covers). Loading checks the manifest before reading a byte of data, so a half written drop is rejected instead of loaded.

## There is no review id

The source has user, product, timestamp and text but no review id. I derive it as `md5(user_id | asin | timestamp)`. In the full dump 7,981 rows ended up in quarantine on that key, 7,261 of them exact repeats of the same user, product and millisecond with the same text, and 720 with empty text. Repeats are kept in `raw.reviews_quarantine` with a reason rather than dropped silently, so the counts reconcile (693,547 loaded + 7,981 quarantined = 701,528).

## Layers

| Layer | Written by | Grain | Notes |
|---|---|---|---|
| `raw.reviews` | ingest | review | delete and insert per batch date in one transaction |
| `raw.reviews_quarantine` | ingest | rejected row | reason column, payload kept as JSON |
| `nlp.scored_reviews` | enrich | review x model version | every scored review, even with no mentions |
| `nlp.mentions` | enrich | mention | label, key, matched text, sentence, polarity |
| `intermediate.int_review_concerns` | dbt incremental | review x version x concern | delete+insert on (review_id, model_version) |
| `intermediate.int_review_types` | dbt incremental | review x version x type | same |
| `marts.product_concern_scores` | dbt table | version x product x concern x type | only rows with at least `min_reviews` |
| `marts.product_concern_evidence` | dbt table | up to 3 sentences per score | most helpful first |
| `marts.product_scores_current` | dbt view | | filtered to the active model version |

`ops` holds `pipeline_runs`, `dq_results`, `load_ledger` and `model_versions`.

One detail in the incremental models matters more than it looks. A review with no concern still gets a row with concern `none`. Without it, if a new rule removes every concern from a review, delete+insert on (review_id, model_version) has no new row for that key, so the old concern rows would never be deleted.

## Extraction

`extract.py` is spaCy as a tokenizer, a rule-based sentencizer and an entity ruler. Labels are CONCERN (10 keys), SKIN_TYPE (5) and HAIR_TYPE (7). An IGNORE label catches look-alikes ("dry shampoo", "blow dry", "thinning shears") and wins because the ruler prefers the longest span. VADER scores the sentence that holds each concern mention.

I started with `en_core_web_sm` so I could match on lemmas. On 20,000 reviews it took 82 seconds against 21 seconds for the blank pipeline, and the lemma patterns were also missing "Broke me out" because spaCy's lemma for a capitalised "Broke" isn't "break". So the verb forms are spelled out in the taxonomy and only lowercase token matching is used. A regex prefilter skips spaCy for reviews with none of the vocabulary words, which is most of them in this category ("nice color", "fast shipping"). Peak memory for the extractor process was about 110 MB on a 20,000 review sample.

Scoring the whole history drop with rules-v1 took 885 seconds of load plus extraction on my laptop, while two other projects were building on the same machine.

### Two model versions

A model version pins the vocabulary and the rules together. rules-v1 is the taxonomy lists plus VADER. rules-v2 came out of reading the extractor's mistakes on the 50 dev reviews (never the test reviews), and adds

* split and multi-word phrases ("broke my face out", "breaking out in a rash" as sensitivity, not breakouts)
* bare "dry" only with an intensifier or a body part, because "put it in wet hair and dry" was a verb
* coordinated types ("sensitive and oily skin", "thick, long hair")
* negated problems scored as a good outcome ("no breakouts", "doesn't irritate"), since VADER scored "this doesn't irritate my face" at -0.128
* hair loss and damage dropped in reviews that mention wigs, bundles, extensions or closures, where "shedding" is about the product's hair, not the buyer's

One bug found later by a unit test, not by the test set. Reviews use both `'` and `’`, and spaCy splits "doesn’t" into "does" and "n’t", so the negation list missed curly apostrophes. Fixing it moved the dev polarity agreement and left every test-split number unchanged.

## Evaluation

`label.py` draws 150 reviews that mention skin, hair, scalp or face and are 40 to 600 characters (seed 115). Those four words are not extractor terms, so the sample isn't chosen by the thing it grades. The first 50 are dev, the other 100 are test. The labels are a manually reviewed label set of 150 reviews, written one review at a time without looking at the extractor's output, with a note on every judgment call. 32 of the 150 reviews have at least one concern, 22 of them in test, so the numbers below have wide error bars.

Labeling rules I used

* a concern needs an explicit problem or outcome ("broke me out", "no frizz", "so hydrating"). "I have sensitive skin" is a type, not a concern.
* the type belongs to whoever's experience is described (the reviewer, or the daughter they bought it for). "Good buy if you have fine hair" doesn't make the reviewer fine haired.
* wig, extension and doll hair problems are product defects, not concerns
* outcome is + (helped, or no problem happened), - (caused it or didn't help), 0 (mentioned with no outcome)

Results on the 100 test reviews (`make eval`)

| | rules-v1 | rules-v2 |
|---|---|---|
| Concern precision | 0.64 | 0.67 |
| Concern recall | 0.57 | 0.57 |
| Type precision | 0.90 | 0.91 |
| Type recall | 0.69 | 0.81 |
| Outcome direction agrees, on 16 concerns both found | 6 / 16 | 10 / 16 |

On dev, which v2 was written against, concern F1 went from 0.55 to 0.87. On test it barely moved. That gap is the honest result. The dev fixes were real but narrow, and the test errors are mostly different kinds of miss

* no vocabulary for the phrasing. "My hair fell out horribly", "gets all the gunk out of my pores", "my hair is growing", "gets to the itch" under braids.
* wig and extension reviews that don't use any of the defect words ("No shedding" on an 18 inch hair bundle).
* heat tools. "Burning smell" on a flat iron and "falling out" for curls dropping are not sensitivity or hair loss.
* "oily" counted twice, as the type in "my oily and sensitive skin" and as an oiliness concern.

The type side is where v2 really helped, mostly from coordinated phrases. Polarity is the weakest part. Sentence level VADER doesn't know that "hydrated" is good or that "burns" is bad, and a sentence often carries two products ("other deodorants gave me a rash, this one didn't").

## Versioned backfill and the view swap

Scores are keyed by review id plus model version. Activating a new model is three steps

1. `make score-v2` registers rules-v2 as a candidate and scores every review it hasn't seen, in 20,000 review chunks that commit as they go, so a stopped backfill resumes where it stopped. Daily runs score candidates too, so the candidate can't fall behind while it backfills.
2. dbt builds rules-v2 marts next to rules-v1 and runs every test on both.
3. `make activate VERSION=rules-v2` refuses to run if any review is missing a v2 score, then flips `ops.model_versions` in one transaction. `product_scores_current` filters on the active version, so the swap is one row update and the old scores stay for rollback (`make activate VERSION=rules-v1`).

What happened when I did it for rules-v2

* scoring all 693,547 reviews took 12 minutes 7 seconds and wrote 139,587 mentions. dbt then built both versions side by side, 38 tests passing.
* concern mentions went from 124,845 to 103,669, mostly bare "dry" no longer matching verbs. Published "any type" dryness scores went from 1,033 to 743 and hair loss from 98 to 71 (wig reviews).
* v2 publishes 2,147 score rows over 1,218 products, against 2,463 over 1,429 for v1. Of the 13 "any type" top 5 lists, 1 came out identical and 40 of the 65 slots kept the same product.
* `activate` took 20 seconds including the dbt build and the new snapshot. The 2,463 v1 rows are still in the table.

## Data quality

Checks on each drop. ERROR blocks the load and alerts, WARN loads and alerts. Everything lands in `ops.dq_results`.

| Check | Severity | Why |
|---|---|---|
| manifest present, file size and md5 match | error (drop rejected) | a half written drop |
| drop not empty, rows match the manifest | error | |
| every review timestamp inside the drop's window | error | a misfiled drop would replace the wrong day |
| user, asin, parent asin, timestamp present | error | no key, no review id |
| rows that lose information (no key, bad rating, empty text) over 5% and at least 10 rows | error | |
| quarantined share over 1% and at least 10 rows | warn | duplicates included |
| reviews for products missing from the catalog over 2% | warn | the catalog file is static here |
| daily volume outside 0.3x to 3x of the trailing 7 days | warn | |

dbt tests run after every load, and an ERROR test skips the publish, so the assistant keeps serving the last good snapshot. They cover the concern and type taxonomy (relationships to seeds), uniqueness on every grain, `n_reviews >= min_reviews` and score between 0 and 100 on the published scores, concern domain matching product domain, exactly one active model version, every scored version being registered, and a warn-level test on the share of reviews with any concern (catches a broken prefilter).

Two thresholds changed after real runs, and I left the history visible in `ops.pipeline_runs`.

* The first version warned whenever more than 1% of a drop was quarantined. In January a day has about 130 reviews, so two exact repeats is already 1.5%. It fired on 8 of the first 14 days. It now also needs 10 rows.
* The blocking rule was 5% of rows quarantined. In the thin 2023 tail a day has 10 to 30 reviews, so one repeated review blocked the day. The first backfill from Jan 17 blocked 31 of 233 days, every one of them because of 1 to 6 exact repeats and at most 3 empty reviews. Repeats lose nothing, so they no longer count toward the blocking rule, and that rule also has a 10 row floor. Rerunning the same range reloaded exactly those 31 days and skipped the other 202 through the load ledger.

The volume warning still fires a few times in September (0.08x to 0.29x of a trailing average of 4 to 12 reviews). That's the dataset ending, and I'd rather see it than tune it away.

## Reruns, backfill, parity

* A drop already in `ops.load_ledger` with the same md5 is skipped. `--force` reloads it.
* I force reloaded 2023-01-10 twice and hashed raw, quarantine, mentions, the incremental model and the marts before and after. Identical.
* `make parity` builds Jan 1 to 31 twice in scratch databases, once with a dbt build after every day (the schedule) and once with every day loaded first and a single build at the end (a backfill). With `min_reviews` at 2 so a one month window publishes something, `int_review_concerns` (3,876 rows), `int_review_types` (3,789), `product_concern_scores` (62) and `product_concern_evidence` (147) came out identical. It took 9 minutes 44 seconds.

## Orchestration

Airflow 3.3.2 with `airflow standalone` for the UI. I picked Airflow over Dagster because it's what most of the job posts I'm aiming at ask for, and the DAG here is simple enough that Dagster's asset model wouldn't buy much. Three tasks per day: `load_check_score`, `dbt_build_and_publish`, `freshness`, with retries and `catchup` over the 2023 window.

* Airflow lives in its own venv and calls the pipeline's venv through the runner CLI. Airflow 3.3's constraint pins and dbt 1.12's pins don't resolve together, and this way neither has to.
* `max_active_runs=1` because DuckDB allows one writer. On Snowflake this could go to several days at once since each day owns its own keys.
* A day with no drop exits 99, which the BashOperator marks skipped, so Sep 5 and 7 don't page anyone.
* I ran 2023-01-15 and 2023-01-16 through `airflow dags test`. 40 and 64 seconds end to end, all three tasks green.
* `airflow standalone` starts the scheduler, API server and the rest by calling `airflow` from PATH, so from a venv that isn't activated it dies with `No such file or directory: 'airflow'`. `make airflow` puts the venv's bin first. With that the health endpoint on 8115 reported the scheduler, triggerer and DAG processor healthy.

## SLA and run times

The target is batch day D scored and published by 06:00 UTC on D+1. `make freshness` checks `ops.pipeline_runs` and alerts if D has no successful load.

Measured for the first 14 days of January, run one day at a time with a dbt build after each

* load, checks and scoring took 0.8 seconds on average, 2.2 at worst (75 to 160 reviews a day)
* dbt build plus publish took 22.5 seconds on average, 44.9 at worst

The dbt build dominates because the marts are rebuilt in full over about 700,000 reviews each time and every test runs. That's fine for a daily job. If it weren't, the marts could be made incremental by product, since only products with new reviews change.

## Snowflake

I don't have a Snowflake account for this, so nothing here has run on Snowflake. What I did to keep the move cheap

* the dbt SQL avoids DuckDB only syntax. `||`, window filters in subqueries instead of `qualify`, `sum(case ...)` instead of `filter`, `delete+insert` as the incremental strategy, which dbt-snowflake supports
* the timestamp conversion (`epoch_ms`) is in the Python load, not in dbt
* `profiles.yml` has a `snowflake` target with key pair auth
* `infra/terraform` creates the database, schemas, an XS warehouse with 60 second auto-suspend, a resource monitor and two roles. `terraform validate` passes in CI. It's never been applied.

The Python ingest would become a COPY INTO from an S3 stage, and the extractor would run as a Snowpark Python job or stay a container that writes to `nlp.mentions`. I haven't built either.

Cost, estimated from Snowflake's published rate of 1 credit per hour for an XS warehouse, billed per second with a 60 second minimum each time it resumes. My daily dbt build is about 25 seconds locally. Assuming it's similar on XS, one resume a day bills about 60 to 90 seconds, which is roughly 0.02 to 0.03 credits a day, or under one credit a month. At typical list prices of 2 to 4 dollars a credit, that's a few dollars a month. These are estimates, not measurements.

## Failure modes

| What breaks | What happens |
|---|---|
| drop half uploaded | no manifest or md5 mismatch, run marked error, alert, nothing loaded |
| drop for the wrong day | timestamp window check blocks it |
| a burst of empty or keyless rows | blocked over 5% and 10 rows, rows kept in quarantine with a reason |
| extraction crashes mid backfill | chunks already committed stay, rerun scores only missing reviews |
| dbt test fails | publish skipped, assistant keeps the last snapshot, alert with the failing test |
| new model scores look wrong after activation | `make activate VERSION=<old>` |
| assistant reads while a run writes | it reads Parquet files replaced by rename, never the DuckDB file |

## Limits

* The test set is 100 reviews with 28 concern labels. Treat the precision and recall as rough.
* Sentence level VADER is a weak polarity model for this domain, see above.
* Product domain (skin or hair) comes from title keywords, so hair tools land in the hair domain and pick up concern scores from how they feel to use. Under v1 a curling wand topped hair sensitivity because reviewers write about burns. Under v2, which reads "no more burns" as a good outcome, a heat resistant mat for hot tools tops it instead.
* Products with lots of reviews that say "not greasy" score well on oiliness. That's the right reading of the text, but it means the oiliness list is full of oils.

## What I'd do next

* Grow the label set to a few hundred reviews and label outcome direction per mention, then try a small fine-tuned classifier for polarity against VADER on the same set.
* Make the marts incremental by product.
* Land the product catalog as its own feed, with SCD2 on title and store.
* Add per-version score drift to the activation step (how many top 5 lists change) and refuse the swap above a threshold without a flag.
