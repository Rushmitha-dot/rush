"""Score reviews with a model version and write nlp.mentions + nlp.scored_reviews.

Two modes.
  batch    rescore one batch_date (delete + insert for those reviews), used by the daily run
  missing  score every review that has no row for this version yet, in chunks that
           commit as they go, so a version backfill can be stopped and resumed
"""
import logging
import time

import pyarrow as pa

from .extract import Extractor, clean_text

log = logging.getLogger("finder.enrich")
CHUNK = 20_000

MENTION_SCHEMA = pa.schema([
    ("review_id", pa.string()), ("model_version", pa.string()), ("mention_no", pa.int32()),
    ("label", pa.string()), ("key", pa.string()), ("matched", pa.string()), ("sentence", pa.string()),
    ("polarity", pa.float64()),
])


def _write(con, version: str, scored: list[tuple[str, list]]) -> int:
    ids = pa.table({"review_id": [rid for rid, _ in scored]})
    counts = pa.table({"review_id": [rid for rid, _ in scored],
                       "n_mentions": pa.array([len(ms) for _, ms in scored], pa.int32())})
    ms = [m for _, mlist in scored for m in mlist]
    mentions = pa.Table.from_pylist(
        [{"review_id": m.review_id, "model_version": version, "mention_no": m.mention_no, "label": m.label,
          "key": m.key, "matched": m.matched, "sentence": m.sentence, "polarity": m.polarity} for m in ms],
        schema=MENTION_SCHEMA)
    con.register("chunk_ids", ids)
    con.register("chunk_counts", counts)
    con.register("chunk_mentions", mentions)
    con.execute("begin")
    try:
        con.execute("delete from nlp.mentions where model_version = ? and review_id in (select review_id from "
                    "chunk_ids)", [version])
        con.execute("delete from nlp.scored_reviews where model_version = ? and review_id in (select review_id "
                    "from chunk_ids)", [version])
        con.execute("insert into nlp.mentions select *, current_timestamp from chunk_mentions")
        con.execute("insert into nlp.scored_reviews select review_id, ?, n_mentions, current_timestamp "
                    "from chunk_counts", [version])
        con.execute("commit")
    except Exception:
        con.execute("rollback")
        raise
    finally:
        for name in ("chunk_ids", "chunk_counts", "chunk_mentions"):
            con.unregister(name)
    return len(ms)


def _score(con, extractor: Extractor, sql: str, params: list) -> tuple[int, int]:
    reviews = mentions = 0
    last = ""
    while True:
        rows = con.execute(sql, params + [last, CHUNK]).fetchall()
        if not rows:
            break
        t0 = time.time()
        scored = list(extractor.run((rid, clean_text(title, text)) for rid, title, text in rows))
        mentions += _write(con, extractor.version, scored)
        reviews += len(rows)
        last = rows[-1][0]
        log.info("%s scored %d reviews (%d so far) in %.1fs", extractor.version, len(rows), reviews, time.time() - t0)
    return reviews, mentions


def score_batch(con, extractor: Extractor, batch_date: str) -> tuple[int, int]:
    sql = """select review_id, title, text from raw.reviews
             where batch_date = ? and review_id > ? order by review_id limit ?"""
    return _score(con, extractor, sql, [batch_date])


def score_missing(con, extractor: Extractor) -> tuple[int, int]:
    # keyset pagination on review_id, and the not-exists makes it resumable
    sql = """select r.review_id, r.title, r.text from raw.reviews r
             where not exists (select 1 from nlp.scored_reviews s
                               where s.review_id = r.review_id and s.model_version = ?)
               and r.review_id > ?
             order by r.review_id limit ?"""
    return _score(con, extractor, sql, [extractor.version])
