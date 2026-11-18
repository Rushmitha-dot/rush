"""Load one drop into raw.reviews.

The source has no review id, so it's derived as md5(user_id|asin|timestamp).
About 1% of the dump is exact repeats of that triple with the same text, which
go to quarantine as duplicates instead of being loaded twice.
"""
import json
from pathlib import Path

from .replay import md5_of


class DropRejected(Exception):
    pass


def read_manifest(folder: Path) -> dict:
    mpath = folder / "_manifest.json"
    if not mpath.exists():
        raise DropRejected(f"no manifest in {folder}, drop incomplete")
    manifest = json.loads(mpath.read_text())
    for f in manifest["files"]:
        path = folder / f["name"]
        if not path.exists():
            raise DropRejected(f"{f['name']} listed in manifest but missing")
        if path.stat().st_size != f["bytes"]:
            raise DropRejected(f"{f['name']} is {path.stat().st_size} bytes, manifest says {f['bytes']}")
        if md5_of(path) != f["md5"]:
            raise DropRejected(f"{f['name']} md5 mismatch")
    return manifest


def already_loaded(con, batch_date: str, md5: str) -> bool:
    row = con.execute("select md5 from ops.load_ledger where batch_date = ?", [batch_date]).fetchone()
    return row is not None and row[0] == md5


STAGE_SQL = """
create or replace temp table stage as
with src as (
    select *, row_number() over () as src_line
    from read_json(?, format='newline_delimited', columns={
        rating: 'double', title: 'varchar', text: 'varchar', asin: 'varchar', parent_asin: 'varchar',
        user_id: 'varchar', timestamp: 'bigint', helpful_vote: 'bigint', verified_purchase: 'boolean'})
),
keyed as (
    select *,
        md5(coalesce(user_id, '') || '|' || coalesce(asin, '') || '|' || coalesce(cast("timestamp" as varchar), ''))
            as review_id,
        epoch_ms("timestamp") as review_ts
    from src
)
select *,
    case
        when user_id is null or asin is null or parent_asin is null or "timestamp" is null then 'missing_key'
        when rating is null or rating not between 1 and 5 then 'bad_rating'
        when text is null or trim(text) = '' then 'empty_text'
        when row_number() over (partition by review_id order by src_line) > 1 then 'duplicate'
    end as reject_reason
from keyed
"""


def stage(con, folder: Path, manifest: dict) -> dict:
    data = folder / manifest["files"][0]["name"]
    con.execute(STAGE_SQL, [str(data)])
    s = con.execute("""
        select count(*),
               count(*) filter (where reject_reason = 'missing_key'),
               count(*) filter (where reject_reason is not null),
               count(*) filter (where cast(review_ts as date) not between ? and ?)
        from stage""", [manifest["window_start"], manifest["window_end"]]).fetchone()
    reasons = dict(con.execute("select reject_reason, count(*) from stage where reject_reason is not null "
                               "group by 1 order by 2 desc").fetchall())
    unknown = con.execute("""
        select count(*) from stage s
        where reject_reason is null
          and not exists (select 1 from raw.products p where p.parent_asin = s.parent_asin)""").fetchone()[0]
    return {"rows_in": s[0], "missing_key": s[1], "quarantined": s[2], "outside_window": s[3],
            "quarantine_reasons": reasons, "unknown_product": unknown,
            "manifest_rows": manifest["files"][0]["rows"]}


def recent_daily_rows(con, batch_date: str, days: int = 7) -> list[int]:
    rows = con.execute("""
        select rows from ops.load_ledger
        where kind = 'daily' and batch_date < ? and batch_date >= cast(? as date) - ?::integer
        order by batch_date""", [batch_date, batch_date, days]).fetchall()
    return [r[0] for r in rows]


def publish_stage(con, batch_date: str, manifest: dict) -> int:
    """Replace the batch in one transaction. A reader sees the old day or the
    new day, never a mix, and a rerun of the same drop changes nothing."""
    f = manifest["files"][0]
    con.execute("begin")
    try:
        con.execute("delete from raw.reviews where batch_date = ?", [batch_date])
        con.execute("delete from raw.reviews_quarantine where batch_date = ?", [batch_date])
        con.execute("""
            insert into raw.reviews
            select review_id, cast(? as date), parent_asin, asin, user_id, rating, title, text, review_ts,
                   helpful_vote, verified_purchase, current_timestamp
            from stage where reject_reason is null""", [batch_date])
        con.execute("""
            insert into raw.reviews_quarantine
            select cast(? as date), review_id, reject_reason,
                   to_json({'asin': asin, 'user_id': user_id, 'timestamp': "timestamp", 'rating': rating,
                            'title': title, 'text': text}),
                   current_timestamp
            from stage where reject_reason is not null""", [batch_date])
        con.execute("delete from ops.load_ledger where batch_date = ?", [batch_date])
        con.execute("insert into ops.load_ledger values (?, ?, ?, ?, ?, current_timestamp)",
                    [batch_date, manifest["kind"], f["name"], f["md5"], f["rows"]])
        loaded = con.execute("select count(*) from raw.reviews where batch_date = ?", [batch_date]).fetchone()[0]
        con.execute("commit")
    except Exception:
        con.execute("rollback")
        raise
    return loaded
