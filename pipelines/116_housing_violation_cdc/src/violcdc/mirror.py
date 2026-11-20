"""Postgres ops mirror: DDL, upserts from the poller, watermark and run log."""

import json
import logging
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg import sql

from .soda import Rejected, SodaClient, columns, parse_ts, to_row

log = logging.getLogger(__name__)
DDL = Path(__file__).resolve().parents[2] / "sql" / "mirror.sql"
SOURCE = "hpd_violations"


def connect(dsn):
    return psycopg.connect(dsn, autocommit=False)


def init(conn):
    conn.execute(DDL.read_text())
    conn.commit()


def ensure_columns(conn, cols: dict) -> list:
    """Add any configured column the mirror doesn't have yet. Returns the names added."""
    have = {r[0] for r in conn.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'mirror' AND table_name = 'violations'""")}
    added = []
    for name, pg_type in cols.items():
        if name not in have:
            conn.execute(sql.SQL("ALTER TABLE mirror.violations ADD COLUMN {} {}").format(
                sql.Identifier(name), sql.SQL(pg_type)))
            added.append(name)
    conn.commit()
    return added


def upsert(conn, rows: list, cols: dict) -> dict:
    """Stage the rows with COPY, then insert or update only rows whose content changed.

    The WHERE on the update is what keeps a re-read page from producing change events. Socrata can
    move :updated_at without changing anything we keep, and the overlap window re-reads rows on purpose.
    """
    if not rows:
        return {"inserted": 0, "updated": 0, "unchanged": 0}
    names = list(cols) + ["source_updated_at"]
    ident = sql.SQL(", ").join(sql.Identifier(n) for n in names)
    # recreated each time so a column added by the drift drill shows up here too
    conn.execute("DROP TABLE IF EXISTS stage")
    conn.execute("CREATE TEMP TABLE stage ON COMMIT DROP AS SELECT * FROM mirror.violations WITH NO DATA")
    with conn.cursor() as cur:
        with cur.copy(sql.SQL("COPY stage ({}) FROM STDIN").format(ident)) as cp:
            for r in rows:
                cp.write_row([r.get(n) for n in names])
        meta = sql.SQL("{} - 'row_hash' - 'source_updated_at' - 'mirrored_at'")
        stmt = sql.SQL("""
            INSERT INTO mirror.violations AS v ({cols}, mirrored_at, row_hash)
            SELECT DISTINCT ON (violationid) {cols}, now(), '' FROM stage ORDER BY violationid, source_updated_at DESC
            ON CONFLICT (violationid) DO UPDATE SET {sets}, mirrored_at = now()
            WHERE ({old}) IS DISTINCT FROM ({new})
            RETURNING (xmax = 0) AS inserted
        """).format(
            cols=ident,
            sets=sql.SQL(", ").join(sql.SQL("{0} = excluded.{0}").format(sql.Identifier(n)) for n in names),
            old=meta.format(sql.SQL("to_jsonb(v)")),
            new=meta.format(sql.SQL("to_jsonb(excluded)")),
        )
        cur.execute(stmt)
        flags = [r[0] for r in cur.fetchall()]
    inserted = sum(flags)
    distinct = len({r["violationid"] for r in rows})
    return {"inserted": inserted, "updated": len(flags) - inserted, "unchanged": distinct - len(flags)}


def quarantine(conn, record: dict, reason: str):
    conn.execute(
        "INSERT INTO mirror.quarantine (violationid, reason, payload, source_updated_at) VALUES (%s, %s, %s, %s)",
        (record.get("violationid"), reason, json.dumps(record), record.get(":updated_at")))


def get_watermark(conn):
    row = conn.execute("SELECT watermark, last_id FROM mirror.poll_state WHERE source = %s", (SOURCE,)).fetchone()
    return (row[0], row[1]) if row else (None, 0)


def set_watermark(conn, watermark: datetime, last_id: int):
    conn.execute("""
        INSERT INTO mirror.poll_state (source, watermark, last_id) VALUES (%s, %s, %s)
        ON CONFLICT (source) DO UPDATE SET watermark = excluded.watermark, last_id = excluded.last_id,
            updated_at = now()
        WHERE (excluded.watermark, excluded.last_id) > (mirror.poll_state.watermark, mirror.poll_state.last_id)
    """, (SOURCE, watermark, last_id))


def start_run(conn, mode, watermark_in, note=None) -> int:
    run_id = conn.execute(
        "INSERT INTO mirror.poll_runs (mode, watermark_in, note) VALUES (%s, %s, %s) RETURNING run_id",
        (mode, watermark_in, note)).fetchone()[0]
    conn.commit()
    return run_id


def finish_run(conn, run_id, status, totals, watermark_out=None):
    conn.execute("""
        UPDATE mirror.poll_runs SET finished_at = now(), status = %s, watermark_out = %s, fetched = %s,
            inserted = %s, updated = %s, unchanged = %s, quarantined = %s, pages = %s WHERE run_id = %s
    """, (status, watermark_out, totals["fetched"], totals["inserted"], totals["updated"], totals["unchanged"],
          totals["quarantined"], totals["pages"], run_id))
    conn.commit()


def _load_page(conn, page, cols, boro, totals):
    good = []
    for rec in page:
        try:
            good.append(to_row(rec, cols, boro))
        except Rejected as e:
            quarantine(conn, rec, str(e))
            totals["quarantined"] += 1
    counts = upsert(conn, good, cols)
    totals["fetched"] += len(page)
    totals["pages"] += 1
    for k, v in counts.items():
        totals[k] += v


def _totals():
    return dict(fetched=0, inserted=0, updated=0, unchanged=0, quarantined=0, pages=0)


def base_where(cfg, start=None, end=None) -> str:
    where = f"boro = '{cfg.boro}' AND inspectiondate >= '{start or cfg.window_start}'"
    if end:
        where += f" AND inspectiondate < '{end}'"
    return where


def select_list(cols) -> str:
    return ", ".join(cols) + ", :updated_at"


def seed(cfg, conn, months: list, until: datetime | None = None, client=None) -> dict:
    """First load, one inspection month at a time. `until` leaves rows changed after it for the poller,
    and becomes the starting watermark."""
    cols = columns(cfg.extra_columns)
    ensure_columns(conn, cols)
    client = client or SodaClient(cfg.soda_url, cfg.page_size, cfg.page_sleep)
    run_id = start_run(conn, "seed", None, f"months {months[0][0]}..{months[-1][1]} until {until}")
    totals, max_seen = _totals(), None
    try:
        for start, end in months:
            where = base_where(cfg, start, end)
            if until:
                where += f" AND :updated_at <= '{until.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}'"
            for page in client.pages_by_id(where, select_list(cols)):
                _load_page(conn, page, cols, cfg.boro, totals)
                seen = max(parse_ts(r[":updated_at"]) for r in page)
                max_seen = seen if max_seen is None or seen > max_seen else max_seen
                conn.commit()
                log.info("seed %s pages=%d fetched=%d", start, totals["pages"], totals["fetched"])
        wm = until or max_seen
        if wm:
            set_watermark(conn, wm, 0)
        conn.commit()
    except Exception:
        conn.rollback()
        finish_run(conn, run_id, "error", totals)
        raise
    finish_run(conn, run_id, "ok", totals, until or max_seen)
    return totals


def poll(cfg, conn, client=None) -> dict:
    """Incremental pull of rows with :updated_at after the stored watermark, minus an overlap window.

    Each page commits its rows and the new watermark together, so a crash mid poll resumes from the
    last committed page and never skips one.
    """
    from datetime import timedelta

    cols = columns(cfg.extra_columns)
    ensure_columns(conn, cols)
    client = client or SodaClient(cfg.soda_url, cfg.page_size, cfg.page_sleep)
    watermark, _ = get_watermark(conn)
    if watermark is None:
        raise RuntimeError("no watermark yet, run the seed first")
    start_from = watermark - timedelta(minutes=cfg.overlap_minutes)
    run_id = start_run(conn, "poll", watermark)
    totals = _totals()
    try:
        for page in client.pages_by_updated_at(base_where(cfg), start_from, 0, select_list(cols)):
            _load_page(conn, page, cols, cfg.boro, totals)
            set_watermark(conn, parse_ts(page[-1][":updated_at"]), int(page[-1]["violationid"]))
            conn.commit()
            log.info("poll pages=%d fetched=%d", totals["pages"], totals["fetched"])
    except Exception:
        conn.rollback()
        finish_run(conn, run_id, "error", totals)
        raise
    wm_out, _ = get_watermark(conn)
    finish_run(conn, run_id, "ok", totals, wm_out)
    return totals


def refetch(cfg, conn, start, end, client=None) -> dict:
    """Re-read an inspection date range from the source. Used by the drift drill, where a new column has
    to be filled in for rows the watermark has already passed."""
    cols = columns(cfg.extra_columns)
    ensure_columns(conn, cols)
    client = client or SodaClient(cfg.soda_url, cfg.page_size, cfg.page_sleep)
    run_id = start_run(conn, "refetch", None, f"{start}..{end}")
    totals = _totals()
    for page in client.pages_by_id(base_where(cfg, start, end), select_list(cols)):
        _load_page(conn, page, cols, cfg.boro, totals)
        conn.commit()
    finish_run(conn, run_id, "ok", totals)
    return totals


def request_snapshot(conn, start: str, end: str) -> str:
    """Ask Debezium for an incremental snapshot of one inspection date range (the backfill)."""
    import uuid

    signal_id = f"backfill-{start}-{end}-{uuid.uuid4().hex[:8]}"
    data = {
        "data-collections": ["mirror.violations"],
        "type": "incremental",
        "additional-conditions": [{
            "data-collection": "mirror.violations",
            "filter": f"inspectiondate >= '{start}' AND inspectiondate < '{end}'",
        }],
    }
    conn.execute("INSERT INTO mirror.debezium_signal (id, type, data) VALUES (%s, 'execute-snapshot', %s)",
                 (signal_id, json.dumps(data)))
    conn.commit()
    return signal_id
