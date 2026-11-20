"""Run log, file ledger and check results in a small SQLite file next to the data.

On a cluster this would be a table in the warehouse. SQLite keeps it local and lets the
ledger survive a crashed Spark session.
"""
import json
import sqlite3
import time
import uuid
from contextlib import contextmanager

SCHEMA = """
create table if not exists ledger (
    step text not null,          -- land | stage | summarize
    year integer not null,
    input_key text not null,     -- what the step read: url + last-modified, or the upstream run ids
    run_id text not null,
    status text not null,        -- ok | blocked | error
    detail text,
    finished_at real not null
);
create table if not exists checks (
    run_id text not null,
    year integer not null,
    name text not null,
    severity text not null,
    passed integer not null,
    value text,
    finished_at real not null
);
create table if not exists runs (
    run_id text primary key,
    step text not null,
    year integer not null,
    status text not null,
    seconds real,
    detail text,
    started_at real not null
);
"""


@contextmanager
def connect(cfg):
    cfg.home.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(cfg.ops_db)
    con.executescript(SCHEMA)
    try:
        yield con
        con.commit()
    finally:
        con.close()


def new_run_id() -> str:
    return time.strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]


def last_ok(cfg, step: str, year: int) -> dict | None:
    with connect(cfg) as con:
        row = con.execute(
            "select input_key, run_id, detail, finished_at from ledger "
            "where step = ? and year = ? and status = 'ok' order by finished_at desc limit 1",
            (step, year)).fetchone()
    if not row:
        return None
    return {"input_key": row[0], "run_id": row[1], "detail": json.loads(row[2] or "{}"), "finished_at": row[3]}


def record(cfg, step: str, year: int, input_key: str, run_id: str, status: str, detail: dict, seconds: float,
           started_at: float) -> None:
    with connect(cfg) as con:
        con.execute("insert into ledger values (?, ?, ?, ?, ?, ?, ?)",
                    (step, year, input_key, run_id, status, json.dumps(detail, default=str), time.time()))
        con.execute("insert or replace into runs values (?, ?, ?, ?, ?, ?, ?)",
                    (run_id, step, year, status, seconds, json.dumps(detail, default=str), started_at))


def record_checks(cfg, run_id: str, year: int, results) -> None:
    now = time.time()
    with connect(cfg) as con:
        con.executemany("insert into checks values (?, ?, ?, ?, ?, ?, ?)",
                        [(run_id, year, r.name, r.severity, int(r.passed), str(r.value), now) for r in results])


def published_years(cfg, step: str) -> list[int]:
    with connect(cfg) as con:
        rows = con.execute("select distinct year from ledger where step = ? and status = 'ok' order by year",
                           (step,)).fetchall()
    return [r[0] for r in rows]


def recent_runs(cfg, limit: int = 20) -> list[tuple]:
    with connect(cfg) as con:
        return con.execute("select run_id, step, year, status, round(seconds, 1), detail from runs "
                           "order by started_at desc limit ?", (limit,)).fetchall()
