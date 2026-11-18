"""Load one night of late-night campus trips into raw.bluebikes_trips.

A night is 10 p.m. to 3 a.m. by trip start time, so night D needs the month
file for D and, near a month end, the next month's file as well.

* Rows that fail a rule go to raw.bluebikes_trips_quarantine with a reason.
* ERROR checks run before writing; if one fails, the night isn't touched.
* A ledger keyed on the source files' ETags makes a rerun a no-op until a
  source file changes. A changed file replaces the night in one transaction.
"""

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

from .bluebikes import months_for_nights
from .checks import CheckResult, after_load_checks, before_load_checks

log = logging.getLogger(__name__)

NIGHT_HOURS = (22, 23, 0, 1, 2)

DDL = """
create schema if not exists raw;
create schema if not exists ops;
create table if not exists raw.bluebikes_trips (
    night date not null,
    ride_id varchar not null,
    rideable_type varchar not null,
    started_at timestamp not null,
    ended_at timestamp not null,
    start_station_id varchar,
    start_station_name varchar,
    end_station_id varchar,
    end_station_name varchar,
    start_lat double,
    start_lng double,
    end_lat double,
    end_lng double,
    member_casual varchar,
    source_month varchar not null,
    loaded_at timestamptz not null
);
create table if not exists raw.bluebikes_trips_quarantine (
    night date not null,
    reason varchar not null,
    ride_id varchar,
    started_at varchar,
    ended_at varchar,
    source_month varchar,
    loaded_at timestamptz not null
);
create table if not exists ops.load_ledger (
    night date primary key,
    source_etags varchar not null,
    trips integer not null,
    quarantined integer not null,
    status varchar not null,
    loaded_at timestamptz not null
);
create table if not exists ops.check_results (
    run_id varchar,
    night date,
    check_name varchar,
    severity varchar,
    passed boolean,
    value double,
    detail varchar,
    checked_at timestamptz
);
"""

REJECT_REASON = """
    case
        when ride_id is null or ride_id = '' then 'missing ride_id'
        when try_cast(started_at as timestamp) is null then 'bad started_at'
        when try_cast(ended_at as timestamp) is null then 'bad ended_at'
        when try_cast(ended_at as timestamp) < try_cast(started_at as timestamp) then 'ended before started'
        when try_cast(ended_at as timestamp) - try_cast(started_at as timestamp) > interval 24 hour
            then 'longer than 24 hours'
        when try_cast(start_lat as double) is null or try_cast(start_lng as double) is null
            then 'missing start coordinates'
        when rideable_type not in ('classic_bike', 'electric_bike') then 'unknown rideable_type'
    end"""


@dataclass
class LoadResult:
    night: date
    status: str  # loaded, loaded_with_warnings, unchanged, blocked
    trips: int = 0
    quarantined: int = 0
    checks: list[CheckResult] = field(default_factory=list)


def connect(path: Path) -> duckdb.DuckDBPyConnection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute(DDL)
    return con


def load_night(con, night: date, calendar: dict, run_id: str = "cli") -> LoadResult:
    months = months_for_nights(night, night)
    files = {m: con.execute("select etag, parquet_path from ops.source_files where month = ?", [m]).fetchone()
             for m in months}
    available = {m: f for m, f in files.items() if f and Path(f[1]).exists()}
    etags = hashlib.md5("|".join(f"{m}:{f[0]}" for m, f in sorted(available.items())).encode()).hexdigest()

    seen = con.execute("select source_etags, status from ops.load_ledger where night = ?", [night]).fetchone()
    if len(available) == len(months) and seen and seen[0] == etags and seen[1].startswith("loaded"):
        log.info("night %s unchanged, nothing to do", night)
        return LoadResult(night, "unchanged")

    if available:
        sources = ", ".join(f"'{f[1]}'" for f in available.values())
        con.execute(f"""
            create or replace temp table incoming as
            with t as (
                select *, regexp_extract(filename, 'month=(\\d{{6}})', 1) as source_month
                from read_parquet([{sources}], filename = true, union_by_name = true)),
            tonight as (
                select * from t
                where hour(try_cast(started_at as timestamp)) in {NIGHT_HOURS}
                  and cast(try_cast(started_at as timestamp) - interval 4 hour as date) = ?)
            select *, {REJECT_REASON} as reject_reason,
                   row_number() over (partition by ride_id order by source_month) > 1 as is_repeat
            from tonight""", [night])
        total, rejected, repeats, trips = con.execute("""
            select count(*), count(reject_reason), count(*) filter (where is_repeat),
                   count(*) filter (where reject_reason is null and not is_repeat)
            from incoming""").fetchone()
    else:
        total = rejected = repeats = trips = 0

    result = LoadResult(night, "blocked", trips, rejected)
    result.checks = before_load_checks(months, available, total, rejected)
    if any(c.blocks for c in result.checks):
        _record_checks(con, run_id, night, result.checks)
        return result

    now = datetime.now().astimezone()
    con.execute("begin transaction")
    try:
        con.execute("delete from raw.bluebikes_trips where night = ?", [night])
        con.execute("delete from raw.bluebikes_trips_quarantine where night = ?", [night])
        con.execute("""
            insert into raw.bluebikes_trips
            select ?, ride_id, rideable_type, started_at::timestamp, ended_at::timestamp,
                   nullif(start_station_id, ''), nullif(start_station_name, ''),
                   nullif(end_station_id, ''), nullif(end_station_name, ''),
                   try_cast(start_lat as double), try_cast(start_lng as double),
                   try_cast(end_lat as double), try_cast(end_lng as double),
                   member_casual, source_month, ?
            from incoming where reject_reason is null and not is_repeat""", [night, now])
        con.execute("""
            insert into raw.bluebikes_trips_quarantine
            select ?, reject_reason, ride_id, started_at, ended_at, source_month, ?
            from incoming where reject_reason is not null""", [night, now])
        con.execute("commit")
    except Exception:
        con.execute("rollback")
        raise

    result.checks += after_load_checks(con, night, trips, repeats, calendar)
    result.status = "loaded_with_warnings" if any(not c.passed for c in result.checks) else "loaded"
    con.execute("insert or replace into ops.load_ledger values (?, ?, ?, ?, ?, ?)",
                [night, etags, trips, rejected, result.status, now])
    _record_checks(con, run_id, night, result.checks)
    return result


def _record_checks(con, run_id: str, night: date, checks: list[CheckResult]) -> None:
    now = datetime.now().astimezone()
    for c in checks:
        con.execute("insert into ops.check_results values (?, ?, ?, ?, ?, ?, ?, ?)",
                    [run_id, night, c.name, c.severity, c.passed, c.value, c.detail, now])


def nights_between(first: date, last: date) -> list[date]:
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]
