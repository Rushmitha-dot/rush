"""Runs the dbt project against a small warehouse built from hand-made trips.

The main check is parity: building night by night through the incremental
models has to give exactly what one full refresh gives.
"""

import os
import subprocess
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path

import duckdb
import pytest

from latenight import stations
from latenight.calendar import load_calendar
from latenight.load import connect, load_night, nights_between
from latenight.settings import DBT_DIR

from .conftest import FORSYTH, HARVARD, JERSEY, LONGWOOD, trip, write_month

pytestmark = pytest.mark.dbt
DBT = str(Path(sys.executable).parent / "dbt")
FIRST, LAST = date(2025, 10, 31), date(2025, 11, 2)


def rows_for(night: date, n: int) -> list[dict]:
    pairs = [(FORSYTH, JERSEY), (JERSEY, LONGWOOD), (LONGWOOD, HARVARD), (HARVARD, FORSYTH), (JERSEY, None)]
    start0 = datetime.combine(night, time(22, 0))
    out = []
    for i in range(n):
        a, b = pairs[i % len(pairs)]
        start = start0 + timedelta(minutes=13 * i)  # spreads the trips from 10 p.m. to about 2:30 a.m.
        end = start + timedelta(minutes=6 + i % 9)
        out.append(trip(f"{night}-{i}", f"{start:%Y-%m-%d %H:%M:%S}.000", f"{end:%Y-%m-%d %H:%M:%S}.000", a, b,
                        bike="electric_bike" if i % 4 == 0 else "classic_bike"))
    return out


def build_warehouse(tmp: Path) -> Path:
    path = tmp / "w.duckdb"
    con = connect(path)
    by_month: dict[str, list] = {}
    for night in nights_between(FIRST, LAST):
        for r in rows_for(night, 14 + night.day % 7):
            by_month.setdefault(r["ended_at"][:7].replace("-", ""), []).append(r)
    by_month["202511"].append(trip("dst", "2025-11-02 01:58:52.458", "2025-11-02 01:06:08.888", FORSYTH, LONGWOOD))
    for month, rows in by_month.items():
        write_month(tmp / "bluebikes", con, month, rows)
    con.execute(stations.DDL)
    con.execute("insert into raw.bluebikes_stations values ('B32049', 'Forsyth St at Huntington Ave', 42.339202, "
                "-71.090511, 15, now(), now())")
    cal = load_calendar()
    for night in nights_between(FIRST, LAST):
        assert load_night(con, night, cal).status.startswith("loaded")
    con.close()
    return path


def dbt(path: Path, tmp: Path, *args: str) -> None:
    env = {**os.environ, "LATENIGHT_WAREHOUSE": str(path), "DBT_PROFILES_DIR": str(DBT_DIR)}
    cmd = [DBT, *args, "--project-dir", str(DBT_DIR), "--target-path", str(tmp / "target"),
           "--log-path", str(tmp / "logs")]
    res = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert res.returncode == 0, res.stdout[-3000:]


def fact(path: Path) -> list[tuple]:
    con = duckdb.connect(str(path), read_only=True)
    rows = con.execute("select * from marts.fct_late_night_trips order by ride_id").fetchall()
    hourly = con.execute("select * from marts.mart_hourly_demand order by night, start_hour").fetchall()
    con.close()
    return rows + hourly


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("dbt")
    path = build_warehouse(tmp)
    dbt(path, tmp, "seed", "-q")
    return tmp, path


def test_incremental_equals_full_refresh(built, tmp_path):
    tmp, path = built
    nightly = tmp_path / "nightly.duckdb"
    full = tmp_path / "full.duckdb"
    for p in (nightly, full):
        p.write_bytes(path.read_bytes())

    for night in nights_between(FIRST, LAST):
        dbt(nightly, tmp_path, "build", "--exclude", "resource_type:seed", "--vars",
            f'{{"first_night": "{night}", "last_night": "{night}"}}')
    # and rerun one night, which must not change anything
    dbt(nightly, tmp_path, "build", "--exclude", "resource_type:seed", "--vars",
        f'{{"first_night": "{FIRST}", "last_night": "{FIRST}"}}')
    dbt(full, tmp_path, "build", "--exclude", "resource_type:seed", "--full-refresh")

    a, b = fact(nightly), fact(full)
    assert len(a) > 50
    assert a == b


def test_fall_back_trip_gets_its_hour_back(built, tmp_path):
    tmp, path = built
    dbt(path, tmp, "build", "--exclude", "resource_type:seed")
    con = duckdb.connect(str(path), read_only=True)
    minutes, shifted = con.execute("""
        select duration_min, ended_at_shifted_for_dst
        from marts.fct_late_night_trips where ride_id = 'dst'""").fetchone()
    con.close()
    assert shifted and 7 < minutes < 8
