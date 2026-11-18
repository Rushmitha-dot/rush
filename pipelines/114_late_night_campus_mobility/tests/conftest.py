import csv
from pathlib import Path

import duckdb
import pytest

from latenight.bluebikes import convert_csv
from latenight.load import connect

HEADER = ["ride_id", "rideable_type", "started_at", "ended_at", "start_station_name", "start_station_id",
          "end_station_name", "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng", "member_casual"]

# real station locations from the trip files
FORSYTH = ("Forsyth St at Huntington Ave", "B32049", 42.339202, -71.090511)
JERSEY = ("Boylston St at Jersey St", "A32017", 42.344496, -71.097029)
LONGWOOD = ("Longwood Ave at Binney St", "A32008", 42.338629, -71.106500)
HARVARD = ("Harvard Square at Mass Ave/ Dunster", "M32018", 42.373268, -71.118579)
CHARLES = ("Beacon St at Charles St", "D32002", 42.356052, -71.069849)


def trip(ride_id, start, end, a, b, bike="classic_bike", member="member"):
    row = {"ride_id": ride_id, "rideable_type": bike, "started_at": start, "ended_at": end,
           "start_station_name": a[0], "start_station_id": a[1], "start_lat": a[2], "start_lng": a[3],
           "member_casual": member}
    if b is None:
        row.update(end_station_name="", end_station_id="", end_lat="", end_lng="")
    else:
        row.update(end_station_name=b[0], end_station_id=b[1], end_lat=b[2], end_lng=b[3])
    return row


def write_month(root: Path, con, month: str, rows: list[dict], etag: str = "etag-1") -> Path:
    """Write a month the way fetch_month leaves it: filtered Parquet plus a ledger row."""
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / f"{month}.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADER, quoting=csv.QUOTE_NONNUMERIC)
        w.writeheader()
        w.writerows(rows)
    parquet = root / f"month={month}" / "trips_near_campus.parquet"
    total, kept = convert_csv(csv_path, parquet)
    con.execute("""
        create table if not exists ops.source_files (month varchar primary key, s3_key varchar, etag varchar,
            last_modified timestamptz, bytes bigint, rows_in_file bigint, rows_kept bigint, parquet_path varchar,
            fetched_at timestamptz)""")
    con.execute("insert or replace into ops.source_files values (?, ?, ?, now(), 0, ?, ?, ?, now())",
                [month, f"{month}-bluebikes-tripdata.zip", etag, total, kept, str(parquet)])
    return parquet


@pytest.fixture
def warehouse(tmp_path) -> duckdb.DuckDBPyConnection:
    con = connect(tmp_path / "w.duckdb")
    yield con
    con.close()
