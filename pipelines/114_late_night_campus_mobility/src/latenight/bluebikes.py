"""Bluebikes monthly trip files from the public hubway-data bucket.

Things I found in the bucket that this code has to handle:

* Files are named by month but not consistently. Most are
  YYYYMM-bluebikes-tripdata.zip, 202511 is .csv.zip and 202609 is a bare
  .csv, so the month is matched on the prefix and the format on the suffix.
* The zips carry a macOS __MACOSX/._ resource file next to the CSV.
* A month's file holds trips that ENDED in that month. A trip that starts
  at 11:50 p.m. on the 31st and ends after midnight is in the next file, so
  the last night of a month needs two files.

Each month is converted once into a Parquet file holding only trips that
start or end near campus, and the raw download is deleted. A ledger in
DuckDB keyed on the S3 ETag makes a rerun a no-op until the file changes.
"""

import logging
import re
import shutil
import tempfile
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

BUCKET_URL = "https://s3.amazonaws.com/hubway-data"
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
MONTH_FILE = re.compile(r"^(\d{6})-bluebikes-tripdata\.(zip|csv\.zip|csv)$")

# campus anchors; a trip is kept if it starts or ends within RADIUS_M of one
ANCHORS = {
    "northeastern": (42.3398, -71.0892),
    "longwood": (42.3376, -71.1043),
    "fenway": (42.3450, -71.0970),
}
RADIUS_M = 1000

SOURCE_DDL = """
create schema if not exists ops;
create table if not exists ops.source_files (
    month varchar primary key,
    s3_key varchar not null,
    etag varchar not null,
    last_modified timestamptz not null,
    bytes bigint not null,
    rows_in_file bigint,
    rows_kept bigint,
    parquet_path varchar,
    fetched_at timestamptz
);
"""


@dataclass(frozen=True)
class MonthFile:
    month: str  # YYYYMM
    key: str
    etag: str
    last_modified: datetime
    size: int

    @property
    def url(self) -> str:
        return f"{BUCKET_URL}/{self.key}"


def list_month_files(timeout: int = 30) -> dict[str, MonthFile]:
    """Every monthly trip file in the bucket, following pagination."""
    out: dict[str, MonthFile] = {}
    token = None
    while True:
        url = f"{BUCKET_URL}?list-type=2" + (f"&continuation-token={urllib.parse.quote(token)}" if token else "")
        root = ET.fromstring(urllib.request.urlopen(url, timeout=timeout).read())
        for c in root.findall("s3:Contents", S3_NS):
            key = c.find("s3:Key", S3_NS).text
            m = MONTH_FILE.match(key)
            if not m:
                continue
            out[m.group(1)] = MonthFile(
                month=m.group(1),
                key=key,
                etag=c.find("s3:ETag", S3_NS).text.strip('"'),
                last_modified=datetime.fromisoformat(c.find("s3:LastModified", S3_NS).text.replace("Z", "+00:00")),
                size=int(c.find("s3:Size", S3_NS).text),
            )
        if root.findtext("s3:IsTruncated", namespaces=S3_NS) != "true":
            return out
        token = root.findtext("s3:NextContinuationToken", namespaces=S3_NS)


def months_for_nights(first: date, last: date) -> list[str]:
    """A night's trips can end the next morning, so include the month after the last night."""
    days = (last - first).days + 2
    return sorted({(first + timedelta(days=i)).strftime("%Y%m") for i in range(days)})


def _distance_sql(lat: str, lon: str) -> str:
    parts = [
        f"6371000 * 2 * asin(sqrt(pow(sin(radians({lat} - {a})/2), 2) + cos(radians({a})) * cos(radians({lat}))"
        f" * pow(sin(radians({lon} - ({b}))/2), 2)))"
        for a, b in ANCHORS.values()
    ]
    return f"least({', '.join(parts)})"


def near_campus_sql(lat: str, lon: str) -> str:
    return f"({_distance_sql(lat, lon)} <= {RADIUS_M})"


def convert_csv(csv_path: Path, parquet_path: Path) -> tuple[int, int]:
    """Type the CSV and keep trips that touch the campus area. Returns (rows in file, rows kept)."""
    con = duckdb.connect()
    # everything as text first; the loader decides what's valid and quarantines the rest
    con.execute(f"create table t as select * from read_csv('{csv_path}', header = true, all_varchar = true)")
    total = con.execute("select count(*) from t").fetchone()[0]
    near = (f"{near_campus_sql('try_cast(start_lat as double)', 'try_cast(start_lng as double)')} "
            f"or {near_campus_sql('try_cast(end_lat as double)', 'try_cast(end_lng as double)')}")
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = parquet_path.with_suffix(".part")
    con.execute(f"copy (select * from t where {near}) to '{tmp}' (format parquet)")
    kept = con.execute(f"select count(*) from read_parquet('{tmp}')").fetchone()[0]
    con.close()
    tmp.replace(parquet_path)
    return total, kept


def fetch_month(con, mf: MonthFile, out_dir: Path) -> Path:
    """Download and convert one month unless the ledger already has this ETag."""
    con.execute(SOURCE_DDL)
    parquet = out_dir / f"month={mf.month}" / "trips_near_campus.parquet"
    row = con.execute("select etag from ops.source_files where month = ?", [mf.month]).fetchone()
    if row and row[0] == mf.etag and parquet.exists():
        log.info("%s unchanged (etag %s), skipping download", mf.month, mf.etag[:8])
        return parquet

    with tempfile.TemporaryDirectory(dir=out_dir.parent if out_dir.parent.exists() else None) as tmp:
        tmp = Path(tmp)
        download = tmp / Path(mf.key).name
        with urllib.request.urlopen(mf.url, timeout=300) as r, open(download, "wb") as f:
            shutil.copyfileobj(r, f)
        if download.suffix == ".zip":
            with zipfile.ZipFile(download) as z:
                csvs = [n for n in z.namelist() if n.endswith(".csv") and not n.startswith("__MACOSX")]
                if len(csvs) != 1:
                    raise ValueError(f"{mf.key}: expected one CSV in the zip, found {csvs}")
                z.extract(csvs[0], tmp)
                csv_path = tmp / csvs[0]
            download.unlink()
        else:
            csv_path = download
        total, kept = convert_csv(csv_path, parquet)

    con.execute("""
        insert or replace into ops.source_files values (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)""",
                [mf.month, mf.key, mf.etag, mf.last_modified, mf.size, total, kept, str(parquet)])
    log.info("%s: %s rows, %s near campus", mf.month, total, kept)
    return parquet
