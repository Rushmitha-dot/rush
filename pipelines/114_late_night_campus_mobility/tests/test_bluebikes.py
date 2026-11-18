import csv
import io
from datetime import date

import duckdb

from latenight import bluebikes
from latenight.settings import SEEDS_DIR

from .conftest import CHARLES, FORSYTH, HARVARD, JERSEY, trip


def test_month_file_names_seen_in_the_bucket():
    names = {
        "202510-bluebikes-tripdata.zip": "202510",
        "202511-bluebikes-tripdata.csv.zip": "202511",
        "202609-bluebikes-tripdata.csv": "202609",
    }
    for key, month in names.items():
        assert bluebikes.MONTH_FILE.match(key).group(1) == month
    for key in ["index.html", "hubway_Trips_2014_1.csv", "Hubway_Stations_2011_2016.csv"]:
        assert bluebikes.MONTH_FILE.match(key) is None


def test_last_night_of_a_month_needs_the_next_file():
    assert bluebikes.months_for_nights(date(2025, 10, 15), date(2025, 10, 15)) == ["202510"]
    assert bluebikes.months_for_nights(date(2025, 10, 31), date(2025, 10, 31)) == ["202510", "202511"]
    assert bluebikes.months_for_nights(date(2025, 9, 2), date(2025, 12, 19)) == [
        "202509", "202510", "202511", "202512"]


def test_anchors_match_the_dbt_seed():
    with open(SEEDS_DIR / "campus_anchors.csv") as f:
        seed = {r["cluster"]: (float(r["lat"]), float(r["lon"])) for r in csv.DictReader(f)}
    assert seed == bluebikes.ANCHORS


def test_convert_keeps_trips_that_touch_campus(tmp_path):
    from .conftest import HEADER
    rows = [
        trip("in", "2025-10-03 22:10:00.000", "2025-10-03 22:20:00.000", FORSYTH, JERSEY),
        trip("out", "2025-10-03 22:10:00.000", "2025-10-03 22:40:00.000", FORSYTH, HARVARD),
        trip("arrive", "2025-10-03 23:10:00.000", "2025-10-03 23:40:00.000", HARVARD, JERSEY),
        trip("far", "2025-10-03 23:10:00.000", "2025-10-03 23:40:00.000", HARVARD, CHARLES),
        trip("lost", "2025-10-03 23:10:00.000", "2025-10-03 23:40:00.000", HARVARD, None, bike="electric_bike"),
    ]
    src = tmp_path / "m.csv"
    with open(src, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        w.writerows(rows)
    total, kept = bluebikes.convert_csv(src, tmp_path / "out.parquet")
    ids = {r[0] for r in duckdb.sql(f"select ride_id from '{tmp_path / 'out.parquet'}'").fetchall()}
    assert (total, kept) == (5, 3)
    assert ids == {"in", "out", "arrive"}


S3_PAGE = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<IsTruncated>{truncated}</IsTruncated>{token}
<Contents><Key>{key}</Key><LastModified>2025-11-04T17:56:10.000Z</LastModified>
<ETag>&quot;abc-2&quot;</ETag><Size>21420071</Size></Contents>
<Contents><Key>index.html</Key><LastModified>2016-05-26T13:27:02.000Z</LastModified>
<ETag>&quot;x&quot;</ETag><Size>6430</Size></Contents>
</ListBucketResult>"""


def test_listing_follows_pagination(monkeypatch):
    pages = [
        S3_PAGE.format(truncated="true", token="<NextContinuationToken>t1</NextContinuationToken>",
                       key="202510-bluebikes-tripdata.zip"),
        S3_PAGE.format(truncated="false", token="", key="202609-bluebikes-tripdata.csv"),
    ]
    urls = []

    def fake_urlopen(url, timeout=None):
        urls.append(url)
        return io.BytesIO(pages[len(urls) - 1].encode())

    monkeypatch.setattr(bluebikes.urllib.request, "urlopen", fake_urlopen)
    files = bluebikes.list_month_files()
    assert sorted(files) == ["202510", "202609"]
    assert files["202510"].etag == "abc-2"
    assert "continuation-token=t1" in urls[1]
