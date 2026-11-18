"""Current station list from the Bluebikes GBFS feed, for dock counts.

The trip files only carry station ids and names, so capacity comes from here.
GBFS short_name is the same code the trip files use as station_id. It's a
snapshot of today's stations, so a station that closed before today has no
capacity row; the marts treat capacity as unknown for those.
"""

import json
import urllib.request
from datetime import UTC, datetime

GBFS_STATIONS = "https://gbfs.lyft.com/gbfs/1.1/bos/en/station_information.json"

DDL = """
create schema if not exists raw;
create table if not exists raw.bluebikes_stations (
    short_name varchar,
    name varchar,
    lat double,
    lon double,
    capacity integer,
    feed_updated_at timestamptz,
    fetched_at timestamptz
);
"""


def refresh(con, timeout: int = 30) -> int:
    payload = json.loads(urllib.request.urlopen(GBFS_STATIONS, timeout=timeout).read())
    updated = datetime.fromtimestamp(payload["last_updated"], tz=UTC)
    rows = [(s.get("short_name"), s["name"], s["lat"], s["lon"], s.get("capacity"), updated)
            for s in payload["data"]["stations"]]
    con.execute(DDL)
    con.execute("begin transaction")
    con.execute("delete from raw.bluebikes_stations")
    con.executemany("insert into raw.bluebikes_stations values (?, ?, ?, ?, ?, ?, current_timestamp)", rows)
    con.execute("commit")
    return len(rows)
