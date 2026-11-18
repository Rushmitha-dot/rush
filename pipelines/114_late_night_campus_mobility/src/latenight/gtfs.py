"""Reference data from the MBTA static GTFS feed, written as dbt seeds.

* gtfs_night_speeds.csv: scheduled bus speed near campus by night type and
  hour, plus a detour factor (bus path length over straight-line distance).
* mbta_night_stops.csv: bus, subway and light rail stops near campus with how many buses leave them
  between 10 p.m. and 3 a.m. on a reference weeknight and weekend night.

The derived CSVs are committed so the warehouse is pinned to one feed version
instead of shifting whenever the MBTA publishes. `make gtfs` refreshes them
and the git diff shows what changed.
"""

import csv
import json
import logging
import shutil
import tempfile
import urllib.request
import zipfile
from datetime import datetime
from pathlib import Path

import duckdb

from .settings import SEEDS_DIR, load_settings

log = logging.getLogger(__name__)

GTFS_URL = "https://cdn.mbta.com/MBTA_GTFS.zip"

# box around the campus area, used to pick bus segments and stops
BOX = {"lat_min": 42.325, "lat_max": 42.355, "lon_min": -71.11, "lon_max": -71.065}
NIGHT_HOURS = [18, 19, 20, 21, 22, 23, 0, 1, 2, 3]
MIN_SEGMENTS = 50

NEEDED = ["stops.txt", "stop_times.txt", "trips.txt", "routes.txt", "calendar.txt", "calendar_dates.txt",
          "feed_info.txt"]

# the MBTA files have quoted fields with commas (trip headsigns) and DuckDB's
# sniffer picked "no quote" on trips.txt, so say it explicitly
CSV_OPTS = "all_varchar=true, header=true, quote='\"', escape='\"'"

HAVERSINE = """6371000 * 2 * asin(sqrt(
    pow(sin(radians({lat2} - {lat1}) / 2), 2)
    + cos(radians({lat1})) * cos(radians({lat2})) * pow(sin(radians({lon2} - {lon1}) / 2), 2)))"""


def download(gtfs_dir: Path) -> Path:
    """Fetch the feed only when Last-Modified changed since the last download."""
    gtfs_dir.mkdir(parents=True, exist_ok=True)
    zip_path = gtfs_dir / "MBTA_GTFS.zip"
    ledger = gtfs_dir / "download.json"
    head = urllib.request.urlopen(urllib.request.Request(GTFS_URL, method="HEAD"), timeout=30)
    last_modified = head.headers.get("Last-Modified")
    cached = json.loads(ledger.read_text()).get("last_modified") if ledger.exists() else None
    if zip_path.exists() and cached == last_modified:
        log.info("GTFS unchanged since %s, using cached zip", last_modified)
        return zip_path
    tmp = zip_path.with_suffix(".part")
    with urllib.request.urlopen(GTFS_URL, timeout=120) as r, open(tmp, "wb") as f:
        shutil.copyfileobj(r, f)
    tmp.replace(zip_path)
    ledger.write_text(json.dumps({"last_modified": last_modified, "downloaded_at": datetime.now().isoformat()}))
    return zip_path


def derive(zip_path: Path, out_dir: Path, segments_path: Path | None = None) -> dict:
    """Write gtfs_night_speeds.csv and mbta_night_stops.csv into out_dir."""
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(zip_path) as z:
            for name in NEEDED:
                z.extract(name, tmp)
        con = duckdb.connect()
        _stage(con, Path(tmp))
        week = con.execute("select min(d) from service_days").fetchone()[0]
        feed_version = con.execute(
            f"select feed_version from read_csv('{tmp}/feed_info.txt', {CSV_OPTS})").fetchone()[0]
        stops = _night_stops(con)
        speeds, detour = _night_speeds(con)
        if segments_path is not None:
            segments_path.parent.mkdir(parents=True, exist_ok=True)
            con.execute(f"copy (select trip_id, stop_id, next_stop_id, seq, secs, m from seg) "
                        f"to '{segments_path}' (format parquet)")
        con.close()

    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "mbta_night_stops.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["stop_id", "stop_name", "lat", "lon", "weeknight_departures", "weekend_departures", "feed_version"])
        for r in stops:
            w.writerow([*r, feed_version])
    with open(out_dir / "gtfs_night_speeds.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["night_type", "hour", "segments", "routes", "bus_kmh", "filled_from_hour", "detour_factor",
                    "feed_version"])
        for r in speeds:
            w.writerow([*r, detour, feed_version])
    return {"feed_version": feed_version, "reference_week": str(week), "stops": len(stops),
            "speed_rows": len(speeds), "detour_factor": detour}


def _stage(con, tmp: Path) -> None:
    con.execute(f"""
        create table stops as
        select stop_id, stop_name, cast(stop_lat as double) as lat, cast(stop_lon as double) as lon
        from read_csv('{tmp}/stops.txt', {CSV_OPTS}) where stop_lat <> ''""")
    # every date in the feed's range with the services running that day
    con.execute(f"""
        create table active_services as
        with info as (select strptime(feed_start_date, '%Y%m%d')::date as start_date,
                             strptime(feed_end_date, '%Y%m%d')::date as end_date
                      from read_csv('{tmp}/feed_info.txt', {CSV_OPTS})),
        days as (select unnest(range(start_date, end_date + 1, interval 1 day))::date as d from info),
        cal as (select * from read_csv('{tmp}/calendar.txt', {CSV_OPTS})),
        base as (
            select days.d, cal.service_id from days join cal
              on days.d between strptime(cal.start_date, '%Y%m%d')::date and strptime(cal.end_date, '%Y%m%d')::date
             and case dayofweek(days.d) when 1 then cal.monday when 2 then cal.tuesday when 3 then cal.wednesday
                   when 4 then cal.thursday when 5 then cal.friday when 6 then cal.saturday else cal.sunday end = '1'),
        ex as (select strptime(date, '%Y%m%d')::date as d, service_id, exception_type
               from read_csv('{tmp}/calendar_dates.txt', {CSV_OPTS}))
        select d, service_id from base
        where (d, service_id) not in (select d, service_id from ex where exception_type = '2')
        union
        select ex.d, ex.service_id from ex join days using (d) where exception_type = '1'""")
    con.execute(f"""
        create table all_trips as
        select t.trip_id, t.route_id, t.service_id, r.route_type
        from read_csv('{tmp}/trips.txt', {CSV_OPTS}) t
        join read_csv('{tmp}/routes.txt', {CSV_OPTS}) r using (route_id)
        where r.route_type in ('0', '1', '3')""")
    # The first week of the Fall 2026 feed had the Green Line E branch replaced
    # by shuttle buses, which hid the Northeastern stop. Use the full Monday to
    # Sunday week with the most subway and light rail trips instead.
    con.execute("""
        create table service_days as
        with rail as (
            select a.d, count(*) as trips from active_services a
            join all_trips t using (service_id) where t.route_type in ('0', '1') group by a.d),
        weeks as (
            select d as monday, (select sum(trips) from rail r2 where r2.d between r.d and r.d + 6) as trips,
                   (select count(*) from rail r2 where r2.d between r.d and r.d + 6) as days
            from rail r where dayofweek(d) = 1),
        pick as (select monday from weeks where days = 7 order by trips desc limit 1)
        select a.d, a.service_id from active_services a, pick where a.d between pick.monday and pick.monday + 6""")
    con.execute("""
        create table transit_trips as
        select t.trip_id, t.route_id, t.route_type, s.d as service_day
        from all_trips t join service_days s using (service_id)""")
    in_box = "{p}lat between {lat_min} and {lat_max} and {p}lon between {lon_min} and {lon_max}"
    con.execute(f"""
        create table seg as
        with st as (
            select trip_id, stop_id, cast(stop_sequence as int) as seq,
                   cast(split_part(departure_time, ':', 1) as int) * 3600
                   + cast(split_part(departure_time, ':', 2) as int) * 60
                   + cast(split_part(departure_time, ':', 3) as int) as dep_s
            from read_csv('{tmp}/stop_times.txt', {CSV_OPTS})
            where trip_id in (select trip_id from transit_trips)),
        x as (
            select st.*, s.lat, s.lon,
                   lead(st.stop_id) over w as next_stop_id, lead(dep_s) over w as next_dep_s,
                   lead(s.lat) over w as nlat, lead(s.lon) over w as nlon
            from st join stops s using (stop_id)
            window w as (partition by trip_id order by seq))
        select x.trip_id, x.stop_id, x.next_stop_id, x.seq, b.route_id, b.route_type, b.service_day,
               next_dep_s - dep_s as secs,
               {HAVERSINE.format(lat1='lat', lon1='lon', lat2='nlat', lon2='nlon')} as m,
               -- GTFS times run past 24:00 for trips after midnight
               b.service_day + to_seconds(dep_s) as departs_at,
               lat as lat1, lon as lon1, nlat as lat2, nlon as lon2
        from x join transit_trips b using (trip_id)
        where next_stop_id is not null
          and {in_box.format(p='', **BOX)} and {in_box.format(p='n', **BOX)}""")


def _night_stops(con) -> list[tuple]:
    # departures from each stop between 10 p.m. and 3 a.m., per night type, over the reference week
    return con.execute("""
        with d as (
            select stop_id, (departs_at - interval 4 hour)::date as night, hour(departs_at) as hr
            from seg)
        select s.stop_id, s.stop_name, round(s.lat, 6), round(s.lon, 6),
               count(*) filter (where dayofweek(night) not in (5, 6)),
               count(*) filter (where dayofweek(night) in (5, 6))
        from d join stops s using (stop_id)
        where hr in (22, 23, 0, 1, 2)
        group by all
        order by 1""").fetchall()


def _night_speeds(con) -> tuple[list[tuple], float]:
    # a night runs 4 a.m. to 4 a.m., so 1 a.m. Saturday belongs to Friday night
    rows = con.execute("""
        with s as (
            select *, hour(departs_at) as hr, (departs_at - interval 4 hour)::date as night
            from seg where secs >= 0 and route_type = '3')
        select case when dayofweek(night) in (5, 6) then 'weekend' else 'weeknight' end as night_type,
               hr, count(*) as segments, count(distinct route_id) as routes,
               sum(m) / nullif(sum(secs), 0) * 3.6 as kmh
        from s group by all""").fetchall()
    by_key = {(r[0], r[1]): r for r in rows}
    out = []
    for night_type in ("weeknight", "weekend"):
        for hr in NIGHT_HOURS:
            r = by_key.get((night_type, hr))
            filled = ""
            if r is None or r[2] < MIN_SEGMENTS:
                # very late hours have only a few owl trips, borrow the closest busy hour
                donor = next(h for h in reversed(NIGHT_HOURS[: NIGHT_HOURS.index(hr)])
                             if by_key.get((night_type, h), (0, 0, 0))[2] >= MIN_SEGMENTS)
                filled = donor
                r = (night_type, hr, r[2] if r else 0, r[3] if r else 0, by_key[(night_type, donor)][4])
            out.append((night_type, hr, r[2], r[3], round(r[4], 2), filled))

    detour = con.execute(f"""
        with t as (
            select trip_id, sum(m) as path_m,
                   {HAVERSINE.format(lat1='arg_min(lat1, seq)', lon1='arg_min(lon1, seq)',
                                     lat2='arg_max(lat2, seq)', lon2='arg_max(lon2, seq)')} as straight_m
            from seg where route_type = '3' group by trip_id)
        select median(path_m / straight_m) from t where straight_m > 700""").fetchone()[0]
    return out, round(detour, 3)


def refresh() -> dict:
    cfg = load_settings()
    zip_path = download(cfg.gtfs_dir)
    return derive(zip_path, SEEDS_DIR, segments_path=cfg.gtfs_dir / "campus_segments.parquet")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(json.dumps(refresh(), indent=2))
