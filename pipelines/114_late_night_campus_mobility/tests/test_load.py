from datetime import date

from latenight.calendar import load_calendar
from latenight.load import load_night

from .conftest import FORSYTH, HARVARD, JERSEY, LONGWOOD, trip, write_month

CAL = load_calendar()


def night_rows():
    return [
        trip("a", "2025-10-17 22:05:00.000", "2025-10-17 22:15:00.000", FORSYTH, JERSEY),
        trip("b", "2025-10-18 01:30:00.000", "2025-10-18 01:45:00.000", JERSEY, LONGWOOD, bike="electric_bike"),
        # starts before 10 p.m. and at 3 a.m., not part of the night
        trip("early", "2025-10-17 21:59:00.000", "2025-10-17 22:10:00.000", FORSYTH, JERSEY),
        trip("late", "2025-10-18 03:00:00.000", "2025-10-18 03:10:00.000", FORSYTH, JERSEY),
        # quarantined
        trip("backwards", "2025-10-17 23:00:00.000", "2025-10-17 22:50:00.000", FORSYTH, JERSEY),
        trip("forever", "2025-10-17 23:00:00.000", "2025-10-19 23:50:00.000", FORSYTH, HARVARD),
        trip("", "2025-10-17 23:10:00.000", "2025-10-17 23:20:00.000", FORSYTH, JERSEY),
    ] + [trip(f"n{i}", "2025-10-17 23:30:00.000", "2025-10-17 23:40:00.000", FORSYTH, JERSEY) for i in range(200)]


def test_night_window_and_quarantine(tmp_path, warehouse):
    write_month(tmp_path, warehouse, "202510", night_rows())
    r = load_night(warehouse, date(2025, 10, 17), CAL)
    assert r.status.startswith("loaded")
    assert r.trips == 202
    ids = {x[0] for x in warehouse.execute("select ride_id from raw.bluebikes_trips").fetchall()}
    assert {"a", "b"} <= ids and not ({"early", "late"} & ids)
    reasons = dict(warehouse.execute(
        "select reason, count(*) from raw.bluebikes_trips_quarantine group by 1").fetchall())
    assert reasons == {"ended before started": 1, "longer than 24 hours": 1, "missing ride_id": 1}


def test_rerun_is_a_no_op_and_a_new_file_replaces_the_night(tmp_path, warehouse):
    write_month(tmp_path, warehouse, "202510", night_rows())
    load_night(warehouse, date(2025, 10, 17), CAL)
    assert load_night(warehouse, date(2025, 10, 17), CAL).status == "unchanged"

    fewer = night_rows()[:2] + night_rows()[7:107]
    write_month(tmp_path, warehouse, "202510", fewer, etag="etag-2")
    r = load_night(warehouse, date(2025, 10, 17), CAL)
    assert r.status.startswith("loaded")
    assert warehouse.execute("select count(*) from raw.bluebikes_trips").fetchone()[0] == 102
    assert warehouse.execute("select count(*) from raw.bluebikes_trips_quarantine").fetchone()[0] == 0


def test_month_end_night_waits_for_next_file(tmp_path, warehouse):
    rows = [trip(f"x{i}", "2025-10-31 22:30:00.000", "2025-10-31 22:40:00.000", FORSYTH, JERSEY) for i in range(5)]
    write_month(tmp_path, warehouse, "202510", rows)
    r = load_night(warehouse, date(2025, 10, 31), CAL)
    assert r.status == "blocked"
    assert [c.name for c in r.checks if c.blocks] == ["source_months_present"]
    assert warehouse.execute("select count(*) from raw.bluebikes_trips").fetchone()[0] == 0

    # trips are filed by end time, so the after-midnight ones are in November's file
    write_month(tmp_path, warehouse, "202511",
                [trip("y", "2025-10-31 23:55:00.000", "2025-11-01 00:10:00.000", FORSYTH, JERSEY)])
    r = load_night(warehouse, date(2025, 10, 31), CAL)
    assert r.trips == 6


def test_fall_back_trips_are_kept(tmp_path, warehouse):
    rows = [trip(f"d{i}", "2025-11-01 23:00:00.000", "2025-11-01 23:10:00.000", FORSYTH, JERSEY) for i in range(50)]
    # starts in the first 1 a.m. (EDT), ends in the second one (EST)
    rows.append(trip("dst", "2025-11-02 01:58:52.458", "2025-11-02 01:06:08.888", FORSYTH, LONGWOOD))
    write_month(tmp_path, warehouse, "202511", rows)
    r = load_night(warehouse, date(2025, 11, 1), CAL)
    assert r.quarantined == 0
    assert warehouse.execute("select count(*) from raw.bluebikes_trips where ride_id = 'dst'").fetchone()[0] == 1


def test_too_many_bad_rows_block_the_night(tmp_path, warehouse):
    rows = night_rows()[:2] + [
        trip(f"bad{i}", "2025-10-17 23:00:00.000", "2025-10-17 22:00:00.000", FORSYTH, JERSEY) for i in range(5)]
    write_month(tmp_path, warehouse, "202510", rows)
    r = load_night(warehouse, date(2025, 10, 17), CAL)
    assert r.status == "blocked"
    assert [c.name for c in r.checks if c.blocks] == ["quarantine_share"]
