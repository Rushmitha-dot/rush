from datetime import date, timedelta

from latenight.calendar import build_calendar, load_calendar
from latenight.checks import _volume_check

CAL = load_calendar()


def seed_ledger(con, nights_and_trips):
    for night, trips in nights_and_trips:
        con.execute("insert into ops.load_ledger values (?, 'x', ?, 0, 'loaded', now())", [night, trips])


def test_volume_compares_like_with_like(warehouse):
    night = date(2025, 10, 17)  # a Friday during classes
    seed_ledger(warehouse, [(night - timedelta(weeks=w), t) for w, t in [(1, 600), (2, 640), (3, 700)]])
    assert _volume_check(warehouse, night, 650, CAL).passed
    low = _volume_check(warehouse, night, 120, CAL)
    assert not low.passed and low.severity == "WARN"


def test_volume_skips_without_a_baseline(warehouse):
    # first night of Thanksgiving break has no earlier break nights to compare with
    r = _volume_check(warehouse, date(2025, 11, 26), 50, CAL)
    assert r.passed and "no comparable" in r.detail


def test_calendar_periods():
    rows = {r["night"]: r for r in build_calendar()}
    assert rows["2025-11-27"]["period"] == "break"
    assert rows["2025-12-15"]["period"] == "finals"
    assert rows["2025-10-17"]["weekend_night"] == "true"
    assert rows["2025-07-04"]["period"] == "no_term"
