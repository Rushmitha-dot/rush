from datetime import date

from latenight.definitions import month_night_ranges


def test_new_month_runs_all_but_its_last_night_until_next_month_lands():
    published = {"202509", "202510"}
    ranges = month_night_ranges(published, done=set(), first_month="202509")
    assert ranges == [("202509", date(2025, 9, 1), date(2025, 9, 30)),
                      ("202510", date(2025, 10, 1), date(2025, 10, 30))]


def test_done_months_are_skipped():
    ranges = month_night_ranges({"202509", "202510", "202511"}, done={"202509"}, first_month="202509")
    assert [m for m, _, _ in ranges] == ["202510", "202511"]
    assert ranges[0][2] == date(2025, 10, 31)
