from datetime import UTC, datetime

from labelfeed.freshness import deadline, evaluate

D18 = "dm_spl_daily_update_11182026.zip"
D19 = "dm_spl_daily_update_11192026.zip"


def at(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def test_deadline_is_midnight_utc_two_days_after_the_file_date():
    assert deadline(D19) == at("2026-11-21T00:00:00")


def test_fresh_when_everything_offered_is_loaded():
    assert evaluate([D18, D19], [D18, D19], at("2026-11-20T21:00:00")) == []


def test_an_offered_file_inside_its_window_is_not_late_yet():
    assert evaluate([D18], [D18, D19], at("2026-11-20T21:00:00")) == []


def test_an_offered_file_past_its_deadline_fails():
    problems = evaluate([D18], [D18, D19], at("2026-11-21T03:00:00"))
    assert len(problems) == 1 and D19 in problems[0] and "3h past" in problems[0]


def test_silence_fails_even_when_the_listing_could_not_be_read():
    problems = evaluate(["bootstrap_20261101.zip", D19], None, at("2026-11-25T01:00:00"))
    assert len(problems) == 1 and "nothing after it" in problems[0]


def test_bootstrap_alone_is_not_fresh():
    assert evaluate(["bootstrap_20261101.zip"], None, at("2026-11-02T00:00:00")) == [
        "no update file has been loaded yet"]
