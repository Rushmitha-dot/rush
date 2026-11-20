from datetime import date

from conftest import change

from violcdc.history import advance, first_image


def test_first_image_of_a_closed_violation_has_a_derived_open_stretch():
    ev = change(status="Close", status_id=19, status_text="VIOLATION CLOSED", issued=date(2026, 4, 3),
                status_date=date(2026, 6, 10))
    rows = first_image(ev)
    assert [(r["violationstatus"], r["valid_from"], r["valid_to"], r["derived"]) for r in rows] == [
        ("Open", date(2026, 4, 3), date(2026, 6, 10), True),
        ("Close", date(2026, 6, 10), None, False),
    ]


def test_status_on_its_issue_day_is_one_row():
    rows = first_image(change(issued=date(2026, 5, 2), status_date=date(2026, 5, 2)))
    assert len(rows) == 1 and rows[0]["is_current"]


def test_advance_is_idempotent():
    events = [change(lsn=100), change(lsn=200, status_id=4, status_text="NOT COMPLIED WITH",
                                      status_date=date(2026, 6, 1))]
    once = advance([], events)
    assert advance(once, events) == once
    assert len(once) == 2


def test_status_date_older_than_open_row_falls_back_to_commit_day():
    rows = advance([], [change(lsn=100, status_date=date(2026, 6, 1))])
    later = change(lsn=200, status_id=4, status_text="NOT COMPLIED WITH", status_date=date(2026, 5, 1))
    rows = advance(rows, [later])
    assert rows[-1]["valid_from"] == later["commit_ts"].date()
    assert rows[-2]["valid_to"] == rows[-1]["valid_from"]


def test_reopen_after_close():
    rows = advance([], [
        change(lsn=100),
        change(lsn=200, status="Close", status_id=19, status_text="VIOLATION CLOSED", status_date=date(2026, 7, 1)),
        change(lsn=300, status="Open", status_id=4, status_text="NOT COMPLIED WITH", status_date=date(2026, 8, 1)),
    ])
    assert [r["violationstatus"] for r in rows] == ["Open", "Close", "Open"]
    assert sum(r["is_current"] for r in rows) == 1
