import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from violcdc.soda import Rejected, SodaClient, columns, soql_ts, to_row

PAGE = json.loads((Path(__file__).parent / "fixtures" / "soda_page.json").read_text())


def test_real_record_to_typed_row():
    row = to_row(PAGE[0], columns(), "BRONX")
    assert row["violationid"] == 18544320 and row["class"] == "C"
    assert row["inspectiondate"] == date(2026, 1, 1)
    assert isinstance(row["latitude"], float)
    assert row["source_updated_at"].tzinfo is not None
    assert "rentimpairing" not in row


def test_extra_column_comes_through_as_text():
    row = to_row(PAGE[0], columns(("rentimpairing",)), "BRONX")
    assert row["rentimpairing"] == PAGE[0]["rentimpairing"]


@pytest.mark.parametrize("change, reason", [
    ({"violationid": None}, "missing violationid"),
    ({"class": "Z"}, "unknown class"),
    ({"violationstatus": "Pending"}, "unknown violationstatus"),
    ({"inspectiondate": "not a date"}, "bad inspectiondate"),
    ({"boro": "QUEENS"}, "outside BRONX"),
    ({"block": "12A"}, "bad block"),
])
def test_rejects_with_a_reason(change, reason):
    rec = {**PAGE[0], **change}
    with pytest.raises(Rejected, match=reason):
        to_row(rec, columns(), "BRONX")


def test_soql_timestamp_is_utc_millis():
    ts = datetime(2026, 11, 20, 16, 3, 26, 91000, tzinfo=UTC)
    assert soql_ts(ts) == "2026-11-20T16:03:26.091"


class FakeSession:
    def __init__(self, pages):
        self.pages, self.calls, self.headers = list(pages), [], {}

    def get(self, url, params, timeout):
        self.calls.append(params)

        class R:
            status_code = 200

            def __init__(self, body):
                self.body = body

            def raise_for_status(self):
                pass

            def json(self):
                return self.body
        return R(self.pages.pop(0) if self.pages else [])


def test_keyset_paging_moves_the_cursor_past_the_last_row():
    a = [{"violationid": "5", ":updated_at": "2026-11-18T16:27:48.285Z"},
         {"violationid": "9", ":updated_at": "2026-11-18T16:27:48.285Z"}]
    b = [{"violationid": "2", ":updated_at": "2026-11-20T16:03:26.091Z"}]
    session = FakeSession([a, b])
    client = SodaClient("http://x", page_size=2, page_sleep=0, session=session)
    pages = list(client.pages_by_updated_at("boro = 'BRONX'", datetime(2026, 11, 14, tzinfo=UTC), 0, "*"))
    assert [len(p) for p in pages] == [2, 1]
    second = session.calls[1]["$where"]
    wm = "2026-11-18T16:27:48.285"
    assert f"(:updated_at > '{wm}' OR (:updated_at = '{wm}' AND violationid > 9))" in second
    assert session.calls[0]["$order"] == ":updated_at,violationid"
