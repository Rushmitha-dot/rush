import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from violcdc.events import BadEvent, extra_fields, parse

EVENT = json.loads((Path(__file__).parent / "fixtures" / "debezium_create.json").read_text())
NOW = datetime(2026, 11, 20, 21, 30, tzinfo=UTC)


def test_real_create_event():
    row = parse(json.dumps(EVENT).encode(), NOW)
    assert row["op"] == "c" and not row["snap"]
    assert row["lsn"] == EVENT["source"]["lsn"]
    assert isinstance(row["inspectiondate"], date)
    assert row["commit_ts"].tzinfo is not None
    assert row["source_updated_at"].year == 2026


def test_snapshot_read_is_flagged():
    ev = {**EVENT, "op": "r", "source": {**EVENT["source"], "snapshot": "incremental"}}
    assert parse(json.dumps(ev).encode(), NOW)["snap"]


@pytest.mark.parametrize("mutate, reason", [
    (lambda e: e.update(op="t"), "unknown op"),
    (lambda e: e["source"].pop("lsn"), "no lsn"),
    (lambda e: e.update(after=None), "no row image"),
    (lambda e: e["after"].update({"class": "X"}), "unknown class"),
])
def test_bad_events(mutate, reason):
    ev = json.loads(json.dumps(EVENT))
    mutate(ev)
    with pytest.raises(BadEvent, match=reason):
        parse(json.dumps(ev).encode(), NOW)


def test_not_json():
    with pytest.raises(BadEvent, match="not json"):
        parse(b"{oops", NOW)


def test_new_column_is_found():
    ev = json.loads(json.dumps(EVENT))
    ev["after"]["rentimpairing"] = "Y"
    row = parse(json.dumps(ev).encode(), NOW)
    assert set(extra_fields([row])) == {"rentimpairing"}
