from datetime import date

from conftest import change

from violcdc.merge import apply_batch


def snapshot_ids(lake):
    return [t.current_snapshot().snapshot_id for t in (lake.changes, lake.current, lake.history)]


def test_insert_then_status_change_builds_history(lake):
    apply_batch(lake, [change(lsn=100)])
    apply_batch(lake, [change(lsn=200, status="Close", status_id=19, status_text="VIOLATION CLOSED",
                              status_date=date(2026, 7, 1))])
    cur = lake.read("violations_current").to_pylist()
    assert len(cur) == 1 and cur[0]["violationstatus"] == "Close" and cur[0]["lsn"] == 200
    hist = sorted(lake.read("violation_status_history").to_pylist(), key=lambda r: r["valid_from"])
    assert [(r["violationstatus"], r["valid_from"], r["valid_to"], r["is_current"]) for r in hist] == [
        ("Open", date(2026, 5, 2), date(2026, 7, 1), False),
        ("Close", date(2026, 7, 1), None, True),
    ]
    assert lake.read("violation_changes").num_rows == 2


def test_replaying_a_batch_changes_nothing(lake):
    batch = [change(lsn=100), change(vid=1002, lsn=101),
             change(lsn=150, status_id=4, status_text="NOT COMPLIED WITH", status_date=date(2026, 6, 1))]
    apply_batch(lake, batch)
    before = snapshot_ids(lake)
    stats = apply_batch(lake, batch)
    assert stats["kept"] == 0 and stats["replay_skipped"] == 3
    assert snapshot_ids(lake) == before


def test_out_of_order_and_duplicate_events_in_one_batch(lake):
    late = change(lsn=300, status="Close", status_id=19, status_text="VIOLATION CLOSED", status_date=date(2026, 8, 1))
    stats = apply_batch(lake, [late, change(lsn=100), late])
    assert stats["kept"] == 2
    cur = lake.read("violations_current").to_pylist()
    assert cur[0]["lsn"] == 300 and cur[0]["violationstatus"] == "Close"


def test_older_event_after_newer_is_ignored(lake):
    apply_batch(lake, [change(lsn=300, status="Close", status_id=19, status_text="VIOLATION CLOSED")])
    stats = apply_batch(lake, [change(lsn=100)])
    assert stats["replay_skipped"] == 1
    assert lake.read("violations_current").to_pylist()[0]["violationstatus"] == "Close"


def test_snapshot_read_of_known_row_is_dropped_by_hash(lake):
    apply_batch(lake, [change(lsn=100)])
    stats = apply_batch(lake, [change(lsn=500, op="r", snap=True)])
    assert stats["same_skipped"] == 1 and stats["kept"] == 0


def test_non_status_change_updates_current_but_not_history(lake):
    apply_batch(lake, [change(lsn=100)])
    apply_batch(lake, [change(lsn=200, apartment="4B")])
    assert lake.read("violations_current").to_pylist()[0]["apartment"] == "4B"
    assert lake.read("violation_status_history").num_rows == 1


def test_delete_closes_history_and_removes_current(lake):
    apply_batch(lake, [change(lsn=100, status_date=date(2026, 5, 2), issued=date(2026, 5, 2))])
    apply_batch(lake, [change(lsn=200, op="d")])
    assert lake.read("violations_current").num_rows == 0
    hist = lake.read("violation_status_history").to_pylist()
    assert [r["is_current"] for r in hist] == [False]


def test_new_column_evolves_schema_without_rewriting_files(lake):
    apply_batch(lake, [change(lsn=100), change(vid=1002, lsn=101)])
    files_before = {f.file.file_path for f in lake.current.scan().plan_files()}
    schema_id = lake.current.schema().schema_id
    stats = apply_batch(lake, [change(vid=1003, lsn=102, rentimpairing="Y")])
    assert stats["new_columns"] == ["rentimpairing"]
    assert lake.current.schema().schema_id > schema_id
    files_after = {f.file.file_path for f in lake.current.scan().plan_files()}
    assert files_before <= files_after
    rows = {r["violationid"]: r["rentimpairing"] for r in lake.read("violations_current").to_pylist()}
    assert rows == {1001: None, 1002: None, 1003: "Y"}
