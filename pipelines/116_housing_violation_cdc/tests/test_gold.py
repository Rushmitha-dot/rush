from datetime import date

from conftest import change

from violcdc import gold
from violcdc.merge import apply_batch
from violcdc.parity import compare, current_vs_current


def closed(vid, lsn, closed_on, building=50, cls="C", inspected=date(2026, 5, 1)):
    return change(vid=vid, lsn=lsn, building=building, cls=cls, inspected=inspected, issued=inspected,
                  status="Close", status_id=19, status_text="VIOLATION CLOSED", status_date=closed_on)


def load(lake):
    apply_batch(lake, [
        closed(1, 10, date(2026, 5, 11)),
        closed(2, 11, date(2026, 5, 31), inspected=date(2026, 5, 15)),
        change(vid=3, lsn=12, inspected=date(2026, 6, 1), issued=date(2026, 6, 1), status_date=date(2026, 6, 1)),
        closed(4, 13, date(2026, 5, 6), building=77, cls="A"),
    ])


def test_publish_writes_gold_when_checks_pass(lake):
    load(lake)
    ok, results = gold.publish(lake)
    assert ok, gold.blocking(results)
    ttc = {(r["class"], r["communityboard"]): r for r in lake.read("gold_time_to_close").to_pylist()}
    c_all = ttc[("C", None)]
    assert (c_all["violations"], c_all["closed"], c_all["still_open"]) == (3, 2, 1)
    assert c_all["median_days_to_close"] == 13.0
    repeat = lake.read("gold_repeat_class_c").to_pylist()
    assert [(r["buildingid"], r["class_c"], r["inspection_days"], r["open_class_c"]) for r in repeat] == [(50, 3, 3, 1)]


def test_broken_history_blocks_publish(lake):
    load(lake)
    gold.publish(lake)
    published = lake.table("gold_time_to_close").current_snapshot().snapshot_id
    hist = lake.history
    rows = hist.scan().to_arrow().to_pylist()
    for r in rows:
        if r["violationid"] == 1:
            r["is_current"] = False
    from violcdc.lake import align
    hist.overwrite(align(rows, hist))
    ok, results = gold.publish(lake)
    assert not ok
    assert [r["check"] for r in gold.blocking(results)] == ["current_row_has_open_history_row"]
    assert lake.table("gold_time_to_close").current_snapshot().snapshot_id == published


def test_parity_compare(lake, tmp_path):
    from violcdc.lake import Lake

    load(lake)
    other = Lake(tmp_path / "other")
    load(other)
    assert current_vs_current(lake, other)["equal"]
    apply_batch(other, [change(vid=3, lsn=99, apartment="2F")])
    out = current_vs_current(lake, other)
    assert not out["equal"] and out["differ"] == 1
    assert compare({1: "a"}, {1: "a", 2: "b"})["only_b"] == 1
