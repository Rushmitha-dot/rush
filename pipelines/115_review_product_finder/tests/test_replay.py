import gzip
import json

from conftest import review

from finder.ingest import DropRejected, read_manifest
from finder.replay import split


def test_split_history_and_daily(tmp_path):
    src = tmp_path / "src.jsonl.gz"
    with gzip.open(src, "wt") as f:
        for r in [review(ts=1577836800000), review(ts=1672531200000), review(ts=1672617600000 + 5)]:
            f.write(json.dumps(r) + "\n")
    from datetime import date
    counts = split(src, tmp_path / "drops", date(2023, 1, 1))
    assert counts == {"2022-12-31": 1, "2023-01-01": 1, "2023-01-02": 1}
    m = read_manifest(tmp_path / "drops" / "dt=2022-12-31")
    assert m["kind"] == "history" and m["window_start"] == "2020-01-01"


def test_missing_manifest_rejected(tmp_path):
    (tmp_path / "dt=2023-01-01").mkdir()
    try:
        read_manifest(tmp_path / "dt=2023-01-01")
    except DropRejected as e:
        assert "no manifest" in str(e)
    else:
        raise AssertionError("expected DropRejected")
