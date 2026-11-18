"""Turn the static review dump into a feed.

Everything before --start goes into one history drop (the initial load), then
one drop per UTC day by review timestamp. Each drop is a gzipped JSONL file plus
a _manifest.json written last, with row count, size and md5.

    python -m finder.replay --source data/source/All_Beauty.jsonl.gz --out data/drops --start 2023-01-01
"""
import argparse
import gzip
import hashlib
import json
import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

log = logging.getLogger("finder.replay")


def review_date(ts_ms: int) -> date:
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).date()


def md5_of(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_manifest(folder: Path, kind: str, rows: int, window_start: date, window_end: date) -> dict:
    data = folder / "reviews.jsonl.gz"
    manifest = {
        "kind": kind,
        "batch_date": folder.name.removeprefix("dt="),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "files": [{"name": data.name, "rows": rows, "bytes": data.stat().st_size, "md5": md5_of(data)}],
    }
    tmp = folder / "_manifest.json.tmp"
    tmp.write_text(json.dumps(manifest, indent=2))
    tmp.rename(folder / "_manifest.json")
    return manifest


def split(source: Path, out: Path, start: date) -> dict[str, int]:
    history_day = start - timedelta(days=1)
    handles: dict[date, gzip.GzipFile] = {}
    counts: dict[date, int] = {}
    oldest = None
    try:
        with gzip.open(source, "rt") as f:
            for line in f:
                ts = json.loads(line)["timestamp"]
                d = review_date(ts)
                oldest = d if oldest is None or d < oldest else oldest
                key = history_day if d < start else d
                if key not in handles:
                    folder = out / f"dt={key.isoformat()}"
                    folder.mkdir(parents=True, exist_ok=True)
                    (folder / "_manifest.json").unlink(missing_ok=True)
                    handles[key] = gzip.open(folder / "reviews.jsonl.gz", "wt")
                    counts[key] = 0
                # lines are passed through untouched so the drop is byte-for-byte source data
                handles[key].write(line if line.endswith("\n") else line + "\n")
                counts[key] += 1
    finally:
        for h in handles.values():
            h.close()

    for d, n in counts.items():
        if d == history_day:
            write_manifest(out / f"dt={d}", "history", n, oldest, history_day)
        else:
            write_manifest(out / f"dt={d}", "daily", n, d, d)
    return {d.isoformat(): n for d, n in sorted(counts.items())}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--start", default="2023-01-01", help="first daily drop; earlier reviews become the history drop")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    counts = split(args.source, args.out, date.fromisoformat(args.start))
    daily = {d: n for d, n in counts.items() if d >= args.start}
    log.info("history drop %s rows, %d daily drops, %d rows", sum(counts.values()) - sum(daily.values()),
             len(daily), sum(daily.values()))


if __name__ == "__main__":
    main()
