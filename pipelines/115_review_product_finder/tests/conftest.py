import gzip
import json

import pytest

from finder.replay import write_manifest


def review(user="u1", asin="B01", ts=1672617600000, text="Love it", rating=5.0, title="ok", parent=None):
    return {"rating": rating, "title": title, "text": text, "images": [], "asin": asin, "parent_asin": parent or asin,
            "user_id": user, "timestamp": ts, "helpful_vote": 0, "verified_purchase": True}


def write_drop(root, day, rows, kind="daily", window=None):
    folder = root / f"dt={day}"
    folder.mkdir(parents=True, exist_ok=True)
    with gzip.open(folder / "reviews.jsonl.gz", "wt") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    from datetime import date
    start, end = window or (date.fromisoformat(day), date.fromisoformat(day))
    return write_manifest(folder, kind, len(rows), start, end)


@pytest.fixture(scope="session")
def v1():
    from finder.extract import Extractor
    return Extractor("rules-v1")


@pytest.fixture(scope="session")
def v2():
    from finder.extract import Extractor
    return Extractor("rules-v2")
