"""Backfill the range into a scratch schema and compare it with the day by day index.

The backfill applies only the last event per set id, so intermediate versions are never
embedded. If that shortcut is right, the two schemas serve the same thing.
"""

import sys
import time
from datetime import date

import numpy as np

from . import dailymed, load, store
from .embed import SentenceEmbedder
from .settings import load_settings


def snapshot(conn) -> dict:
    return {
        "labels": set(conn.execute("select set_id::text, version, status from labels").fetchall()),
        "chunks": set(conn.execute("select chunk_id, version, content_hash, body from chunks").fetchall()),
        "tombstones": set(conn.execute("select set_id::text, last_version from tombstones").fetchall()),
    }


def vectors(conn) -> dict[str, np.ndarray]:
    return {h: v.to_numpy() for h, v in conn.execute("select content_hash, embedding from chunk_vectors")}


def main(argv=None) -> int:
    argv = argv or sys.argv[1:]
    first, last = date.fromisoformat(argv[0]), date.fromisoformat(argv[1])
    main_settings = load_settings()
    scratch = load_settings(schema=main_settings.schema + "_parity")
    names = [p.name for p in main_settings.updates_dir.glob("*.zip")
             if (d := dailymed.file_date(p.name)) and (p.name.startswith("bootstrap_") or first <= d <= last)]

    store.drop_schema(scratch)
    conn = store.connect(scratch)
    store.init_schema(conn)
    t = time.monotonic()
    # vectors come from the main schema's embedding cache when they're there. The model is
    # deterministic for a given text, and this check is about which chunks end up served
    cache = load.EmbeddingCache(store.connect(main_settings, autocommit=True))
    stats = load.backfill(conn, SentenceEmbedder(scratch.model), main_settings.updates_dir, names, cache=cache)
    elapsed = time.monotonic() - t
    print(f"backfill of {len(names)} files: {stats.labels_applied} labels, {stats.embedded} texts embedded, "
          f"{stats.from_cache} from the cache, "
          f"{stats.tombstoned} tombstones, {elapsed:.0f}s")

    daily = store.connect(main_settings)
    a, b = snapshot(daily), snapshot(conn)
    ok = True
    for key in a:
        only_daily, only_backfill = len(a[key] - b[key]), len(b[key] - a[key])
        print(f"{key:<11} day by day {len(a[key]):>6}  backfill {len(b[key]):>6}  "
              f"only day by day {only_daily}  only backfill {only_backfill}")
        ok &= only_daily == only_backfill == 0
    va, vb = vectors(daily), vectors(conn)
    same_keys = va.keys() == vb.keys()
    worst = max((float(np.max(np.abs(va[h] - vb[h]))) for h in va.keys() & vb.keys()), default=0.0)
    print(f"vectors     day by day {len(va):>6}  backfill {len(vb):>6}  same hashes {same_keys}  "
          f"largest difference {worst:.2e}")
    # batching changes float rounding a little, so vectors are compared with a tolerance
    ok &= same_keys and worst < 1e-4
    # the reads above hold locks until their transactions end, and drop schema waits on them
    conn.close()
    daily.close()
    cache.conn.close()
    store.drop_schema(scratch)
    print("parity ok" if ok else "PARITY FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
