"""Apply one batch of change events to the Iceberg tables.

Order of writes is changes log, then history, then current. Each step is safe to repeat on its own, so a
crash between two commits is fixed by replaying the batch (offsets are committed only after all three).
"""

import logging
import time
from collections import defaultdict
from datetime import UTC, datetime

import duckdb
import pyarrow as pa
from pyiceberg.expressions import In

from . import history
from .events import extra_fields
from .lake import Lake, align, change_schema, evolve

log = logging.getLogger(__name__)


def _ids_filter(ids):
    return In("violationid", {int(i) for i in ids})


def select_new(events: pa.Table, current_keys: pa.Table) -> tuple[pa.Table, int, int]:
    """Events that move a violation forward.

    Dropped: anything at or before the log position already applied for that violation (replays), and
    events that don't change the row hash (a snapshot read of a row we already have, for example).
    """
    con = duckdb.connect()
    con.register("ev", events)
    con.register("cur", current_keys)
    con.execute("""
        CREATE TEMP TABLE newer AS
        WITH e AS (SELECT DISTINCT ON (violationid, lsn, snap) * FROM ev ORDER BY violationid, lsn, snap)
        SELECT e.* FROM e LEFT JOIN cur c USING (violationid)
        WHERE c.violationid IS NULL OR e.lsn > c.lsn OR (e.lsn = c.lsn AND e.snap AND NOT c.snap)
    """)
    kept = con.sql("""
        WITH l AS (
            SELECT n.*, lag(row_hash) OVER (PARTITION BY violationid ORDER BY lsn, snap) AS prev_hash FROM newer n
        )
        SELECT l.* EXCLUDE (prev_hash) FROM l LEFT JOIN cur c USING (violationid)
        WHERE l.op = 'd' OR l.row_hash IS DISTINCT FROM coalesce(l.prev_hash, c.row_hash)
        ORDER BY violationid, lsn, snap
    """).to_arrow_table()
    n_newer = con.execute("SELECT count(*) FROM newer").fetchone()[0]
    return kept, events.num_rows - n_newer, n_newer - kept.num_rows


def apply_batch(lake: Lake, rows: list) -> dict:
    stats = dict(events=len(rows), kept=0, replay_skipped=0, same_skipped=0, new_columns=[], lags=[])
    if not rows:
        return stats
    extra = extra_fields(rows)
    changes, current, hist = lake.changes, lake.current, lake.history
    full_schema = change_schema(extra)
    stats["new_columns"] = evolve(changes, full_schema)
    evolve(current, full_schema)

    events = align(rows, changes)
    keys = current.scan(selected_fields=("violationid", "lsn", "snap", "row_hash")).to_arrow()
    kept, stats["replay_skipped"], stats["same_skipped"] = select_new(events, keys)
    stats["kept"] = kept.num_rows
    if not kept.num_rows:
        return stats
    kept_rows = kept.to_pylist()
    ids = {r["violationid"] for r in kept_rows}

    # 1. change log, skipping events a crashed earlier attempt already appended
    logged = changes.scan(selected_fields=("violationid", "lsn", "snap")).to_arrow()
    done = set(zip(logged["violationid"].to_pylist(), logged["lsn"].to_pylist(), logged["snap"].to_pylist(),
                   strict=True)) if logged.num_rows else set()
    fresh = [r for r in kept_rows if (r["violationid"], r["lsn"], r["snap"]) not in done]
    if fresh:
        changes.append(align(fresh, changes))

    # 2. history, rebuilt for the touched violations only
    by_id = defaultdict(list)
    for r in kept_rows:
        by_id[r["violationid"]].append(r)
    old = hist.scan(row_filter=_ids_filter(ids)).to_arrow().to_pylist() if hist.current_snapshot() else []
    old_by_id = defaultdict(list)
    for r in old:
        old_by_id[r["violationid"]].append(r)
    new_hist = []
    for vid, evs in by_id.items():
        new_hist.extend(history.advance(old_by_id.get(vid, []), evs))
    hist.overwrite(align(new_hist, hist), overwrite_filter=_ids_filter(ids))

    # 3. current state, the last kept event per violation (a delete removes the row)
    last = {}
    for r in kept_rows:
        last[r["violationid"]] = r
    live = [r for r in last.values() if r["op"] != "d"]
    current.overwrite(align(live, current), overwrite_filter=_ids_filter(ids))

    committed = datetime.now(UTC)
    stats["lags"] = [(committed - r["commit_ts"]).total_seconds() for r in kept_rows if not r["snap"]]
    stats["iceberg_committed_at"] = committed
    return stats


def timed(fn, *args):
    t0 = time.monotonic()
    out = fn(*args)
    return out, time.monotonic() - t0
