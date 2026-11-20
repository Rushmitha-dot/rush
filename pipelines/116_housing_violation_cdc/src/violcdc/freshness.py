"""Is a change in the mirror visible in Iceberg within the SLA? Exit code and alert, nothing fancier."""

import logging
from datetime import UTC, datetime, timedelta

import duckdb

from . import connect
from .alerts import send_alert
from .lake import Lake

log = logging.getLogger(__name__)


def check(cfg, conn, lake: Lake) -> dict:
    now = datetime.now(UTC)
    out = {"checked_at": now.isoformat()}
    try:
        out["connector_running"] = connect.is_running(connect.status(cfg.connect_url))
    except Exception as e:  # Connect down is a finding, not a crash
        out["connector_running"] = False
        out["connector_error"] = str(e)[:200]
    slot = conn.execute("""
        SELECT active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
        FROM pg_replication_slots WHERE slot_name = 'hpd_iceberg'""").fetchone()
    out["slot_active"], out["slot_lag_bytes"] = (slot[0], int(slot[1])) if slot else (False, None)

    mirror = conn.execute("SELECT violationid, row_hash, mirrored_at FROM mirror.violations").fetchall()
    con = duckdb.connect()
    con.register("m", _arrow(mirror))
    cur = lake.read("violations_current", ["violationid", "row_hash"])
    con.register("cur", cur)
    pending = con.sql("""
        SELECT count(*), min(m.mirrored_at) FROM m LEFT JOIN cur c USING (violationid)
        WHERE c.row_hash IS DISTINCT FROM m.row_hash""").fetchall()[0]
    out["mirror_rows"], out["lake_rows"] = len(mirror), cur.num_rows
    out["pending_rows"] = pending[0]
    oldest = pending[1]
    out["oldest_pending_minutes"] = round((now - oldest).total_seconds() / 60, 1) if oldest else 0.0
    last_poll = conn.execute("SELECT max(finished_at) FROM mirror.poll_runs WHERE status = 'ok'").fetchone()[0]
    out["last_poll_minutes_ago"] = round((now - last_poll).total_seconds() / 60, 1) if last_poll else None

    problems = []
    if not out["connector_running"]:
        problems.append("debezium connector not running")
    if oldest and now - oldest > timedelta(minutes=cfg.sla_minutes):
        problems.append(f"{out['pending_rows']} mirror rows not in Iceberg, oldest {out['oldest_pending_minutes']} min")
    out["ok"] = not problems
    if problems:
        send_alert(cfg, "hpd cdc freshness", "; ".join(problems))
    return out


def _arrow(rows):
    import pyarrow as pa

    ids, hashes, ts = zip(*rows, strict=True) if rows else ((), (), ())
    return pa.table({"violationid": pa.array(ids, pa.int64()), "row_hash": pa.array(hashes, pa.string()),
                     "mirrored_at": pa.array(ts, pa.timestamp("us", tz="UTC"))})
