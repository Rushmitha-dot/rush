"""End to end on the running stack (make up), in a separate database, connector and topic so the real
mirror is never touched. Skipped when the stack isn't up."""

import json
import time
import uuid
from pathlib import Path

import psycopg
import pytest
import requests

from violcdc import connect, mirror
from violcdc.consumer import run as consume
from violcdc.lake import Lake
from violcdc.settings import load_settings
from violcdc.soda import columns, to_row

PAGE = json.loads((Path(__file__).parent / "fixtures" / "soda_page.json").read_text())
BASE = load_settings()


def _stack_up():
    try:
        requests.get(f"{BASE.connect_url}/connectors", timeout=2).raise_for_status()
        psycopg.connect(BASE.pg_dsn, connect_timeout=2).close()
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.stack, pytest.mark.skipif(not _stack_up(), reason="stack not running")]


@pytest.fixture(scope="module")
def stack():
    suffix = uuid.uuid4().hex[:6]
    db, prefix, name = f"hpd_test_{suffix}", f"hpdtest{suffix}", f"hpd-test-{suffix}"
    with psycopg.connect(BASE.pg_dsn, autocommit=True) as admin:
        admin.execute(f"CREATE DATABASE {db}")
    dsn = BASE.pg_dsn.rsplit("/", 1)[0] + f"/{db}"
    cfg = load_settings(pg_dsn=dsn, topic_prefix=prefix)
    conn = mirror.connect(dsn)
    mirror.init(conn)
    connect.register(cfg.connect_url, name=name, **{
        "database.dbname": db, "topic.prefix": prefix, "slot.name": f"hpd_test_{suffix}",
        "publication.name": f"hpd_test_{suffix}", "heartbeat.interval.ms": "5000"})
    for _ in range(60):
        try:
            if connect.is_running(connect.status(cfg.connect_url, name)):
                break
        except requests.HTTPError:
            pass
        time.sleep(1)
    yield cfg, conn
    conn.close()
    connect.delete(cfg.connect_url, name)
    time.sleep(3)
    with psycopg.connect(BASE.pg_dsn, autocommit=True) as admin:
        admin.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                      "WHERE slot_name = %s AND NOT active", (f"hpd_test_{suffix}",))
        admin.execute(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")


def drain(cfg, lake, group, **kw):
    return consume(cfg, lake, group, idle_seconds=8, **kw)


def test_change_reaches_iceberg_and_replay_is_a_no_op(stack, tmp_path):
    cfg, conn = stack
    cols = columns()
    rows = [to_row(r, cols, "BRONX") for r in PAGE]
    mirror.upsert(conn, rows, cols)
    conn.commit()
    lake = Lake(tmp_path / "lake")
    drain(cfg, lake, "t-live")
    assert lake.read("violations_current").num_rows == 3

    changed = dict(rows[0], currentstatus="NOT COMPLIED WITH", currentstatusid=4, violationstatus="Open")
    assert mirror.upsert(conn, [changed], cols)["updated"] == 1
    conn.commit()
    batches = drain(cfg, lake, "t-live")
    hist = [r for r in lake.read("violation_status_history").to_pylist() if r["violationid"] == rows[0]["violationid"]]
    assert hist[-1]["currentstatus"] == "NOT COMPLIED WITH" and sum(r["is_current"] for r in hist) == 1
    # mirror commit to Iceberg commit, against the 15 minute SLA
    assert max(b["lag_max_s"] for b in batches if b["lag_max_s"] is not None) < 15 * 60

    snaps = {n: lake.table(n).current_snapshot().snapshot_id
             for n in ("violation_changes", "violations_current", "violation_status_history")}
    stats = drain(cfg, lake, "t-replay", from_beginning=True)
    assert sum(b["kept"] for b in stats) == 0
    assert snaps == {n: lake.table(n).current_snapshot().snapshot_id for n in snaps}


def test_added_source_column_evolves_the_iceberg_schema(stack, tmp_path):
    cfg, conn = stack
    cols = columns(("rentimpairing",))
    assert mirror.ensure_columns(conn, cols) == ["rentimpairing"]
    rows = [to_row(r, cols, "BRONX") for r in PAGE]
    mirror.upsert(conn, rows, cols)
    conn.commit()
    lake = Lake(tmp_path / "lake")
    batches = drain(cfg, lake, "t-drift", from_beginning=True)
    assert "rentimpairing" in lake.current.schema().column_names
    assert any(b["new_columns"] for b in batches)
    values = {r["violationid"]: r["rentimpairing"] for r in lake.read("violations_current").to_pylist()}
    assert values == {int(r["violationid"]): r.get("rentimpairing") for r in PAGE}
