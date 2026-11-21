import argparse
import json
import logging
import sys
import time
from datetime import UTC, date, datetime

from . import connect, freshness, gold, mirror, parity
from .alerts import send_alert
from .consumer import run as consume
from .lake import Lake
from .settings import load_settings

log = logging.getLogger("violcdc")


def months(start: str, end: str | None = None) -> list:
    """[first, next first) pairs from start's month through end's month (default this month)."""
    d = date.fromisoformat(start).replace(day=1)
    stop = date.fromisoformat(end) if end else date.today()
    out = []
    while d <= stop:
        nxt = date(d.year + d.month // 12, d.month % 12 + 1, 1)
        out.append((max(d, date.fromisoformat(start)).isoformat(), nxt.isoformat()))
        d = nxt
    return out


def snapshot_ids(lake: Lake) -> dict:
    out = {}
    for name in ("violation_changes", "violations_current", "violation_status_history"):
        snap = lake.table(name).current_snapshot() if lake.exists(name) else None
        out[name] = snap.snapshot_id if snap else None
    return out


def print_json(obj):
    print(json.dumps(obj, indent=2, default=str))


def main(argv=None):
    p = argparse.ArgumentParser(prog="violcdc")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init")
    sub.add_parser("register")
    s = sub.add_parser("seed")
    s.add_argument("--until", help="leave rows changed after this UTC time for the poller")
    sub.add_parser("poll")
    s = sub.add_parser("poll-loop")
    s.add_argument("--every", type=int, default=300)
    s = sub.add_parser("consume")
    s.add_argument("--lake", default=None)
    s.add_argument("--group", default="hpd-iceberg-live")
    s.add_argument("--from-beginning", action="store_true")
    s.add_argument("--since")
    s.add_argument("--snapshots-only", action="store_true")
    s.add_argument("--follow", action="store_true", help="keep running instead of exiting when idle")
    s.add_argument("--publish-every", type=int, default=300)
    s.add_argument("--max-batch", type=int, default=20000)
    sub.add_parser("publish")
    s = sub.add_parser("backfill")
    s.add_argument("start")
    s.add_argument("end")
    s = sub.add_parser("parity")
    s.add_argument("start")
    s.add_argument("end")
    s.add_argument("--other", default="backfill")
    sub.add_parser("rebuild")
    sub.add_parser("replay-check")
    s = sub.add_parser("refetch")
    s.add_argument("start")
    s.add_argument("end")
    sub.add_parser("freshness")
    sub.add_parser("status")
    s = sub.add_parser("gold")
    s.add_argument("--limit", type=int, default=15)
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = load_settings()

    if args.cmd == "init":
        conn = mirror.connect(cfg.pg_dsn)
        mirror.init(conn)
        print("mirror ready")
    elif args.cmd == "register":
        print_json(connect.register(cfg.connect_url))
    elif args.cmd == "seed":
        until = datetime.fromisoformat(args.until).replace(tzinfo=UTC) if args.until else None
        t0 = time.monotonic()
        totals = mirror.seed(cfg, mirror.connect(cfg.pg_dsn), months(cfg.window_start), until)
        print_json({**totals, "seconds": round(time.monotonic() - t0, 1)})
    elif args.cmd == "poll":
        t0 = time.monotonic()
        totals = mirror.poll(cfg, mirror.connect(cfg.pg_dsn))
        print_json({**totals, "seconds": round(time.monotonic() - t0, 1)})
    elif args.cmd == "poll-loop":
        conn = mirror.connect(cfg.pg_dsn)
        while True:
            try:
                log.info("poll %s", mirror.poll(cfg, conn))
            except Exception as e:
                log.exception("poll failed: %s", e)
            time.sleep(args.every)
    elif args.cmd == "consume":
        lake = Lake(cfg.data_dir / (args.lake or cfg.lake))
        since = datetime.fromisoformat(args.since) if args.since else None
        last = [time.monotonic()]

        def maybe_publish(rec):
            if args.follow and time.monotonic() - last[0] > args.publish_every:
                last[0] = time.monotonic()
                try:
                    gold.publish(lake, cfg)
                except Exception as e:  # a gold failure must not stop the merge
                    log.exception("publish failed")
                    send_alert(cfg, "hpd gold publish failed", str(e)[:500])

        t0 = time.monotonic()
        batches = consume(cfg, lake, args.group, args.from_beginning, since, args.snapshots_only,
                          once=not args.follow, max_batch=args.max_batch, on_batch=maybe_publish)
        keys = ("messages", "events", "kept", "replay_skipped", "same_skipped", "quarantined", "snapshot_events")
        print_json({k: sum(b[k] for b in batches) for k in keys} | {
            "batches": len(batches), "seconds": round(time.monotonic() - t0, 1)})
    elif args.cmd == "publish":
        lake = Lake(cfg.lake_dir)
        ok, results = gold.publish(lake, cfg)
        print_json(results)
        print("published" if ok else "BLOCKED")
        return 0 if ok else 1
    elif args.cmd == "backfill":
        conn = mirror.connect(cfg.pg_dsn)
        since = datetime.now(UTC)
        signal_id = mirror.request_snapshot(conn, args.start, args.end)
        print_json({"signal": signal_id, "since": since.isoformat(),
                    "next": f"make backfill-apply SINCE={since.isoformat()} then make parity"})
    elif args.cmd == "parity":
        live, other = Lake(cfg.lake_dir), Lake(cfg.data_dir / args.other)
        conn = mirror.connect(cfg.pg_dsn)
        out = {
            "backfill_vs_incremental": parity.current_vs_current(other, live, args.start, args.end),
            "incremental_vs_mirror": parity.current_vs_mirror(live, conn, args.start, args.end),
            "backfill_vs_mirror": parity.current_vs_mirror(other, conn, args.start, args.end),
        }
        ga, gb = parity.gold_for_range(other, args.start, args.end), parity.gold_for_range(live, args.start, args.end)
        out["time_to_close_for_range"] = parity.compare(ga, gb)
        print_json(out)
        return 0 if all(v["equal"] for v in out.values()) else 1
    elif args.cmd == "rebuild":
        live, rebuilt = Lake(cfg.lake_dir), Lake(cfg.data_dir / "rebuild")
        t0 = time.monotonic()
        batches = consume(cfg, rebuilt, f"hpd-rebuild-{int(time.time())}", from_beginning=True)
        out = parity.full(live, rebuilt)
        out["messages"] = sum(b["messages"] for b in batches)
        out["seconds"] = round(time.monotonic() - t0, 1)
        print_json(out)
        return 0 if all(v["equal"] for v in out.values() if isinstance(v, dict)) else 1
    elif args.cmd == "replay-check":
        lake = Lake(cfg.lake_dir)
        before = snapshot_ids(lake)
        batches = consume(cfg, lake, f"hpd-replay-{int(time.time())}", from_beginning=True)
        after = snapshot_ids(lake)
        out = {"messages": sum(b["messages"] for b in batches), "kept": sum(b["kept"] for b in batches),
               "replay_skipped": sum(b["replay_skipped"] for b in batches),
               "same_skipped": sum(b["same_skipped"] for b in batches), "snapshots_unchanged": before == after}
        print_json(out)
        return 0 if out["snapshots_unchanged"] and out["kept"] == 0 else 1
    elif args.cmd == "refetch":
        print_json(mirror.refetch(cfg, mirror.connect(cfg.pg_dsn), args.start, args.end))
    elif args.cmd == "freshness":
        out = freshness.check(cfg, mirror.connect(cfg.pg_dsn), Lake(cfg.lake_dir))
        print_json(out)
        return 0 if out["ok"] else 2
    elif args.cmd == "status":
        print_json(status(cfg))
    elif args.cmd == "gold":
        show_gold(Lake(cfg.lake_dir), args.limit)
    return 0


def status(cfg) -> dict:
    conn = mirror.connect(cfg.pg_dsn)
    lake = Lake(cfg.lake_dir)
    out = {"mirror": dict(zip(("rows", "quarantined"), conn.execute(
        "SELECT (SELECT count(*) FROM mirror.violations), (SELECT count(*) FROM mirror.quarantine)").fetchone(),
        strict=True))}
    out["watermark"] = str(mirror.get_watermark(conn)[0])
    out["polls"] = [dict(zip(("mode", "status", "fetched", "inserted", "updated", "unchanged", "quarantined", "s"), r,
                             strict=True)) for r in conn.execute("""
        SELECT mode, status, fetched, inserted, updated, unchanged, quarantined,
               round(extract(epoch FROM finished_at - started_at)) FROM mirror.poll_runs ORDER BY run_id""")]
    out["lake"] = {}
    for name in ("violation_changes", "violations_current", "violation_status_history", "quarantine",
                 "gold_time_to_close", "gold_repeat_class_c"):
        if lake.exists(name):
            t = lake.table(name)
            out["lake"][name] = {"rows": t.scan().count(), "snapshots": len(t.snapshots()),
                                 "schema_id": t.schema().schema_id}
    out["consumer"] = dict(zip(("batches", "messages", "kept", "quarantined", "lag_p50_s", "lag_max_s"),
                               lake.ops.execute("""
        SELECT count(*), sum(messages), sum(kept), sum(quarantined), avg(lag_p50_s), max(lag_max_s)
        FROM consumer_batches""").fetchone(), strict=True))
    return out


def show_gold(lake: Lake, limit: int):
    import duckdb

    con = duckdb.connect()
    for name in ("gold_time_to_close", "gold_repeat_class_c"):
        con.register(name, lake.read(name))
        print(f"\n{name}")
        order = "ORDER BY community_board <> 'all', class" if name == "gold_time_to_close" else ""
        con.sql(f"SELECT * FROM {name} {order} LIMIT {limit}").show(max_width=200)


if __name__ == "__main__":
    sys.exit(main())
