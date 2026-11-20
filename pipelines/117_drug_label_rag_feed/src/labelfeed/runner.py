"""Command line entry point. `run` is what cron calls once a day."""

import argparse
import json
import sys
import time
from datetime import date, datetime

from . import bootstrap, dailymed, freshness, lineage, load, store
from .alerts import alert
from .embed import SentenceEmbedder
from .settings import load_settings

# the subset. Everything the downloads page offered on 2026-11-20 from this day on
WINDOW_START = date(2026, 11, 2)


def _conn(settings):
    conn = store.connect(settings)
    store.init_schema(conn)
    return conn


def _local_updates(settings) -> list[str]:
    names = [p.name for p in settings.updates_dir.glob("*.zip") if dailymed.file_date(p.name)]
    return sorted(names, key=dailymed.order_key)


def cmd_fetch(settings, args) -> int:
    offered = dailymed.available_updates(dailymed.listing())
    names = args.names or [n for n in offered if dailymed.file_date(n) >= WINDOW_START]
    # a weekly file repeats the dailies it covers. Loading both is safe, the second copy of a
    # label is the same version and document id and is skipped.
    for name in names:
        manifest = dailymed.fetch_update(name, settings.updates_dir, settings.data_dir / "incoming",
                                         listed_md5=offered.get(name))
        m = json.loads(manifest.read_text())
        print(f"{name}  {m['source_bytes'] / 1e6:.0f} MB -> {m['slim_bytes'] / 1e6:.1f} MB, "
              f"{m['rx_added']} Rx added, {m['rx_deleted']} Rx deleted")
    return 0


def cmd_bootstrap(settings, args) -> int:
    paths = [settings.updates_dir / n for n in _local_updates(settings) if not n.startswith("bootstrap_")]
    manifest = json.loads(bootstrap.build(paths, WINDOW_START, settings.updates_dir).read_text())
    print(f"{manifest['update_file']}: {manifest['rx_added']} labels, {len(manifest['errors'])} errors, "
          f"{manifest['latest_only']} only available as the latest version")
    return 0


def _load_one(settings, conn, embedder, name, force=False, cache=None) -> str:
    run = lineage.Run(settings, "labelfeed.load", [name])
    run.emit("START")
    try:
        outcome, stats = load.load_file(conn, embedder, settings.updates_dir, name, force=force,
                                        cache=cache)
    except load.Blocked as e:
        run.emit("FAIL", {"errorMessage": {"message": str(e)}})
        alert(settings, "error", f"{name} blocked, nothing published: {e}")
        return "blocked"
    if stats:
        print(f"{name}: {stats.labels_applied} applied, {stats.labels_skipped} unchanged, "
              f"{stats.quarantined} quarantined, {stats.tombstoned} tombstoned, "
              f"chunks +{stats.chunks_added} -{stats.chunks_removed} ={stats.chunks_kept}, "
              f"{stats.embedded} embedded, {stats.from_cache} from the embedding cache")
    else:
        print(f"{name}: already loaded with the same checksum, nothing to do")
    run.emit("COMPLETE")
    return outcome


def cmd_load(settings, args) -> int:
    conn = _conn(settings)
    embedder = SentenceEmbedder(settings.model)
    cache = load.EmbeddingCache(store.connect(settings, autocommit=True))
    outcome = _load_one(settings, conn, embedder, args.name, force=args.force, cache=cache)
    return 1 if outcome == "blocked" else 0


def cmd_run(settings, args) -> int:
    """Fetch what's new, apply every local file not in the ledger in order, check freshness."""
    if not args.offline:
        try:
            cmd_fetch(settings, argparse.Namespace(names=None))
        except Exception as e:  # a DailyMed outage should still run the freshness check
            alert(settings, "error", f"fetch failed: {e}")
    conn = _conn(settings)
    embedder = SentenceEmbedder(settings.model)
    cache = load.EmbeddingCache(store.connect(settings, autocommit=True))
    blocked = 0
    for name in _local_updates(settings):
        if dailymed.file_date(name) > (args.until or date.max):
            continue
        t = time.monotonic()
        outcome = _load_one(settings, conn, embedder, name, cache=cache)
        blocked += outcome == "blocked"
        if outcome == "loaded":
            print(f"  {time.monotonic() - t:.1f}s")
        if outcome == "blocked":
            break  # later files assume this one was applied
    code = freshness.check(settings, conn, offline=args.offline)
    return 1 if blocked else code


def cmd_backfill(settings, args) -> int:
    names = [n for n in _local_updates(settings)
             if n.startswith("bootstrap_") or args.first <= dailymed.file_date(n) <= args.last]
    conn = _conn(settings)
    t = time.monotonic()
    run = lineage.Run(settings, "labelfeed.backfill", names)
    run.emit("START")
    try:
        stats = load.backfill(conn, SentenceEmbedder(settings.model), settings.updates_dir, names,
                              cache=load.EmbeddingCache(store.connect(settings, autocommit=True)))
    except load.Blocked as e:
        conn.rollback()
        run.emit("FAIL", {"errorMessage": {"message": str(e)}})
        alert(settings, "error", f"backfill blocked, nothing published: {e}")
        return 1
    run.emit("COMPLETE")
    print(f"backfill of {len(names)} files: {stats.labels_applied} labels applied, {stats.quarantined} quarantined, "
          f"{stats.tombstoned} tombstoned, {stats.chunks_added} chunks, {stats.embedded} embedded, "
          f"{time.monotonic() - t:.1f}s")
    return 0


def cmd_freshness(settings, args) -> int:
    now = datetime.fromisoformat(args.now) if args.now else None
    return freshness.check(settings, _conn(settings), offline=args.offline, now=now)


def cmd_status(settings, args) -> int:
    conn = _conn(settings)
    for sql in [
        "select status, count(*) from labels group by 1 order by 1",
        "select count(*) as chunks, count(distinct content_hash) as distinct_texts from chunks",
        "select count(*) as vectors from chunk_vectors",
        "select count(*) as tombstones from tombstones",
        "select reason, count(*) from quarantine group by 1 order by 2 desc",
        """select update_file, mode, status, labels_in, labels_applied, labels_skipped, quarantined, tombstoned,
                  chunks_added, chunks_removed, chunks_kept, embedded, seconds
           from load_ledger order by file_date nulls first, finished_at""",
    ]:
        cur = conn.execute(sql)
        cols = [d.name for d in cur.description]
        print(" | ".join(cols))
        for row in cur.fetchall():
            print(" | ".join(str(v) for v in row))
        print()
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="labelfeed")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init")
    f = sub.add_parser("fetch")
    f.add_argument("names", nargs="*")
    sub.add_parser("bootstrap")
    ld = sub.add_parser("load")
    ld.add_argument("name")
    ld.add_argument("--force", action="store_true")
    r = sub.add_parser("run")
    r.add_argument("--offline", action="store_true", help="skip DailyMed, only apply local files")
    r.add_argument("--until", type=date.fromisoformat)
    b = sub.add_parser("backfill")
    b.add_argument("first", type=date.fromisoformat)
    b.add_argument("last", type=date.fromisoformat)
    fr = sub.add_parser("freshness")
    fr.add_argument("--offline", action="store_true")
    fr.add_argument("--now", help="pretend it is this UTC time, to test the alert")
    sub.add_parser("status")
    args = p.parse_args(argv)
    settings = load_settings()
    if args.cmd == "init":
        _conn(settings)
        print(f"schema {settings.schema} ready")
        return 0
    return {
        "fetch": cmd_fetch, "bootstrap": cmd_bootstrap, "load": cmd_load, "run": cmd_run,
        "backfill": cmd_backfill, "freshness": cmd_freshness, "status": cmd_status,
    }[args.cmd](settings, args)


if __name__ == "__main__":
    sys.exit(main())
