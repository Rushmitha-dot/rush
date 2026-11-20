import argparse
import json
import logging
import sys
from pathlib import Path

from . import alerts, ops, parity, pipeline, sources, summarize
from .settings import Settings
from .spark import get_spark

log = logging.getLogger("claims_perf")


def _years(values: list[str]) -> list[int]:
    return [int(v) for v in values] if values else sorted(sources.YEAR_URLS)


def _print(rows):
    for r in rows:
        print(json.dumps(r, default=str))


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(prog="claims_perf")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("land", "stage", "summarize"):
        s = sub.add_parser(name)
        s.add_argument("years", nargs="+")
        s.add_argument("--force", action="store_true")
        if name == "summarize":
            s.add_argument("--baseline", action="store_true", help="the job as first written, for comparison")
    s = sub.add_parser("run", help="what the schedule runs, every configured year, skipping what's current")
    s.add_argument("years", nargs="*")
    s = sub.add_parser("backfill")
    s.add_argument("first", type=int)
    s.add_argument("last", type=int)
    s = sub.add_parser("parity")
    s.add_argument("other_home", type=Path)
    s.add_argument("years", nargs="*")
    s = sub.add_parser("freshness")
    s.add_argument("--discover", action="store_true", help="also ask the CMS catalog for new data years")
    sub.add_parser("status")
    a = p.parse_args(argv)
    cfg = Settings()

    if a.cmd == "status":
        for r in ops.recent_runs(cfg):
            print(r[:5], r[5][:160])
        return 0
    if a.cmd == "freshness":
        catalog = sorted(sources.discover()) if a.discover else None
        problems = pipeline.freshness(cfg, sorted(sources.YEAR_URLS), catalog)
        if problems:
            alerts.send_alert(cfg, "claims marts not fresh", "; ".join(problems))
            return 1
        print("fresh:", sorted(sources.YEAR_URLS))
        return 0
    if a.cmd == "land":
        _print(pipeline.land_step(cfg, int(y), force=a.force) for y in a.years)
        return 0

    spark = get_spark(cfg)
    try:
        if a.cmd == "stage":
            out = [pipeline.stage_step(spark, cfg, int(y), force=a.force) for y in a.years]
        elif a.cmd == "summarize":
            opts = summarize.BASELINE if a.baseline else summarize.TUNED
            out = [pipeline.summarize_step(spark, cfg, int(y), force=a.force, opts=opts) for y in a.years]
        elif a.cmd == "run":
            out = pipeline.run_years(spark, cfg, _years(a.years))
        elif a.cmd == "backfill":
            out = pipeline.backfill(spark, cfg, a.first, a.last)
        elif a.cmd == "parity":
            out = parity.compare(spark, cfg.home, a.other_home, _years(a.years))
            _print(out)
            return 0 if all(r["same"] for r in out) else 1
        _print(out)
        return 1 if any(r["status"] in ("blocked", "error") for r in out) else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
