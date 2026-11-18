"""Backfill parity. Builds the same window twice into two scratch DuckDB files,
once day by day with a dbt build after every day (how the daily schedule runs)
and once with every day loaded first and a single dbt build at the end (how a
backfill runs). The incremental models and marts must come out identical.

    python -m finder.parity --start 2023-01-01 --end 2023-01-31
"""
import argparse
import logging
import shutil
import tempfile
from dataclasses import replace
from pathlib import Path

import duckdb

from . import runner, warehouse
from .settings import Settings

log = logging.getLogger("finder.parity")

COMPARE = {
    "intermediate.int_review_concerns": "review_id, model_version, concern, mentions, round(polarity, 6)",
    "intermediate.int_review_types": "review_id, model_version, type_group, type_key",
    "marts.product_concern_scores": "*",
    "marts.product_concern_evidence": "*",
}


def build(cfg: Settings, start: str, end: str, dbt_once: bool) -> None:
    with warehouse.connect(cfg.db_path) as con:
        warehouse.init(con)
        warehouse.load_catalog(con, cfg.catalog)
        con.execute("insert into ops.model_versions (model_version, status) values ('rules-v1', 'active')")
    runner.dbt(cfg, "seed")
    res = runner.backfill(cfg, start, end, dbt_once=dbt_once)
    bad = {d: s for d, s in res.items() if s not in ("ok", "warn", "skipped")}
    if bad:
        raise SystemExit(f"build failed: {bad}")


def diff(a: Path, b: Path) -> dict[str, tuple[int, int, int]]:
    con = duckdb.connect()
    con.execute(f"attach '{a}' as a (read_only)")
    con.execute(f"attach '{b}' as b (read_only)")
    out = {}
    for table, cols in COMPARE.items():
        n = con.execute(f"select count(*) from a.{table}").fetchone()[0]
        only_a = con.execute(f"select count(*) from (select {cols} from a.{table} except all "
                             f"select {cols} from b.{table})").fetchone()[0]
        only_b = con.execute(f"select count(*) from (select {cols} from b.{table} except all "
                             f"select {cols} from a.{table})").fetchone()[0]
        out[table] = (n, only_a, only_b)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2023-01-01")
    ap.add_argument("--end", default="2023-01-31")
    ap.add_argument("--min-reviews", type=int, default=2, help="low so a one-month window publishes something")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    tmp = Path(tempfile.mkdtemp(prefix="finder_parity_"))
    base = Settings.from_env()
    daily = replace(base, db_path=tmp / "daily.duckdb", serving=tmp / "serving_daily", min_reviews=args.min_reviews)
    once = replace(base, db_path=tmp / "once.duckdb", serving=tmp / "serving_once", min_reviews=args.min_reviews)
    build(daily, args.start, args.end, dbt_once=False)
    build(once, args.start, args.end, dbt_once=True)
    result = diff(daily.db_path, once.db_path)
    ok = True
    for table, (n, only_daily, only_once) in result.items():
        log.info("%-40s rows %7d  only in daily %d  only in backfill %d", table, n, only_daily, only_once)
        ok &= only_daily == 0 and only_once == 0
    shutil.rmtree(tmp, ignore_errors=True)
    log.info("parity %s", "OK" if ok else "FAILED")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
