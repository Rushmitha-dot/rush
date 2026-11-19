"""Command line entry point. The Databricks job calls the same commands.

    python -m provider_lake.runner refs
    python -m provider_lake.runner run                     # what the schedule runs, new files only
    python -m provider_lake.runner load data/downloads/X.zip
    python -m provider_lake.runner partd 2023 2024
    python -m provider_lake.runner gold
    python -m provider_lake.runner backfill [--since 2026-11-08]
    python -m provider_lake.runner parity monthly data/downloads/<next monthly>.zip
    python -m provider_lake.runner parity rebuild data/rebuild
    python -m provider_lake.runner api-check --sample 300
    python -m provider_lake.runner freshness
"""
import argparse
import json
import logging
import os
import sys
import urllib.request
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

from pyspark.sql import functions as F

from . import api_check, gold, ops, parity, partd, pipeline
from .alerts import send_alert
from .governance import create_analyst_views
from .settings import Settings
from .sources import NUCC_TAXONOMY_URL, USER_AGENT, download, list_published
from .spark import get_spark

log = logging.getLogger("provider_lake")


def cmd_refs(spark, cfg, args, run_id):
    path = cfg.downloads / "nucc_taxonomy_261.csv"
    if not path.exists():
        download(NUCC_TAXONOMY_URL, path)
    return {"taxonomy_codes": gold.load_taxonomy(spark, cfg, str(path))}


def cmd_load(spark, cfg, args, run_id):
    return [pipeline.load_nppes_zip(spark, cfg, Path(p), run_id) for p in args.files]


def cmd_run(spark, cfg, args, run_id):
    """Pick up whatever NPPES has published that the ledger hasn't loaded, oldest first."""
    loaded = {r["source_file"] for r in ops.loaded_files(spark, cfg).collect()}
    out = []
    for f in list_published(cfg.nppes_base_url + "NPI_Files.html"):
        if f.kind == "monthly" and f.name in loaded:
            # 1.1 GB to re-download just to confirm a checksum. Weeklies are always re-checked.
            out.append({"source_file": f.name, "status": "skipped"})
            continue
        path = cfg.downloads / f.name
        if not path.exists():
            download(cfg.nppes_base_url + f.name, path)
        out.append(pipeline.load_nppes_zip(spark, cfg, path, run_id))
    if any(r["status"].startswith("loaded") for r in out):
        out.append({"gold": cmd_gold(spark, cfg, args, run_id)})
    return out


def cmd_partd(spark, cfg, args, run_id):
    out = []
    for year in args.years:
        manifest_path = cfg.landing / "partd" / f"partd_{year}" / "manifest.json"
        if not manifest_path.exists():
            partd.land_year(year, cfg.landing / "partd", cfg.scope_states)
        m = json.loads(manifest_path.read_text())
        entry = ops.ledger_entry(spark, cfg, m["source_file"])
        if entry and entry["status"].startswith("loaded") and entry["source_sha256"] == m["source_sha256"]:
            out.append({"year": year, "status": "skipped"})
            continue
        rows, checks = gold.load_partd_year(spark, cfg, m)
        ops.save_checks(spark, cfg, run_id, m["source_file"], checks)
        errors = [c for c in checks if not c.passed and c.severity == "error"]
        warns = [c for c in checks if not c.passed and c.severity == "warn"]
        status = "blocked" if errors else ("loaded_with_warnings" if warns else "loaded")
        ops.record_ledger(spark, cfg, {
            "source_file": m["source_file"], "source_sha256": m["source_sha256"], "kind": "partd",
            "period_start": date(year, 1, 1), "period_end": date(year, 12, 31), "status": status,
            "rows_read": m["rows_read"], "rows_landed": m["rows_kept"], "run_id": run_id})
        if errors or warns:
            send_alert(cfg, f"Part D {year} {status}", ", ".join(f"{c.name} ({c.observed:g})" for c in errors + warns))
        out.append({"year": year, "status": status, "rows": rows})
    return out


def cmd_gold(spark, cfg, args, run_id):
    full = getattr(args, "full", False)
    out = {"dim_provider": gold.refresh_dim_provider(spark, cfg, full=full)}
    years = [r["claim_year"] for r in
             spark.table(cfg.table("silver", "partd_prescriber_drug")).select("claim_year").distinct().collect()] \
        if spark.catalog.tableExists(cfg.table("silver", "partd_prescriber_drug")) else []
    out["facts"] = [gold.build_year(spark, cfg, y) for y in sorted(years)]
    if years:
        gold.build_marts(spark, cfg)
    create_analyst_views(spark, cfg)
    return out


def cmd_backfill(spark, cfg, args, run_id):
    since = date.fromisoformat(args.since) if args.since else None
    dim = cfg.table("silver", "dim_provider")
    before = pipeline.table_version(spark, dim) if spark.catalog.tableExists(dim) else None
    results = pipeline.backfill(spark, cfg, since, run_id)
    # what the replay changed compared with the dimension it replaced, normally nothing
    diff = parity.compare_versions(spark, dim, before, pipeline.table_version(spark, dim)) if before else None
    args.full = True  # the restore rewound silver, so gold can't follow the change feed across it
    return {"files": results, "versus_before": diff, "gold": cmd_gold(spark, cfg, args, run_id)}


def cmd_parity(spark, cfg, args, run_id):
    if args.mode == "monthly":
        state_end = ops.last_loaded_period_end(spark, cfg)
        return parity.check_monthly(spark, cfg, Path(args.target), state_end)
    if args.mode == "rebuild":
        other = Path(args.target) / "lake"
        out = {}
        for t in ("silver.db/dim_provider", "gold.db/fact_prescriber_drug"):
            out[t] = parity.compare_tables(spark, str(cfg.warehouse / t), str(other / t))
        return out
    a, b = args.target.split(",")
    return parity.compare_versions(spark, cfg.table("silver", "dim_provider"), int(a), int(b))


def cmd_api_check(spark, cfg, args, run_id):
    state_end = ops.last_loaded_period_end(spark, cfg)
    return api_check.run(spark, cfg, state_end, sample_size=args.sample)


def cmd_freshness(spark, cfg, args, run_id):
    """SLA: a weekly file is loaded within 24 hours of CMS publishing it."""
    loaded = {r["source_file"] for r in ops.loaded_files(spark, cfg).collect()}
    late = []
    for f in list_published(cfg.nppes_base_url + "NPI_Files.html"):
        if f.name in loaded:
            continue
        req = urllib.request.Request(cfg.nppes_base_url + f.name, method="HEAD", headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=30) as resp:
            published = parsedate_to_datetime(resp.headers["Last-Modified"])
        hours = (datetime.now(UTC) - published).total_seconds() / 3600
        if hours > 24:
            late.append({"source_file": f.name, "hours_since_publish": round(hours, 1)})
    if late:
        send_alert(cfg, "NPPES freshness SLA missed", json.dumps(late))
    return {"late": late, "ok": not late}


def cmd_report(spark, cfg, args, run_id):
    t = cfg.table
    dim = spark.table(t("silver", "dim_provider"))
    out = {
        "ledger": [r.asDict() for r in ops.ledger_current(spark, cfg).orderBy("period_end").collect()],
        "dim_rows": dim.count(), "dim_npis": dim.select("npi").distinct().count(),
        "dim_current_active": dim.filter("is_current and is_active").count(),
        "versions_per_npi": {r["version_no"]: r["count"] for r in
                             dim.groupBy("version_no").count().collect()},
        "changes_by_type": {f"{r['source_file'][25:38]} {r['change_type']}": r["count"] for r in
                            spark.table(t("silver", "provider_change_log"))
                            .groupBy("source_file", "change_type").count().collect()},
        "changed_fields": {r["f"]: r["count"] for r in spark.table(t("silver", "provider_change_log"))
                           .filter(F.col("change_type").isin("changed", "same_day_fix"))
                           .select(F.explode("changed_fields").alias("f")).groupBy("f").count()
                           .orderBy(F.desc("count")).limit(12).collect()},
        "quarantine": {f"{r['source_file'][25:38]} {r['reason']}": r["count"] for r in
                       spark.table(t("silver", "provider_quarantine")).groupBy("source_file", "reason").count()
                       .collect()},
        "failed_checks": [r.asDict() for r in spark.table(t("ops", "dq_results")).filter("not passed")
                          .select("source_file", "check_name", "severity", "observed", "detail").distinct()
                          .collect()],
    }
    return out


COMMANDS = {"refs": cmd_refs, "load": cmd_load, "run": cmd_run, "partd": cmd_partd, "gold": cmd_gold,
            "backfill": cmd_backfill, "parity": cmd_parity, "api-check": cmd_api_check,
            "freshness": cmd_freshness, "report": cmd_report}


def main(argv=None):
    p = argparse.ArgumentParser(prog="provider_lake")
    # Databricks serverless tasks can't set environment variables, so the job passes these instead
    p.add_argument("--catalog", help="Unity Catalog name, sets LAKE_CATALOG")
    p.add_argument("--home", help="landing and download root, sets LAKE_HOME")
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("refs")
    sub.add_parser("run")
    sub.add_parser("freshness")
    sub.add_parser("report")
    sp = sub.add_parser("load")
    sp.add_argument("files", nargs="+")
    sp = sub.add_parser("partd")
    sp.add_argument("years", nargs="+", type=int)
    sp = sub.add_parser("gold")
    sp.add_argument("--full", action="store_true")
    sp = sub.add_parser("backfill")
    sp.add_argument("--since", help="replay files ending after this date; omit to rebuild from nothing")
    sp = sub.add_parser("parity")
    sp.add_argument("mode", choices=["monthly", "rebuild", "versions"])
    sp.add_argument("target", help="monthly zip, rebuilt lake home, or 'a,b' Delta versions")
    sp = sub.add_parser("api-check")
    sp.add_argument("--sample", type=int, default=300)
    args = p.parse_args(argv)
    if args.catalog:
        os.environ["LAKE_CATALOG"] = args.catalog
    if args.home:
        os.environ["LAKE_HOME"] = args.home

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = Settings()
    spark = get_spark(cfg)
    ops.ensure_ops(spark, cfg)
    run_id, started = ops.new_run_id(), ops.now()
    target = " ".join(str(v) for k, v in vars(args).items() if k not in ("command", "catalog", "home") and v)
    try:
        result = COMMANDS[args.command](spark, cfg, args, run_id)
    except Exception as e:
        ops.log_run(spark, cfg, run_id, args.command, target, "error", started, repr(e))
        send_alert(cfg, f"provider_lake {args.command} failed", repr(e))
        raise
    ops.log_run(spark, cfg, run_id, args.command, target, "ok", started, json.dumps(result, default=str)[:2000])
    print(json.dumps(result, indent=1, default=str))
    return result


if __name__ == "__main__":
    sys.exit(0 if main() is not None else 1)
