"""Pipeline entry point. Airflow calls the same commands, one per task.

    python -m finder.runner init
    python -m finder.runner run 2023-01-05                 land + check + score + dbt build + publish
    python -m finder.runner backfill 2022-12-31 2023-09-08 [--dbt-once]
    python -m finder.runner score-version rules-v2         versioned backfill next to the active version
    python -m finder.runner activate rules-v2              swap the current views, old scores stay
    python -m finder.runner freshness --expect 2023-09-08
    python -m finder.runner status

Every attempt gets a row in ops.pipeline_runs and its checks in ops.dq_results.
"""
import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from . import checks, enrich, ingest, warehouse
from .alerts import send_alert
from .extract import Extractor
from .settings import Settings

log = logging.getLogger("finder")


class StepFailed(Exception):
    pass


def start_run(con, command, trigger, batch_date=None) -> int:
    return con.execute("insert into ops.pipeline_runs (batch_date, command, trigger, status) "
                       "values (?, ?, ?, 'running') returning run_id", [batch_date, command, trigger]).fetchone()[0]


def finish_run(con, run_id, status, t0, error=None, **counts):
    con.execute("""update ops.pipeline_runs set status = ?, finished_at = current_timestamp, rows_in = ?,
                   rows_loaded = ?, rows_quarantined = ?, reviews_scored = ?, seconds = ?, error = ?
                   where run_id = ?""",
                [status, counts.get("rows_in"), counts.get("rows_loaded"), counts.get("rows_quarantined"),
                 counts.get("reviews_scored"), round(time.time() - t0, 2), error, run_id])


def save_checks(con, run_id, batch_date, results):
    con.executemany("insert into ops.dq_results values (?, ?, ?, ?, ?, ?, ?, ?)",
                    [(run_id, batch_date, r.name, r.severity, r.passed, r.observed, r.threshold, r.detail)
                     for r in results])


def dbt(cfg: Settings, *args: str) -> None:
    exe = Path(sys.executable).parent / "dbt"
    # target and logs sit next to the warehouse file, so tests and parity runs
    # with their own scratch database never share dbt/target with the real one
    work = cfg.db_path.parent / f"dbt_{cfg.db_path.stem}"
    cmd = [str(exe), *args, "--project-dir", str(cfg.dbt_dir), "--profiles-dir", str(cfg.dbt_dir),
           "--target-path", str(work / "target"), "--log-path", str(work / "logs"),
           "--vars", json.dumps({"min_reviews": cfg.min_reviews})]
    env = {**os.environ, "FINDER_DB": str(cfg.db_path)}
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        tail = "\n".join(proc.stdout.splitlines()[-25:])
        raise StepFailed(f"dbt {' '.join(args)} failed\n{tail}")
    summary = [line for line in proc.stdout.splitlines() if "Done. PASS" in line]
    log.info("dbt %s %s", " ".join(args), summary[-1].split("|", 1)[-1].strip() if summary else "ok")


def publish(cfg: Settings) -> dict:
    """Export the current views to Parquet for the chatbot. The app never opens
    the DuckDB file, so a pipeline run holding the write lock can't break it.
    Files are written to a temp name and renamed, so a reader never sees half a file."""
    cfg.serving.mkdir(parents=True, exist_ok=True)
    exports = {
        "scores": "select * from marts.product_scores_current",
        "evidence": "select * from marts.product_evidence_current",
        "concerns": "select * from ref.concern_taxonomy",
        "types": "select * from ref.type_taxonomy",
    }
    with warehouse.connect(cfg.db_path) as con:
        for name, sql in exports.items():
            tmp = cfg.serving / f".{name}.parquet.tmp"
            con.execute(f"copy ({sql}) to '{tmp}' (format parquet)")
            os.replace(tmp, cfg.serving / f"{name}.parquet")
        info = con.execute("""
            select (select model_version from ops.model_versions where status = 'active'),
                   (select max(batch_date) from ops.load_ledger),
                   (select count(*) from marts.product_scores_current)""").fetchone()
    meta = {"model_version": info[0], "through_batch": str(info[1]), "score_rows": info[2],
            "published_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    (cfg.serving / "_published.json").write_text(json.dumps(meta, indent=2))
    return meta


def load_day(cfg: Settings, batch_date: str, trigger: str, extractors: dict, force: bool = False) -> str:
    """Land, check and score one drop. Returns ok, warn, skipped, failed_checks or error."""
    t0 = time.time()
    folder = cfg.drops / f"dt={batch_date}"
    with warehouse.connect(cfg.db_path) as con:
        run_id = start_run(con, "load", trigger, batch_date)
        try:
            manifest = ingest.read_manifest(folder)
            if not force and ingest.already_loaded(con, batch_date, manifest["files"][0]["md5"]):
                finish_run(con, run_id, "skipped", t0, error="same file already loaded, use --force to reload")
                log.info("%s already loaded, skipping", batch_date)
                return "skipped"

            stats = ingest.stage(con, folder, manifest)
            results = checks.run_checks(stats, ingest.recent_daily_rows(con, batch_date), manifest["kind"])
            save_checks(con, run_id, batch_date, results)
            errors = checks.failed(results, checks.ERROR)
            counts = {"rows_in": stats["rows_in"], "rows_quarantined": stats["quarantined"]}
            if errors:
                names = ", ".join(r.name for r in errors)
                finish_run(con, run_id, "failed_checks", t0, error=names, **counts)
                send_alert(cfg, f"product finder {batch_date} blocked by data checks", names)
                return "failed_checks"

            counts["rows_loaded"] = ingest.publish_stage(con, batch_date, manifest)
            scored = 0
            for version in warehouse.active_versions(con):
                if version not in extractors:
                    extractors[version] = Extractor(version)
                n, _ = enrich.score_batch(con, extractors[version], batch_date)
                scored += n
            counts["reviews_scored"] = scored

            warns = checks.failed(results, checks.WARN)
            status = "warn" if warns else "ok"
            finish_run(con, run_id, status, t0, **counts)
            if warns:
                send_alert(cfg, f"product finder {batch_date} loaded with warnings",
                           "; ".join(f"{r.name}={r.observed} ({r.detail})" for r in warns))
            log.info("%s %s, %d in, %d loaded, %d quarantined in %.1fs", batch_date, status, stats["rows_in"],
                     counts["rows_loaded"], stats["quarantined"], time.time() - t0)
            return status
        except ingest.DropRejected as e:
            finish_run(con, run_id, "error", t0, error=f"drop rejected: {e}")
            send_alert(cfg, f"product finder {batch_date} drop rejected", str(e))
            return "error"
        except Exception as e:
            finish_run(con, run_id, "error", t0, error=repr(e)[:2000])
            send_alert(cfg, f"product finder {batch_date} failed", repr(e)[:2000])
            raise


def transform(cfg: Settings, trigger: str, batch_date: str | None = None) -> str:
    """dbt build (models + tests) then publish. A failing ERROR test stops the
    publish, so the chatbot keeps serving the last good snapshot."""
    t0 = time.time()
    with warehouse.connect(cfg.db_path) as con:
        run_id = start_run(con, "transform", trigger, batch_date)
    try:
        dbt(cfg, "build")
        meta = publish(cfg)
        status, error = "ok", None
        log.info("published %s", meta)
    except StepFailed as e:
        status, error = "failed_tests", str(e)[:4000]
        send_alert(cfg, "product finder dbt build failed, publish skipped", str(e)[:2000])
    with warehouse.connect(cfg.db_path) as con:
        finish_run(con, run_id, status, t0, error=error)
    return status


def run_day(cfg, batch_date, trigger="manual", force=False) -> str:
    status = load_day(cfg, batch_date, trigger, {}, force)
    if status in ("ok", "warn", "skipped"):
        t = transform(cfg, trigger, batch_date)
        if t != "ok":
            return t
    return status


def daterange(start: str, end: str):
    d, last = date.fromisoformat(start), date.fromisoformat(end)
    while d <= last:
        yield d.isoformat()
        d += timedelta(days=1)


def backfill(cfg, start, end, dbt_once=False, keep_going=False, force=False) -> dict:
    extractors: dict = {}
    results = {}
    for d in daterange(start, end):
        if not (cfg.drops / f"dt={d}").exists():
            log.info("%s no drop (no reviews that day), skipping", d)
            continue
        results[d] = load_day(cfg, d, "backfill", extractors, force)
        if results[d] in ("ok", "warn", "skipped") and not dbt_once:
            t = transform(cfg, "backfill", d)
            if t != "ok":
                results[d] = t
        if results[d] not in ("ok", "warn", "skipped") and not keep_going:
            break
    if dbt_once and results:
        results["transform"] = transform(cfg, "backfill")
    return results


def score_version(cfg, version: str) -> tuple[int, int]:
    """Versioned backfill. Registers the version as a candidate (so daily runs
    keep scoring it), fills every review it hasn't seen, then builds and tests.
    The current views don't move until activate."""
    t0 = time.time()
    with warehouse.connect(cfg.db_path) as con:
        con.execute("insert into ops.model_versions (model_version, status) values (?, 'candidate') "
                    "on conflict do nothing", [version])
        run_id = start_run(con, f"score-version {version}", "manual")
        n, m = enrich.score_missing(con, Extractor(version))
        finish_run(con, run_id, "ok", t0, reviews_scored=n)
    log.info("%s scored %d reviews, %d mentions", version, n, m)
    transform(cfg, "score-version")
    return n, m


def activate(cfg, version: str) -> dict:
    with warehouse.connect(cfg.db_path) as con:
        row = con.execute("select status from ops.model_versions where model_version = ?", [version]).fetchone()
        if row is None:
            raise SystemExit(f"{version} is not registered, run score-version first")
        missing = con.execute("""select count(*) from raw.reviews r where not exists (
                                   select 1 from nlp.scored_reviews s
                                   where s.review_id = r.review_id and s.model_version = ?)""",
                              [version]).fetchone()[0]
        if missing:
            raise SystemExit(f"{version} is missing scores for {missing} reviews, finish score-version first")
        con.execute("begin")
        con.execute("update ops.model_versions set status = 'retired' where status = 'active'")
        con.execute("update ops.model_versions set status = 'active', activated_at = current_timestamp "
                    "where model_version = ?", [version])
        con.execute("commit")
    log.info("active model version is now %s", version)
    if transform(cfg, "activate") != "ok":
        raise SystemExit("dbt build failed after the swap, check ops.pipeline_runs before rolling back")
    return json.loads((cfg.serving / "_published.json").read_text())


def freshness(cfg, expect: str) -> bool:
    with warehouse.connect(cfg.db_path, read_only=True) as con:
        row = con.execute("""select status from ops.pipeline_runs where batch_date = ? and command = 'load'
                             and status in ('ok', 'warn', 'skipped') order by run_id desc limit 1""",
                          [expect]).fetchone()
    if row is None:
        send_alert(cfg, f"product finder SLA miss for {expect}", "no successful load for that batch")
    return row is not None


def status(cfg) -> None:
    with warehouse.connect(cfg.db_path, read_only=True) as con:
        for r in con.execute("""select run_id, batch_date, command, status, rows_in, rows_loaded, rows_quarantined,
                                       reviews_scored, seconds from ops.pipeline_runs
                                order by run_id desc limit 15""").fetchall():
            print(*r, sep="\t")
        print(con.execute("select * from ops.model_versions order by registered_at").fetchall())


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init")
    r = sub.add_parser("run")
    r.add_argument("batch_date")
    r.add_argument("--force", action="store_true", help="reload even if the same file is in the ledger")
    r.add_argument("--trigger", default="manual")
    ld = sub.add_parser("load", help="land, check and score only (Airflow task)")
    ld.add_argument("batch_date")
    ld.add_argument("--force", action="store_true")
    ld.add_argument("--trigger", default="manual")
    tr = sub.add_parser("transform", help="dbt build + publish only (Airflow task)")
    tr.add_argument("--batch-date")
    tr.add_argument("--trigger", default="manual")
    b = sub.add_parser("backfill")
    b.add_argument("start")
    b.add_argument("end")
    b.add_argument("--dbt-once", action="store_true", help="load every day, then one dbt build at the end")
    b.add_argument("--keep-going", action="store_true")
    b.add_argument("--force", action="store_true")
    sv = sub.add_parser("score-version")
    sv.add_argument("version")
    a = sub.add_parser("activate")
    a.add_argument("version")
    f = sub.add_parser("freshness")
    f.add_argument("--expect", required=True)
    sub.add_parser("status")
    sub.add_parser("publish")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = Settings.from_env()

    if args.cmd == "init":
        with warehouse.connect(cfg.db_path) as con:
            warehouse.init(con)
            n = warehouse.load_catalog(con, cfg.catalog)
            con.execute("insert into ops.model_versions (model_version, status, activated_at) "
                        "select 'rules-v1', 'active', current_timestamp "
                        "where not exists (select 1 from ops.model_versions)")
        log.info("warehouse ready at %s, %d products in catalog", cfg.db_path, n)
        dbt(cfg, "seed")
    elif args.cmd == "run":
        s = run_day(cfg, args.batch_date, args.trigger, args.force)
        sys.exit(0 if s in ("ok", "warn", "skipped") else 1)
    elif args.cmd == "load":
        s = load_day(cfg, args.batch_date, args.trigger, {}, args.force)
        sys.exit(0 if s in ("ok", "warn", "skipped") else 1)
    elif args.cmd == "transform":
        sys.exit(0 if transform(cfg, args.trigger, args.batch_date) == "ok" else 1)
    elif args.cmd == "backfill":
        res = backfill(cfg, args.start, args.end, args.dbt_once, args.keep_going, args.force)
        bad = {d: s for d, s in res.items() if s not in ("ok", "warn", "skipped")}
        counts = {s: sum(1 for v in res.values() if v == s) for s in set(res.values())}
        log.info("backfill done: %s %s", counts, bad or "")
        sys.exit(1 if bad else 0)
    elif args.cmd == "score-version":
        score_version(cfg, args.version)
    elif args.cmd == "activate":
        print(activate(cfg, args.version))
    elif args.cmd == "freshness":
        sys.exit(0 if freshness(cfg, args.expect) else 1)
    elif args.cmd == "status":
        status(cfg)
    elif args.cmd == "publish":
        print(publish(cfg))


if __name__ == "__main__":
    main()
