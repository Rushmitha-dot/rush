"""land -> stage -> summarize for one year, with the ledger deciding what needs to run.

Each step records what it read as its input key. A step runs again only when that key changes,
so a rerun of the same years does nothing, and republishing 2023 makes 2024's marts stale
(2024's code_specialty mart carries 2023's payments as the prior year).
"""
import logging
import os
import time

from . import alerts, checks, land, ops, sources, stage, summarize

log = logging.getLogger(__name__)


def source_url(year: int) -> str:
    # SOURCE_URL_2024=file:///... points a year at a local file, which is how the tests run
    url = os.getenv(f"SOURCE_URL_{year}") or sources.YEAR_URLS.get(year)
    if not url:
        raise KeyError(f"no source url for {year}")
    return url


def _timed(cfg, step, year, input_key, fn):
    run_id, started = ops.new_run_id(), time.time()
    try:
        status, detail = fn(run_id)
    except Exception as e:
        ops.record(cfg, step, year, input_key, run_id, "error", {"error": repr(e)[:500]}, time.time() - started,
                   started)
        alerts.send_alert(cfg, f"{step} {year} failed", repr(e)[:500])
        raise
    seconds = time.time() - started
    detail["seconds"] = round(seconds, 1)
    ops.record(cfg, step, year, input_key, run_id, status, detail, seconds, started)
    log.info("%s %s %s in %.1fs", step, year, status, seconds)
    return {"step": step, "year": year, "status": status, "run_id": run_id, **detail}


def land_step(cfg, year: int, force: bool = False) -> dict:
    url = source_url(year)
    key = f"{url}|{sources.head(url)['last_modified']}"
    done = ops.last_ok(cfg, "land", year)
    if not force and done and done["input_key"] == key and land.read_manifest(cfg, year):
        return {"step": "land", "year": year, "status": "skipped", "run_id": done["run_id"]}

    def go(run_id):
        m = land.land_year(cfg, year, url, key.split("|", 1)[1])
        return "ok", {k: m[k] for k in ("rows", "source_bytes", "sha256", "files")}
    return _timed(cfg, "land", year, key, go)


def stage_step(spark, cfg, year: int, force: bool = False, gates: checks.Gates | None = None) -> dict:
    landed = ops.last_ok(cfg, "land", year)
    if not landed:
        raise RuntimeError(f"{year} has not been landed")
    key = landed["run_id"]
    done = ops.last_ok(cfg, "stage", year)
    if not force and done and done["input_key"] == key:
        return {"step": "stage", "year": year, "status": "skipped", "run_id": done["run_id"]}
    prior = ops.last_ok(cfg, "stage", year - 1)
    prior_numbers = {"rows": prior["detail"]["rows"], "payment": prior["detail"]["payment"]} if prior else None

    def go(run_id):
        results, detail = stage.stage_year(spark, cfg, year, gates or checks.Gates(), prior_numbers)
        ops.record_checks(cfg, run_id, year, results)
        failed = [f"{r.name}={r.value}" for r in results if not r.passed]
        blocked = any(r.severity == "error" and not r.passed for r in results)
        if failed:
            alerts.send_alert(cfg, f"stage {year} {'blocked' if blocked else 'warnings'}", ", ".join(failed))
        detail["failed_checks"] = failed
        return ("blocked" if blocked else "ok"), detail
    return _timed(cfg, "stage", year, key, go)


def summarize_key(cfg, year: int) -> str:
    staged = ops.last_ok(cfg, "stage", year)
    if not staged:
        raise RuntimeError(f"{year} has no published stage")
    prior = ops.last_ok(cfg, "summarize", year - 1)
    return f"{staged['run_id']}|{prior['run_id'] if prior else '-'}"


def summarize_step(spark, cfg, year: int, force: bool = False,
                   opts: summarize.JobOptions = summarize.TUNED) -> dict:
    key = summarize_key(cfg, year)
    done = ops.last_ok(cfg, "summarize", year)
    if not force and done and done["input_key"] == key:
        return {"step": "summarize", "year": year, "status": "skipped", "run_id": done["run_id"]}

    def go(run_id):
        return "ok", summarize.write_marts(spark, cfg, year, opts)
    return _timed(cfg, "summarize", year, key, go)


def run_years(spark, cfg, years: list[int], force: bool = False, gates=None) -> list[dict]:
    """Years go in order, because a year's marts read the year before."""
    out = []
    for year in sorted(years):
        out.append(land_step(cfg, year))
        st = stage_step(spark, cfg, year, force=force, gates=gates)
        out.append(st)
        if st["status"] == "blocked" and not ops.last_ok(cfg, "stage", year):
            continue  # nothing good was ever published for this year
        out.append(summarize_step(spark, cfg, year, force=force))
    return out


def backfill(spark, cfg, first: int, last: int, gates=None) -> list[dict]:
    """Rebuilds stage and marts for every year in the range from what's landed. Landing is redone
    only if CMS changed the file."""
    return run_years(spark, cfg, list(range(first, last + 1)), force=True, gates=gates)


def freshness(cfg, expected_years: list[int], catalog_years: list[int] | None = None) -> list[str]:
    problems = []
    for year in expected_years:
        st = ops.last_ok(cfg, "stage", year)
        sm = ops.last_ok(cfg, "summarize", year)
        if not st:
            problems.append(f"{year} not staged")
        elif not sm:
            problems.append(f"{year} not summarized")
        elif sm["input_key"] != summarize_key(cfg, year):
            problems.append(f"{year} marts are older than their inputs")
    if catalog_years:
        new = sorted(y for y in catalog_years if y > max(expected_years))
        if new:
            problems.append(f"CMS lists data years not configured here: {new}")
    return problems
