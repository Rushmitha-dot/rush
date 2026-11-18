"""Dagster code location.

One partition per night (10 p.m. to 3 a.m., keyed by the evening's date):

  bluebikes_month_files -> raw_bluebikes_trips -> dbt models -> night_report

Bluebikes publishes a month at a time, so a sensor watches the bucket and,
when a new month file shows up, asks for a run over that month's nights.
Every partitioned asset has a single-run backfill policy, so a semester is
one run and dbt gets the whole range in one invocation.
"""

import json
import os
import sys
import warnings
from datetime import date, timedelta
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    BackfillPolicy,
    BetaWarning,
    DagsterRunStatus,
    DailyPartitionsDefinition,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    FreshnessPolicy,
    MaterializeResult,
    RunFailureSensorContext,
    RunRequest,
    RunStatusSensorContext,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    asset,
    define_asset_job,
    in_process_executor,
    run_failure_sensor,
    run_status_sensor,
    sensor,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from . import bluebikes, gtfs, report, stations
from .alerts import send_alert
from .calendar import load_calendar
from .load import connect, load_night, nights_between
from .settings import DBT_DIR, TZ, load_settings

# backfill_policy on @asset is marked beta; single-run backfills are the point here
warnings.filterwarnings("ignore", category=BetaWarning)

cfg = load_settings()
# dbt reads the warehouse path from the environment (see dbt/profiles.yml)
os.environ.setdefault("LATENIGHT_WAREHOUSE", str(cfg.warehouse))

nights = DailyPartitionsDefinition(start_date="2025-09-01", timezone=TZ)
single_run = BackfillPolicy.single_run()

dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
dbt_project.prepare_if_dev()

LOAD_CHECKS = {
    "source_months_present": AssetCheckSeverity.ERROR,
    "trips_present": AssetCheckSeverity.ERROR,
    "quarantine_share": AssetCheckSeverity.ERROR,
    "trip_volume": AssetCheckSeverity.WARN,
    "repeated_ride_ids": AssetCheckSeverity.WARN,
    "no_end_station_share": AssetCheckSeverity.WARN,
}


def _run_nights(context: AssetExecutionContext) -> list[date]:
    r = context.partition_key_range
    return nights_between(date.fromisoformat(r.start), date.fromisoformat(r.end))


@asset(
    partitions_def=nights,
    backfill_policy=single_run,
    group_name="nightly",
    kinds={"s3", "parquet"},
    description="Monthly Bluebikes trip files the run's nights need, cut down to trips near campus.",
)
def bluebikes_month_files(context: AssetExecutionContext) -> MaterializeResult:
    run = _run_nights(context)
    published = bluebikes.list_month_files()
    con = connect(cfg.warehouse)
    fetched, missing = [], []
    try:
        for month in bluebikes.months_for_nights(run[0], run[-1]):
            if month not in published:
                # not published yet; the load's ERROR check blocks the nights that need it
                missing.append(month)
                continue
            bluebikes.fetch_month(con, published[month], cfg.bluebikes_dir)
            fetched.append(month)
    finally:
        con.close()
    return MaterializeResult(metadata={"months": ", ".join(fetched), "not_published": ", ".join(missing)})


@asset(
    partitions_def=nights,
    backfill_policy=single_run,
    group_name="nightly",
    kinds={"duckdb"},
    deps=[bluebikes_month_files],
    check_specs=[AssetCheckSpec(name, asset="raw_bluebikes_trips", blocking=sev == AssetCheckSeverity.ERROR)
                 for name, sev in LOAD_CHECKS.items()],
    description="Late-night campus trips per night. ERROR checks block the night (and dbt), WARN checks alert.",
)
def raw_bluebikes_trips(context: AssetExecutionContext):
    calendar = load_calendar()
    con = connect(cfg.warehouse)
    results = []
    try:
        for night in _run_nights(context):
            results.append(load_night(con, night, calendar, run_id=context.run_id))
    finally:
        con.close()

    status: dict[str, int] = {}
    for r in results:
        status[r.status] = status.get(r.status, 0) + 1
    yield MaterializeResult(metadata={
        "nights": len(results),
        "status": json.dumps(status),
        "trips_loaded": sum(r.trips for r in results),
        "rows_quarantined": sum(r.quarantined for r in results),
    })

    problems = []
    for name, severity in LOAD_CHECKS.items():
        checked = [(r.night, c) for r in results for c in r.checks if c.name == name]
        failed = [(n, c) for n, c in checked if not c.passed]
        problems += [f"{n} {name}: {c.detail}" for n, c in failed]
        yield AssetCheckResult(
            check_name=name,
            passed=not failed,
            severity=severity,
            metadata={
                "nights_checked": len(checked),
                "failed_nights": ", ".join(str(n) for n, _ in failed[:20]),
                "first_failure": failed[0][1].detail if failed else "",
            },
        )
    if problems:
        send_alert(cfg.alert_webhook, f"late night load: {len(problems)} check failures", "\n".join(problems[:30]),
                   level="warn")


class Translator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            name = dbt_resource_props["name"]
            if name == "bluebikes_trips":
                return AssetKey("raw_bluebikes_trips")
            if name == "bluebikes_stations":
                return AssetKey("bluebikes_stations")
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        return "reference" if dbt_resource_props["resource_type"] == "seed" else "nightly"


@dbt_assets(
    manifest=dbt_project.manifest_path,
    exclude="resource_type:seed",
    partitions_def=nights,
    backfill_policy=single_run,
    dagster_dbt_translator=Translator(),
)
def late_night_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    r = context.partition_key_range
    dbt_vars = {"first_night": r.start, "last_night": r.end}
    yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream()


@dbt_assets(manifest=dbt_project.manifest_path, select="resource_type:seed", dagster_dbt_translator=Translator(),
            name="reference_seeds")
def reference_seeds(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["seed"], context=context).stream()


@asset(group_name="reference", kinds={"duckdb"},
       description="Current station list and dock counts from the Bluebikes GBFS feed.")
def bluebikes_stations() -> MaterializeResult:
    con = connect(cfg.warehouse)
    try:
        n = stations.refresh(con)
    finally:
        con.close()
    return MaterializeResult(metadata={"stations": n})


@asset(group_name="gtfs", kinds={"python"},
       description="Downloads MBTA GTFS (only if it changed) and rewrites the night speed and night stop seeds.")
def mbta_gtfs_reference() -> MaterializeResult:
    return MaterializeResult(metadata=gtfs.refresh())


MARTS = ["mart_nightly_demand", "mart_hourly_demand", "mart_duration_buckets", "mart_station_hotspots",
         "mart_peak_windows", "mart_transit_gap", "mart_bike_vs_bus_speed", "mart_data_quality"]


@asset(
    group_name="publish",
    kinds={"html"},
    deps=[AssetKey(["marts", m]) for m in MARTS],
    # the SLA: the page is rebuilt and current by 7 a.m. every day
    freshness_policy=FreshnessPolicy.cron(deadline_cron="0 7 * * *", lower_bound_delta=timedelta(hours=24),
                                          timezone=TZ),
    description="Static HTML report in site/, what GitHub Pages serves.",
)
def night_report() -> MaterializeResult:
    path = report.build(cfg.warehouse, cfg.site_dir)
    return MaterializeResult(metadata={"path": str(path), "bytes": path.stat().st_size})


nightly_job = define_asset_job("nightly", selection=AssetSelection.groups("nightly"), partitions_def=nights)
report_job = define_asset_job("report", selection=AssetSelection.assets(night_report, bluebikes_stations))

# rebuild the page every morning even when no new month landed, so the 7 a.m. SLA is about the pipeline running
report_schedule = ScheduleDefinition(job=report_job, cron_schedule="30 6 * * *", execution_timezone=TZ,
                                     default_status=DefaultScheduleStatus.RUNNING)


def month_night_ranges(published: set[str], done: set[str], first_month: str) -> list[tuple[str, date, date]]:
    """Night ranges to run for months that are published and not run yet.

    The last night of a month needs the next month's file (trips that end after
    midnight are filed by end time), so it waits until that file is out.
    """
    out = []
    for month in sorted(m for m in published if m >= first_month and m not in done):
        start = date(int(month[:4]), int(month[4:]), 1)
        end = (start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        if (end + timedelta(days=1)).strftime("%Y%m") not in published:
            end -= timedelta(days=1)
        out.append((month, start, end))
    return out


@sensor(job=nightly_job, minimum_interval_seconds=6 * 3600, default_status=DefaultSensorStatus.RUNNING)
def new_bluebikes_month(context: SensorEvaluationContext):
    published = bluebikes.list_month_files()
    done = set(json.loads(context.cursor)) if context.cursor else set()
    ranges = month_night_ranges(set(published), done, nights.start.strftime("%Y%m"))
    if not ranges:
        return SkipReason("no new month files")
    # a month is only marked done once its last night is covered, so it's picked up again next month
    complete = {m for m, start, end in ranges if (end + timedelta(days=1)).month != start.month}
    context.update_cursor(json.dumps(sorted(done | complete)))
    return [
        RunRequest(run_key=f"{m}:{published[m].etag}:{end}",
                   tags={"dagster/asset_partition_range_start": start.isoformat(),
                         "dagster/asset_partition_range_end": end.isoformat()})
        for m, start, end in ranges
    ]


@run_status_sensor(run_status=DagsterRunStatus.SUCCESS, monitored_jobs=[nightly_job], request_job=report_job,
                   default_status=DefaultSensorStatus.RUNNING)
def report_after_nightly(context: RunStatusSensorContext):
    return RunRequest(run_key=context.dagster_run.run_id)


@run_failure_sensor(default_status=DefaultSensorStatus.RUNNING)
def alert_on_failure(context: RunFailureSensorContext):
    send_alert(cfg.alert_webhook, f"late night run failed: {context.dagster_run.job_name}",
               f"run {context.dagster_run.run_id}\n{context.failure_event.message}")


defs = Definitions(
    assets=[bluebikes_month_files, raw_bluebikes_trips, late_night_dbt_models, reference_seeds, bluebikes_stations,
            mbta_gtfs_reference, night_report],
    jobs=[nightly_job, report_job],
    schedules=[report_schedule],
    sensors=[new_bluebikes_month, report_after_nightly, alert_on_failure],
    # use the dbt next to this interpreter, the venv isn't always on PATH
    resources={"dbt": DbtCliResource(project_dir=dbt_project, dbt_executable=str(Path(sys.executable).parent / "dbt"))},
    # DuckDB allows one writer, so steps run one after another in one process
    executor=in_process_executor,
)
