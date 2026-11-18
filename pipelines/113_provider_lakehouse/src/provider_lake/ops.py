"""Operational tables: load ledger, run log, check results, change log.

All Delta, in the ops schema next to the data, so on Databricks they get the
same Unity Catalog permissions and lineage as everything else.
"""
import uuid
from datetime import UTC, datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from .settings import Settings

DDL = {
    "load_ledger": """
        source_file string, source_sha256 string, kind string, period_start date, period_end date,
        status string, rows_read bigint, rows_landed bigint, rows_quarantined bigint,
        new_providers bigint, versions_opened bigint, same_day_fixes bigint, unchanged bigint, stale_skipped bigint,
        dim_version_before bigint, run_id string, loaded_at timestamp""",
    "pipeline_runs": """
        run_id string, command string, target string, status string, started_at timestamp,
        finished_at timestamp, seconds double, detail string""",
    "dq_results": """
        run_id string, source_file string, check_name string, severity string, passed boolean,
        observed double, threshold string, detail string, checked_at timestamp""",
    "watermarks": "name string, value bigint, updated_at timestamp",
    # stand-ins for Unity Catalog groups when running locally, see governance.py
    "group_members": "user_name string, group_name string",
    "analyst_state_access": "user_name string, state string",
}


def ensure_ops(spark: SparkSession, cfg: Settings) -> None:
    for schema in ("bronze", "silver", "gold", "ops", "analyst"):
        spark.sql(f"create schema if not exists {cfg.schema(schema)}")
    for name, cols in DDL.items():
        spark.sql(f"create table if not exists {cfg.table('ops', name)} ({cols}) using delta")


def now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def new_run_id() -> str:
    return uuid.uuid4().hex[:12]


def log_run(spark, cfg, run_id, command, target, status, started_at, detail=""):
    finished = now()
    row = [(run_id, command, target, status, started_at, finished, (finished - started_at).total_seconds(),
            detail[:2000])]
    spark.createDataFrame(row, spark.table(cfg.table("ops", "pipeline_runs")).schema) \
        .write.format("delta").mode("append").saveAsTable(cfg.table("ops", "pipeline_runs"))


def save_checks(spark, cfg, run_id, source_file, results):
    if not results:
        return
    t = now()
    rows = [(run_id, source_file, r.name, r.severity, r.passed, float(r.observed), r.threshold, r.detail, t)
            for r in results]
    spark.createDataFrame(rows, spark.table(cfg.table("ops", "dq_results")).schema) \
        .write.format("delta").mode("append").saveAsTable(cfg.table("ops", "dq_results"))


def ledger_current(spark, cfg):
    """Latest ledger row per file. The ledger is append-only, a rollback is just a newer row."""
    w = Window.partitionBy("source_file").orderBy(F.col("loaded_at").desc())
    return (spark.table(cfg.table("ops", "load_ledger")).withColumn("rn", F.row_number().over(w))
            .filter("rn = 1").drop("rn"))


def ledger_entry(spark, cfg, source_file):
    rows = ledger_current(spark, cfg).filter(F.col("source_file") == source_file).collect()
    return rows[0] if rows else None


def record_ledger(spark, cfg, entry: dict):
    entry = {**entry, "loaded_at": now()}
    schema = spark.table(cfg.table("ops", "load_ledger")).schema
    row = [tuple(entry.get(f.name) for f in schema.fields)]
    spark.createDataFrame(row, schema).write.format("delta").mode("append") \
        .saveAsTable(cfg.table("ops", "load_ledger"))


def loaded_files(spark, cfg):
    return ledger_current(spark, cfg).filter(F.col("status").startswith("loaded"))


def last_loaded_period_end(spark, cfg):
    return loaded_files(spark, cfg).filter(F.col("kind").isin("monthly", "weekly")) \
        .agg(F.max("period_end")).collect()[0][0]


def recent_rows(spark, cfg, kind, n=4) -> list[int]:
    rows = (loaded_files(spark, cfg).filter(F.col("kind") == kind)
            .orderBy(F.col("period_end").desc()).limit(n).select("rows_landed").collect())
    return [r[0] for r in rows]


def get_watermark(spark, cfg, name) -> int | None:
    rows = spark.table(cfg.table("ops", "watermarks")).filter(F.col("name") == name).collect()
    return rows[0]["value"] if rows else None


def set_watermark(spark, cfg, name, value: int):
    t = cfg.table("ops", "watermarks")
    spark.createDataFrame([(name, int(value), now())], spark.table(t).schema).createOrReplaceTempView("wm_src")
    spark.sql(f"""merge into {t} t using wm_src s on t.name = s.name
                  when matched then update set * when not matched then insert *""")
