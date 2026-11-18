"""Proof that the incremental path ends in the same place as a full load.

Three comparisons:

1. monthly_parity: when a monthly full file arrives on top of weekly-built state,
   every row the weeklies already covered should hash the same. This runs inside
   every monthly load and as a dry run (check_monthly) before one.
2. compare_tables: a from-scratch rebuild (separate lake) against the lake that
   was built one file at a time.
3. compare_versions: the dimension before and after a backfill, by Delta version.
"""
import logging
import tempfile
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .bronze import read_landed_csv
from .checks import WARN, CheckResult, quarantine_reason
from .landing import land_zip
from .settings import Settings
from .silver import classify, stage

log = logging.getLogger(__name__)

MISS_TYPES = ("new", "changed", "deactivated", "reactivated", "same_day_fix")
NOT_COMPARED = ("loaded_at",)


def monthly_parity(spark: SparkSession, cfg: Settings, classified: DataFrame, state_end: date) -> dict:
    covered = classified.filter(F.col("valid_from") <= F.lit(state_end))
    by_type = {r["change_type"]: r["count"] for r in covered.groupBy("change_type").count().collect()}
    misses = sum(by_type.get(t, 0) for t in MISS_TYPES)

    current = spark.table(cfg.table("silver", "dim_provider")).filter("is_current").select("npi")
    missing = current.join(classified.select("npi"), "npi", "left_anti").count()
    sample = [r["npi"] for r in covered.filter(F.col("change_type").isin(*MISS_TYPES)).select("npi").limit(10)
              .collect()]
    out = {"state_end": str(state_end), "rows_covered": sum(by_type.values()), "by_type": by_type,
           "misses": misses, "in_state_not_in_full": missing, "sample_npis": sample,
           "newer_than_state": classified.filter(F.col("valid_from") > F.lit(state_end)).count()}
    log.info("monthly parity %s", out)
    return out


def as_check(result: dict) -> CheckResult:
    bad = result["misses"] + result["in_state_not_in_full"]
    return CheckResult("monthly_parity_misses", WARN, bad == 0, bad, "== 0",
                       f"{result['misses']} changed rows the weeklies should have had, "
                       f"{result['in_state_not_in_full']} NPIs missing from the full file")


def check_monthly(spark: SparkSession, cfg: Settings, zip_path: Path, state_end: date) -> dict:
    """Dry run: land a monthly full into a temp folder and compare it to current state, no merge."""
    from .pipeline import known_npis, table_version

    with tempfile.TemporaryDirectory(dir=cfg.home) as tmp:
        landed = land_zip(zip_path, "monthly", Path(tmp), cfg.scope_states, known_npis(spark, cfg))
        staged = stage(read_landed_csv(spark, landed.path, landed.header), landed.period_end)
        clean = staged.filter(quarantine_reason({c: F.col(c) for c in staged.columns}).isNull())
        version = table_version(spark, cfg.table("silver", "dim_provider"))
        return monthly_parity(spark, cfg, classify(spark, cfg, clean, version), state_end)


def _diff(a: DataFrame, b: DataFrame) -> dict:
    cols = sorted(c for c in a.columns if c not in NOT_COMPARED)
    a, b = a.select(cols), b.select(cols)
    return {"rows_a": a.count(), "rows_b": b.count(), "only_in_a": a.exceptAll(b).count(),
            "only_in_b": b.exceptAll(a).count()}


def compare_tables(spark: SparkSession, path_a: str, path_b: str) -> dict:
    out = _diff(spark.read.format("delta").load(path_a), spark.read.format("delta").load(path_b))
    out["equal"] = out["only_in_a"] == 0 and out["only_in_b"] == 0
    return out


def compare_versions(spark: SparkSession, table: str, version_a: int, version_b: int) -> dict:
    a = spark.read.option("versionAsOf", version_a).table(table)
    b = spark.read.option("versionAsOf", version_b).table(table)
    out = _diff(a, b)
    out["equal"] = out["only_in_a"] == 0 and out["only_in_b"] == 0
    return out
