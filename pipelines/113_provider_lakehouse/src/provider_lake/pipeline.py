"""One NPPES file through the lake: ledger -> land -> bronze -> gates -> SCD2 merge.

Reruns are safe twice over. The ledger skips a file whose name and checksum are
already loaded, and if that is bypassed (a backfill, a bug), every row hashes
the same as the current version and the merge changes nothing.
"""
import logging
import time
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import ops, parity
from .alerts import send_alert
from .bronze import contract_columns, load_bronze
from .checks import ERROR, US_STATE_CODES, WARN, failed, file_gates, quarantine_reason
from .landing import LandedFile, land_zip
from .settings import Settings
from .silver import classify, create_dimension, dedupe_report, merge_dimension, stage
from .sources import classify as classify_name
from .sources import sha256_of

log = logging.getLogger(__name__)


class LedgerConflict(Exception):
    """Same file name, different checksum. CMS republished it, or the download is bad."""


def table_version(spark: SparkSession, table: str) -> int:
    return spark.sql(f"describe history {table} limit 1").collect()[0]["version"]


def known_npis(spark: SparkSession, cfg: Settings) -> set[str]:
    dim = cfg.table("silver", "dim_provider")
    if not spark.catalog.tableExists(dim):
        return set()
    return {r[0] for r in spark.table(dim).select("npi").distinct().collect()}


def landed_manifest(cfg: Settings, zip_name: str) -> Path:
    return cfg.landing / "nppes" / Path(zip_name).stem / "manifest.json"


def load_nppes_zip(spark: SparkSession, cfg: Settings, zip_path: Path, run_id: str) -> dict:
    meta = classify_name(zip_path.name)
    if meta is None:
        raise ValueError(f"not an NPPES V2 file name: {zip_path.name}")
    sha = sha256_of(zip_path)
    entry = ops.ledger_entry(spark, cfg, zip_path.name)
    if entry and entry["status"].startswith("loaded"):
        if entry["source_sha256"] == sha:
            log.info("%s already loaded with the same checksum, nothing to do", zip_path.name)
            return {"source_file": zip_path.name, "status": "skipped"}
        raise LedgerConflict(f"{zip_path.name} was loaded with sha256 {entry['source_sha256'][:12]}, "
                             f"this copy is {sha[:12]}")

    # the landed subset doubles as our archive, NPPES drops old files from its page
    manifest = landed_manifest(cfg, zip_path.name)
    if manifest.exists() and LandedFile.read(manifest).source_sha256 == sha:
        landed = LandedFile.read(manifest)
        log.info("reusing landed subset for %s", zip_path.name)
    else:
        landed = land_zip(zip_path, meta.kind, cfg.landing / "nppes", cfg.scope_states,
                          known_npis(spark, cfg), sha256=sha)
    return process_landed(spark, cfg, landed, run_id)


def process_landed(spark: SparkSession, cfg: Settings, landed: LandedFile, run_id: str) -> dict:
    t0 = time.time()
    create_dimension(spark, cfg)
    dim = cfg.table("silver", "dim_provider")
    dim_version_before = table_version(spark, dim)
    state_end = ops.last_loaded_period_end(spark, cfg)

    load_bronze(spark, cfg, landed)
    bronze = spark.table(cfg.table("bronze", "nppes_npidata")).filter(F.col("_source_file") == landed.source_file)
    staged = stage(bronze, landed.period_end)
    staged = staged.withColumn("reason", quarantine_reason({c: F.col(c) for c in staged.columns})).cache()

    quarantine = staged.filter(F.col("reason").isNotNull()).select(
        F.lit(landed.source_file).alias("source_file"), "npi", "reason", "entity_type", "last_update_raw",
        F.to_json(F.struct(*[c for c in staged.columns if c != "reason"])).alias("row_json"),
        F.current_timestamp().alias("quarantined_at"))
    quarantine.write.format("delta").mode("overwrite") \
        .option("replaceWhere", f"source_file = '{landed.source_file}'") \
        .saveAsTable(cfg.table("silver", "provider_quarantine"))

    clean = staged.filter(F.col("reason").isNull()).drop("reason")
    rows = staged.count()
    n_quarantined = rows - clean.count()
    taxonomy = spark.table(cfg.table("gold", "dim_taxonomy")).select(F.col("taxonomy_code").alias("primary_taxonomy"))
    unknown_tax = clean.filter(F.col("primary_taxonomy").isNotNull()) \
        .join(F.broadcast(taxonomy), "primary_taxonomy", "left_anti").count()
    bad_state = clean.filter(F.col("entity_type").isNotNull()
                             & (F.col("practice_country").isNull() | (F.col("practice_country") == "US"))
                             & ~F.col("practice_state").isin(*US_STATE_CODES)).count()

    gates = file_gates(
        header=landed.header, contract=contract_columns(), rows=rows, quarantined=n_quarantined,
        duplicate_npis=dedupe_report(clean), period_start=date.fromisoformat(landed.period_start),
        period_end=date.fromisoformat(landed.period_end), last_period_end=state_end, kind=landed.kind,
        unknown_taxonomy=unknown_tax, invalid_state=bad_state,
        no_primary_flag=clean.filter("no_primary_flag").count(),
        recent_rows=ops.recent_rows(spark, cfg, landed.kind))

    ledger = {"source_file": landed.source_file, "source_sha256": landed.source_sha256, "kind": landed.kind,
              "period_start": date.fromisoformat(landed.period_start),
              "period_end": date.fromisoformat(landed.period_end), "rows_read": landed.rows_read,
              "rows_landed": landed.rows_kept, "rows_quarantined": n_quarantined,
              "dim_version_before": dim_version_before, "run_id": run_id}

    errors = failed(gates, ERROR)
    if errors:
        ops.save_checks(spark, cfg, run_id, landed.source_file, gates)
        ops.record_ledger(spark, cfg, {**ledger, "status": "blocked"})
        names = ", ".join(f"{r.name} ({r.observed})" for r in errors)
        send_alert(cfg, f"NPPES {landed.source_file} blocked", names)
        staged.unpersist()
        return {"source_file": landed.source_file, "status": "blocked", "errors": names}

    classified = classify(spark, cfg, clean, dim_version_before).cache()
    counts = {r["change_type"]: r["count"] for r in classified.groupBy("change_type").count().collect()}

    # A monthly full on top of existing state should change nothing the weeklies already
    # covered. Anything it does change is a miss in the incremental path.
    if landed.kind == "monthly" and state_end is not None:
        result = parity.monthly_parity(spark, cfg, classified, state_end)
        gates.append(parity.as_check(result))

    merge_dimension(spark, cfg, classified, landed.source_file)
    changes = classified.filter(~F.col("change_type").isin("unchanged")).select(
        F.lit(landed.source_file).alias("source_file"), "npi", "change_type", "changed_fields", "version_no",
        "valid_from", F.current_timestamp().alias("logged_at"))
    changes.write.format("delta").mode("overwrite") \
        .option("replaceWhere", f"source_file = '{landed.source_file}'") \
        .saveAsTable(cfg.table("silver", "provider_change_log"))
    classified.unpersist()
    staged.unpersist()

    ops.save_checks(spark, cfg, run_id, landed.source_file, gates)
    warns = failed(gates, WARN)
    status = "loaded_with_warnings" if warns else "loaded"
    ops.record_ledger(spark, cfg, {
        **ledger, "status": status, "new_providers": counts.get("new", 0),
        "versions_opened": sum(counts.get(k, 0) for k in ("changed", "deactivated", "reactivated")),
        "same_day_fixes": counts.get("same_day_fix", 0), "unchanged": counts.get("unchanged", 0),
        "stale_skipped": counts.get("stale", 0)})
    if warns:
        send_alert(cfg, f"NPPES {landed.source_file} loaded with warnings",
                   ", ".join(f"{r.name} ({r.observed:g})" for r in warns))
    out = {"source_file": landed.source_file, "status": status, "rows": rows, "quarantined": n_quarantined,
           "changes": counts, "seconds": round(time.time() - t0, 1)}
    log.info("%s", out)
    return out


def landed_files(cfg: Settings) -> list[LandedFile]:
    files = [LandedFile.read(p) for p in (cfg.landing / "nppes").glob("*/manifest.json")]
    return sorted(files, key=lambda f: (f.period_end, 0 if f.kind == "monthly" else 1))


def backfill(spark: SparkSession, cfg: Settings, since: date | None, run_id: str) -> list[dict]:
    """Rewind the dimension and replay landed files.

    since=None rebuilds from nothing. Otherwise the dimension is restored (Delta time travel)
    to the version it had before the first file ending after `since`, and every file from
    there on is replayed in order.
    """
    create_dimension(spark, cfg)
    dim = cfg.table("silver", "dim_provider")
    loaded = {r["source_file"]: r for r in ops.loaded_files(spark, cfg).collect()}
    files = landed_files(cfg)
    if since is not None:
        files = [f for f in files if date.fromisoformat(f.period_end) > since]
    if not files:
        log.info("nothing to backfill")
        return []

    first_loaded = [loaded[f.source_file] for f in files if f.source_file in loaded]
    if since is None:
        spark.sql(f"delete from {dim}")
    elif first_loaded:
        version = min(r["dim_version_before"] for r in first_loaded)
        log.info("restoring %s to version %s", dim, version)
        spark.sql(f"restore table {dim} to version as of {version}")

    names = [f.source_file for f in files]
    for t in ("provider_change_log", "provider_quarantine"):
        spark.table(cfg.table("silver", t))  # make sure it exists before deleting from it
        spark.sql(f"delete from {cfg.table('silver', t)} where source_file in ({_quoted(names)})")
    for name in names:
        if name in loaded:
            ops.record_ledger(spark, cfg, {**loaded[name].asDict(), "status": "rolled_back", "run_id": run_id})

    return [process_landed(spark, cfg, f, run_id) for f in files]


def _quoted(names):
    return ", ".join("'" + n.replace("'", "''") + "'" for n in names)
