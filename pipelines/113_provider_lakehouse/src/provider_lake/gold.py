"""Gold star: Part D prescriber-by-drug facts joined to the provider version as of the claim year.

    fact_prescriber_drug -> dim_provider (SCD2 version valid on Dec 31 of the claim year)
                         -> dim_drug
    dim_provider         -> dim_taxonomy
"""
import csv
import logging

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from .checks import ERROR, WARN, CheckResult, npi_valid_col
from .settings import Settings

log = logging.getLogger(__name__)


def load_taxonomy(spark: SparkSession, cfg: Settings, csv_path: str) -> int:
    # NUCC definitions have line breaks inside quoted fields, so read it with the csv module
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        rows = [(r["Code"].strip(), r["Grouping"], r["Classification"], r["Specialization"] or None,
                 r["Display Name"], r["Section"]) for r in csv.DictReader(f)]
    df = spark.createDataFrame(rows, "taxonomy_code string, grouping string, classification string, "
                                     "specialization string, display_name string, section string")
    df.write.format("delta").mode("overwrite").saveAsTable(cfg.table("gold", "dim_taxonomy"))
    return len(rows)


def load_partd_year(spark: SparkSession, cfg: Settings, manifest: dict) -> tuple[int, list[CheckResult]]:
    """Landed Part D csv for one claim year -> silver, replacing only that year."""
    year = manifest["claim_year"]
    raw = spark.read.option("header", "true").option("escape", '"').option("mode", "FAILFAST") \
        .csv(manifest["path"])
    df = raw.select(
        F.lit(year).alias("claim_year"),
        F.col("Prscrbr_NPI").alias("npi"),
        F.col("Prscrbr_State_Abrvtn").alias("partd_state"),
        F.col("Prscrbr_Type").alias("partd_prescriber_type"),
        F.col("Prscrbr_Type_Src").alias("partd_type_source"),
        F.col("Brnd_Name").alias("brand_name"),
        F.col("Gnrc_Name").alias("generic_name"),
        F.col("Tot_Clms").cast("int").alias("total_claims"),
        F.col("Tot_30day_Fills").cast("double").alias("total_30day_fills"),
        F.col("Tot_Day_Suply").cast("int").alias("total_day_supply"),
        F.col("Tot_Drug_Cst").cast("decimal(14,2)").alias("total_drug_cost"),
        # blank when fewer than 11 beneficiaries, CMS suppresses small counts
        F.col("Tot_Benes").cast("int").alias("total_beneficiaries"),
        F.col("GE65_Tot_Clms").cast("int").alias("ge65_claims"),
        F.col("GE65_Sprsn_Flag").alias("ge65_suppression_flag"),
        F.lit(manifest["source_file"]).alias("source_file"),
    ).withColumn("npi_valid", npi_valid_col(F.col("npi")))

    rows = df.count()
    bad_npi = df.filter(~F.col("npi_valid")).count()
    dupes = df.groupBy("npi", "brand_name", "generic_name").count().filter("count > 1").count()
    bad_claims = df.filter(F.col("total_claims").isNull() | (F.col("total_claims") < 11)).count()
    results = [
        CheckResult("partd_rows_present", ERROR, rows > 0, rows, "> 0"),
        CheckResult("partd_npi_check_digit", ERROR, bad_npi == 0, bad_npi, "== 0"),
        CheckResult("partd_grain_unique", ERROR, dupes == 0, dupes, "== 0", "npi x brand x generic"),
        # CMS only publishes rows with 11+ claims, anything under that means the file changed
        CheckResult("partd_min_claims", WARN, bad_claims == 0, bad_claims, "== 0", "rows with < 11 claims"),
    ]
    if any(not r.passed and r.severity == ERROR for r in results):
        return rows, results

    table = cfg.table("silver", "partd_prescriber_drug")
    w = df.drop("npi_valid").write.format("delta").mode("overwrite").partitionBy("claim_year")
    if spark.catalog.tableExists(table):
        w = w.option("replaceWhere", f"claim_year = {year}")
    w.saveAsTable(table)
    return rows, results


def refresh_dim_provider(spark: SparkSession, cfg: Settings, full: bool = False) -> dict:
    """Gold provider dimension, kept in step with silver through the Change Data Feed.

    A full refresh is used on the first run and after a backfill, because a RESTORE on
    silver rewinds the table and the feed from before it no longer describes the state.
    """
    from .ops import get_watermark, set_watermark

    silver = cfg.table("silver", "dim_provider")
    gold = cfg.table("gold", "dim_provider")
    latest = spark.sql(f"describe history {silver} limit 1").collect()[0]["version"]
    wm = None if full else get_watermark(spark, cfg, "gold_dim_provider")
    tax = spark.table(cfg.table("gold", "dim_taxonomy")).select(
        F.col("taxonomy_code").alias("primary_taxonomy"), F.col("classification").alias("taxonomy_classification"),
        F.col("specialization").alias("taxonomy_specialization"))

    if wm is None or not spark.catalog.tableExists(gold):
        snap = spark.table(silver).join(F.broadcast(tax), "primary_taxonomy", "left")
        if spark.catalog.tableExists(gold):
            # insert overwrite keeps the table, so Unity Catalog masks and row filters stay attached.
            # Replacing the table with saveAsTable would drop them.
            snap.select(*spark.table(gold).columns).write.insertInto(gold, overwrite=True)
        else:
            snap.write.format("delta").saveAsTable(gold)
        set_watermark(spark, cfg, "gold_dim_provider", latest)
        return {"mode": "full", "rows": spark.table(gold).count(), "silver_version": latest}
    if wm >= latest:
        return {"mode": "noop", "rows": 0, "silver_version": latest}

    changes = (spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", wm + 1)
               .table(silver).filter(F.col("_change_type").isin("insert", "update_postimage")))
    w = Window.partitionBy("provider_sk").orderBy(F.col("_commit_version").desc())
    latest_rows = (changes.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
                   .drop("rn", "_change_type", "_commit_version", "_commit_timestamp")
                   .join(F.broadcast(tax), "primary_taxonomy", "left"))
    n = latest_rows.count()
    latest_rows.createOrReplaceTempView("gold_dim_changes")
    spark.sql(f"""merge into {gold} t using gold_dim_changes s on t.provider_sk = s.provider_sk
                  when matched then update set * when not matched then insert *""")
    set_watermark(spark, cfg, "gold_dim_provider", latest)
    return {"mode": "cdf", "rows": n, "silver_version": latest}


def build_year(spark: SparkSession, cfg: Settings, year: int) -> dict:
    facts = spark.table(cfg.table("silver", "partd_prescriber_drug")).filter(F.col("claim_year") == year)
    dim = spark.table(cfg.table("silver", "dim_provider")).select("npi", "provider_sk", "version_no",
                                                                   "valid_from", "valid_to")
    year_end = F.make_date(F.lit(year), F.lit(12), F.lit(31))

    # The dimension only knows a provider's state from its Last Update Date onward. If that
    # date is after the claim year, there's no version for the year end, and the fact falls
    # back to the earliest version we have. The flag says which one was used.
    npis = facts.select("npi").distinct()
    cand = (npis.join(dim, "npi")
            .withColumn("covers", (F.col("valid_from") <= year_end) & (F.col("valid_to") > year_end)))
    w = Window.partitionBy("npi").orderBy(F.col("covers").desc(), F.col("version_no"))
    pick = (cand.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
            .select("npi", "provider_sk",
                    F.when(F.col("covers"), "as_of_year_end").otherwise("earliest_known").alias("provider_match")))

    fact = (facts.join(pick, "npi", "left")
            .withColumn("provider_match", F.coalesce("provider_match", F.lit("not_in_registry_scope")))
            .withColumn("drug_sk", F.xxhash64("brand_name", "generic_name"))
            .select("claim_year", "provider_sk", "drug_sk", "npi", "provider_match", "partd_prescriber_type",
                    "total_claims", "total_30day_fills", "total_day_supply", "total_drug_cost",
                    "total_beneficiaries", "ge65_claims"))

    drugs = facts.select(F.xxhash64("brand_name", "generic_name").alias("drug_sk"), "brand_name",
                         "generic_name").distinct()
    dim_drug = cfg.table("gold", "dim_drug")
    if spark.catalog.tableExists(dim_drug):
        drugs.createOrReplaceTempView("drug_src")
        spark.sql(f"merge into {dim_drug} t using drug_src s on t.drug_sk = s.drug_sk when not matched then insert *")
    else:
        drugs.write.format("delta").saveAsTable(dim_drug)

    table = cfg.table("gold", "fact_prescriber_drug")
    w = fact.write.format("delta").mode("overwrite").partitionBy("claim_year")
    if spark.catalog.tableExists(table):
        w = w.option("replaceWhere", f"claim_year = {year}")
    w.saveAsTable(table)

    stats = {r["provider_match"]: r["n"] for r in spark.table(table).filter(F.col("claim_year") == year)
             .groupBy("provider_match").agg(F.countDistinct("npi").alias("n")).collect()}
    return {"year": year, "rows": spark.table(table).filter(F.col("claim_year") == year).count(),
            "prescribers_by_match": stats}


def build_marts(spark: SparkSession, cfg: Settings) -> None:
    spark.sql(f"""
        create or replace table {cfg.table('gold', 'mart_cost_by_specialty')} using delta as
        select f.claim_year,
               coalesce(p.taxonomy_classification, 'not in registry scope') as taxonomy_classification,
               count(distinct f.npi) as prescribers,
               sum(f.total_claims) as claims,
               sum(f.total_drug_cost) as drug_cost
        from {cfg.table('gold', 'fact_prescriber_drug')} f
        left join {cfg.table('gold', 'dim_provider')} p on f.provider_sk = p.provider_sk
        group by 1, 2
    """)
