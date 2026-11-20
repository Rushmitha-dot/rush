"""Landed strings to a typed claims fact, a provider dimension and an HCPCS dimension for one year.

Everything for the year is written to staging folders first. Gates run on what was written,
and only if no ERROR gate fails are the folders swapped in. A blocked year leaves the last
good version in place.
"""
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import checks, land, publish, sources

KEY = ["npi", "hcpcs_cd", "place_of_service"]

PROVIDER_COLS = {
    "Rndrng_NPI": "npi", "Rndrng_Prvdr_Last_Org_Name": "last_org_name", "Rndrng_Prvdr_First_Name": "first_name",
    "Rndrng_Prvdr_Crdntls": "credentials", "Rndrng_Prvdr_Ent_Cd": "entity_cd", "Rndrng_Prvdr_City": "city",
    "Rndrng_Prvdr_State_Abrvtn": "state", "Rndrng_Prvdr_State_FIPS": "state_fips", "Rndrng_Prvdr_Zip5": "zip5",
    "Rndrng_Prvdr_RUCA": "ruca", "Rndrng_Prvdr_Cntry": "country", "Rndrng_Prvdr_Type": "provider_type",
    "Rndrng_Prvdr_Mdcr_Prtcptg_Ind": "participating",
}

TABLES = ("claims", "providers", "hcpcs", "quarantine")

# a single file for California is ~1M rows. This keeps files around 50 MB without making many small ones.
MAX_RECORDS_PER_FILE = 500_000


def flag_rows(raw: DataFrame) -> DataFrame:
    df = checks.typed(raw.withColumnRenamed("Rndrng_Prvdr_State_Abrvtn", "state"))
    df = (df.withColumnRenamed("Rndrng_NPI", "npi").withColumnRenamed("HCPCS_Cd", "hcpcs_cd")
          .withColumnRenamed("Place_Of_Srvc", "place_of_service"))
    # Duplicates are found with an aggregate on the key (a narrow shuffle) and the few hits broadcast
    # back, rather than a window that would shuffle every wide row.
    dups = df.groupBy(*KEY).count().filter("count > 1").select(*KEY, F.lit(True).alias("_dup"))
    df = df.join(F.broadcast(dups), KEY, "left")
    return df.withColumn(
        "reasons",
        F.when(F.col("_dup"), F.concat(F.col("reasons"), F.array(F.lit("duplicate_key")))).otherwise(F.col("reasons")),
    ).drop("_dup")


def fact_columns(df: DataFrame, year: int) -> DataFrame:
    return df.select(
        F.lit(year).alias("year"), "state", *KEY, F.col("HCPCS_Drug_Ind").alias("drug_ind"),
        F.col("tot_benes").cast("long").alias("tot_benes"), "tot_srvcs",
        F.col("tot_bene_day_srvcs").cast("long").alias("tot_bene_day_srvcs"),
        "avg_submitted", "avg_allowed", "avg_payment", "avg_standardized",
        (F.col("avg_submitted") * F.col("tot_srvcs")).alias("submitted_amt"),
        (F.col("avg_allowed") * F.col("tot_srvcs")).alias("allowed_amt"),
        (F.col("avg_payment") * F.col("tot_srvcs")).alias("payment_amt"),
        (F.col("avg_standardized") * F.col("tot_srvcs")).alias("standardized_amt"),
    )


def write_fact(fact: DataFrame, path: str, layout: str = "by_state") -> None:
    """by_state is what the pipeline uses. as_is is the first version, kept for the small files experiment."""
    w = fact.drop("year")
    if layout == "by_state":
        w = w.repartition("state").write.option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
    elif layout == "as_is":
        w = w.write
    else:
        raise ValueError(layout)
    w.mode("overwrite").partitionBy("state").parquet(path)


def stage_year(spark: SparkSession, cfg, year: int, gates: checks.Gates, prior: dict | None,
               layout: str = "by_state") -> tuple[list[checks.Result], dict]:
    manifest = land.read_manifest(cfg, year)
    if manifest is None:
        raise FileNotFoundError(f"no landing manifest for {year}, run land first")
    for t in TABLES:
        publish.clean_leftovers(cfg.lake / t)
    raw = spark.read.parquet(str(cfg.landing / f"year={year}"))
    landed_columns = raw.columns
    flagged = flag_rows(raw).persist(StorageLevel.MEMORY_AND_DISK)
    good = flagged.filter(F.size("reasons") == 0)
    bad = flagged.filter(F.size("reasons") > 0)

    staging = {t: publish.staging_dir(cfg.lake / t, year) for t in TABLES}
    try:
        write_fact(fact_columns(good, year), str(staging["claims"]), layout)
        providers = good.select(
            "npi", *[F.col(src).alias(dst) for src, dst in PROVIDER_COLS.items() if dst not in ("npi", "state")],
            "state").dropDuplicates()
        providers.coalesce(1).write.mode("overwrite").parquet(str(staging["providers"]))
        (good.select("hcpcs_cd", F.col("HCPCS_Desc").alias("hcpcs_desc"), F.col("HCPCS_Drug_Ind").alias("drug_ind"))
         .dropDuplicates(["hcpcs_cd"]).coalesce(1).write.mode("overwrite").parquet(str(staging["hcpcs"])))
        (bad.select(F.concat_ws(";", "reasons").alias("reason"), *[c for c in raw.columns if c not in
                    ("Rndrng_NPI", "HCPCS_Cd", "Place_Of_Srvc", "Rndrng_Prvdr_State_Abrvtn")],
                    "npi", "hcpcs_cd", "place_of_service", "state")
         .coalesce(1).write.mode("overwrite").parquet(str(staging["quarantine"])))
        flagged.unpersist()

        fact = spark.read.parquet(str(staging["claims"]))
        summary = fact.agg(F.count("*").alias("rows"), F.sum("payment_amt").alias("payment"),
                           F.countDistinct(F.when(F.col("state").isin(*checks.US_STATES), F.col("state")))
                           .alias("us_states")).first()
        prov = spark.read.parquet(str(staging["providers"]))
        n_prov, n_npi = prov.count(), prov.select("npi").distinct().count()
        quarantined = spark.read.parquet(str(staging["quarantine"]))
        n_bad = quarantined.count()
        reasons = {r["reason"]: r["count"] for r in quarantined.groupBy("reason").count().collect()}
        results = checks.gate_results(
            manifest=manifest, landed_rows=raw.count(), landed_columns=landed_columns,
            expected_columns=[c for c in sources.COLUMNS if c not in sources.DROPPED_AT_LANDING],
            good_rows=summary["rows"], quarantined=n_bad, us_states=summary["us_states"],
            payment=float(summary["payment"] or 0), prior=prior, gates=gates)
        results.append(checks.Result("one_provider_row_per_npi", "error", n_prov == n_npi,
                                     f"{n_prov} rows, {n_npi} npis"))
        for name, value in checks.soft_flags(fact).items():
            results.append(checks.Result(f"soft_{name}", "warn", True, value))
        detail = {"rows": summary["rows"], "payment": float(summary["payment"] or 0), "quarantined": n_bad,
                  "providers": n_npi, "reasons": reasons, "files": count_files(staging["claims"]),
                  "landing_sha256": manifest["sha256"]}
        if any(r.severity == "error" and not r.passed for r in results):
            for p in staging.values():
                publish.discard(p)
            return results, detail
        for t, p in staging.items():
            publish.swap_in(p, cfg.lake / t, year)
        return results, detail
    except BaseException:
        flagged.unpersist()
        for p in staging.values():
            publish.discard(p)
        raise


def count_files(path) -> int:
    from pathlib import Path
    return sum(1 for _ in Path(path).rglob("*.parquet"))
