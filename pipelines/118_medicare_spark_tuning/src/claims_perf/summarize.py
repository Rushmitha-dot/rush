"""The nightly job. Provider payments by service code and specialty for one data year.

Three marts come out of one enriched fact (claims joined to the provider and HCPCS dimensions)

  code_specialty   year x HCPCS code x specialty, with the prior year's payment next to it
  specialty_state  year x specialty x provider state
  code_outliers    provider lines billing far more services per beneficiary than peers on the same code

JobOptions switches each fix on or off, so the first version of the job ("baseline") and the
tuned one run the same code path.
"""
from dataclasses import asdict, dataclass

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import codes, publish
from .spark import restore_conf, set_conf

MARTS = ("code_specialty", "specialty_state", "code_outliers")

# a line is an outlier when services per beneficiary is over both 3x the code's median and its 99th percentile
OUTLIER_MEDIAN_MULTIPLE = 3.0
MIN_PEERS = 30


@dataclass(frozen=True)
class JobOptions:
    aqe: bool = True
    broadcast_joins: bool = True
    native_family: bool = True
    cache_enriched: bool = True
    files_per_mart: int | None = 1

    def conf(self) -> dict:
        return {"spark.sql.adaptive.enabled": str(self.aqe).lower()}


BASELINE = JobOptions(aqe=False, broadcast_joins=False, native_family=False, cache_enriched=False,
                      files_per_mart=None)
TUNED = JobOptions()


def read_year(spark: SparkSession, root, table: str, year: int) -> DataFrame:
    return spark.read.parquet(str(root / table / f"year={year}"))


def enriched(spark: SparkSession, cfg, year: int, opts: JobOptions) -> DataFrame:
    claims = read_year(spark, cfg.lake, "claims", year)
    providers = read_year(spark, cfg.lake, "providers", year).select("npi", "provider_type", "entity_cd", "ruca")
    hcpcs = read_year(spark, cfg.lake, "hcpcs", year).select("hcpcs_cd", "hcpcs_desc")
    if opts.broadcast_joins:
        # 1.2M providers is ~20 MB on disk after pruning to these columns, over the 10 MB auto threshold,
        # but it fits in memory easily and saves shuffling 9.8M claim rows
        providers = F.broadcast(providers)
    df = claims.join(providers, "npi").join(F.broadcast(hcpcs), "hcpcs_cd")
    family = codes.code_family_col(F.col("hcpcs_cd")) if opts.native_family else codes.code_family_udf("hcpcs_cd")
    df = df.withColumn("code_family", family)
    return df


def code_specialty(df: DataFrame, prior: DataFrame | None) -> DataFrame:
    out = (df.groupBy("hcpcs_cd", "hcpcs_desc", "code_family", "provider_type")
           .agg(F.countDistinct("npi").alias("providers"), F.count("*").alias("lines"),
                F.sum("tot_srvcs").alias("services"), F.sum("submitted_amt").alias("submitted_amt"),
                F.sum("allowed_amt").alias("allowed_amt"), F.sum("payment_amt").alias("payment_amt")))
    by_code = Window.partitionBy("hcpcs_cd")
    out = (out.withColumn("payment_per_service", F.col("payment_amt") / F.col("services"))
           .withColumn("share_of_code_payment", F.col("payment_amt") / F.sum("payment_amt").over(by_code)))
    if prior is None:
        return out.withColumn("payment_amt_prior_year", F.lit(None).cast("double"))
    prior = prior.select("hcpcs_cd", "provider_type", F.col("payment_amt").alias("payment_amt_prior_year"))
    return out.join(prior, ["hcpcs_cd", "provider_type"], "left")


def specialty_state(df: DataFrame) -> DataFrame:
    return (df.groupBy("provider_type", "state")
            .agg(F.countDistinct("npi").alias("providers"), F.sum("tot_srvcs").alias("services"),
                 F.sum("payment_amt").alias("payment_amt"))
            .withColumn("payment_per_provider", F.col("payment_amt") / F.col("providers")))


def code_stats(df: DataFrame) -> DataFrame:
    ratio = F.col("tot_srvcs") / F.col("tot_benes")
    return (df.groupBy("hcpcs_cd", "place_of_service")
            .agg(F.count("*").alias("peers"),
                 F.percentile_approx(ratio, 0.5, 10000).alias("p50_srvcs_per_bene"),
                 F.percentile_approx(ratio, 0.99, 10000).alias("p99_srvcs_per_bene"))
            .filter(F.col("peers") >= MIN_PEERS))


def flag_outliers(df: DataFrame, stats: DataFrame) -> DataFrame:
    j = df.join(stats, ["hcpcs_cd", "place_of_service"])
    ratio = F.col("tot_srvcs") / F.col("tot_benes")
    return (j.withColumn("srvcs_per_bene", ratio)
            .filter((F.col("srvcs_per_bene") > OUTLIER_MEDIAN_MULTIPLE * F.col("p50_srvcs_per_bene"))
                    & (F.col("srvcs_per_bene") > F.col("p99_srvcs_per_bene")))
            .select("npi", "provider_type", "state", "hcpcs_cd", "hcpcs_desc", "code_family", "place_of_service",
                    "tot_benes", "tot_srvcs", "srvcs_per_bene", "p50_srvcs_per_bene", "p99_srvcs_per_bene",
                    "peers", "payment_amt"))


def build_marts(spark: SparkSession, cfg, year: int, opts: JobOptions) -> dict[str, DataFrame]:
    df = enriched(spark, cfg, year, opts)
    if opts.cache_enriched:
        cols = ["npi", "state", "hcpcs_cd", "hcpcs_desc", "code_family", "provider_type", "place_of_service",
                "tot_benes", "tot_srvcs", "submitted_amt", "allowed_amt", "payment_amt"]
        df = df.select(*cols).persist(StorageLevel.MEMORY_AND_DISK)
    prior_path = cfg.marts / "code_specialty" / f"year={year - 1}"
    prior = spark.read.parquet(str(prior_path)) if prior_path.exists() else None
    stats = code_stats(df)
    return {
        "code_specialty": code_specialty(df, prior),
        "specialty_state": specialty_state(df),
        # Spark can't tell from plan stats that an aggregate over 9.8M rows comes out at ~9k rows, so
        # without the hint (or AQE) it plans a sort merge join and shuffles the fact on hcpcs_cd
        "code_outliers": flag_outliers(df, F.broadcast(stats) if opts.broadcast_joins else stats),
        "_enriched": df,
    }


def write_marts(spark: SparkSession, cfg, year: int, opts: JobOptions, out_root=None) -> dict:
    """Builds and writes the three marts for a year. out_root lets experiments write to scratch."""
    root = out_root or cfg.marts
    old = set_conf(spark, opts.conf())
    staging = {}
    try:
        marts = build_marts(spark, cfg, year, opts)
        counts = {}
        for name in MARTS:
            publish.clean_leftovers(root / name)
            staging[name] = publish.staging_dir(root / name, year)
            df = marts[name]
            if opts.files_per_mart:
                # repartition, not coalesce. coalesce(1) would run the outlier join itself in one task.
                df = df.repartition(opts.files_per_mart)
            df.write.mode("overwrite").parquet(str(staging[name]))
            counts[name] = spark.read.parquet(str(staging[name])).count()
        if opts.cache_enriched:
            marts["_enriched"].unpersist()
        for name, p in staging.items():
            publish.swap_in(p, root / name, year)
        return {"rows": counts, "options": asdict(opts)}
    except BaseException:
        for p in staging.values():
            publish.discard(p)
        raise
    finally:
        restore_conf(spark, old)
