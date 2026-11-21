"""Experiment 1. Skew in the outlier join.

The outlier mart joins every claim line to its code's peer stats on (hcpcs_cd, place_of_service).
The stats side is an aggregate, and without CBO Spark can't tell it's small, so the first version
planned a sort merge join and shuffled 9.8M lines on the code. A handful of office visit codes
(99214, 99213, 99204) hold over 12% of all lines, so their partitions are many times the median.

Variants
  smj               AQE off, the first version
  salted            AQE off, hot codes split 16 ways on the fact side, their stats rows copied 16 times
  aqe_skew_default  AQE on with its skew join defaults (runtime broadcast turned off to keep the join a SMJ)
  aqe_skew_tuned    same, with the skew threshold lowered to a size this data can reach
  broadcast         broadcast hint on the stats, what the job does now
"""
from pyspark.sql import functions as F

from .. import summarize
from . import common

SALT = 16
HOT_SHARE = 0.01  # codes with over 1% of all lines get salted

KEEP_SMJ = {"spark.sql.adaptive.autoBroadcastJoinThreshold": "-1"}


def base(spark, cfg, year):
    opts = summarize.JobOptions(cache_enriched=False)
    return summarize.enriched(spark, cfg, year, opts)


def salted_outliers(df, stats, hot_codes):
    hot = F.col("hcpcs_cd").isin(hot_codes)
    left = df.withColumn("salt", F.when(hot, F.pmod(F.xxhash64("npi"), F.lit(SALT))).otherwise(F.lit(0)))
    right = stats.withColumn(
        "salt", F.explode(F.when(F.col("hcpcs_cd").isin(hot_codes), F.sequence(F.lit(0), F.lit(SALT - 1)))
                          .otherwise(F.array(F.lit(0)))))
    j = left.join(right, ["hcpcs_cd", "place_of_service", "salt"]).drop("salt")
    ratio = F.col("tot_srvcs") / F.col("tot_benes")
    return j.filter((ratio > summarize.OUTLIER_MEDIAN_MULTIPLE * F.col("p50_srvcs_per_bene"))
                    & (ratio > F.col("p99_srvcs_per_bene")))


def main(argv=None):
    args = common.parse_args("skew", argv)
    spark, cfg, owned = common.session("skew")
    df = base(spark, cfg, args.year)
    total = df.count()
    top = (df.groupBy("hcpcs_cd").count().orderBy(F.desc("count")).limit(5).collect())
    hot_codes = [r["hcpcs_cd"] for r in df.groupBy("hcpcs_cd").count().filter(F.col("count") > HOT_SHARE * total)
                 .collect()]
    plans = {}

    def plain(name, broadcast=False):
        def go(s):
            stats = summarize.code_stats(df)
            out = summarize.flag_outliers(df, F.broadcast(stats) if broadcast else stats)
            a = out.agg(F.count("*").alias("n"), F.round(F.sum("payment_amt"), 2).alias("pay"))
            r = a.first()
            plans[name] = common.final_plan(a)
            return {"flagged_lines": r["n"], "flagged_payment": float(r["pay"] or 0)}
        return go

    def salted(s):
        stats = summarize.code_stats(df)
        a = salted_outliers(df, stats, hot_codes).agg(F.count("*").alias("n"),
                                                      F.round(F.sum("payment_amt"), 2).alias("pay"))
        r = a.first()
        plans["salted"] = common.final_plan(a)
        return {"flagged_lines": r["n"], "flagged_payment": float(r["pay"] or 0)}

    def observed(name):
        # the provider and HCPCS joins are always broadcast, so any sort merge join is the stats join
        return lambda s: {"join": "SortMergeJoin" if "SortMergeJoin" in plans.get(name, "") else
                          "BroadcastHashJoin", "skew_split_by_aqe": "skew=true" in plans.get(name, "")}

    aqe_off = {"spark.sql.adaptive.enabled": "false"}
    variants = [
        common.Variant("smj", plain("smj"), aqe_off, "first version", after=observed("smj")),
        common.Variant("salted", salted, aqe_off, f"{len(hot_codes)} hot codes x {SALT} salts",
                       after=observed("salted")),
        common.Variant("aqe_skew_default", plain("aqe_skew_default"),
                       {"spark.sql.adaptive.enabled": "true", **KEEP_SMJ}, "skew threshold 256 MB, factor 5",
                       after=observed("aqe_skew_default")),
        common.Variant("aqe_skew_tuned", plain("aqe_skew_tuned"),
                       {"spark.sql.adaptive.enabled": "true", **KEEP_SMJ,
                        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "8MB",
                        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "8MB"},
                       "skew threshold 8 MB, factor 5", after=observed("aqe_skew_tuned")),
        common.Variant("broadcast", plain("broadcast", broadcast=True), aqe_off, "broadcast hint on the stats",
                       after=observed("broadcast")),
    ]
    # percentile_approx merges its sketches in whatever order the plan produces, so p50 and p99 move
    # slightly between plans and a few borderline lines flip. The answers are compared within 0.5%.
    result = common.run("skew", variants, args, spark, cfg, same_answer=False, extra={
        "lines": total, "hot_codes": hot_codes, "salt_buckets": SALT,
        "top_codes": [{"hcpcs_cd": r["hcpcs_cd"], "lines": r["count"], "share": round(r["count"] / total, 4)}
                      for r in top]})
    counts = [v["answer"]["flagged_lines"] for v in result["variants"]]
    if (max(counts) - min(counts)) > 0.005 * max(counts):
        raise AssertionError(f"skew: variants disagree beyond 0.5% {counts}")
    common.finish(spark, owned)


if __name__ == "__main__":
    main()
