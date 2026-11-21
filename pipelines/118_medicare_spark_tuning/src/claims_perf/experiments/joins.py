"""Experiment 2. Broadcast vs sort merge join for the provider dimension.

Claims (9.8M lines) get the specialty from the provider dimension (1.2M NPIs). Spark broadcasts
on its own only when its size estimate for a side is under spark.sql.autoBroadcastJoinThreshold,
10 MB by default. The pruned provider side estimates above that, so the first version shuffled
both sides and sorted them.

Variants
  smj_forced         AQE off, broadcast turned off, the sort merge join a bigger dimension would get
  smj_default        AQE off, default 10 MB threshold. On the real data Spark estimated the pruned
                     provider side at 7.6 MB and broadcast it anyway, see the README
  broadcast_hint     AQE off, broadcast() on the provider side
  threshold_64mb     AQE off, threshold raised so Spark picks the broadcast itself
  aqe_default        AQE on, no hint. AQE can switch to a broadcast at runtime if the shuffled
                     provider side turns out small enough
"""
from pyspark.sql import functions as F

from .. import summarize
from . import common


def joined(spark, cfg, year, hint: bool):
    claims = summarize.read_year(spark, cfg.lake, "claims", year)
    providers = summarize.read_year(spark, cfg.lake, "providers", year).select("npi", "provider_type", "entity_cd",
                                                                               "ruca")
    if hint:
        providers = F.broadcast(providers)
    return claims.join(providers, "npi"), providers


def size_estimate(df) -> int:
    return int(str(df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()))


def main(argv=None):
    args = common.parse_args("joins", argv)
    spark, cfg, owned = common.session("joins")
    _, providers = joined(spark, cfg, args.year, hint=False)
    estimate = size_estimate(providers)
    plans = {}

    def make(name, hint):
        def go(s):
            df, _ = joined(s, cfg, args.year, hint)
            a = df.groupBy("provider_type").agg(F.count("*").alias("n"), F.round(F.sum("payment_amt"), 2).alias("p"))
            rows = sorted((r["provider_type"], r["n"], float(r["p"])) for r in a.collect())
            plans[name] = common.final_plan(a)
            return {"specialties": len(rows), "lines": sum(r[1] for r in rows),
                    "payment": round(sum(r[2] for r in rows), 2)}
        return go

    def observed(name):
        def f(s):
            p = plans.get(name, "")
            return {"join": "BroadcastHashJoin" if "BroadcastHashJoin" in p else "SortMergeJoin"}
        return f

    off = {"spark.sql.adaptive.enabled": "false"}
    variants = [
        common.Variant("smj_forced", make("smj_forced", False), {**off, "spark.sql.autoBroadcastJoinThreshold": "-1"},
                       "broadcast off, what a dimension over the threshold gets", after=observed("smj_forced")),
        common.Variant("smj_default", make("smj_default", False), off, "10 MB threshold",
                       after=observed("smj_default")),
        common.Variant("broadcast_hint", make("broadcast_hint", True), off, "broadcast() on providers",
                       after=observed("broadcast_hint")),
        common.Variant("threshold_64mb", make("threshold_64mb", False),
                       {**off, "spark.sql.autoBroadcastJoinThreshold": str(64 * 1024 * 1024)}, "64 MB threshold",
                       after=observed("threshold_64mb")),
        common.Variant("aqe_default", make("aqe_default", False), {"spark.sql.adaptive.enabled": "true"},
                       "AQE on, no hint", after=observed("aqe_default")),
    ]
    common.run("joins", variants, args, spark, cfg, extra={
        "provider_side_estimate_bytes": estimate,
        "auto_broadcast_threshold_bytes": 10 * 1024 * 1024,
        "providers": providers.count(),
        "provider_files_bytes": sum(p.stat().st_size for p in (cfg.lake / "providers" / f"year={args.year}")
                                    .glob("*.parquet")),
    })
    common.finish(spark, owned)


if __name__ == "__main__":
    main()
