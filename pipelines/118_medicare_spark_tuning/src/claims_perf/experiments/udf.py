"""Experiment 4. HCPCS code family as a Python UDF, a pandas UDF and native column functions.

A row at a time Python UDF ships every value to a Python worker and back through pickling.
A pandas UDF moves Arrow batches, which is much cheaper. Native functions stay in the JVM and
Spark can generate code for them, so there's no Python worker at all.
"""
from pyspark.sql import functions as F

from .. import codes, summarize
from ..spark import arrow_works
from . import common


def main(argv=None):
    args = common.parse_args("udf", argv)
    spark, cfg, owned = common.session("udf")
    claims = summarize.read_year(spark, cfg.lake, "claims", args.year).select("hcpcs_cd", "payment_amt")

    def make(fn):
        def go(s):
            a = claims.withColumn("family", fn(F.col("hcpcs_cd"))).groupBy("family") \
                .agg(F.count("*").alias("n"), F.round(F.sum("payment_amt"), 2).alias("p"))
            return {r["family"]: [r["n"], float(r["p"])] for r in a.collect()}
        return go

    arrow_ok, why = arrow_works(spark)
    variants = [
        common.Variant("python_udf", make(codes.code_family_udf), note="row at a time, the first version"),
        common.Variant("pandas_udf", make(codes.code_family_pandas), note="Arrow batches of 10,000"),
        common.Variant("native", make(codes.code_family_col), note="when / substring / rlike, what the job uses"),
    ]
    if not arrow_ok:
        variants = [v for v in variants if v.name != "pandas_udf"]
    common.run("udf", variants, args, spark, cfg, extra={"lines": claims.count(),
                                                         "pandas_udf_skipped": why or None})
    common.finish(spark, owned)


if __name__ == "__main__":
    main()
