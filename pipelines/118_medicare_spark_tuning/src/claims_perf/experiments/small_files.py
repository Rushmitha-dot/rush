"""Experiment 3. Small files from partitionBy("state") after a shuffle, and what fixes them.

The first version of the stage job dropped duplicate keys with dropDuplicates, which shuffles
into spark.sql.shuffle.partitions (200) tasks. Each task then writes one file for every state it
holds, so a year came out as about 200 x 60 files.

Variants (each writes the year's fact to scratch, then a downstream read query is timed)
  shuffle_200       AQE off, dropDuplicates then partitionBy
  aqe_coalesce      AQE on, it merges small shuffle partitions before the write
  repartition_state AQE on, repartition("state") before partitionBy plus maxRecordsPerFile, what the job does
  compact           the shuffle_200 output read back and rewritten per state, the fix after the fact
"""
import shutil
import time

from pyspark.sql import functions as F

from .. import stage, summarize
from . import common


def files_and_bytes(path) -> dict:
    files = [p for p in path.rglob("*.parquet")]
    total = sum(p.stat().st_size for p in files)
    return {"files": len(files), "bytes": total, "avg_file_kb": round(total / len(files) / 1024, 1) if files else 0}


def read_query(spark, path) -> dict:
    """Timed separately from the write. What a downstream job does, aggregate the whole year."""
    t = time.perf_counter()
    df = spark.read.parquet(str(path))
    r = (df.groupBy("state", "hcpcs_cd").agg(F.sum("payment_amt").alias("p"))
         .agg(F.count("*").alias("groups"), F.round(F.sum("p"), 2).alias("pay")).first())
    return {"read_query_s": round(time.perf_counter() - t, 2), "groups": r["groups"], "pay": float(r["pay"])}


def main(argv=None):
    args = common.parse_args("small_files", argv)
    spark, cfg, owned = common.session("small_files")
    scratch = cfg.scratch / "small_files"
    shutil.rmtree(scratch, ignore_errors=True)
    scratch.mkdir(parents=True)
    src = summarize.read_year(spark, cfg.lake, "claims", args.year)
    reads = {}

    def target(name):
        return scratch / name

    def write(name, layout):
        def go(s):
            p = target(name)
            shutil.rmtree(p, ignore_errors=True)
            df = src.dropDuplicates(stage.KEY)
            if layout == "by_state":
                df.repartition("state").write.option("maxRecordsPerFile", stage.MAX_RECORDS_PER_FILE) \
                    .mode("overwrite").partitionBy("state").parquet(str(p))
            else:
                df.write.mode("overwrite").partitionBy("state").parquet(str(p))
            return files_and_bytes(p)
        return go

    def compact(s):
        src_path = target("shuffle_200")
        p = target("compact")
        shutil.rmtree(p, ignore_errors=True)
        s.read.parquet(str(src_path)).repartition("state").write \
            .option("maxRecordsPerFile", stage.MAX_RECORDS_PER_FILE).mode("overwrite") \
            .partitionBy("state").parquet(str(p))
        return files_and_bytes(p)

    def measure_read(name):
        def f(s):
            reads[name] = read_query(s, target(name))
            return reads[name]
        return f

    variants = [
        common.Variant("shuffle_200", write("shuffle_200", "as_is"), {"spark.sql.adaptive.enabled": "false"},
                       "dropDuplicates, 200 shuffle partitions", after=measure_read("shuffle_200")),
        common.Variant("aqe_coalesce", write("aqe_coalesce", "as_is"), {"spark.sql.adaptive.enabled": "true"},
                       "AQE merges shuffle partitions toward 64 MB", after=measure_read("aqe_coalesce")),
        common.Variant("repartition_state", write("repartition_state", "by_state"),
                       {"spark.sql.adaptive.enabled": "true"}, "repartition(state) + maxRecordsPerFile",
                       after=measure_read("repartition_state")),
        common.Variant("compact", compact, {"spark.sql.adaptive.enabled": "true"},
                       "rewrite the shuffle_200 output per state", after=measure_read("compact")),
    ]
    result = common.run("small_files", variants, args, spark, cfg, same_answer=False)
    # file counts differ by design. The data must not, so the downstream query results are compared instead.
    answers = {k: (v["groups"], v["pay"]) for k, v in reads.items()}
    if len(set(answers.values())) != 1:
        raise AssertionError(f"layouts disagree {answers}")
    shutil.rmtree(scratch, ignore_errors=True)
    common.finish(spark, owned)
    return result


if __name__ == "__main__":
    main()
