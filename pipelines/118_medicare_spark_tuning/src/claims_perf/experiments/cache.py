"""Experiment 5. Caching the enriched fact that all three marts read.

Without a cache every mart is its own job that scans claims again and redoes both joins.
With a cache the first mart fills it and the next two read it from memory.

Variants
  no_cache          the first version
  memory_and_disk   persist(MEMORY_AND_DISK) on only the columns the marts use, what the job does
  disk_only         persist(DISK_ONLY), for a machine where memory is the problem
"""
import json

from pyspark import StorageLevel
from pyspark.sql import functions as F

from .. import summarize
from . import common

COLS = ["npi", "state", "hcpcs_cd", "hcpcs_desc", "code_family", "provider_type", "place_of_service",
        "tot_benes", "tot_srvcs", "submitted_amt", "allowed_amt", "payment_amt"]


def main(argv=None):
    args = common.parse_args("cache", argv)
    spark, cfg, owned = common.session("cache")
    seen = {}

    def make(name, level):
        def go(s):
            df = summarize.enriched(s, cfg, args.year, summarize.JobOptions(cache_enriched=False)).select(*COLS)
            if level is not None:
                df = df.persist(level)
            stats = summarize.code_stats(df)
            marts = {"code_specialty": summarize.code_specialty(df, None),
                     "specialty_state": summarize.specialty_state(df),
                     "code_outliers": summarize.flag_outliers(df, F.broadcast(stats))}
            out = {}
            for mart, m in marts.items():
                r = m.agg(F.count("*").alias("n"), F.round(F.sum("payment_amt"), 2).alias("p")).first()
                out[mart] = [r["n"], float(r["p"] or 0)]
            seen[name] = common.storage_info(s)
            if level is not None:
                df.unpersist(blocking=True)
            return out
        return go

    variants = [
        common.Variant("no_cache", make("no_cache", None), note="each mart rescans and rejoins"),
        common.Variant("memory_and_disk", make("memory_and_disk", StorageLevel.MEMORY_AND_DISK),
                       note="pruned columns, filled by the first mart", after=lambda s: seen.get("memory_and_disk")),
        common.Variant("disk_only", make("disk_only", StorageLevel.DISK_ONLY), note="pruned columns on local disk",
                       after=lambda s: seen.get("disk_only")),
    ]
    # code_outliers uses percentile_approx, which can move a few borderline lines between plans
    result = common.run("cache", variants, args, spark, cfg, same_answer=False)
    exact = {json.dumps({k: a[k] for k in ("code_specialty", "specialty_state")}) for a in
             (v["answer"] for v in result["variants"])}
    flagged = [v["answer"]["code_outliers"][0] for v in result["variants"]]
    if len(exact) != 1 or max(flagged) - min(flagged) > 0.005 * max(flagged):
        raise AssertionError(f"cache: variants disagree {[v['answer'] for v in result['variants']]}")
    common.finish(spark, owned)


if __name__ == "__main__":
    main()
