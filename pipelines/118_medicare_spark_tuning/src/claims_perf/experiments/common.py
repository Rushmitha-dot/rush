"""Timing harness shared by the experiments.

Each variant is a Spark conf plus a function that runs one action and returns a small value.
Variants run round robin (A, B, C, A, B, C, ...) so a slow minute on the shared machine hits
all of them, not just one. The first round is a warm-up and isn't counted, unless --no-warmup.
Every run gets its own job group, which is how its stages are found in the event log.
"""
import argparse
import json
import os
import platform
import statistics
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .. import metrics
from ..settings import Settings
from ..spark import get_spark, restore_conf, set_conf


@dataclass
class Variant:
    name: str
    action: Callable[[SparkSession], object]
    conf: dict = field(default_factory=dict)
    note: str = ""
    before: Callable[[SparkSession], None] | None = None
    after: Callable[[SparkSession], dict | None] | None = None


def parse_args(name: str, argv=None):
    p = argparse.ArgumentParser(prog=f"claims_perf.experiments.{name}")
    p.add_argument("--year", type=int, default=2024)
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--no-warmup", action="store_true")
    p.add_argument("--out", type=Path, default=Path("results") / f"{name}.json")
    return p.parse_args(argv)


def machine() -> dict:
    cpu = platform.processor()
    try:
        if platform.system() == "Darwin":
            cpu = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout
        mem = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (OSError, ValueError):
        mem = None
    return {"system": platform.system(), "cpu": cpu.strip(), "machine_cores": os.cpu_count(),
            "machine_ram_gb": round(mem / 2**30, 1) if mem else None}


def noop_write(df: DataFrame) -> None:
    """Runs the whole plan without writing anything, so only compute is timed."""
    df.write.format("noop").mode("overwrite").save()


def final_plan(df: DataFrame) -> str:
    return df._jdf.queryExecution().executedPlan().toString()


def run(name: str, variants: list[Variant], args, spark: SparkSession, cfg: Settings, extra: dict | None = None,
        same_answer: bool = True) -> dict:
    sc = spark.sparkContext
    rounds = args.runs + (0 if args.no_warmup else 1)
    timings = {v.name: [] for v in variants}
    runs_metrics = {v.name: [] for v in variants}
    answers, notes, warmups = {}, {}, {}
    for i in range(rounds):
        for v in variants:
            if v.before:
                v.before(spark)
            old = set_conf(spark, v.conf)
            group = f"{name}/{v.name}/{i}"
            sc.setJobGroup(group, group)
            started = time.perf_counter()
            try:
                answer = v.action(spark)
            finally:
                seconds = time.perf_counter() - started
                sc.setLocalProperty("spark.jobGroup.id", None)
                sc.setLocalProperty("spark.job.description", None)
                restore_conf(spark, old)
            m = metrics.group_metrics(spark, cfg, group)
            seen = (v.after(spark) or {}) if v.after else None
            answers[v.name] = answer
            if i == 0 and not args.no_warmup:
                warmups[v.name] = round(seconds, 2)
                continue
            if seen is not None:
                notes.setdefault(v.name, []).append(seen)
            timings[v.name].append(round(seconds, 2))
            runs_metrics[v.name].append(m)
            print(f"{name} {v.name} run {i}: {seconds:.2f}s", flush=True)

    out_variants = []
    for v in variants:
        ts = timings[v.name]
        med = statistics.median(ts)
        # metrics of the run closest to the median, with per-stage detail
        idx = min(range(len(ts)), key=lambda k: abs(ts[k] - med))
        m = runs_metrics[v.name][idx]
        out_variants.append({
            "name": v.name, "note": v.note, "conf": v.conf, "seconds": ts, "median_s": round(med, 2),
            "min_s": min(ts), "max_s": max(ts), "warmup_s": warmups.get(v.name), "answer": answers[v.name],
            "metrics": m, **({"observed": notes[v.name]} if v.name in notes else {}),
        })
    vals = [json.dumps(a, sort_keys=True, default=str) for a in answers.values()]
    result = {
        "experiment": name, "year": args.year, "runs": args.runs, "warmup": not args.no_warmup,
        "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "spark": {"version": spark.version, "master": sc.master,
                  "driver_memory": sc.getConf().get("spark.driver.memory"),
                  "java": sc._jvm.System.getProperty("java.version")},
        "machine": machine(),
        "all_variants_same_answer": len(set(vals)) == 1 if same_answer else None,
        **(extra or {}),
        "variants": out_variants,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, default=str))
    print(f"wrote {args.out}")
    if same_answer and not result["all_variants_same_answer"]:
        raise AssertionError(f"{name}: variants disagree {answers}")
    return result


def session(name: str) -> tuple[SparkSession, Settings, bool]:
    """The tests run every experiment inside their own session, so only stop a session we started."""
    cfg = Settings()
    owned = SparkSession.getActiveSession() is None
    return get_spark(cfg, app=f"claims_perf_{name}"), cfg, owned


def finish(spark: SparkSession, owned: bool) -> None:
    if owned:
        spark.stop()


def storage_info(spark: SparkSession) -> dict:
    infos = spark.sparkContext._jsc.sc().getRDDStorageInfo()
    return {"cached_rdds": len(infos), "mem_bytes": sum(i.memSize() for i in infos),
            "disk_bytes": sum(i.diskSize() for i in infos)}
