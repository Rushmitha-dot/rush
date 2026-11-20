"""Stage and task numbers for one job group, read back from Spark's event log.

The Spark UI shows the same numbers, but it's gone when the session stops, and CI has no UI.
The event log is a JSON line per listener event. Job start events carry the job group in their
properties, so every timed run sets its own group and gets back only its own stages.
"""
import json
import statistics
import time
from pathlib import Path


def event_log_path(spark, cfg) -> Path:
    app_id = spark.sparkContext.applicationId
    matches = sorted(cfg.eventlogs.glob(f"{app_id}*"))
    if not matches:
        raise FileNotFoundError(f"no event log for {app_id} in {cfg.eventlogs}")
    return matches[0]


def _events(path: Path):
    with open(path) as f:
        for line in f:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                return  # last line still being written


def group_metrics(spark, cfg, group: str, timeout: float = 30.0) -> dict:
    """Waits for the listener bus to write the group's job end events, then sums up its stages."""
    path = event_log_path(spark, cfg)
    deadline = time.time() + timeout
    while True:
        jobs, ended, stages, tasks = set(), set(), {}, {}
        for e in _events(path):
            kind = e.get("Event")
            if kind == "SparkListenerJobStart":
                if (e.get("Properties") or {}).get("spark.jobGroup.id") == group:
                    jobs.add(e["Job ID"])
                    for sid in e.get("Stage IDs", []):
                        stages.setdefault(sid, None)
            elif kind == "SparkListenerJobEnd" and e.get("Job ID") in jobs:
                ended.add(e["Job ID"])
            elif kind == "SparkListenerStageCompleted":
                info = e["Stage Info"]
                if info["Stage ID"] in stages:
                    stages[info["Stage ID"]] = info
            elif kind == "SparkListenerTaskEnd" and e.get("Stage ID") in stages:
                tasks.setdefault(e["Stage ID"], []).append(e)
        if jobs and jobs == ended:
            break
        if time.time() > deadline:
            raise TimeoutError(f"job group {group} never finished in the event log")
        time.sleep(0.3)
    return summarize_stages(stages, tasks, len(jobs))


def _task_numbers(t: dict) -> dict:
    m = t.get("Task Metrics") or {}
    sr = m.get("Shuffle Read Metrics") or {}
    sw = m.get("Shuffle Write Metrics") or {}
    info = t.get("Task Info") or {}
    return {
        "ms": (info.get("Finish Time", 0) - info.get("Launch Time", 0)),
        "run_ms": m.get("Executor Run Time", 0),
        "gc_ms": m.get("JVM GC Time", 0),
        "shuffle_read": sr.get("Remote Bytes Read", 0) + sr.get("Local Bytes Read", 0),
        "shuffle_records": sr.get("Total Records Read", 0),
        "shuffle_write": sw.get("Shuffle Bytes Written", 0),
        "spill_disk": m.get("Disk Bytes Spilled", 0),
        "spill_mem": m.get("Memory Bytes Spilled", 0),
        "input": (m.get("Input Metrics") or {}).get("Bytes Read", 0),
        "output": (m.get("Output Metrics") or {}).get("Bytes Written", 0),
    }


def summarize_stages(stages: dict, tasks: dict, n_jobs: int) -> dict:
    per_stage = []
    for sid, info in stages.items():
        if info is None or "Completion Time" not in info:
            continue  # skipped stage, its shuffle output was reused
        ts = [_task_numbers(t) for t in tasks.get(sid, [])]
        if not ts:
            continue
        durations = sorted(t["run_ms"] for t in ts)
        per_stage.append({
            "stage_id": sid,
            "name": info.get("Stage Name", "")[:80],
            "tasks": len(ts),
            "seconds": round((info["Completion Time"] - info["Submission Time"]) / 1000, 2),
            "task_ms_max": durations[-1],
            "task_ms_median": statistics.median(durations),
            "task_ms_sum": sum(durations),
            "shuffle_read_bytes": sum(t["shuffle_read"] for t in ts),
            "shuffle_read_records_max": max(t["shuffle_records"] for t in ts),
            "shuffle_read_records_median": statistics.median(t["shuffle_records"] for t in ts),
            "shuffle_write_bytes": sum(t["shuffle_write"] for t in ts),
            "spill_disk_bytes": sum(t["spill_disk"] for t in ts),
            "input_bytes": sum(t["input"] for t in ts),
            "output_bytes": sum(t["output"] for t in ts),
            "gc_ms": sum(t["gc_ms"] for t in ts),
        })
    per_stage.sort(key=lambda s: s["stage_id"])
    # the stage with the longest single task is where skew shows, if there is any
    worst = max(per_stage, key=lambda s: s["task_ms_max"], default=None)
    return {
        "jobs": n_jobs,
        "stages": len(per_stage),
        "tasks": sum(s["tasks"] for s in per_stage),
        "stage_seconds": round(sum(s["seconds"] for s in per_stage), 2),
        "shuffle_read_bytes": sum(s["shuffle_read_bytes"] for s in per_stage),
        "shuffle_write_bytes": sum(s["shuffle_write_bytes"] for s in per_stage),
        "spill_disk_bytes": sum(s["spill_disk_bytes"] for s in per_stage),
        "gc_ms": sum(s["gc_ms"] for s in per_stage),
        "worst_stage": worst,
        "per_stage": per_stage,
    }
