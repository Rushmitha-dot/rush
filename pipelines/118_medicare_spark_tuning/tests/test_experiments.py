"""Every experiment runs once on the fixture lake, end to end, including the event log metrics.
The timings mean nothing at this size. The point is that the scripts and the answer checks work."""
import json

import pytest
from conftest import TEST_GATES

from claims_perf import metrics, pipeline
from claims_perf.experiments import cache, job, joins, skew, small_files, udf


@pytest.fixture
def lake(spark, cfg, sources_2y):
    pipeline.run_years(spark, cfg, [2023, 2024], gates=TEST_GATES)
    return cfg


@pytest.mark.parametrize("mod", [skew, joins, small_files, udf, cache, job], ids=lambda m: m.__name__.split(".")[-1])
def test_experiment_runs(lake, tmp_path, mod):
    out = tmp_path / "result.json"
    mod.main(["--year", "2024", "--runs", "1", "--no-warmup", "--out", str(out)])
    result = json.loads(out.read_text())
    assert result["variants"] and all(v["seconds"] for v in result["variants"])
    assert all(v["metrics"]["stages"] > 0 for v in result["variants"])
    if mod is not small_files:
        assert result["all_variants_same_answer"]


def test_join_choice_is_observed(lake, tmp_path):
    out = tmp_path / "joins.json"
    joins.main(["--year", "2024", "--runs", "1", "--no-warmup", "--out", str(out)])
    seen = {v["name"]: v["observed"][0]["join"] for v in json.loads(out.read_text())["variants"]}
    assert seen["broadcast_hint"] == "BroadcastHashJoin"


def test_group_metrics_only_counts_its_group(spark, cfg):
    sc = spark.sparkContext
    sc.setJobGroup("metrics-test-a", "a")
    spark.range(1000).repartition(3).count()
    sc.setJobGroup("metrics-test-b", "b")
    spark.range(10).count()
    sc.setLocalProperty("spark.jobGroup.id", None)
    a = metrics.group_metrics(spark, cfg, "metrics-test-a")
    assert a["jobs"] >= 1 and a["shuffle_write_bytes"] > 0
    b = metrics.group_metrics(spark, cfg, "metrics-test-b")
    assert b["shuffle_write_bytes"] < a["shuffle_write_bytes"]


def test_summarize_stages_skew_numbers():
    stages = {1: {"Stage ID": 1, "Submission Time": 0, "Completion Time": 5000, "Stage Name": "join"}, 2: None}
    tasks = {1: [{"Task Info": {"Launch Time": 0, "Finish Time": ms}, "Task Metrics": {"Executor Run Time": ms}}
                 for ms in (100, 100, 120, 900)]}
    s = metrics.summarize_stages(stages, tasks, 1)
    assert s["stages"] == 1
    assert s["worst_stage"]["task_ms_max"] == 900 and s["worst_stage"]["task_ms_median"] == 110
