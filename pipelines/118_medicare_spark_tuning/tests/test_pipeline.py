import shutil

import pytest
from conftest import TEST_GATES, bad_lines, good_lines, line, write_csv
from pyspark.sql import functions as F

from claims_perf import land, ops, parity, pipeline, sources, stage, summarize
from claims_perf.settings import Settings


def fingerprints(spark, cfg, years=(2023, 2024)):
    out = {}
    for year in years:
        for layer, tables in (("lake", stage.TABLES), ("marts", summarize.MARTS)):
            for t in tables:
                out[(year, t)] = parity.fingerprint(spark.read.parquet(str(cfg.home / layer / t / f"year={year}")))
    return out


def test_two_years_end_to_end(spark, cfg, sources_2y):
    out = pipeline.run_years(spark, cfg, [2023, 2024], gates=TEST_GATES)
    assert [(r["step"], r["year"], r["status"]) for r in out] == [
        ("land", 2023, "ok"), ("stage", 2023, "ok"), ("summarize", 2023, "ok"),
        ("land", 2024, "ok"), ("stage", 2024, "ok"), ("summarize", 2024, "ok")]
    staged = out[4]
    assert staged["rows"] == len(good_lines(2024))
    assert staged["quarantined"] == len(bad_lines())
    assert staged["reasons"] == {"npi_check_digit": 1, "missing_hcpcs": 1, "benes_below_floor": 1,
                                 "payment_over_allowed": 1, "not_a_number": 1}
    claims = spark.read.parquet(str(cfg.lake / "claims"))
    assert sorted(r[0] for r in claims.select("state").distinct().collect()) == ["CA", "FL", "NY", "TX"]
    assert claims.filter("year = 2024").count() == len(good_lines(2024))
    cs = spark.read.parquet(str(cfg.marts / "code_specialty" / "year=2024"))
    # 2024 carries 2023's payment as the prior year
    assert cs.filter(F.col("payment_amt_prior_year").isNull()).count() == 0
    shares = cs.groupBy("hcpcs_cd").agg(F.sum("share_of_code_payment").alias("s")).collect()
    assert all(abs(r["s"] - 1) < 1e-9 for r in shares)


def test_rerun_changes_nothing(spark, cfg, sources_2y):
    pipeline.run_years(spark, cfg, [2023, 2024], gates=TEST_GATES)
    before = fingerprints(spark, cfg)
    runs_before = len(ops.recent_runs(cfg, 100))
    again = pipeline.run_years(spark, cfg, [2023, 2024], gates=TEST_GATES)
    assert {r["status"] for r in again} == {"skipped"}
    assert len(ops.recent_runs(cfg, 100)) == runs_before
    forced = pipeline.run_years(spark, cfg, [2023, 2024], force=True, gates=TEST_GATES)
    assert [r["status"] for r in forced if r["step"] != "land"] == ["ok"] * 4
    assert fingerprints(spark, cfg) == before


def test_backfill_matches_incremental(spark, cfg, sources_2y, tmp_path):
    pipeline.run_years(spark, cfg, [2023], gates=TEST_GATES)
    pipeline.run_years(spark, cfg, [2024], gates=TEST_GATES)
    other = Settings(home=tmp_path / "rebuild")
    shutil.copytree(cfg.landing, other.landing)
    for year in (2023, 2024):
        m = land.read_manifest(other, year)
        ops.record(other, "land", year, f"{m['url']}|{m['last_modified']}", f"copied-{year}", "ok", m, 0, 0)
    pipeline.backfill(spark, other, 2023, 2024, gates=TEST_GATES)
    result = parity.compare(spark, cfg.home, other.home, [2023, 2024])
    assert len(result) == 14 and all(r["same"] for r in result), result


def test_blocked_year_keeps_last_good_version(spark, cfg, sources_2y, monkeypatch, tmp_path):
    pipeline.run_years(spark, cfg, [2024], gates=TEST_GATES)
    before = fingerprints(spark, cfg, years=(2024,))
    # CMS republishes the year and half the rows are broken
    broken = write_csv(tmp_path / "broken.csv", good_lines(2024) + bad_lines() * 8)
    monkeypatch.setenv("SOURCE_URL_2024", f"file://{broken}")
    out = pipeline.run_years(spark, cfg, [2024], gates=TEST_GATES)
    assert [(r["step"], r["status"]) for r in out] == [("land", "ok"), ("stage", "blocked"), ("summarize", "skipped")]
    assert "quarantine_share" in out[1]["failed_checks"][0]
    assert fingerprints(spark, cfg, years=(2024,)) == before
    assert not list((cfg.lake / "claims").glob("_*"))


def test_republished_year_makes_next_year_stale(spark, cfg, sources_2y, tmp_path, monkeypatch):
    pipeline.run_years(spark, cfg, [2023, 2024], gates=TEST_GATES)
    assert pipeline.freshness(cfg, [2023, 2024]) == []
    extra = line("1000000108", "Cardiology", "NY", "93000", srvcs="55", allowed="16.2", payment="12.7")
    new = write_csv(tmp_path / "republished_2023.csv", good_lines(2023) + bad_lines() + [extra])
    monkeypatch.setenv("SOURCE_URL_2023", f"file://{new}")
    pipeline.run_years(spark, cfg, [2023], gates=TEST_GATES)
    assert pipeline.freshness(cfg, [2023, 2024]) == ["2024 marts are older than their inputs"]
    out = pipeline.run_years(spark, cfg, [2023, 2024], gates=TEST_GATES)
    assert [(r["step"], r["year"], r["status"]) for r in out][3:] == [
        ("land", 2024, "skipped"), ("stage", 2024, "skipped"), ("summarize", 2024, "ok")]
    assert pipeline.freshness(cfg, [2023, 2024]) == []


def test_state_dropped_on_rerun_leaves_no_old_files(spark, cfg, sources_2y, tmp_path, monkeypatch):
    pipeline.run_years(spark, cfg, [2024], gates=checks_without_states())
    no_fl = [r for r in good_lines(2024) if r["Rndrng_Prvdr_State_Abrvtn"] != "FL"]
    p = write_csv(tmp_path / "no_fl.csv", no_fl)
    monkeypatch.setenv("SOURCE_URL_2024", f"file://{p}")
    pipeline.run_years(spark, cfg, [2024], gates=checks_without_states())
    states = {d.name for d in (cfg.lake / "claims" / "year=2024").iterdir() if d.is_dir()}
    assert states == {"state=CA", "state=NY", "state=TX"}


def checks_without_states():
    from claims_perf import checks
    return checks.Gates(max_quarantine_share=0.2, min_states=3)


def test_header_change_is_refused(cfg, tmp_path):
    p = tmp_path / "renamed.csv"
    write_csv(p, good_lines(2024))
    text = p.read_text().replace("Tot_Srvcs", "Tot_Services", 1)
    p.write_text(text)
    with pytest.raises(land.HeaderMismatch):
        land.land_year(cfg, 2024, f"file://{p}", "x")
    assert not (cfg.landing / "year=2024").exists()


def test_outlier_flag(spark):
    df = spark.createDataFrame(
        [("1", "99213", "F", 11, 400.0), ("2", "99213", "F", 20, 12.0)],
        "npi string, hcpcs_cd string, place_of_service string, tot_benes long, tot_srvcs double")
    df = (df.withColumn("provider_type", F.lit("Internal Medicine")).withColumn("state", F.lit("NY"))
          .withColumn("hcpcs_desc", F.lit("x")).withColumn("code_family", F.lit("evaluation_management"))
          .withColumn("payment_amt", F.lit(1.0)))
    stats = spark.createDataFrame([("99213", "F", 40, 0.7, 2.0)],
                                  "hcpcs_cd string, place_of_service string, peers long, p50_srvcs_per_bene double, "
                                  "p99_srvcs_per_bene double")
    out = summarize.flag_outliers(df, stats).collect()
    assert [r["npi"] for r in out] == ["1"]


def test_source_urls_cover_configured_years():
    assert set(sources.YEAR_URLS) == {2023, 2024}
    assert all(u.startswith("https://data.cms.gov/") for u in sources.YEAR_URLS.values())
