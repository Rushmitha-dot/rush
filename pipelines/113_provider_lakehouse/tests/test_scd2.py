"""End to end on hand-written fixture files: monthly full, then weeklies, through the real merge."""
import pytest
from conftest import MONTHLY, WEEK1, WEEK2, WEEK_GAP, A, B, C, D, E, deactivation, make_zip, provider

from provider_lake import ops, parity
from provider_lake.pipeline import LedgerConflict, backfill, load_nppes_zip, table_version


def dim(spark, npi=None):
    df = spark.table("silver.dim_provider")
    if npi:
        df = df.filter(f"npi = '{npi}'")
    return sorted(df.collect(), key=lambda r: (r["npi"], r["version_no"]))


@pytest.fixture
def monthly(zips):
    return make_zip(zips, MONTHLY, "20050523-20261102", [
        provider(A, updated="10/01/2026"),
        provider(B, updated="10/01/2026"),
        provider(C, updated="10/01/2026"),
        provider(E, entity="2", org="NORTH TEXAS CLINIC", taxonomy="261QP2300X", updated="10/01/2026"),
    ])


@pytest.fixture
def week1(zips):
    return make_zip(zips, WEEK1, "20261102-20261108", [
        provider(A, address="200 ELM ST", updated="11/03/2026"),    # moved: new version
        provider(B, updated="11/04/2026"),                          # only Last Update Date moved: no version
        deactivation(C, "11/05/2026"),                              # bare deactivation row
        provider(D, updated="11/06/2026"),                          # new provider
    ])


def test_monthly_then_weekly(spark, cfg, monthly, week1):
    r = load_nppes_zip(spark, cfg, monthly, "t")
    assert r["status"] == "loaded" and r["changes"] == {"new": 4}

    r = load_nppes_zip(spark, cfg, week1, "t")
    assert r["changes"] == {"changed": 1, "unchanged": 1, "deactivated": 1, "new": 1}

    a = dim(spark, A)
    assert [(v["version_no"], v["practice_address_1"], v["is_current"]) for v in a] == [
        (1, "100 MAIN ST", False), (2, "200 ELM ST", True)]
    assert str(a[0]["valid_to"]) == "2026-11-03" and str(a[1]["valid_from"]) == "2026-11-03"

    assert len(dim(spark, B)) == 1  # a bumped date alone is not a change

    c = dim(spark, C)
    assert len(c) == 2
    assert c[1]["is_active"] is False and c[1]["last_name"] == "DOE"  # kept who the provider was
    assert c[1]["practice_address_1"] == "100 MAIN ST"

    log = {r["npi"]: r for r in spark.table("silver.provider_change_log").filter(f"source_file = '{WEEK1}'")
           .collect()}
    assert log[A]["changed_fields"] == ["mail_address_1", "practice_address_1"]
    assert log[C]["changed_fields"] == ["deactivation_date"]


def test_rerun_is_a_noop(spark, cfg, monthly, week1):
    load_nppes_zip(spark, cfg, monthly, "t")
    load_nppes_zip(spark, cfg, week1, "t")
    before = table_version(spark, "silver.dim_provider")
    assert load_nppes_zip(spark, cfg, week1, "t")["status"] == "skipped"
    assert table_version(spark, "silver.dim_provider") == before


def test_republished_file_with_new_checksum_is_refused(spark, cfg, monthly, week1, zips):
    load_nppes_zip(spark, cfg, monthly, "t")
    load_nppes_zip(spark, cfg, week1, "t")
    make_zip(zips, WEEK1, "20261102-20261108", [provider(A, address="999 OTHER ST", updated="11/03/2026")])
    with pytest.raises(LedgerConflict):
        load_nppes_zip(spark, cfg, week1, "t")


def test_merge_is_idempotent_even_without_the_ledger(spark, cfg, monthly, week1):
    """If the ledger is bypassed, the hashes still stop duplicate versions."""
    from provider_lake.landing import LandedFile
    from provider_lake.pipeline import landed_manifest, process_landed

    load_nppes_zip(spark, cfg, monthly, "t")
    load_nppes_zip(spark, cfg, week1, "t")
    rows_before = spark.table("silver.dim_provider").count()
    landed = LandedFile.read(landed_manifest(cfg, WEEK1))
    spark.sql(f"delete from ops.load_ledger where source_file = '{WEEK1}'")
    r = process_landed(spark, cfg, landed, "t")
    assert r["changes"] == {"unchanged": 4}
    assert spark.table("silver.dim_provider").count() == rows_before


def test_same_day_correction_updates_in_place(spark, cfg, monthly, week1, zips):
    load_nppes_zip(spark, cfg, monthly, "t")
    load_nppes_zip(spark, cfg, week1, "t")
    w2 = make_zip(zips, WEEK2, "20261109-20261115", [provider(A, address="201 ELM ST", updated="11/03/2026")])
    r = load_nppes_zip(spark, cfg, w2, "t")
    assert r["changes"] == {"same_day_fix": 1}
    a = dim(spark, A)
    assert len(a) == 2 and a[1]["practice_address_1"] == "201 ELM ST" and a[1]["is_current"]


def test_older_row_does_not_roll_state_back(spark, cfg, monthly, week1, zips):
    load_nppes_zip(spark, cfg, monthly, "t")
    load_nppes_zip(spark, cfg, week1, "t")
    w2 = make_zip(zips, WEEK2, "20261109-20261115", [provider(A, address="OLD ADDRESS", updated="10/20/2026")])
    assert load_nppes_zip(spark, cfg, w2, "t")["changes"] == {"stale": 1}
    assert dim(spark, A)[-1]["practice_address_1"] == "200 ELM ST"


def test_missing_week_blocks_and_leaves_dimension_alone(spark, cfg, monthly, week1, zips):
    load_nppes_zip(spark, cfg, monthly, "t")
    load_nppes_zip(spark, cfg, week1, "t")
    before = table_version(spark, "silver.dim_provider")
    gap = make_zip(zips, WEEK_GAP, "20261123-20261129", [provider(A, address="300 OAK ST", updated="11/24/2026")])
    r = load_nppes_zip(spark, cfg, gap, "t")
    assert r["status"] == "blocked" and "weekly_continuity" in r["errors"]
    assert table_version(spark, "silver.dim_provider") == before
    assert ops.ledger_entry(spark, cfg, WEEK_GAP)["status"] == "blocked"


def test_bad_check_digit_goes_to_quarantine(spark, cfg, zips):
    bad = A[:-1] + str((int(A[-1]) + 1) % 10)
    rows = [provider(n, updated="10/01/2026") for n in (B, C, D, E, bad)]
    # 1 bad row in 5 is over the 0.5% gate, so the file is blocked and nothing merges
    m = make_zip(zips, MONTHLY, "20050523-20261102", rows)
    r = load_nppes_zip(spark, cfg, m, "t")
    assert r["status"] == "blocked"
    q = spark.table("silver.provider_quarantine").collect()
    assert [(x["npi"], x["reason"]) for x in q] == [(bad, "npi_check_digit")]
    assert spark.table("silver.dim_provider").count() == 0


def test_backfill_since_matches_incremental(spark, cfg, monthly, week1, zips):
    load_nppes_zip(spark, cfg, monthly, "t")
    load_nppes_zip(spark, cfg, week1, "t")
    w2 = make_zip(zips, WEEK2, "20261109-20261115", [provider(D, city="PLANO", updated="11/10/2026")])
    load_nppes_zip(spark, cfg, w2, "t")
    v_incremental = table_version(spark, "silver.dim_provider")

    from datetime import date
    backfill(spark, cfg, date(2026, 11, 2), "bf")
    diff = parity.compare_versions(spark, "silver.dim_provider", v_incremental,
                                   table_version(spark, "silver.dim_provider"))
    assert diff["equal"], diff
    statuses = [r["status"] for r in spark.table("ops.load_ledger").filter(f"source_file = '{WEEK1}'")
                .orderBy("loaded_at").collect()]
    assert statuses == ["loaded", "rolled_back", "loaded"]


def test_full_rebuild_matches_incremental(spark, cfg, monthly, week1):
    load_nppes_zip(spark, cfg, monthly, "t")
    load_nppes_zip(spark, cfg, week1, "t")
    v = table_version(spark, "silver.dim_provider")
    backfill(spark, cfg, None, "bf")
    assert parity.compare_versions(spark, "silver.dim_provider", v, table_version(spark, "silver.dim_provider"))[
        "equal"]


def test_next_monthly_parity_counts_misses(spark, cfg, monthly, week1, zips):
    load_nppes_zip(spark, cfg, monthly, "t")
    load_nppes_zip(spark, cfg, week1, "t")
    # December full file: agrees with the weekly state for A, C, D, E, has a change to B dated
    # inside the weekly window that the weeklies never sent, and a change after the window.
    dec = make_zip(zips, "NPPES_Data_Dissemination_December_2026_V2.zip", "20050523-20261207", [
        provider(A, address="200 ELM ST", updated="11/03/2026"),
        provider(B, city="FRISCO", updated="11/05/2026"),
        deactivation(C, "11/05/2026"),
        provider(D, updated="11/06/2026"),
        provider(E, entity="2", org="NORTH TEXAS CLINIC LLC", taxonomy="261QP2300X", updated="12/01/2026"),
    ])
    from datetime import date
    result = parity.check_monthly(spark, cfg, dec, date(2026, 11, 8))
    assert result["misses"] == 1 and result["by_type"] == {"unchanged": 3, "changed": 1}
    assert result["newer_than_state"] == 1
    assert result["sample_npis"] == [B]
