"""As-of join and analyst access, on a few hand-written provider and Part D rows."""
from conftest import MONTHLY, WEEK1, A, B, E, make_zip, provider

from provider_lake import gold
from provider_lake.governance import add_to_group, create_analyst_views, grant_states
from provider_lake.pipeline import load_nppes_zip


def load_providers(spark, cfg, zips):
    load_nppes_zip(spark, cfg, make_zip(zips, MONTHLY, "20050523-20261102", [
        provider(A, updated="06/01/2023"),
        provider(B, updated="03/01/2025"),
        provider(E, entity="2", org="NORTH TEXAS CLINIC", taxonomy="261QP2300X", updated="01/01/2020"),
    ]), "t")
    load_nppes_zip(spark, cfg, make_zip(zips, WEEK1, "20261102-20261108", [
        provider(A, taxonomy="207R00000X", updated="11/03/2026"),
    ]), "t")


def partd(spark, year, npis):
    from decimal import Decimal
    rows = [(year, n, "TX", "Family Practice", "S", "Lisinopril", "Lisinopril", 40, 40.0, 1200, Decimal("120.50"),
             None, None, "*", "fixture") for n in npis]
    df = spark.createDataFrame(rows, "claim_year int, npi string, partd_state string, partd_prescriber_type string, "
                                     "partd_type_source string, brand_name string, generic_name string, "
                                     "total_claims int, total_30day_fills double, total_day_supply int, "
                                     "total_drug_cost decimal(14,2), total_beneficiaries int, ge65_claims int, "
                                     "ge65_suppression_flag string, source_file string")
    w = df.write.format("delta").mode("overwrite").partitionBy("claim_year")
    if spark.catalog.tableExists("silver.partd_prescriber_drug"):
        w = w.option("replaceWhere", f"claim_year = {year}")
    w.saveAsTable("silver.partd_prescriber_drug")


def test_fact_uses_version_valid_at_year_end(spark, cfg, zips):
    load_providers(spark, cfg, zips)
    unknown = "1316031123"
    partd(spark, 2024, [A, B, unknown])
    out = gold.build_year(spark, cfg, 2024)
    assert out["prescribers_by_match"] == {"as_of_year_end": 1, "earliest_known": 1, "not_in_registry_scope": 1}

    facts = {r["npi"]: r for r in spark.table("gold.fact_prescriber_drug").collect()}
    versions = {(r["npi"], r["version_no"]): r["provider_sk"] for r in spark.table("silver.dim_provider").collect()}
    # A changed taxonomy in Nov 2026, so 2024 claims belong to version 1
    assert facts[A]["provider_sk"] == versions[(A, 1)]
    # B's record was last updated in 2025, nothing is known about it at the end of 2024
    assert facts[B]["provider_match"] == "earliest_known"
    assert facts[unknown]["provider_sk"] is None


def test_year_reload_replaces_only_that_year(spark, cfg, zips):
    load_providers(spark, cfg, zips)
    partd(spark, 2023, [A])
    gold.build_year(spark, cfg, 2023)
    partd(spark, 2024, [A, B])
    gold.build_year(spark, cfg, 2024)
    gold.build_year(spark, cfg, 2024)
    counts = {r["claim_year"]: r["count"] for r in
              spark.table("gold.fact_prescriber_drug").groupBy("claim_year").count().collect()}
    assert counts == {2023: 1, 2024: 2}


def test_gold_dim_follows_change_feed(spark, cfg, zips):
    load_nppes_zip(spark, cfg, make_zip(zips, MONTHLY, "20050523-20261102", [provider(A, updated="06/01/2023")]),
                   "t")
    assert gold.refresh_dim_provider(spark, cfg)["mode"] == "full"
    assert gold.refresh_dim_provider(spark, cfg)["mode"] == "noop"
    load_nppes_zip(spark, cfg, make_zip(zips, WEEK1, "20261102-20261108", [
        provider(A, taxonomy="207R00000X", updated="11/03/2026")]), "t")
    r = gold.refresh_dim_provider(spark, cfg)
    assert r["mode"] == "cdf" and r["rows"] == 2  # old version closed, new version opened
    rows = spark.table("gold.dim_provider").orderBy("version_no").collect()
    assert [(x["is_current"], x["taxonomy_classification"]) for x in rows] == [
        (False, "Family Medicine"), (True, "Internal Medicine")]


def test_analyst_view_masks_individuals_and_filters_states(spark, cfg, zips):
    load_nppes_zip(spark, cfg, make_zip(zips, MONTHLY, "20050523-20261102", [
        provider(A, updated="06/01/2023"),
        provider(E, entity="2", org="NORTH TEXAS CLINIC", taxonomy="261QP2300X", updated="01/01/2020"),
        provider(B, state="TX", mail_state="TX", updated="01/01/2021"),
    ]), "t")
    spark.sql(f"update silver.dim_provider set practice_state = 'OK' where npi = '{B}'")
    gold.refresh_dim_provider(spark, cfg, full=True)
    create_analyst_views(spark, cfg)
    me = spark.sql("select current_user()").first()[0]

    assert spark.table("analyst.dim_provider").count() == 0  # no grant, no rows

    grant_states(spark, cfg, me, ["TX"])
    rows = {r["npi"]: r for r in spark.table("analyst.dim_provider").collect()}
    assert set(rows) == {A, E}
    assert rows[A]["practice_address_1"] == "***" and rows[A]["practice_phone"] == "***"
    assert rows[A]["practice_postal_code"] == "752"
    assert rows[E]["practice_address_1"] == "100 MAIN ST"  # organizations aren't masked

    add_to_group(spark, cfg, me, "provider_admins")
    rows = {r["npi"]: r for r in spark.table("analyst.dim_provider").collect()}
    assert set(rows) == {A, B, E} and rows[A]["practice_address_1"] == "100 MAIN ST"


def test_partd_gates_on_landed_csv(spark, cfg, tmp_path):
    import gzip

    from provider_lake.partd import EXPECTED_HEADER
    path = tmp_path / "partd.csv.gz"
    rows = [[A, "Doe", "Jane", "Dallas", "TX", "48", "Family Practice", "S", "Lisinopril", "Lisinopril",
             "40", "40", "1200", "123.45", "", "*", "", "", "", "", "*", ""],
            [A, "Doe", "Jane", "Dallas", "TX", "48", "Family Practice", "S", "Lisinopril", "Lisinopril",
             "12", "12", "360", "40.00", "", "*", "", "", "", "", "*", ""]]
    with gzip.open(path, "wt") as f:
        f.write(",".join(EXPECTED_HEADER) + "\n" + "\n".join(",".join(r) for r in rows) + "\n")
    n, checks = gold.load_partd_year(spark, cfg, {"claim_year": 2024, "path": str(path),
                                                  "source_file": "fixture.csv"})
    failed = {c.name for c in checks if not c.passed}
    assert n == 2 and failed == {"partd_grain_unique"}
    assert not spark.catalog.tableExists("silver.partd_prescriber_drug")
