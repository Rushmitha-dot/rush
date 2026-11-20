from conftest import PROVIDERS, bad_lines, good_lines, with_check_digit
from pyspark.sql import functions as F

from claims_perf import checks, sources


def raw_df(spark, rows):
    keep = [c for c in sources.COLUMNS if c not in sources.DROPPED_AT_LANDING]
    return spark.createDataFrame([[r[c] or None for c in keep] for r in rows], ", ".join(f"`{c}` string" for c in keep))


def test_npi_check_digit(spark):
    good = with_check_digit("123456789")
    df = spark.createDataFrame([(good,), ("1234567890" if good != "1234567890" else "1234567891",)], "npi string")
    assert [r[0] for r in df.select(checks.npi_check_digit_ok(F.col("npi"))).collect()] == [True, False]


def test_real_npi_passes(spark):
    # an NPI from the first line of the 2024 file
    df = spark.createDataFrame([("1003000126",)], "npi string")
    assert df.select(checks.npi_check_digit_ok(F.col("npi"))).first()[0]


def test_each_bad_line_gets_its_reason(spark):
    out = checks.typed(raw_df(spark, bad_lines())).select("reasons").collect()
    assert [r[0] for r in out] == [["npi_check_digit"], ["missing_hcpcs"], ["benes_below_floor"],
                                   ["payment_over_allowed"], ["not_a_number"]]


def test_good_lines_have_no_reason(spark):
    out = checks.typed(raw_df(spark, good_lines(2024))).filter(F.size("reasons") > 0).count()
    assert out == 0


def test_gates_block_on_quarantine_share_and_missing_states():
    manifest = {"rows": 100}
    cols = ["a", "b"]
    res = checks.gate_results(manifest=manifest, landed_rows=100, landed_columns=cols, expected_columns=cols,
                              good_rows=90, quarantined=10, us_states=3, payment=1.0, prior=None,
                              gates=checks.Gates(max_quarantine_share=0.05, min_states=51))
    failed = {r.name for r in res if not r.passed}
    assert failed == {"quarantine_share", "us_states_present"}


def test_year_over_year_warn_and_error():
    cols = ["a"]
    common = dict(manifest={"rows": 100}, landed_rows=100, landed_columns=cols, expected_columns=cols,
                  quarantined=0, us_states=51, gates=checks.Gates(min_states=51))
    warn = checks.gate_results(good_rows=100, payment=120.0, prior={"rows": 100, "payment": 100.0}, **common)
    assert {r.name for r in warn if not r.passed} == {"payment_vs_prior_year_warn"}
    err = checks.gate_results(good_rows=100, payment=40.0, prior={"rows": 100, "payment": 100.0}, **common)
    assert {(r.name, r.severity) for r in err if not r.passed} == {("payment_vs_prior_year", "error"),
                                                                   ("payment_vs_prior_year_warn", "warn")}


def test_fixture_providers_have_valid_npis(spark):
    df = spark.createDataFrame([(p[0],) for p in PROVIDERS], "npi string")
    assert df.filter(~checks.npi_check_digit_ok(F.col("npi"))).count() == 0
