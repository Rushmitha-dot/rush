import pytest
from pyspark.sql import functions as F

from claims_perf import codes
from claims_perf.spark import arrow_works

CASES = {
    "99213": "evaluation_management",
    "99214": "evaluation_management",
    "99499": "evaluation_management",
    "99500": "medicine",
    "93000": "medicine",
    "71046": "radiology",
    "80053": "pathology_lab",
    "00100": "anesthesia",
    "27447": "surgery",
    "1036F": "cpt_category_2",
    "0042T": "cpt_category_3",
    "0241U": "pathology_lab",
    "J1100": "drugs",
    "G0439": "procedures_g",
    "A0425": "transport_supplies",
    "Z1234": "level2_other",
    "ABCDE": "other",
    "9921": "other",
    " 99213 ": "evaluation_management",
    "09999": "other",
}


@pytest.mark.parametrize("code,family", CASES.items())
def test_python_version(code, family):
    assert codes.code_family(code) == family


def test_none_is_none():
    assert codes.code_family(None) is None


def frame(spark):
    return spark.createDataFrame([(c,) for c in CASES] + [(None,)], "hcpcs_cd string")


def test_udf_and_native_agree(spark):
    out = frame(spark).select("hcpcs_cd", codes.code_family_udf("hcpcs_cd").alias("py"),
                              codes.code_family_col(F.col("hcpcs_cd")).alias("native")).collect()
    for r in out:
        assert r["py"] == r["native"], r
        assert r["py"] == (CASES[r["hcpcs_cd"]] if r["hcpcs_cd"] is not None else None)


def test_pandas_version_agrees(spark):
    ok, why = arrow_works(spark)
    if not ok:
        pytest.skip(why)
    out = frame(spark).select(codes.code_family_udf("hcpcs_cd").alias("py"),
                              codes.code_family_pandas("hcpcs_cd").alias("pd")).collect()
    assert all(r["py"] == r["pd"] for r in out)
