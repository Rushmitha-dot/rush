"""Row rules and batch gates.

A row that fails a rule goes to quarantine with a reason. A failed ERROR gate blocks the publish.
"""
import os
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

NUMERIC = {
    "tot_benes": "Tot_Benes", "tot_srvcs": "Tot_Srvcs", "tot_bene_day_srvcs": "Tot_Bene_Day_Srvcs",
    "avg_submitted": "Avg_Sbmtd_Chrg", "avg_allowed": "Avg_Mdcr_Alowd_Amt", "avg_payment": "Avg_Mdcr_Pymt_Amt",
    "avg_standardized": "Avg_Mdcr_Stdzd_Amt",
}

# 50 states and DC. Territories, military (AA, AE, AP) and the CMS unknown/foreign codes (XX, ZZ) are kept
# but don't count toward the "is this a whole file" gate.
US_STATES = (
    "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH "
    "OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY"
).split()

# CMS leaves out any row with fewer than 11 beneficiaries, so a smaller number means the source changed
BENE_FLOOR = 11


@dataclass(frozen=True)
class Gates:
    max_quarantine_share: float = float(os.getenv("MAX_QUARANTINE_SHARE", "0.001"))
    min_states: int = int(os.getenv("MIN_STATES", "51"))
    warn_yoy_change: float = 0.15
    error_yoy_change: float = 0.5


@dataclass
class Result:
    name: str
    severity: str  # error | warn
    passed: bool
    value: object


def npi_check_digit_ok(npi: Column) -> Column:
    """Luhn over the 9 base digits with the 80840 prefix (constant 24), same rule NPPES uses."""
    digits = [F.substring(npi, i, 1).cast("int") for i in range(1, 11)]
    total = F.lit(24)
    for pos, d in enumerate(reversed(digits[:9])):
        if pos % 2 == 0:
            doubled = d * 2
            total = total + F.when(doubled > 9, doubled - 9).otherwise(doubled)
        else:
            total = total + d
    return F.pmod(F.lit(10) - F.pmod(total, F.lit(10)), F.lit(10)) == digits[9]


def typed(raw: DataFrame) -> DataFrame:
    """Cast the landed strings and attach an array of reasons. An empty array means the row is good."""
    df = raw
    for new, src in NUMERIC.items():
        df = df.withColumn(new, F.col(src).cast("double"))
    bad_number = F.lit(False)
    for new, src in NUMERIC.items():
        bad_number = bad_number | (F.col(src).isNotNull() & F.col(new).isNull()) | F.col(src).isNull()
    npi = F.col("Rndrng_NPI")
    reasons = [
        F.when(~npi.rlike("^[0-9]{10}$") | npi.isNull(), F.lit("bad_npi"))
         .when(~npi_check_digit_ok(npi), F.lit("npi_check_digit")),
        F.when(F.col("HCPCS_Cd").isNull() | (F.length(F.trim("HCPCS_Cd")) == 0), F.lit("missing_hcpcs")),
        F.when(~F.col("Place_Of_Srvc").isin("F", "O") | F.col("Place_Of_Srvc").isNull(), F.lit("bad_place_of_service")),
        F.when(F.col("Rndrng_Prvdr_Type").isNull(), F.lit("missing_provider_type")),
        F.when(bad_number, F.lit("not_a_number")),
        F.when(F.col("tot_srvcs") <= 0, F.lit("services_not_positive")),
        F.when(F.col("tot_benes") < BENE_FLOOR, F.lit("benes_below_floor")),
        F.when((F.col("avg_submitted") < 0) | (F.col("avg_allowed") < 0) | (F.col("avg_payment") < 0),
               F.lit("negative_amount")),
        F.when(F.col("avg_payment") > F.col("avg_allowed") + 0.01, F.lit("payment_over_allowed")),
    ]
    return df.withColumn("reasons", F.filter(F.array(*reasons), lambda r: r.isNotNull()))


def gate_results(*, manifest: dict, landed_rows: int, landed_columns: list[str], expected_columns: list[str],
                 good_rows: int, quarantined: int, us_states: int, payment: float,
                 prior: dict | None, gates: Gates) -> list[Result]:
    total = good_rows + quarantined
    share = quarantined / total if total else 1.0
    out = [
        Result("rows_match_manifest", "error", landed_rows == manifest["rows"], f"{landed_rows} vs {manifest['rows']}"),
        Result("column_contract", "error", landed_columns == expected_columns,
               ",".join(sorted(set(expected_columns) ^ set(landed_columns))) or "ok"),
        Result("rows_reconcile", "error", total == landed_rows, f"{good_rows} + {quarantined} vs {landed_rows}"),
        Result("quarantine_share", "error", share <= gates.max_quarantine_share, round(share, 6)),
        Result("us_states_present", "error", us_states >= gates.min_states, us_states),
    ]
    if prior:
        for name, now, before in (("rows_vs_prior_year", good_rows, prior["rows"]),
                                  ("payment_vs_prior_year", payment, prior["payment"])):
            change = (now - before) / before if before else 0.0
            out.append(Result(name, "error", abs(change) <= gates.error_yoy_change, round(change, 4)))
            out.append(Result(name + "_warn", "warn", abs(change) <= gates.warn_yoy_change, round(change, 4)))
    return out


def soft_flags(df: DataFrame) -> dict:
    """Odd but real values. They stay in the fact and are counted so a jump shows up in the checks table."""
    row = df.agg(
        F.sum(F.when(F.col("tot_bene_day_srvcs") > F.col("tot_srvcs"), 1).otherwise(0))
        .alias("bene_days_over_services"),
        F.sum(F.when(F.col("tot_benes") > F.col("tot_srvcs"), 1).otherwise(0)).alias("benes_over_services"),
        F.sum(F.when(~F.col("state").isin(*US_STATES), 1).otherwise(0)).alias("non_state_rows"),
        F.sum(F.when(F.col("avg_allowed") == 0, 1).otherwise(0)).alias("zero_allowed"),
    ).first()
    return {k: int(v or 0) for k, v in row.asDict().items()}
