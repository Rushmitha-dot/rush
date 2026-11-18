"""Row rules and file gates.

Rows that break a row rule go to quarantine with a reason and never reach the
dimension. File gates decide whether the file is merged at all. ERROR blocks
the merge, WARN is recorded in ops.dq_results and alerted on.
"""
from dataclasses import asdict, dataclass
from functools import reduce

from pyspark.sql import Column
from pyspark.sql import functions as F

ERROR, WARN = "error", "warn"

# 50 states, DC, territories, and the military mail codes NPPES accepts
US_STATE_CODES = frozenset("""
AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK
OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC PR GU VI AS MP FM MH PW AA AE AP
""".split())


def npi_is_valid(npi: str | None) -> bool:
    """Luhn check digit over '80840' + the first nine digits (CMS NPI standard)."""
    if not npi or len(npi) != 10 or not npi.isdigit():
        return False
    total = 0
    # walk the 9 base digits right to left, doubling every other one starting with the rightmost
    for i, ch in enumerate(reversed(npi[:9])):
        d = int(ch)
        if i % 2 == 0:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    total += 24  # the doubled-and-summed digits of the 80840 prefix
    return (10 - total % 10) % 10 == int(npi[9])


def npi_valid_col(col: Column) -> Column:
    """Same rule as npi_is_valid, as a native expression so Spark doesn't ship rows to Python."""
    digits = [F.substring(col, i, 1).cast("int") for i in range(1, 11)]
    parts = []
    for pos in range(9):  # pos 0 is the leftmost digit; pos 8 (rightmost base digit) is doubled
        d = digits[pos]
        if (8 - pos) % 2 == 0:
            parts.append(F.when(d * 2 > 9, d * 2 - 9).otherwise(d * 2))
        else:
            parts.append(d)
    total = reduce(lambda a, b: a + b, parts) + F.lit(24)
    check = (F.lit(10) - total % 10) % 10
    return F.coalesce(col.rlike(r"^\d{10}$") & (check == digits[9]), F.lit(False))


@dataclass
class CheckResult:
    name: str
    severity: str
    passed: bool
    observed: float
    threshold: str
    detail: str = ""

    def as_dict(self):
        return asdict(self)


def failed(results: list[CheckResult], severity: str) -> list[CheckResult]:
    return [r for r in results if not r.passed and r.severity == severity]


def quarantine_reason(staged_cols) -> Column:
    """First rule a row breaks, or null if it is clean. staged_cols is a dict-like of columns."""
    c = staged_cols
    bare_deactivation = c["entity_type"].isNull() & c["deactivation_date"].isNotNull()
    return (
        F.when(~npi_valid_col(c["npi"]), F.lit("npi_check_digit"))
        .when(~c["entity_type"].isin("1", "2") & ~bare_deactivation, F.lit("entity_type"))
        .when((c["entity_type"] == "1") & c["last_name"].isNull(), F.lit("individual_without_last_name"))
        .when((c["entity_type"] == "2") & c["org_name"].isNull(), F.lit("organization_without_name"))
        .when(c["last_update_raw"].isNotNull() & c["last_update_date"].isNull(), F.lit("unparseable_date"))
    )


def file_gates(*, header: list[str], contract: list[str], rows: int, quarantined: int, duplicate_npis: int,
               period_start, period_end, last_period_end, kind: str, unknown_taxonomy: int,
               invalid_state: int, no_primary_flag: int, recent_rows: list[int]) -> list[CheckResult]:
    out = []
    drift = sorted(set(header) ^ set(contract))
    out.append(CheckResult("header_matches_contract", ERROR, header == contract, len(drift), "== 0",
                           ", ".join(drift[:5])))
    out.append(CheckResult("rows_present", ERROR, rows > 0, rows, "> 0"))
    out.append(CheckResult("npi_unique_in_file", ERROR, duplicate_npis == 0, duplicate_npis, "== 0"))

    rate = quarantined / rows if rows else 0.0
    out.append(CheckResult("quarantine_rate", ERROR, rate <= 0.005, round(rate, 6), "<= 0.5%",
                           f"{quarantined} rows quarantined"))

    # A skipped week means changes we never see. A weekly has to start on or before
    # the day after the last loaded file ends, and has to move the state forward.
    if last_period_end is not None:
        if kind == "weekly":
            gap = (period_start - last_period_end).days
            ok = gap <= 1 and period_end > last_period_end
            out.append(CheckResult("weekly_continuity", ERROR, ok, gap, "start <= last end + 1 day",
                                   f"last loaded end {last_period_end}, file {period_start} to {period_end}"))
        else:
            ok = period_end >= last_period_end
            out.append(CheckResult("monthly_not_older_than_state", ERROR, ok, (period_end - last_period_end).days,
                                   ">= 0 days", f"last loaded end {last_period_end}, file ends {period_end}"))

    for name, n in (("unknown_taxonomy", unknown_taxonomy), ("invalid_practice_state", invalid_state),
                    ("no_primary_taxonomy_flag", no_primary_flag)):
        share = n / rows if rows else 0.0
        out.append(CheckResult(name, WARN, n == 0, n, "== 0", f"{share:.4%} of rows"))

    if recent_rows:
        avg = sum(recent_rows) / len(recent_rows)
        ratio = rows / avg if avg else 0.0
        out.append(CheckResult(f"{kind}_volume_vs_recent", WARN, 0.5 <= ratio <= 2.0, round(ratio, 3),
                               "0.5x to 2x of recent files", f"recent avg {avg:.0f}"))
    return out
