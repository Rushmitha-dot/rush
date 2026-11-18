"""Data quality rules on a staged drop. ERROR blocks the load, WARN is recorded,
alerted on and lets the day through."""
from dataclasses import asdict, dataclass

ERROR, WARN = "error", "warn"


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


def run_checks(stats: dict, recent_daily_rows: list[int], kind: str) -> list[CheckResult]:
    """stats comes from ingest.stage_stats. Kept as plain numbers so the rules
    are testable without a database."""
    out = []
    rows_in = stats["rows_in"]
    out.append(CheckResult("drop_not_empty", ERROR, rows_in > 0, rows_in, "> 0"))

    out.append(CheckResult("rows_match_manifest", ERROR, rows_in == stats["manifest_rows"], rows_in,
                           f"== {stats['manifest_rows']}"))

    outside = stats["outside_window"]
    out.append(CheckResult("review_ts_in_window", ERROR, outside == 0, outside, "== 0",
                           "a misfiled drop would overwrite the wrong day"))

    out.append(CheckResult("required_keys_present", ERROR, stats["missing_key"] == 0, stats["missing_key"], "== 0",
                           "user_id, asin, parent_asin, timestamp"))

    # Exact repeats are dropped without losing anything, so they never block a load.
    # Rows that do lose information (no key, bad rating, empty text) block when they
    # are both over 5% and at least 10 rows. Without the floor, one bad review on a
    # 15-review day in the thin 2023 tail blocked the day (31 days on the first run).
    dupes = stats["quarantine_reasons"].get("duplicate", 0)
    lost = stats["quarantined"] - dupes
    lost_share = lost / rows_in if rows_in else 0.0
    out.append(CheckResult("rejected_rows_max", ERROR, lost_share <= 0.05 or lost < 10, round(lost_share, 5),
                           "<= 5% or under 10 rows", f"{lost} rows: {stats['quarantine_reasons']}"))

    q_share = stats["quarantined"] / rows_in if rows_in else 0.0
    # a 130-row day with two exact repeats is already over 1%, which fired on 8 of the
    # first 14 days. The warning now needs a real count as well as the share.
    out.append(CheckResult("quarantine_share", WARN, q_share <= 0.01 or stats["quarantined"] < 10,
                           round(q_share, 5), "<= 1% or under 10 rows",
                           f"{stats['quarantined']} rows: {stats['quarantine_reasons']}"))

    loaded = rows_in - stats["quarantined"]
    unknown = stats["unknown_product"] / loaded if loaded else 0.0
    out.append(CheckResult("product_in_catalog", WARN, unknown <= 0.02, round(unknown, 5), "<= 2% unknown",
                           f"{stats['unknown_product']} reviews for products missing from the catalog"))

    # daily volume in this category is 10 to 150 reviews, so the band is wide on purpose
    if kind == "daily" and len(recent_daily_rows) >= 3:
        avg = sum(recent_daily_rows) / len(recent_daily_rows)
        ratio = rows_in / avg if avg else 0.0
        out.append(CheckResult("volume_vs_trailing_7", WARN, 0.3 <= ratio <= 3.0, round(ratio, 3),
                               "0.3x to 3x trailing avg", f"trailing avg {avg:.0f}"))
    return out


def failed(results: list[CheckResult], severity: str) -> list[CheckResult]:
    return [r for r in results if not r.passed and r.severity == severity]
