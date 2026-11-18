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

    q_share = stats["quarantined"] / rows_in if rows_in else 0.0
    out.append(CheckResult("quarantine_share_max", ERROR, q_share <= 0.05, round(q_share, 5), "<= 5%"))
    out.append(CheckResult("quarantine_share", WARN, q_share <= 0.01, round(q_share, 5), "<= 1%",
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
