"""Data quality checks for one night of trips.

ERROR checks run before anything is written and block the night (and the
dbt models after it). WARN checks run after the load, go to
ops.check_results and raise an alert, but the night still goes through.
"""

from dataclasses import dataclass
from datetime import date, timedelta

MAX_QUARANTINE_SHARE = 0.02
# late-night bike trips swing a lot with weather and events, so the band is wide
VOLUME_RANGE = (0.4, 2.0)
MAX_NO_END_STATION_SHARE = 0.05


@dataclass
class CheckResult:
    name: str
    severity: str  # ERROR or WARN
    passed: bool
    value: float | None = None
    detail: str = ""

    @property
    def blocks(self) -> bool:
        return self.severity == "ERROR" and not self.passed


def before_load_checks(months: list[str], available: dict, total: int, rejected: int) -> list[CheckResult]:
    missing = [m for m in months if m not in available]
    out = [CheckResult("source_months_present", "ERROR", not missing, len(available),
                       f"missing month files {missing}" if missing else f"months {months}")]
    trips = total - rejected
    # campus late-night trips have never been zero in the data I've loaded; zero means a broken or partial file
    out.append(CheckResult("trips_present", "ERROR", trips > 0 or bool(missing), trips, f"{trips} valid trips"))
    share = rejected / total if total else 0.0
    out.append(CheckResult("quarantine_share", "ERROR", share <= MAX_QUARANTINE_SHARE, round(share, 5),
                           f"{rejected} of {total} rows quarantined"))
    return out


def after_load_checks(con, night: date, trips: int, repeats: int, calendar: dict) -> list[CheckResult]:
    out = [_volume_check(con, night, trips, calendar)]
    out.append(CheckResult("repeated_ride_ids", "WARN", repeats == 0, repeats,
                           f"{repeats} ride_ids seen in more than one month file"))
    no_end = con.execute("""
        select count(*) filter (where end_station_id is null) / nullif(count(*), 0)
        from raw.bluebikes_trips where night = ?""", [night]).fetchone()[0] or 0.0
    out.append(CheckResult("no_end_station_share", "WARN", no_end <= MAX_NO_END_STATION_SHARE, round(no_end, 4),
                           "trips that ended away from a dock"))
    return out


def _volume_check(con, night: date, trips: int, calendar: dict) -> CheckResult:
    """Compare with the same weekday in the same kind of period over the last 4 weeks.

    A trailing average would flag the first night of a break and then get
    used to it within a week, so the baseline is like-for-like instead.
    """
    period = calendar.get(night, {}).get("period")
    same = [night - timedelta(weeks=w) for w in range(1, 5)]
    same = [d for d in same if calendar.get(d, {}).get("period") == period]
    if not same:
        return CheckResult("trip_volume", "WARN", True, trips, "no comparable nights yet")
    placeholders = ", ".join("?" for _ in same)
    history = sorted(r[0] for r in con.execute(
        f"select trips from ops.load_ledger where night in ({placeholders})", same).fetchall())
    if len(history) < 2:
        return CheckResult("trip_volume", "WARN", True, trips, f"only {len(history)} comparable nights")
    baseline = history[len(history) // 2]
    ratio = trips / baseline if baseline else 0.0
    lo, hi = VOLUME_RANGE
    return CheckResult("trip_volume", "WARN", lo <= ratio <= hi, round(ratio, 3),
                       f"{trips} trips vs {baseline} on comparable nights")
