"""Academic calendar labels for 2025 and 2026, written to dbt/seeds/academic_calendar.csv.

These are labels for slicing the trip data (classes, finals, breaks). The
dates follow the usual Boston pattern (Labor Day start, a March spring
break, Thanksgiving week, Patriots' Day) and are close to the schools around
the campus area, but they aren't any one university's official calendar.
"""

import csv
from datetime import date, timedelta

from .settings import SEEDS_DIR

TERMS = [
    ("fall_2025", date(2025, 9, 2), date(2025, 12, 19)),
    ("spring_2026", date(2026, 1, 12), date(2026, 4, 29)),
    ("fall_2026", date(2026, 9, 8), date(2026, 12, 18)),
]

# later entries win over the default "classes"
PERIODS = [
    ("first_week", date(2025, 9, 2), date(2025, 9, 7)),
    ("midterms", date(2025, 10, 20), date(2025, 10, 31)),
    ("break", date(2025, 11, 26), date(2025, 11, 30)),
    ("finals", date(2025, 12, 11), date(2025, 12, 19)),
    ("first_week", date(2026, 1, 12), date(2026, 1, 18)),
    ("midterms", date(2026, 2, 23), date(2026, 3, 6)),
    ("break", date(2026, 3, 7), date(2026, 3, 15)),
    ("finals", date(2026, 4, 22), date(2026, 4, 29)),
    ("first_week", date(2026, 9, 8), date(2026, 9, 13)),
    ("midterms", date(2026, 10, 19), date(2026, 10, 30)),
    ("break", date(2026, 11, 25), date(2026, 11, 29)),
    ("finals", date(2026, 12, 10), date(2026, 12, 18)),
]

HOLIDAYS = {
    date(2025, 9, 1): "Labor Day",
    date(2025, 10, 13): "Indigenous Peoples Day",
    date(2025, 11, 11): "Veterans Day",
    date(2026, 1, 19): "MLK Day",
    date(2026, 2, 16): "Presidents Day",
    date(2026, 4, 20): "Patriots Day",
    date(2026, 9, 7): "Labor Day",
    date(2026, 10, 12): "Indigenous Peoples Day",
    date(2026, 11, 11): "Veterans Day",
}


def build_calendar(first: date = date(2025, 1, 1), last: date = date(2026, 12, 31)) -> list[dict]:
    rows = []
    d = first
    while d <= last:
        term = next((t for t, start, end in TERMS if start <= d <= end), None)
        period = "no_term"
        if term:
            period = "classes"
            for name, start, end in PERIODS:
                if start <= d <= end:
                    period = name
        rows.append({
            "night": d.isoformat(),
            "term": term or "",
            "period": period,
            "holiday": HOLIDAYS.get(d, ""),
            # Friday and Saturday nights, the ones without class the next morning
            "weekend_night": str(d.weekday() in (4, 5)).lower(),
        })
        d += timedelta(days=1)
    return rows


def write_seed() -> None:
    rows = build_calendar()
    with open(SEEDS_DIR / "academic_calendar.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def load_calendar() -> dict[date, dict]:
    with open(SEEDS_DIR / "academic_calendar.csv") as f:
        return {date.fromisoformat(r["night"]): r for r in csv.DictReader(f)}


if __name__ == "__main__":
    write_seed()
