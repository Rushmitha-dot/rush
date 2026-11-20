"""Freshness SLA. A label version in the update file for day D must be searchable by
00:00 UTC on D+2. DailyMed posts the file for D in the evening US Eastern time, so that is
a little over 24 hours from publication.

Two checks, both exit 2 and alert.
1. A file the downloads page offers is past its deadline and not in the ledger.
2. Nothing newer than the last loaded file for 4 days. This catches the case where the
   listing itself can't be read or changed format, which check 1 would miss.
"""

from datetime import UTC, datetime, timedelta

from . import dailymed
from .alerts import alert

STALE = 2
MAX_SILENCE = timedelta(days=4)


def deadline(name: str) -> datetime:
    d = dailymed.file_date(name)
    return datetime(d.year, d.month, d.day, tzinfo=UTC) + timedelta(days=2)


def evaluate(loaded: list[str], offered: list[str] | None, now: datetime) -> list[str]:
    problems = []
    done = set(loaded)
    for name in offered or []:
        if name not in done and now > deadline(name):
            late = now - deadline(name)
            problems.append(f"{name} is offered by DailyMed and not loaded, {late.total_seconds() / 3600:.0f}h "
                            f"past its deadline")
    feed = [n for n in loaded if not n.startswith("bootstrap_")]
    if not feed:
        problems.append("no update file has been loaded yet")
    else:
        newest = max(feed, key=dailymed.order_key)
        silence = now - deadline(newest)
        if silence > MAX_SILENCE:
            problems.append(f"newest loaded file is {newest}, {silence.days} days past its deadline "
                            f"with nothing after it")
    return problems


def check(settings, conn, offline: bool = False, now: datetime | None = None) -> int:
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    loaded = [r[0] for r in conn.execute("select update_file from load_ledger where status = 'loaded'").fetchall()]
    offered = None
    if not offline:
        try:
            offered = [n for n in dailymed.available_updates(dailymed.listing()) if "daily" in n]
        except OSError as e:
            alert(settings, "warn", f"could not read the DailyMed listing, only the silence check ran: {e}")
    problems = evaluate(loaded, offered, now)
    if problems:
        for p in problems:
            alert(settings, "error", f"freshness: {p}")
        return STALE
    newest = max((n for n in loaded if not n.startswith("bootstrap_")), key=dailymed.order_key)
    print(f"fresh: newest loaded file {newest}, deadline {deadline(newest):%Y-%m-%d %H:%M} UTC")
    return 0
