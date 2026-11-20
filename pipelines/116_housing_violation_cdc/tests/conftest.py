from datetime import UTC, date, datetime, timedelta

import pytest

from violcdc.lake import Lake

T0 = datetime(2026, 11, 20, 16, 5, tzinfo=UTC)


def change(vid=1001, lsn=100, op="c", snap=False, status="Open", status_id=2, status_text="NOV SENT OUT",
           status_date=date(2026, 5, 2), cls="C", building=50, inspected=date(2026, 5, 1), issued=date(2026, 5, 2),
           row_hash=None, **extra):
    row = dict(violationid=vid, buildingid=building, boro="BRONX", housenumber="10", streetname="GRAND CONCOURSE",
               communityboard=4, **{"class": cls}, inspectiondate=inspected, novissueddate=issued,
               violationstatus=status, currentstatusid=status_id, currentstatus=status_text,
               currentstatusdate=status_date, novdescription="PROVIDE HEAT", lsn=lsn, snap=snap, op=op,
               commit_ts=T0 + timedelta(seconds=lsn), consumed_at=T0 + timedelta(seconds=lsn + 1),
               source_updated_at=T0, mirrored_at=T0)
    row.update(extra)
    row["row_hash"] = row_hash or f"h-{vid}-{status}-{status_id}-{status_date}-{sorted(extra.items())}"
    return row


@pytest.fixture
def lake(tmp_path):
    return Lake(tmp_path / "lake")
