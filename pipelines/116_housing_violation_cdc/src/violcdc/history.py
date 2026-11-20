"""SCD2 status history for one violation, advanced by its change events in log order."""

STATUS_KEY = ("violationstatus", "currentstatusid", "currentstatus")


def _order(ev):
    return (ev["lsn"], ev["snap"])


def _seen(rows):
    return max([r["lsn"] for r in rows] + [r["closed_lsn"] for r in rows if r["closed_lsn"]], default=-1)


def _row(ev, valid_from, derived=False, status=None):
    status = status or {k: ev.get(k) for k in STATUS_KEY}
    return dict(violationid=ev["violationid"], **status, valid_from=valid_from, valid_to=None, is_current=True,
                derived=derived, lsn=ev["lsn"], closed_lsn=None, recorded_at=ev["commit_ts"])


def _close(row, valid_to, ev):
    row.update(valid_to=valid_to, is_current=False, closed_lsn=ev["lsn"])


def first_image(ev) -> list:
    """The first time we see a violation we only have its current status. The source dates still say when
    it was issued, so the stretch before the current status is kept as a derived Open row."""
    issued = ev.get("novissueddate") or ev.get("inspectiondate")
    status_date = ev.get("currentstatusdate")
    if status_date and issued and status_date > issued:
        before = _row(ev, issued, derived=True,
                      status={"violationstatus": "Open", "currentstatusid": None, "currentstatus": None})
        _close(before, status_date, ev)
        return [before, _row(ev, status_date)]
    return [_row(ev, status_date or issued)]


def advance(rows: list, events: list) -> list:
    """Existing history rows for one violation plus its new events, to the full new list of rows.

    Events at or before the last log position already in the history are skipped, so applying the same
    events twice gives the same rows.
    """
    rows = sorted((dict(r) for r in rows), key=lambda r: (r["valid_from"], r["lsn"]))
    seen = _seen(rows)
    for ev in sorted(events, key=_order):
        # a snapshot read at the same position as the last change carries the same status, so lsn alone is enough
        if ev["lsn"] <= seen:
            continue
        current = next((r for r in rows if r["is_current"]), None)
        commit_day = ev["commit_ts"].date()
        if ev["op"] == "d":
            if current:
                _close(current, max(commit_day, current["valid_from"]), ev)
        elif not rows:
            rows = first_image(ev)
        elif current is None:
            rows.append(_row(ev, commit_day))
        elif any(ev.get(k) != current[k] for k in STATUS_KEY):
            status_date = ev.get("currentstatusdate")
            valid_from = status_date if status_date and status_date >= current["valid_from"] else commit_day
            valid_from = max(valid_from, current["valid_from"])
            _close(current, valid_from, ev)
            rows.append(_row(ev, valid_from))
        seen = ev["lsn"]
    return rows
