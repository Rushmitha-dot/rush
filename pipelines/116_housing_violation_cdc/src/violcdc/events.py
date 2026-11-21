"""Debezium change events (JSON converter, schemas off) to flat change rows."""

import json
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa

from .soda import CLASSES, COLUMNS, STATUSES

EPOCH = date(1970, 1, 1)
DATE_COLUMNS = {n for n, t in COLUMNS.items() if t == "date"}
TS_COLUMNS = {"source_updated_at", "mirrored_at"}
SNAPSHOT_FLAGS = {"true", "first", "last", "first_in_data_collection", "last_in_data_collection", "incremental"}
KNOWN = set(COLUMNS) | TS_COLUMNS | {"row_hash"}


class BadEvent(ValueError):
    pass


def _ts(ms):
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def _iso_ts(value):
    return None if value is None else datetime.fromisoformat(value.replace("Z", "+00:00"))


def parse(value: bytes, consumed_at: datetime) -> dict:
    """One message value to a change row. Raises BadEvent with the quarantine reason."""
    try:
        msg = json.loads(value)
    except (TypeError, ValueError):
        raise BadEvent("not json") from None
    op, source = msg.get("op"), msg.get("source") or {}
    if op not in ("c", "u", "r", "d"):
        raise BadEvent(f"unknown op {op!r}")
    lsn = source.get("lsn")
    snap = op == "r" or str(source.get("snapshot")) in SNAPSHOT_FLAGS
    if lsn is None and snap and source.get("sequence"):
        # incremental snapshot reads carry no lsn of their own. The sequence holds the stream position
        # the chunk was read at, and anything that changed the row later comes after it in the topic.
        lsn = json.loads(source["sequence"])[-1]
    if lsn is None:
        raise BadEvent("no lsn")
    image = msg.get("before") if op == "d" else msg.get("after")
    if not image or image.get("violationid") is None:
        raise BadEvent("no row image with a violationid")
    row = {}
    for name, value in image.items():
        if name in DATE_COLUMNS:
            value = None if value is None else EPOCH + timedelta(days=value)
        elif name in TS_COLUMNS:
            value = _iso_ts(value)
        row[name] = value
    if op != "d":
        if row.get("class") not in CLASSES:
            raise BadEvent(f"unknown class {row.get('class')!r}")
        if row.get("violationstatus") not in STATUSES:
            raise BadEvent(f"unknown violationstatus {row.get('violationstatus')!r}")
        if row.get("inspectiondate") is None:
            raise BadEvent("missing inspectiondate")
    row.update(
        lsn=int(lsn),
        snap=snap,
        op=op,
        commit_ts=_ts(source["ts_ms"]),
        consumed_at=consumed_at,
    )
    return row


def extra_fields(rows: list) -> dict:
    """Columns in the events that the base schema doesn't know, with an Arrow type from the values."""
    extra = {}
    for r in rows:
        for name, value in r.items():
            if name in KNOWN or name in extra or name in ("lsn", "snap", "op", "commit_ts", "consumed_at"):
                continue
            if value is None:
                continue
            if isinstance(value, bool):
                extra[name] = pa.bool_()
            elif isinstance(value, int):
                extra[name] = pa.int64()
            elif isinstance(value, float):
                extra[name] = pa.float64()
            else:
                extra[name] = pa.string()
    return extra


def heartbeat_ts(value: bytes):
    msg = json.loads(value)
    after = msg.get("after") or {}
    return _iso_ts(after.get("beat_at"))
