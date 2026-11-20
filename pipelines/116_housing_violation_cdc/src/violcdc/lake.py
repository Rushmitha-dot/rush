"""Iceberg tables on a local SQL catalog (sqlite) and a local warehouse folder."""

import sqlite3
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError

from .soda import COLUMNS

NS = "hpd"
ARROW_TYPES = {
    "bigint": pa.int64(),
    "integer": pa.int64(),
    "smallint": pa.int64(),
    "text": pa.string(),
    "date": pa.date32(),
    "double precision": pa.float64(),
}
TS = pa.timestamp("us", tz="UTC")

CHANGE_META = [
    ("lsn", pa.int64()), ("snap", pa.bool_()), ("op", pa.string()), ("commit_ts", TS), ("consumed_at", TS),
    ("source_updated_at", TS), ("mirrored_at", TS), ("row_hash", pa.string()),
]

HISTORY = pa.schema([
    ("violationid", pa.int64()), ("violationstatus", pa.string()), ("currentstatusid", pa.int64()),
    ("currentstatus", pa.string()), ("valid_from", pa.date32()), ("valid_to", pa.date32()),
    ("is_current", pa.bool_()), ("derived", pa.bool_()), ("lsn", pa.int64()), ("closed_lsn", pa.int64()),
    ("recorded_at", TS),
])

QUARANTINE = pa.schema([
    ("reason", pa.string()), ("payload", pa.string()), ("topic", pa.string()), ("partition", pa.int64()),
    ("offset", pa.int64()), ("lsn", pa.int64()), ("consumed_at", TS),
])

OPS_DDL = """
CREATE TABLE IF NOT EXISTS consumer_batches (
    batch_id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT, finished_at TEXT, messages INTEGER,
    events INTEGER, kept INTEGER, replay_skipped INTEGER, same_skipped INTEGER, quarantined INTEGER,
    heartbeats INTEGER, snapshot_events INTEGER, new_columns TEXT, first_offset INTEGER, last_offset INTEGER,
    min_commit_ts TEXT, max_commit_ts TEXT, iceberg_committed_at TEXT, lag_p50_s REAL, lag_p95_s REAL,
    lag_max_s REAL, heartbeat_lag_s REAL, seconds REAL
);
CREATE TABLE IF NOT EXISTS publish_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT, run_at TEXT, status TEXT, checks TEXT, seconds REAL
);
"""


def change_schema(extra: dict | None = None) -> pa.Schema:
    fields = [(name, ARROW_TYPES[t]) for name, t in COLUMNS.items()]
    fields += list((extra or {}).items())
    return pa.schema(fields + CHANGE_META)


class Lake:
    def __init__(self, root: Path):
        self.root = Path(root)
        (self.root / "warehouse").mkdir(parents=True, exist_ok=True)
        self.catalog = SqlCatalog(
            NS, uri=f"sqlite:///{self.root / 'catalog.db'}", warehouse=f"file://{self.root / 'warehouse'}")
        try:
            self.catalog.create_namespace(NS)
        except NamespaceAlreadyExistsError:
            pass
        self.ops = sqlite3.connect(self.root / "ops.sqlite")
        self.ops.executescript(OPS_DDL)

    def table(self, name: str, schema: pa.Schema | None = None):
        ident = f"{NS}.{name}"
        if schema is None:
            return self.catalog.load_table(ident)
        return self.catalog.create_table_if_not_exists(ident, schema=schema)

    def exists(self, name: str) -> bool:
        return self.catalog.table_exists(f"{NS}.{name}")

    @property
    def changes(self):
        return self.table("violation_changes", change_schema())

    @property
    def current(self):
        return self.table("violations_current", change_schema())

    @property
    def history(self):
        return self.table("violation_status_history", HISTORY)

    @property
    def quarantine(self):
        return self.table("quarantine", QUARANTINE)

    def read(self, name: str, columns=None) -> pa.Table:
        if not self.exists(name):
            return None
        scan = self.table(name).scan(selected_fields=tuple(columns)) if columns else self.table(name).scan()
        return scan.to_arrow()


def evolve(table, schema: pa.Schema) -> list:
    """Add columns the table doesn't have yet. Iceberg tracks columns by id, so existing data files are
    left alone and read the new column as null."""
    have = set(table.schema().column_names)
    new = [f.name for f in schema if f.name not in have]
    if new:
        with table.update_schema() as update:
            update.union_by_name(pa.schema([schema.field(n) for n in new]))
    return new


def align(rows: list, table) -> pa.Table:
    """Rows (dicts) to an Arrow table in the Iceberg table's column order and types."""
    target = table.schema().as_arrow()
    return pa.Table.from_pylist([{n: r.get(n) for n in target.names} for r in rows], schema=target)
