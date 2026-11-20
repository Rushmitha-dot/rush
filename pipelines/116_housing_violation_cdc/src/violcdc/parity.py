"""Compare two lakes, or a lake against the Postgres mirror, row by row on content hashes."""

import duckdb

from .gold import TIME_TO_CLOSE
from .lake import Lake

# columns that legitimately differ between two runs of the same events
VOLATILE = {"consumed_at"}


def _rows(lake: Lake, name: str, start=None, end=None):
    data = lake.read(name)
    if data is None:
        return None
    con = duckdb.connect()
    con.register("t", data)
    keep = [c for c in data.column_names if c not in VOLATILE]
    where = ""
    if start and "inspectiondate" in data.column_names:
        where = f"WHERE inspectiondate >= DATE '{start}' AND inspectiondate < DATE '{end}'"
    return con, keep, where


def table_hashes(lake: Lake, name: str, key: list, start=None, end=None) -> dict:
    con, keep, where = _rows(lake, name, start, end)
    cols = ", ".join(f'"{c}"' for c in keep)
    q = f"SELECT {', '.join(key)}, md5(CAST(struct_pack({cols}) AS VARCHAR)) FROM t {where}"
    return {tuple(r[:-1]): r[-1] for r in con.sql(q).fetchall()}


def compare(a: dict, b: dict) -> dict:
    only_a = a.keys() - b.keys()
    only_b = b.keys() - a.keys()
    differ = [k for k in a.keys() & b.keys() if a[k] != b[k]]
    return dict(rows_a=len(a), rows_b=len(b), only_a=len(only_a), only_b=len(only_b), differ=len(differ),
                equal=not (only_a or only_b or differ), sample=sorted(list(only_a) + list(only_b) + differ)[:5])


def current_vs_current(a: Lake, b: Lake, start=None, end=None, columns=("row_hash",)) -> dict:
    """Current state of two lakes over an inspection date range, on the source row hash."""
    out = {}
    for lake, side in ((a, "a"), (b, "b")):
        data = lake.read("violations_current", ["violationid", "inspectiondate", *columns])
        con = duckdb.connect()
        con.register("t", data)
        where = f"WHERE inspectiondate >= DATE '{start}' AND inspectiondate < DATE '{end}'" if start else ""
        out[side] = {r[0]: r[1:] for r in con.sql(
            f"SELECT violationid, {', '.join(columns)} FROM t {where}").fetchall()}
    return compare(out["a"], out["b"])


def current_vs_mirror(lake: Lake, conn, start=None, end=None) -> dict:
    where, params = "", ()
    if start:
        where, params = "WHERE inspectiondate >= %s AND inspectiondate < %s", (start, end)
    mirror = {r[0]: (r[1],) for r in conn.execute(f"SELECT violationid, row_hash FROM mirror.violations {where}",
                                                  params).fetchall()}
    data = lake.read("violations_current", ["violationid", "inspectiondate", "row_hash"])
    con = duckdb.connect()
    con.register("t", data)
    w = f"WHERE inspectiondate >= DATE '{start}' AND inspectiondate < DATE '{end}'" if start else ""
    lake_rows = {r[0]: (r[1],) for r in con.sql(f"SELECT violationid, row_hash FROM t {w}").fetchall()}
    return compare(mirror, lake_rows)


def gold_for_range(lake: Lake, start, end) -> dict:
    con = duckdb.connect()
    cur = lake.read("violations_current")
    con.register("all_cur", cur)
    con.execute(f"CREATE VIEW cur AS SELECT * FROM all_cur WHERE inspectiondate >= DATE '{start}' "
                f"AND inspectiondate < DATE '{end}'")
    con.register("hist", lake.read("violation_status_history"))
    return {(r[0], r[1]): r[2:] for r in con.sql(TIME_TO_CLOSE).fetchall()}


def full(a: Lake, b: Lake) -> dict:
    """Every table of two lakes built from the same events, minus columns that record when it ran."""
    return {
        "violations_current": compare(table_hashes(a, "violations_current", ["violationid"]),
                                      table_hashes(b, "violations_current", ["violationid"])),
        "violation_changes": compare(table_hashes(a, "violation_changes", ["violationid", "lsn", "snap"]),
                                     table_hashes(b, "violation_changes", ["violationid", "lsn", "snap"])),
        "violation_status_history": compare(
            table_hashes(a, "violation_status_history", ["violationid", "valid_from", "lsn"]),
            table_hashes(b, "violation_status_history", ["violationid", "valid_from", "lsn"])),
    }
