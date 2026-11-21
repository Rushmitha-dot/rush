"""Gold tables and the checks that gate publishing them. DuckDB over Arrow scans of the Iceberg tables."""

import json
import logging
import time
from datetime import UTC, datetime

import duckdb

from .alerts import send_alert
from .lake import Lake

log = logging.getLogger(__name__)

# days from issue to the first Close row in the history
CLOSE_TIMES = """
    SELECT c.violationid, c.class, c.communityboard, c.buildingid, c.inspectiondate,
           coalesce(c.novissueddate, c.inspectiondate) AS opened_on, h.closed_on,
           datediff('day', coalesce(c.novissueddate, c.inspectiondate), h.closed_on) AS days_to_close
    FROM cur c
    LEFT JOIN (SELECT violationid, min(valid_from) AS closed_on FROM hist
               WHERE violationstatus = 'Close' GROUP BY 1) h USING (violationid)
"""

TIME_TO_CLOSE = f"""
    WITH t AS ({CLOSE_TIMES}), as_of AS (SELECT max(inspectiondate) AS d FROM cur)
    SELECT class,
           CASE WHEN grouping(communityboard) = 1 THEN 'all'
                ELSE coalesce(communityboard::varchar, 'unknown') END AS community_board,
           count(*) AS violations,
           count(closed_on) AS closed,
           count(*) - count(closed_on) AS still_open,
           median(days_to_close)::double AS median_days_to_close,
           round(quantile_cont(days_to_close, 0.9), 1)::double AS p90_days_to_close,
           count(*) FILTER (WHERE opened_on <= (SELECT d FROM as_of) - 90) AS issued_90d_before,
           round(avg(CASE WHEN closed_on IS NOT NULL AND days_to_close <= 90 THEN 1.0 ELSE 0.0 END)
                 FILTER (WHERE opened_on <= (SELECT d FROM as_of) - 90), 4)::double AS closed_within_90d,
           median(datediff('day', opened_on, (SELECT d FROM as_of)))
                 FILTER (WHERE closed_on IS NULL)::double AS median_open_age_days
    FROM t
    GROUP BY GROUPING SETS ((class, communityboard), (class))
    ORDER BY class, grouping(communityboard) DESC, communityboard NULLS LAST
"""

REPEAT_CLASS_C = f"""
    WITH t AS ({CLOSE_TIMES})
    SELECT c.buildingid,
           any_value(c.housenumber || ' ' || c.streetname) AS address,
           any_value(c.nta) AS nta,
           any_value(c.communityboard) AS communityboard,
           count(*) AS class_c,
           count(DISTINCT c.inspectiondate) AS inspection_days,
           count(*) FILTER (WHERE c.violationstatus = 'Open') AS open_class_c,
           min(c.inspectiondate) AS first_inspection,
           max(c.inspectiondate) AS last_inspection,
           median(t.days_to_close)::double AS median_days_to_close
    FROM cur c JOIN t USING (violationid)
    WHERE c.class = 'C'
    GROUP BY c.buildingid
    HAVING count(DISTINCT c.inspectiondate) >= 3
    ORDER BY inspection_days DESC, class_c DESC, c.buildingid
"""

# name, severity, sql returning the number of failing rows
CHECKS = [
    ("current_unique_violationid", "error", "SELECT count(*) - count(DISTINCT violationid) FROM cur"),
    ("current_class_known", "error", "SELECT count(*) FROM cur WHERE class NOT IN ('A', 'B', 'C', 'I')"),
    ("current_row_has_open_history_row", "error", """
        SELECT count(*) FROM cur c
        WHERE (SELECT count(*) FROM hist h WHERE h.violationid = c.violationid AND h.is_current) <> 1"""),
    ("history_valid_to_after_valid_from", "error", "SELECT count(*) FROM hist WHERE valid_to < valid_from"),
    ("history_rows_are_contiguous", "error", """
        SELECT count(*) FROM (
            SELECT valid_from, lag(valid_to) OVER (PARTITION BY violationid ORDER BY valid_from, lsn) AS prev_to,
                   row_number() OVER (PARTITION BY violationid ORDER BY valid_from, lsn) AS rn
            FROM hist) WHERE rn > 1 AND prev_to IS DISTINCT FROM valid_from"""),
    ("history_status_matches_current", "error", """
        SELECT count(*) FROM cur c JOIN hist h ON h.violationid = c.violationid AND h.is_current
        WHERE h.violationstatus IS DISTINCT FROM c.violationstatus
           OR h.currentstatusid IS DISTINCT FROM c.currentstatusid"""),
    ("current_is_last_logged_change", "error", """
        SELECT count(*) FROM cur c JOIN (SELECT violationid, max(lsn) AS lsn FROM changes GROUP BY 1) l
        USING (violationid) WHERE l.lsn <> c.lsn"""),
    ("closed_before_issued", "warn", f"SELECT count(*) FROM ({CLOSE_TIMES}) WHERE days_to_close < 0"),
    ("closed_without_status_date", "warn",
     "SELECT count(*) FROM cur WHERE violationstatus = 'Close' AND currentstatusdate IS NULL"),
]


def connect(lake: Lake):
    con = duckdb.connect()
    con.register("cur", lake.read("violations_current"))
    con.register("hist", lake.read("violation_status_history"))
    con.register("changes", lake.read("violation_changes", ["violationid", "lsn"]))
    return con


def run_checks(con, lake: Lake | None = None) -> list:
    results = []
    for name, severity, q in CHECKS:
        failed = con.sql(q).fetchall()[0][0]
        results.append(dict(check=name, severity=severity, failed=int(failed)))
    if lake is not None:
        # share of change events quarantined over the last 24 hours of batches
        msgs, bad = lake.ops.execute("""
            SELECT coalesce(sum(messages), 0), coalesce(sum(quarantined), 0) FROM consumer_batches
            WHERE started_at >= datetime('now', '-1 day')""").fetchone()
        share = bad / msgs if msgs else 0.0
        results.append(dict(check="quarantine_share_24h", severity="error" if msgs >= 100 and share > 0.05 else
                            "warn", failed=int(bad), detail=f"{bad} of {msgs}"))
    return results


def blocking(results) -> list:
    return [r for r in results if r["severity"] == "error" and r["failed"]]


def publish(lake: Lake, cfg=None) -> tuple[bool, list]:
    t0 = time.monotonic()
    con = connect(lake)
    results = run_checks(con, lake)
    failed = blocking(results)
    status = "blocked" if failed else "published"
    if not failed:
        for name, q in (("gold_time_to_close", TIME_TO_CLOSE), ("gold_repeat_class_c", REPEAT_CLASS_C)):
            data = con.sql(q).to_arrow_table()
            table = lake.table(name, data.schema)
            table.overwrite(data.cast(table.schema().as_arrow()))
    lake.ops.execute("INSERT INTO publish_runs (run_at, status, checks, seconds) VALUES (?, ?, ?, ?)",
                     (datetime.now(UTC).isoformat(), status, json.dumps(results), round(time.monotonic() - t0, 2)))
    lake.ops.commit()
    if failed and cfg is not None:
        send_alert(cfg, "hpd gold publish blocked", ", ".join(f"{r['check']}={r['failed']}" for r in failed))
    return not failed, results
