"""Read HPD violations from the NYC Open Data SODA API and turn them into typed mirror rows."""

import logging
import time
from datetime import UTC, date, datetime

import requests

log = logging.getLogger(__name__)

# source field -> Postgres type. Field names stay as the source spells them, so a new source column
# can be added by name without a mapping step.
COLUMNS = {
    "violationid": "bigint",
    "buildingid": "bigint",
    "registrationid": "bigint",
    "boroid": "smallint",
    "boro": "text",
    "housenumber": "text",
    "streetname": "text",
    "zip": "text",
    "apartment": "text",
    "story": "text",
    "block": "integer",
    "lot": "integer",
    "class": "text",
    "inspectiondate": "date",
    "approveddate": "date",
    "originalcertifybydate": "date",
    "originalcorrectbydate": "date",
    "newcertifybydate": "date",
    "newcorrectbydate": "date",
    "certifieddate": "date",
    "ordernumber": "text",
    "novid": "bigint",
    "novdescription": "text",
    "novissueddate": "date",
    "currentstatusid": "integer",
    "currentstatus": "text",
    "currentstatusdate": "date",
    "novtype": "text",
    "violationstatus": "text",
    "latitude": "double precision",
    "longitude": "double precision",
    "communityboard": "integer",
    "councildistrict": "integer",
    "censustract": "text",
    "bin": "bigint",
    "bbl": "text",
    "nta": "text",
}

CLASSES = {"A", "B", "C", "I"}
STATUSES = {"Open", "Close"}
INT_TYPES = {"bigint", "integer", "smallint"}


class Rejected(ValueError):
    pass


def columns(extra=()) -> dict:
    cols = dict(COLUMNS)
    for name in extra:
        cols.setdefault(name, "text")
    return cols


def _parse(value, pg_type, name):
    if value is None or value == "":
        return None
    try:
        if pg_type in INT_TYPES:
            return int(value)
        if pg_type == "double precision":
            return float(value)
        if pg_type == "date":
            return date.fromisoformat(value[:10])
    except ValueError:
        raise Rejected(f"bad {name}: {value!r}") from None
    return str(value).strip()


def to_row(record: dict, cols: dict, boro: str) -> dict:
    """One API record to a mirror row. Raises Rejected with the reason for quarantine."""
    if not record.get("violationid"):
        raise Rejected("missing violationid")
    row = {name: _parse(record.get(name), pg_type, name) for name, pg_type in cols.items()}
    if row["class"] not in CLASSES:
        raise Rejected(f"unknown class {row['class']!r}")
    if row["violationstatus"] not in STATUSES:
        raise Rejected(f"unknown violationstatus {row['violationstatus']!r}")
    if row["inspectiondate"] is None:
        raise Rejected("missing inspectiondate")
    if row["boro"] != boro:
        raise Rejected(f"boro {row['boro']!r} outside {boro}")
    row["source_updated_at"] = parse_ts(record[":updated_at"])
    return row


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def soql_ts(ts: datetime) -> str:
    # SODA compares :updated_at against a floating literal read as UTC, millisecond precision
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


class SodaClient:
    def __init__(self, url, page_size=5000, page_sleep=1.0, session=None, timeout=300, retries=5):
        self.url = url
        self.page_size = page_size
        self.page_sleep = page_sleep
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", "hpd-violation-cdc/0.1 (portfolio pipeline)")
        self.timeout = timeout
        self.retries = retries

    def get(self, params: dict) -> list:
        for attempt in range(1, self.retries + 1):
            try:
                r = self.session.get(self.url, params=params, timeout=self.timeout)
                if r.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"{r.status_code} {r.text[:200]}")
                r.raise_for_status()
                return r.json()
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
                if attempt == self.retries:
                    raise
                wait = min(60, 5 * 2 ** (attempt - 1))
                log.warning("soda attempt %d failed (%s), retrying in %ss", attempt, e, wait)
                time.sleep(wait)
        return []

    def pages_by_updated_at(self, base_where: str, watermark: datetime, last_id: int, select: str):
        """Keyset pages ordered by (:updated_at, violationid), strictly after (watermark, last_id)."""
        while True:
            wm = soql_ts(watermark)
            after = f"(:updated_at > '{wm}' OR (:updated_at = '{wm}' AND violationid > {int(last_id)}))"
            where = f"{base_where} AND {after}"
            page = self.get({"$select": select, "$where": where, "$order": ":updated_at,violationid",
                             "$limit": self.page_size})
            if not page:
                return
            yield page
            watermark, last_id = parse_ts(page[-1][":updated_at"]), int(page[-1]["violationid"])
            if len(page) < self.page_size:
                return
            time.sleep(self.page_sleep)

    def pages_by_id(self, base_where: str, select: str):
        """Keyset pages ordered by violationid. Used by the seed, where ordering by :updated_at over a
        whole month of rows timed out on the API side."""
        last_id = 0
        while True:
            where = f"{base_where} AND violationid > {last_id}"
            page = self.get({"$select": select, "$where": where, "$order": "violationid", "$limit": self.page_size})
            if not page:
                return
            yield page
            last_id = int(page[-1]["violationid"])
            if len(page) < self.page_size:
                return
            time.sleep(self.page_sleep)
