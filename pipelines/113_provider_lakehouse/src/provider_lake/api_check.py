"""Spot check the dimension against the live NPPES Registry API.

The next monthly full file isn't out yet, so this is the outside check I can run
today. The API serves the provider as of right now. NPIs the API says were
updated after our last loaded weekly are skipped, since we can't have seen
those changes yet.
"""
import json
import logging
import random
import re
import time
import urllib.request
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .settings import Settings
from .sources import USER_AGENT

log = logging.getLogger(__name__)
API = "https://npiregistry.cms.hhs.gov/api/?version=2.1&number={}"
FIELDS = ("name", "practice_address_1", "practice_city", "practice_state", "practice_postal_code",
          "primary_taxonomy", "is_active")


def fetch(npi: str) -> dict | None:
    req = urllib.request.Request(API.format(npi), headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read())
    results = body.get("results") or []
    return results[0] if results else None


def from_api(rec: dict | None) -> dict:
    if rec is None:
        # the API leaves deactivated NPIs out entirely
        return {"is_active": False}
    basic = rec.get("basic", {})
    loc = next((a for a in rec.get("addresses", []) if a.get("address_purpose") == "LOCATION"), {})
    prim = next((t for t in rec.get("taxonomies", []) if t.get("primary")), {})
    name = basic.get("organization_name") if rec.get("enumeration_type") == "NPI-2" else basic.get("last_name")
    return {"name": name, "practice_address_1": loc.get("address_1"), "practice_city": loc.get("city"),
            "practice_state": loc.get("state"), "practice_postal_code": loc.get("postal_code"),
            "primary_taxonomy": prim.get("code"), "is_active": basic.get("status") == "A",
            "last_updated": basic.get("last_updated")}


def norm(v):
    if v is None or v == "":
        return None
    return re.sub(r"\s+", " ", str(v)).strip().upper()


def run(spark: SparkSession, cfg: Settings, state_end: date, sample_size: int = 300, seed: int = 113,
        pause: float = 0.25) -> dict:
    dim = spark.table(cfg.table("silver", "dim_provider")).filter("is_current")
    changed = [r["npi"] for r in spark.table(cfg.table("silver", "provider_change_log"))
               .filter(F.col("change_type") != "new").select("npi").distinct().collect()]
    everyone = [r["npi"] for r in dim.select("npi").collect()]
    rng = random.Random(seed)
    half = sample_size // 2
    picked = rng.sample(sorted(changed), min(half, len(changed)))
    picked += rng.sample(sorted(set(everyone) - set(picked)), sample_size - len(picked))

    ours = {r["npi"]: r for r in dim.filter(F.col("npi").isin(picked)).collect()}
    results = {"compared": 0, "skipped_newer": 0, "errors": 0, "field_mismatches": {f: 0 for f in FIELDS},
               "rows_all_match": 0, "examples": []}
    for npi in picked:
        try:
            api = from_api(fetch(npi))
        except OSError as e:
            log.warning("api error for %s: %s", npi, e)
            results["errors"] += 1
            continue
        finally:
            time.sleep(pause)  # the registry has no published rate limit, be polite
        if api.get("last_updated") and date.fromisoformat(api["last_updated"]) > state_end:
            results["skipped_newer"] += 1
            continue
        row = ours[npi]
        mine = {"name": row["org_name"] if row["entity_type"] == "2" else row["last_name"],
                "practice_address_1": row["practice_address_1"], "practice_city": row["practice_city"],
                "practice_state": row["practice_state"], "practice_postal_code": row["practice_postal_code"],
                "primary_taxonomy": row["primary_taxonomy"], "is_active": row["is_active"]}
        results["compared"] += 1
        # a deactivated NPI has no record in the API, so status is the only thing to compare.
        # The dimension keeps the last known name and address on purpose.
        fields = FIELDS if api.get("is_active") or "name" in api else ("is_active",)
        if fields == ("is_active",):
            results["deactivated_in_api"] = results.get("deactivated_in_api", 0) + 1
        bad = [f for f in fields if norm(mine[f]) != norm(api.get(f))]
        for f in bad:
            results["field_mismatches"][f] += 1
        if not bad:
            results["rows_all_match"] += 1
        elif len(results["examples"]) < 10:
            results["examples"].append({"npi": npi, "fields": {f: [mine[f], api.get(f)] for f in bad}})
    return results
