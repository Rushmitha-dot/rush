"""CMS "Medicare Physician & Other Practitioners - by Provider and Service", one CSV per data year.

data.cms.gov moves files to a new folder when it republishes a year, so the URL plus the
Last-Modified header is what identifies a version of a year in the ledger.
"""
import json
import re
import urllib.request

CATALOG_URL = "https://data.cms.gov/data.json"
TITLE = "Medicare Physician & Other Practitioners - by Provider and Service"

YEAR_URLS = {
    2023: "https://data.cms.gov/sites/default/files/2025-04/e3f823f8-db5b-4cc7-ba04-e7ae92b99757/"
          "MUP_PHY_R25_P05_V20_D23_Prov_Svc.csv",
    2024: "https://data.cms.gov/sites/default/files/2026-05/b5ebab5a-f490-418a-9bce-4b9f31419356/"
          "PHY_R26_P05_V10_D24_Prov_Svc.csv",
}

# the CMS CDN refuses urllib's default agent
USER_AGENT = "claims-perf/0.1 (portfolio pipeline)"

COLUMNS = [
    "Rndrng_NPI", "Rndrng_Prvdr_Last_Org_Name", "Rndrng_Prvdr_First_Name", "Rndrng_Prvdr_MI",
    "Rndrng_Prvdr_Crdntls", "Rndrng_Prvdr_Ent_Cd", "Rndrng_Prvdr_St1", "Rndrng_Prvdr_St2",
    "Rndrng_Prvdr_City", "Rndrng_Prvdr_State_Abrvtn", "Rndrng_Prvdr_State_FIPS", "Rndrng_Prvdr_Zip5",
    "Rndrng_Prvdr_RUCA", "Rndrng_Prvdr_RUCA_Desc", "Rndrng_Prvdr_Cntry", "Rndrng_Prvdr_Type",
    "Rndrng_Prvdr_Mdcr_Prtcptg_Ind", "HCPCS_Cd", "HCPCS_Desc", "HCPCS_Drug_Ind", "Place_Of_Srvc",
    "Tot_Benes", "Tot_Srvcs", "Tot_Bene_Day_Srvcs", "Avg_Sbmtd_Chrg", "Avg_Mdcr_Alowd_Amt",
    "Avg_Mdcr_Pymt_Amt", "Avg_Mdcr_Stdzd_Amt",
]

# street address, middle initial and the RUCA text aren't used downstream and are most of the bytes
DROPPED_AT_LANDING = ["Rndrng_Prvdr_St1", "Rndrng_Prvdr_St2", "Rndrng_Prvdr_MI", "Rndrng_Prvdr_RUCA_Desc"]


def open_url(url: str, method: str = "GET", timeout: int = 120):
    req = urllib.request.Request(url, method=method, headers={"User-Agent": USER_AGENT})
    return urllib.request.urlopen(req, timeout=timeout)


def head(url: str) -> dict:
    """Last-Modified for http(s) urls. file:// urls (tests) return the file's mtime."""
    if url.startswith("file://"):
        from pathlib import Path
        return {"last_modified": str(int(Path(url[7:]).stat().st_mtime))}
    with open_url(url, method="HEAD", timeout=30) as r:
        return {"last_modified": r.headers.get("Last-Modified", "")}


def discover(catalog_url: str = CATALOG_URL) -> dict[int, str]:
    """Every data year CMS lists for this dataset, from the data.json catalog."""
    with open_url(catalog_url, timeout=120) as r:
        catalog = json.load(r)
    found = {}
    for ds in catalog.get("dataset", []):
        title = ds.get("title", "")
        if not title.startswith(TITLE):
            continue
        m = re.search(r":\s*(\d{4})-\d{2}-\d{2}$", title)
        csv = [d.get("downloadURL") for d in ds.get("distribution", []) if d.get("mediaType") == "text/csv"]
        if m and csv:
            found[int(m.group(1))] = csv[0]
    return found
