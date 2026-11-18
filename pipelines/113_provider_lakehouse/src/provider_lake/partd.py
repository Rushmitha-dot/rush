"""Medicare Part D Prescribers by Provider and Drug, one CSV per data year.

Each national file is several GB, so it is streamed straight from data.cms.gov,
hashed on the way through, and only scope-state rows are written to landing.
"""
import csv
import gzip
import hashlib
import io
import json
import logging
import time
import urllib.request
from pathlib import Path

from .sources import PARTD_URLS, USER_AGENT

log = logging.getLogger(__name__)

EXPECTED_HEADER = [
    "Prscrbr_NPI", "Prscrbr_Last_Org_Name", "Prscrbr_First_Name", "Prscrbr_City", "Prscrbr_State_Abrvtn",
    "Prscrbr_State_FIPS", "Prscrbr_Type", "Prscrbr_Type_Src", "Brnd_Name", "Gnrc_Name", "Tot_Clms",
    "Tot_30day_Fills", "Tot_Day_Suply", "Tot_Drug_Cst", "Tot_Benes", "GE65_Sprsn_Flag", "GE65_Tot_Clms",
    "GE65_Tot_30day_Fills", "GE65_Tot_Drug_Cst", "GE65_Tot_Day_Suply", "GE65_Bene_Sprsn_Flag", "GE65_Tot_Benes",
]


class _HashingReader(io.RawIOBase):
    def __init__(self, raw):
        self.raw, self.sha, self.bytes = raw, hashlib.sha256(), 0

    def readable(self):
        return True

    def readinto(self, b):
        chunk = self.raw.read(len(b))
        self.sha.update(chunk)
        self.bytes += len(chunk)
        b[: len(chunk)] = chunk
        return len(chunk)


def land_year(year: int, out_dir: Path, states, url: str | None = None) -> dict:
    url = url or PARTD_URLS[year]
    states = set(states)
    t0 = time.time()
    dest = out_dir / f"partd_{year}"
    dest.mkdir(parents=True, exist_ok=True)
    out_path = dest / "prescriber_drug.csv.gz"
    tmp = out_path.with_suffix(".gz.part")

    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=300) as resp:
        hashed = _HashingReader(resp)
        reader = csv.reader(io.TextIOWrapper(io.BufferedReader(hashed, 1 << 20), encoding="utf-8-sig", newline=""))
        header = next(reader)
        if header != EXPECTED_HEADER:
            raise ValueError(f"Part D {year} header changed: {header}")
        read = kept = 0
        with gzip.open(tmp, "wt", encoding="utf-8", newline="", compresslevel=5) as out:
            writer = csv.writer(out)
            writer.writerow(header)
            for row in reader:
                read += 1
                if row[4] in states:
                    writer.writerow(row)
                    kept += 1
    tmp.rename(out_path)

    manifest = {"source_file": url.rsplit("/", 1)[1], "source_url": url, "source_sha256": hashed.sha.hexdigest(),
                "source_bytes": hashed.bytes, "claim_year": year, "rows_read": read, "rows_kept": kept,
                "path": str(out_path), "seconds": round(time.time() - t0, 1)}
    (dest / "manifest.json").write_text(json.dumps(manifest, indent=1))
    log.info("Part D %s, kept %s of %s rows (%.1f GB streamed)", year, kept, read, hashed.bytes / 1e9)
    return manifest
