"""Where the files come from, and how to tell them apart by name.

NPPES publishes one monthly full file and the last two weekly files at a time.
Older files are removed from the page, so anything we want to replay later has
to be kept in our own landing area.
"""
import hashlib
import re
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

NPPES_PAGE = "https://download.cms.gov/nppes/NPI_Files.html"
NUCC_TAXONOMY_URL = "https://www.nucc.org/images/stories/CSV/nucc_taxonomy_261.csv"

# data.cms.gov, "Medicare Part D Prescribers - by Provider and Drug", one CSV per data year
PARTD_URLS = {
    2023: "https://data.cms.gov/sites/default/files/2025-04/0d5915ce-002c-4d87-bde8-24ffb08bb6cc/"
          "MUP_DPR_RY25_P04_V10_DY23_NPIBN.csv",
    2024: "https://data.cms.gov/sites/default/files/2026-05/0ae165f4-eb44-495d-8cac-67f4571b6b83/"
          "MUP_DPR_RY26_P04_V10_DY24_NPIBN.csv",
}

# the CMS CDN returns 403 to urllib's default agent
USER_AGENT = "provider-lake/0.1 (portfolio pipeline; rushi.rushmitha@gmail.com)"

_WEEKLY = re.compile(r"NPPES_Data_Dissemination_(\d{6})_(\d{6})_Weekly_V2\.zip$")
_MONTHLY = re.compile(r"NPPES_Data_Dissemination_([A-Za-z]+)_(\d{4})_V2\.zip$")
_INNER = re.compile(r"npidata_pfile_(\d{8})-(\d{8})\.csv$")


@dataclass(frozen=True)
class NppesFile:
    name: str
    kind: str  # monthly | weekly

    @property
    def order_key(self) -> tuple:
        # monthly first within the same period so a replay rebuilds from the full file
        return (self.sort_date, 0 if self.kind == "monthly" else 1)

    @property
    def sort_date(self) -> date:
        m = _WEEKLY.search(self.name)
        if m:
            return datetime.strptime(m.group(1), "%m%d%y").date()
        m = _MONTHLY.search(self.name)
        return datetime.strptime(f"1 {m.group(1)} {m.group(2)}", "%d %B %Y").date()


def classify(name: str) -> NppesFile | None:
    if _WEEKLY.search(name):
        return NppesFile(name, "weekly")
    if _MONTHLY.search(name):
        return NppesFile(name, "monthly")
    return None


def inner_period(inner_name: str) -> tuple[date, date]:
    """The csv inside the zip carries the real coverage, e.g. npidata_pfile_20050523-20261102.csv."""
    m = _INNER.search(inner_name)
    if not m:
        raise ValueError(f"unexpected npidata file name {inner_name}")
    return tuple(datetime.strptime(x, "%Y%m%d").date() for x in m.groups())


def list_published(page_url: str = NPPES_PAGE) -> list[NppesFile]:
    html = _get(page_url).decode("utf-8", "replace")
    names = sorted(set(re.findall(r"NPPES_Data_Dissemination_[A-Za-z0-9_]+_V2\.zip", html)))
    files = [f for f in (classify(n) for n in names) if f]
    return sorted(files, key=lambda f: f.order_key)


def download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp, open(tmp, "wb") as out:
        while chunk := resp.read(1 << 20):
            out.write(chunk)
    tmp.rename(dest)  # only a complete file gets the real name
    return dest


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()
