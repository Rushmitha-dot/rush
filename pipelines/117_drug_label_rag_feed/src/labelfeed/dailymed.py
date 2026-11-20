"""DailyMed downloads. Update zips are slimmed to the human prescription XML on arrival.

A daily update zip holds one inner zip per label (XML plus package photos) for every
category, and is 60 to 230 MB. The feed only answers from human Rx labels and never reads
the images, so each update is rewritten as a zip of just those XML files plus the
added/deleted lists, with a manifest written last. The original is deleted after that.
"""

import hashlib
import io
import json
import re
import time
import urllib.request
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

BASE = "https://dailymed.nlm.nih.gov/dailymed"
FILES = "https://dailymed-data.nlm.nih.gov/public-release-files"
LISTING = f"{BASE}/spl-resources-all-drug-labels.cfm"
USER_AGENT = "labelfeed/0.1 (portfolio data pipeline)"

DAILY = re.compile(r"dm_spl_daily_update_(\d{2})(\d{2})(\d{4})\.zip")
WEEKLY = re.compile(r"dm_spl_weekly_update_\d{6}_(\d{2})(\d{2})(\d{2})\.zip")
BOOTSTRAP = re.compile(r"bootstrap_(\d{4})(\d{2})(\d{2})\.zip")
MEMBER = re.compile(r"^prescription/(\d{8})_([0-9a-f-]{36})\.(zip|xml)$")


def file_date(name: str) -> date | None:
    """The last day an update file covers."""
    if m := DAILY.fullmatch(name):
        return date(int(m[3]), int(m[1]), int(m[2]))
    if m := WEEKLY.fullmatch(name):
        return date(2000 + int(m[3]), int(m[1]), int(m[2]))
    if m := BOOTSTRAP.fullmatch(name):
        return date(int(m[1]), int(m[2]), int(m[3]))
    return None


def order_key(name: str) -> tuple:
    # bootstrap first, then by last day covered, a weekly before a daily for the same day
    return (not name.startswith("bootstrap_"), file_date(name) or date.min, "daily" in name)


def daily_name(day: date) -> str:
    return f"dm_spl_daily_update_{day:%m%d%Y}.zip"


def _get(url: str, timeout: int = 120, tries: int = 6) -> bytes:
    for attempt in range(tries):
        wait = 2 ** attempt
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 404 or attempt == tries - 1:
                raise
            if e.code == 429:  # the web services rate limit, back off harder
                wait = 10 * 2 ** attempt
        except (urllib.error.URLError, TimeoutError):
            if attempt == tries - 1:
                raise
        time.sleep(wait)
    raise AssertionError("unreachable")


def listing() -> str:
    return _get(LISTING, timeout=60).decode("utf-8", "replace")


def available_updates(html: str) -> dict[str, str | None]:
    """Daily and weekly update files on the downloads page, with the MD5 the page lists."""
    out: dict[str, str | None] = {}
    text = re.sub(r"<[^>]+>", " ", html)
    for m in re.finditer(r"(dm_spl_(?:daily|weekly)_update_[0-9_]+\.zip)", text):
        md5 = re.search(r"MD5 checksum:\s*([0-9a-f]{32})", text[m.end():m.end() + 600])
        if out.get(m[1]) is None:
            out[m[1]] = md5[1] if md5 else None
    return dict(sorted(out.items(), key=lambda kv: order_key(kv[0])))


def md5_file(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _rx_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if MEMBER.match(line.strip())]


def slim(original: Path, out_dir: Path, source_url: str | None = None) -> Path:
    """Rewrite an update zip as Rx XML only. Returns the manifest path."""
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / original.name
    tmp = target.with_suffix(".zip.part")
    added = deleted = []
    n_xml = 0
    with zipfile.ZipFile(original) as src, zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as dst:
        names = set(src.namelist())
        if "files_added.txt" in names:
            added = _rx_lines(src.read("files_added.txt").decode("utf-8", "replace"))
        if "files_deleted.txt" in names:
            deleted = _rx_lines(src.read("files_deleted.txt").decode("utf-8", "replace"))
        for member in sorted(names):
            m = MEMBER.match(member)
            if not m or m[3] != "zip":
                continue
            with zipfile.ZipFile(io.BytesIO(src.read(member))) as inner:
                xmls = [n for n in inner.namelist() if n.endswith(".xml")]
                if len(xmls) != 1:
                    continue  # the loader reports the missing XML against files_added
                dst.writestr(member[:-4] + ".xml", inner.read(xmls[0]))
                n_xml += 1
        dst.writestr("files_added.txt", "\n".join(added) + "\n")
        dst.writestr("files_deleted.txt", "\n".join(deleted) + "\n")
    tmp.rename(target)
    manifest = {
        "update_file": original.name,
        "source_url": source_url,
        "source_bytes": original.stat().st_size,
        "source_md5": md5_file(original),
        "rx_added": len(added),
        "rx_deleted": len(deleted),
        "rx_xml": n_xml,
        "slim_bytes": target.stat().st_size,
        "slim_md5": md5_file(target),
        "written_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    path = out_dir / (original.name + ".manifest.json")
    path.write_text(json.dumps(manifest, indent=2))
    return path


class ChecksumMismatch(ValueError):
    pass


def fetch_update(name: str, out_dir: Path, incoming: Path, listed_md5: str | None = None,
                 keep_original: bool = False) -> Path:
    """Download one update zip, check it against the MD5 the downloads page lists, slim it
    and return its manifest. Skips work already done."""
    manifest = out_dir / (name + ".manifest.json")
    if manifest.exists() and (out_dir / name).exists():
        return manifest
    incoming.mkdir(parents=True, exist_ok=True)
    raw = incoming / name
    url = f"{FILES}/{name}"
    if not raw.exists():
        part = raw.with_suffix(".part")
        part.write_bytes(_get(url, timeout=900))
        part.rename(raw)
    if listed_md5 and md5_file(raw) != listed_md5:
        raw.rename(raw.with_suffix(".bad"))
        raise ChecksumMismatch(f"{name}: md5 does not match the {listed_md5} DailyMed lists")
    path = slim(raw, out_dir, source_url=url)
    if listed_md5:
        manifest_data = json.loads(path.read_text())
        manifest_data["listed_md5"] = listed_md5
        path.write_text(json.dumps(manifest_data, indent=2))
    if not keep_original:
        raw.unlink()
    return path


def history(set_id: str) -> list[dict]:
    body = json.loads(_get(f"{BASE}/services/v2/spls/{set_id}/history.json?pagesize=100"))
    out = []
    for row in body["data"]["history"]:
        published = datetime.strptime(row["published_date"], "%b %d, %Y").date()
        out.append({"version": int(row["spl_version"]), "published": published})
    return out


def label_xml(set_id: str, version: int | None = None) -> bytes:
    """One label's XML, a specific version or the latest DailyMed still serves."""
    if version is None:
        url = f"{BASE}/downloadzipfile.cfm?setId={set_id}"
    else:
        url = f"{BASE}/getFile.cfm?type=zip&setid={set_id}&version={version}"
    with zipfile.ZipFile(io.BytesIO(_get(url))) as z:
        xmls = [n for n in z.namelist() if n.endswith(".xml")]
        if len(xmls) != 1:
            raise ValueError(f"{set_id} v{version}: expected one xml, got {len(xmls)}")
        return z.read(xmls[0])


def is_listed(set_id: str) -> bool:
    """True if DailyMed's current label list has the set id. Removed labels drop out of it."""
    body = json.loads(_get(f"{BASE}/services/v2/spls.json?setid={set_id}"))
    return body["metadata"]["total_elements"] > 0


def days(first: date, last: date) -> list[date]:
    return [first + timedelta(n) for n in range((last - first).days + 1)]
