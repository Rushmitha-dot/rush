"""Starting state for the subset, as of the day before the first update file.

The full human Rx release is six zips of about 1.6 GB each. Instead of loading it, I take
every Rx label that an update file in the window deletes an older file for (so it existed
before the window) and fetch the version it had then from DailyMed's version archive. The
result is written as one more update file, so it goes through the same loader and ledger.
"""

import json
import re
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError

from . import dailymed, spl

DELETED = re.compile(r"^prescription/(\d{8})_([0-9a-f-]{36})\.(?:zip|xml)$")


def prior_files(update_paths: list[Path], window_start: date) -> dict[str, date]:
    """set id -> publish date of the file that was current before the window."""
    out: dict[str, date] = {}
    for path in update_paths:
        with zipfile.ZipFile(path) as z:
            for line in z.read("files_deleted.txt").decode().splitlines():
                m = DELETED.match(line.strip())
                if not m:
                    continue
                published = datetime.strptime(m[1], "%Y%m%d").date()
                if published < window_start and m[2] not in out:
                    out[m[2]] = published
    return out


def _fetch_one(set_id: str, published: date, cache: Path) -> dict:
    hits = list(cache.glob(f"{set_id}_v*.xml"))
    if hits:
        asked = int(hits[0].stem.split("_v")[1])
        got = spl.parse(hits[0].read_bytes()).version
        source = "archive" if got == asked else "latest_only"
        return {"set_id": set_id, "version": got, "source": source, "path": str(hits[0])}
    try:
        versions = dailymed.history(set_id)
    except (HTTPError, OSError, KeyError, ValueError) as e:
        return {"set_id": set_id, "error": f"history: {e}"}
    # the version published on the date in the deleted file name, allowing a day of
    # difference between the file stamp and the API's publish date
    before = [v for v in versions if v["published"] <= published + timedelta(days=1)]
    if not before:
        return {"set_id": set_id, "error": "no version before the window"}
    version = max(v["version"] for v in before)
    source = "archive"
    try:
        xml = dailymed.label_xml(set_id, version)
    except HTTPError as e:
        if e.code != 404:
            return {"set_id": set_id, "error": f"archive v{version}: {e}"}
        # archived (withdrawn) labels don't serve old versions, only the last one
        try:
            xml = dailymed.label_xml(set_id)
        except (HTTPError, OSError, ValueError) as e2:
            return {"set_id": set_id, "error": f"latest: {e2}"}
        source = "latest_only"
    except (OSError, ValueError) as e:
        return {"set_id": set_id, "error": f"archive v{version}: {e}"}
    path = cache / f"{set_id}_v{version}.xml"
    path.write_bytes(xml)
    return {"set_id": set_id, "version": version, "source": source, "path": str(path)}


def build(update_paths: list[Path], window_start: date, out_dir: Path, workers: int = 2) -> Path:
    cache = out_dir.parent / "bootstrap_cache"
    cache.mkdir(parents=True, exist_ok=True)
    wanted = prior_files(update_paths, window_start)
    print(f"bootstrap: {len(wanted)} set ids existed before {window_start}", file=sys.stderr)
    results = []
    with ThreadPoolExecutor(workers) as pool:
        futures = [pool.submit(_fetch_one, s, d, cache) for s, d in sorted(wanted.items())]
        for i, f in enumerate(futures, 1):
            results.append(f.result())
            if i % 100 == 0:
                print(f"  {i}/{len(futures)}", file=sys.stderr)

    day_before = window_start - timedelta(days=1)
    name = f"bootstrap_{day_before:%Y%m%d}.zip"
    target = out_dir / name
    tmp = target.with_suffix(".zip.part")
    added = []
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as z:
        for r in results:
            if "path" not in r:
                continue
            member = f"prescription/{wanted[r['set_id']]:%Y%m%d}_{r['set_id']}.xml"
            z.write(r["path"], member)
            added.append(member)
        z.writestr("files_added.txt", "\n".join(added) + "\n")
        z.writestr("files_deleted.txt", "")
    tmp.rename(target)

    errors = [r for r in results if "error" in r]
    (out_dir.parent / "bootstrap_results.json").write_text(json.dumps(results, indent=1))
    manifest = {
        "update_file": name,
        "source_url": None,
        "window_start": window_start.isoformat(),
        "set_ids_wanted": len(wanted),
        "rx_added": len(added),
        "rx_deleted": 0,
        "rx_xml": len(added),
        "latest_only": sum(r.get("source") == "latest_only" for r in results),
        "errors": errors,
        "slim_bytes": target.stat().st_size,
        "slim_md5": dailymed.md5_file(target),
        "written_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    manifest["source_md5"] = manifest["slim_md5"]
    path = out_dir / (name + ".manifest.json")
    path.write_text(json.dumps(manifest, indent=2))
    return path
