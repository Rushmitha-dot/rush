"""Two outside checks on the index.

1. openFDA's drug label API for a random sample of active labels. openFDA rebuilds weekly,
   so it is expected to lag. A label where openFDA is newer than us means we missed a file.
2. Every tombstone against DailyMed's current label list. A tombstoned set id that is still
   listed means a removal was read wrong.
"""

import json
import random
import sys
import time
import urllib.error
import urllib.parse
from datetime import date

from . import dailymed, store
from .settings import ROOT, load_settings

OPENFDA = "https://api.fda.gov/drug/label.json"


def openfda_version(set_id: str) -> tuple[int, str] | None:
    search = 'set_id:"' + set_id + '"'
    query = urllib.parse.urlencode({"search": search, "limit": 1})
    url = f"{OPENFDA}?{query}"
    try:
        body = json.loads(dailymed._get(url, timeout=30))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    row = body["results"][0]
    return int(row["version"]), row.get("effective_time", "")


def main(argv=None) -> int:
    argv = argv or sys.argv[1:]
    sample_size = int(argv[0]) if argv else 100
    settings = load_settings()
    conn = store.connect(settings)
    active = conn.execute("select set_id::text, version from labels where status = 'active' order by 1").fetchall()
    sample = random.Random(117).sample(active, min(sample_size, len(active)))
    counts = {"same": 0, "openfda_behind": 0, "openfda_ahead": 0, "not_in_openfda": 0}
    ahead = []
    for set_id, version in sample:
        got = openfda_version(set_id)
        if got is None:
            counts["not_in_openfda"] += 1
        elif got[0] == version:
            counts["same"] += 1
        elif got[0] < version:
            counts["openfda_behind"] += 1
        else:
            counts["openfda_ahead"] += 1
            ahead.append({"set_id": set_id, "ours": version, "openfda": got[0]})
        time.sleep(0.3)  # 240 requests a minute without a key

    tombstoned = [r[0] for r in conn.execute("select distinct set_id::text from tombstones order by 1")]
    still_listed = []
    for set_id in tombstoned:
        if dailymed.is_listed(set_id):
            still_listed.append(set_id)
        time.sleep(0.3)

    result = {
        "run_on": date.today().isoformat(),
        "openfda_sample": len(sample),
        "openfda": counts,
        "openfda_ahead": ahead,
        "tombstones_checked": len(tombstoned),
        "tombstones_still_listed": still_listed,
    }
    out = ROOT / "eval" / "crosscheck.json"
    out.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 1 if ahead or still_listed else 0


if __name__ == "__main__":
    sys.exit(main())
