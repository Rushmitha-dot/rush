"""The alert path is a log line, a line in data/alerts.log and a non-zero exit code.

If LABELFEED_ALERT_WEBHOOK is set the same message is posted there as JSON. I haven't
wired that to a real channel.
"""

import json
import os
import sys
import urllib.request
from datetime import UTC, datetime

from .settings import Settings


def alert(settings: Settings, level: str, message: str) -> None:
    line = f"{datetime.now(UTC).isoformat(timespec='seconds')} {level.upper()} {message}"
    print(line, file=sys.stderr)
    settings.alerts_log.parent.mkdir(parents=True, exist_ok=True)
    with open(settings.alerts_log, "a") as f:
        f.write(line + "\n")
    hook = os.environ.get("LABELFEED_ALERT_WEBHOOK")
    if hook:
        req = urllib.request.Request(hook, data=json.dumps({"text": line}).encode(),
                                     headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=10)
        except OSError as e:
            print(f"alert webhook failed: {e}", file=sys.stderr)
