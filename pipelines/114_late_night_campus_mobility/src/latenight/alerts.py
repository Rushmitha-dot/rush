import json
import logging
import urllib.request

log = logging.getLogger(__name__)


def send_alert(webhook: str | None, subject: str, body: str, level: str = "error") -> None:
    """Always logs. Posts to a Slack-style webhook when one is configured."""
    (log.error if level == "error" else log.warning)("ALERT %s | %s", subject, body)
    if not webhook:
        return
    req = urllib.request.Request(
        webhook,
        data=json.dumps({"text": f"[{level}] *{subject}*\n{body}"}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=5)
    except OSError as e:
        # a broken webhook shouldn't mask the failure we were reporting
        log.warning("webhook alert failed: %s", e)
