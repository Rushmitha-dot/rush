import json
import logging
import urllib.request

log = logging.getLogger(__name__)


def send_alert(cfg, subject: str, body: str) -> None:
    """Always the log, plus a Slack-style webhook if ALERT_WEBHOOK is set.

    On Databricks the job's email/webhook notifications cover task failures. This is for
    the cases where the task succeeds but someone should still look (WARN gates, a
    republished file, a late weekly).
    """
    log.error("ALERT %s | %s", subject, body)
    if not cfg.alert_webhook:
        return
    req = urllib.request.Request(
        cfg.alert_webhook,
        data=json.dumps({"text": f"*{subject}*\n{body}"}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=5)
    except OSError as e:
        # an alert failing shouldn't hide the original failure
        log.warning("webhook alert failed: %s", e)
