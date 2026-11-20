import json
import logging
import urllib.request

log = logging.getLogger(__name__)


def send_alert(cfg, subject: str, body: str) -> None:
    """Always the log, plus a Slack-style webhook when ALERT_WEBHOOK is set."""
    log.error("ALERT %s | %s", subject, body)
    if not cfg.alert_webhook:
        return
    req = urllib.request.Request(
        cfg.alert_webhook,
        data=json.dumps({"text": f"*{subject}*\n{body}"}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:  # an alert failing must not fail the run
        log.warning("alert webhook failed: %s", e)
