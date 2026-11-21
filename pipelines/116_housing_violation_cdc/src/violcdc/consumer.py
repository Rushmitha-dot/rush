"""Kafka consumer that merges Debezium change events into Iceberg, one batch at a time.

Offsets are committed after the Iceberg commits, so a crash replays the last batch. That is safe
because the merge drops anything at or before the log position it already has.
"""

import logging
import statistics
import time
from datetime import UTC, datetime

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from . import events
from .alerts import send_alert
from .lake import Lake, align
from .merge import apply_batch

log = logging.getLogger(__name__)


def _quantile(values, q):
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))]


def process(lake: Lake, msgs: list, snapshots_only=False, cfg=None) -> dict:
    t0 = time.monotonic()
    started = datetime.now(UTC)
    rows, bad, beats, snaps = [], [], [], 0
    for m in msgs:
        if m.value() is None:
            continue
        if m.topic().endswith(".heartbeat"):
            beat = events.heartbeat_ts(m.value())
            if beat:
                beats.append((started - beat).total_seconds())
            continue
        try:
            row = events.parse(m.value(), started)
        except events.BadEvent as e:
            bad.append(dict(reason=str(e), payload=m.value().decode(errors="replace")[:20000], topic=m.topic(),
                            partition=m.partition(), offset=m.offset(), lsn=None, consumed_at=started))
            continue
        snaps += row["snap"]
        if snapshots_only and not row["snap"]:
            continue
        rows.append(row)
    if bad:
        lake.quarantine.append(align(bad, lake.quarantine))
    stats = apply_batch(lake, rows)
    lags = stats.pop("lags")
    offsets = [m.offset() for m in msgs]
    rec = dict(
        started_at=started.isoformat(), finished_at=datetime.now(UTC).isoformat(), messages=len(msgs),
        events=stats["events"], kept=stats["kept"], replay_skipped=stats["replay_skipped"],
        same_skipped=stats["same_skipped"], quarantined=len(bad), heartbeats=len(beats), snapshot_events=snaps,
        new_columns=",".join(stats["new_columns"]) or None, first_offset=min(offsets), last_offset=max(offsets),
        min_commit_ts=min((r["commit_ts"] for r in rows), default=None),
        max_commit_ts=max((r["commit_ts"] for r in rows), default=None),
        iceberg_committed_at=stats.get("iceberg_committed_at"),
        lag_p50_s=statistics.median(lags) if lags else None, lag_p95_s=_quantile(lags, 0.95),
        lag_max_s=max(lags) if lags else None, heartbeat_lag_s=max(beats) if beats else None,
        seconds=round(time.monotonic() - t0, 2),
    )
    for k in ("min_commit_ts", "max_commit_ts", "iceberg_committed_at"):
        rec[k] = rec[k].isoformat() if rec[k] else None
    lake.ops.execute(f"INSERT INTO consumer_batches ({', '.join(rec)}) VALUES ({', '.join('?' * len(rec))})",
                     list(rec.values()))
    lake.ops.commit()
    if cfg and len(msgs) >= 100 and len(bad) / len(msgs) > 0.05:
        send_alert(cfg, "hpd cdc quarantine", f"{len(bad)} of {len(msgs)} change events quarantined")
    return rec


def run(cfg, lake: Lake, group: str, from_beginning=False, since: datetime | None = None, snapshots_only=False,
        once=True, idle_seconds=15, max_batch=20000, on_batch=None) -> list:
    consumer = Consumer({
        "bootstrap.servers": cfg.kafka,
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 900000,
        # pick up the change topic soon after Debezium creates it on the first write
        "topic.metadata.refresh.interval.ms": 30000,
    })

    def on_assign(c, parts):
        if from_beginning:
            for p in parts:
                p.offset = OFFSET_BEGINNING
        elif since:
            ms = int(since.timestamp() * 1000)
            parts = c.offsets_for_times([TopicPartition(p.topic, p.partition, ms) for p in parts], timeout=10)
        c.assign(parts)

    # Subscribing before Debezium has created the topic left the consumer idle until its next metadata
    # refresh, so wait for the change topic first.
    for _ in range(60):
        if cfg.topic in consumer.list_topics(timeout=10).topics:
            break
        time.sleep(2)
    consumer.subscribe([cfg.topic, cfg.heartbeat_topic], on_assign=on_assign)
    batches, idle_since = [], time.monotonic()
    try:
        while True:
            msgs = []
            deadline = time.monotonic() + 5
            while len(msgs) < max_batch and time.monotonic() < deadline:
                got = consumer.consume(num_messages=min(5000, max_batch - len(msgs)), timeout=1.0)
                for m in got:
                    if m.error():
                        log.warning("kafka error %s", m.error())
                    else:
                        msgs.append(m)
                if not got and msgs:
                    break
            if not msgs:
                if once and time.monotonic() - idle_since > idle_seconds:
                    break
                continue
            rec = process(lake, msgs, snapshots_only, cfg)
            if rec["events"] or rec["quarantined"]:
                # heartbeats alone don't count as work, or a drain would never go idle
                idle_since = time.monotonic()
            consumer.commit(asynchronous=False)
            batches.append(rec)
            log.info("batch messages=%d kept=%d replay=%d same=%d quarantined=%d %.1fs lag_p50=%s",
                     rec["messages"], rec["kept"], rec["replay_skipped"], rec["same_skipped"], rec["quarantined"],
                     rec["seconds"], rec["lag_p50_s"])
            if on_batch:
                on_batch(rec)
    finally:
        consumer.close()
    return batches
