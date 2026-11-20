"""Data quality rules. Label rules quarantine one label. Publish gates block the whole file."""

from datetime import date, timedelta

from . import spl

# a file where more than this share of labels fail is more likely a broken file or a parser
# regression than a bad day at the FDA, so nothing from it is published
MAX_QUARANTINE_SHARE = 0.10
MIN_QUARANTINED_TO_BLOCK = 5


def label_problem(label: spl.Label, member_set_id: str, today: date) -> tuple[str, str] | None:
    if label.set_id != member_set_id:
        return "set_id_mismatch", f"file name says {member_set_id}, xml says {label.set_id}"
    if label.doc_type != spl.HUMAN_RX:
        return "not_human_rx", f"document type {label.doc_type}"
    if label.effective_date > today + timedelta(days=1):
        return "future_effective_date", str(label.effective_date)
    if label.effective_date < date(1990, 1, 1):
        return "implausible_effective_date", str(label.effective_date)
    if not label.drug_name:
        return "no_product_name", "no manufacturedProduct name"
    if not label.chunks:
        return "no_target_sections", "none of the kept LOINC sections has text"
    return None


def publish_gates(cur, stats) -> list[str]:
    failed = []
    if stats.labels_in and stats.quarantined >= MIN_QUARANTINED_TO_BLOCK \
            and stats.quarantined / stats.labels_in > MAX_QUARANTINE_SHARE:
        failed.append(f"{stats.quarantined} of {stats.labels_in} labels quarantined {stats.reasons}")
    queries = {
        "active labels with no chunks": """select count(*) from labels l where l.status = 'active'
            and not exists (select 1 from chunks c where c.set_id = l.set_id)""",
        "chunks of removed labels": """select count(*) from chunks c join labels l using (set_id)
            where l.status = 'removed'""",
        "chunks not from the label's current version": """select count(*) from chunks c join labels l
            using (set_id) where c.version <> l.version""",
        "vectors from another model": "select count(*) from chunk_vectors where model <> %(model)s",
        "orphan vectors": """select count(*) from chunk_vectors v
            where not exists (select 1 from chunks c where c.content_hash = v.content_hash)""",
    }
    model = cur.execute("select model from chunk_vectors limit 1").fetchone()
    for what, sql in queries.items():
        n = cur.execute(sql, {"model": model[0] if model else ""}).fetchone()[0]
        if n:
            failed.append(f"{n} {what}")
    return failed
