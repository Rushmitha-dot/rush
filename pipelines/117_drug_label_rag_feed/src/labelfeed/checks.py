"""Data quality rules. Label rules quarantine one label. Publish gates block the whole file."""

from datetime import date, timedelta

from . import spl

# a file where more than this share of labels fail is more likely a broken file or a parser
# regression than a bad day at the FDA, so nothing from it is published
MAX_QUARANTINE_SHARE = 0.10
MIN_QUARANTINED_TO_BLOCK = 5
# medical gases (oxygen, nitrogen, carbon dioxide) are Rx labels with only a package label
# and nothing to answer from. They were 12% of the bootstrap, so they get their own, looser
# limit. A parser that stopped finding sections would still trip it.
NO_SECTIONS = "no_target_sections"
MAX_NO_SECTIONS_SHARE = 0.30


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
    no_sections = stats.reasons.get(NO_SECTIONS, 0)
    bad = stats.quarantined - no_sections
    if stats.labels_in and bad >= MIN_QUARANTINED_TO_BLOCK and bad / stats.labels_in > MAX_QUARANTINE_SHARE:
        failed.append(f"{bad} of {stats.labels_in} labels quarantined {stats.reasons}")
    if stats.labels_in and no_sections >= MIN_QUARANTINED_TO_BLOCK \
            and no_sections / stats.labels_in > MAX_NO_SECTIONS_SHARE:
        failed.append(f"{no_sections} of {stats.labels_in} labels have none of the kept sections")
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
