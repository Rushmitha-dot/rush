"""Apply update files to the index.

One update file is one transaction. Readers see the index before the file or after it,
never half of it. Only chunks whose content hash has no vector yet are embedded.
"""

import json
import time
import zipfile
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import psycopg

from . import checks, dailymed, spl


class Blocked(Exception):
    """The file must not be published. The transaction is rolled back, but the quarantined
    rows are written again afterwards so the reasons are kept."""

    def __init__(self, message: str, stats: "Stats | None" = None):
        super().__init__(message)
        self.stats = stats


@dataclass
class Update:
    name: str
    path: Path
    manifest: dict
    added: list[str]
    deleted: list[str]

    @property
    def md5(self) -> str:
        return self.manifest["source_md5"]

    @property
    def day(self) -> date | None:
        return dailymed.file_date(self.name)

    def xml(self, member: str) -> bytes | None:
        with zipfile.ZipFile(self.path) as z:
            name = member.rsplit(".", 1)[0] + ".xml"
            try:
                return z.read(name)
            except KeyError:
                return None


def open_update(updates_dir: Path, name: str) -> Update:
    """Refuses a file without its manifest or whose bytes don't match it."""
    path = updates_dir / name
    manifest_path = updates_dir / (name + ".manifest.json")
    if not manifest_path.exists():
        raise Blocked(f"{name}: no manifest, the download or slim step did not finish")
    manifest = json.loads(manifest_path.read_text())
    if not path.exists() or dailymed.md5_file(path) != manifest["slim_md5"]:
        raise Blocked(f"{name}: file missing or md5 differs from its manifest")
    with zipfile.ZipFile(path) as z:
        added = [x.strip() for x in z.read("files_added.txt").decode().splitlines() if x.strip()]
        deleted = [x.strip() for x in z.read("files_deleted.txt").decode().splitlines() if x.strip()]
    return Update(name, path, manifest, added, deleted)


def set_id_of(member: str) -> str:
    return dailymed.MEMBER.match(member)[2]


def removals(update: Update) -> list[str]:
    """Set ids whose old file was deleted and that came back under no new file."""
    added = {set_id_of(m) for m in update.added}
    return sorted({set_id_of(m) for m in update.deleted} - added)


@dataclass
class Stats:
    labels_in: int = 0
    labels_applied: int = 0
    labels_skipped: int = 0
    quarantined: int = 0
    tombstoned: int = 0
    deletes_not_indexed: int = 0
    chunks_added: int = 0
    chunks_removed: int = 0
    chunks_kept: int = 0
    embedded: int = 0
    from_cache: int = 0
    reasons: dict = field(default_factory=dict)
    quarantine_rows: list = field(default_factory=list)


@dataclass
class Event:
    kind: str  # add or remove
    set_id: str
    update: str
    member: str | None = None
    xml: bytes | None = None
    # backfill only. The label this removal cancels, when it was added earlier in the range
    prior: bytes | None = None


def events_for(update: Update) -> list[Event]:
    out = [Event("add", set_id_of(m), update.name, m, update.xml(m)) for m in update.added]
    out += [Event("remove", s, update.name) for s in removals(update)]
    return out


def net_events(updates: list[Update], today: date | None = None) -> list[Event]:
    """For a backfill. The last valid event per set id across the range, in file order.

    An add that fails the label checks never becomes the label's last event, the same as
    day by day, where it would be quarantined and the earlier version kept serving. Those
    adds are still returned so they reach the quarantine table.
    """
    today = today or date.today()
    last: dict[str, Event] = {}
    rejected: list[Event] = []
    for update in updates:
        for event in events_for(update):
            current = last.get(event.set_id)
            if event.kind == "add":
                version = _valid_version(event, today)
                if version is None:
                    rejected.append(event)
                    continue
                if current is not None and current.kind == "add" and version < _valid_version(current, today):
                    continue
            if event.kind == "remove" and current is not None:
                event.prior = current.xml if current.kind == "add" else current.prior
            last[event.set_id] = event
    return list(last.values()) + rejected


def _valid_version(event: Event, today: date) -> int | None:
    if event.xml is None:
        return None
    try:
        label = spl.parse(event.xml)
    except spl.SplError:
        return None
    if checks.label_problem(label, event.set_id, today):
        return None
    return label.version


def _quarantine(stats: Stats, event: Event, reason: str, detail: str, version: int | None = None):
    stats.quarantined += 1
    stats.reasons[reason] = stats.reasons.get(reason, 0) + 1
    stats.quarantine_rows.append((event.update, event.member or "", event.set_id, version, reason, detail[:2000]))


def write_quarantine(cur, stats: Stats) -> None:
    cur.executemany(
        "insert into quarantine (update_file, member, set_id, version, reason, detail) values (%s, %s, %s, %s, %s, %s)",
        stats.quarantine_rows,
    )


CACHE_BATCH = 2000


class EmbeddingCache:
    """Vectors already computed, on its own autocommit connection so they outlive a rollback."""

    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    def get(self, model: str, hashes: list[str]) -> dict:
        rows = self.conn.execute("select content_hash, embedding from embedding_cache "
                                 "where model = %s and content_hash = any(%s)", (model, hashes)).fetchall()
        return dict(rows)

    def put(self, model: str, vectors: dict) -> None:
        with self.conn.cursor() as cur:
            cur.executemany("insert into embedding_cache (model, content_hash, embedding) values (%s, %s, %s) "
                            "on conflict do nothing", [(model, h, v) for h, v in vectors.items()])


def apply(conn: psycopg.Connection, embedder, events: list[Event], today: date | None = None,
          cache: EmbeddingCache | None = None) -> Stats:
    """Apply events inside the caller's transaction. Raises Blocked if a publish gate fails."""
    today = today or date.today()
    stats = Stats(labels_in=sum(e.kind == "add" for e in events))
    cur = conn.cursor()
    current = {str(r[0]): (r[1], str(r[2]), r[3]) for r in
               cur.execute("select set_id, version, document_id, status from labels").fetchall()}

    plans: list[tuple[Event, spl.Label]] = []
    for event in events:
        if event.kind != "add":
            continue
        if event.xml is None:
            _quarantine(stats, event, "missing_xml", "listed in files_added but no XML in the zip")
            continue
        try:
            label = spl.parse(event.xml)
        except spl.SplError as e:
            reason = str(e).split(":", 1)[0]
            _quarantine(stats, event, reason, str(e))
            continue
        problem = checks.label_problem(label, event.set_id, today)
        if problem:
            _quarantine(stats, event, problem[0], problem[1], label.version)
            continue
        have = current.get(label.set_id)
        if have and have[2] == "active":
            if label.version < have[0]:
                _quarantine(stats, event, "older_than_indexed",
                            f"v{label.version} arrived after v{have[0]}", label.version)
                continue
            if label.version == have[0] and label.document_id == have[1]:
                stats.labels_skipped += 1
                continue
        plans.append((event, label))

    # embed every new text once, even when several labels in the file share it
    hashes = {c.content_hash: c.embed_text for _, label in plans for c in label.chunks}
    known = set()
    if hashes:
        known = {r[0] for r in cur.execute(
            "select content_hash from chunk_vectors where content_hash = any(%s)", (list(hashes),)).fetchall()}
    missing = [h for h in hashes if h not in known]
    if missing:
        found = cache.get(embedder.model, missing) if cache else {}
        todo = [h for h in missing if h not in found]
        for start in range(0, len(todo), CACHE_BATCH):
            batch = todo[start:start + CACHE_BATCH]
            vectors = dict(zip(batch, embedder.encode([hashes[h] for h in batch]), strict=True))
            if cache:
                cache.put(embedder.model, vectors)
            found.update(vectors)
        cur.executemany(
            "insert into chunk_vectors (content_hash, model, embedding) values (%s, %s, %s) on conflict do nothing",
            [(h, embedder.model, found[h]) for h in missing],
        )
        stats.embedded = len(todo)
        stats.from_cache = len(missing) - len(todo)

    touched_hashes: set[str] = set()
    for event, label in plans:
        old = {r[0] for r in cur.execute("select content_hash from chunks where set_id = %s",
                                         (label.set_id,)).fetchall()}
        new = {c.content_hash for c in label.chunks}
        stats.chunks_kept += len(old & new)
        stats.chunks_added += len(new - old)
        stats.chunks_removed += len(old - new)
        touched_hashes |= old - new
        cur.execute("delete from chunks where set_id = %s", (label.set_id,))
        cur.execute(
            """insert into labels (set_id, version, document_id, effective_date, doc_type, drug_name,
                                   generic_name, labeler, status, update_file, updated_at)
               values (%s, %s, %s, %s, %s, %s, %s, %s, 'active', %s, now())
               on conflict (set_id) do update set version = excluded.version, document_id = excluded.document_id,
                 effective_date = excluded.effective_date, doc_type = excluded.doc_type,
                 drug_name = excluded.drug_name, generic_name = excluded.generic_name, labeler = excluded.labeler,
                 status = 'active', update_file = excluded.update_file, updated_at = now()""",
            (label.set_id, label.version, label.document_id, label.effective_date, label.doc_type,
             label.drug_name, label.generic_name, label.labeler, event.update),
        )
        cur.execute("update label_versions set superseded_at = now() where set_id = %s and superseded_at is null",
                    (label.set_id,))
        cur.execute(
            """insert into label_versions (set_id, version, document_id, effective_date, update_file, n_chunks)
               values (%s, %s, %s, %s, %s, %s)
               on conflict (set_id, version) do update set document_id = excluded.document_id,
                 update_file = excluded.update_file, n_chunks = excluded.n_chunks, superseded_at = null""",
            (label.set_id, label.version, label.document_id, label.effective_date, event.update, len(label.chunks)),
        )
        cur.executemany(
            """insert into chunks (chunk_id, set_id, version, section_code, section_name, heading, ordinal,
                                   effective_date, content_hash, body)
               values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            [(c.chunk_id, label.set_id, label.version, c.section_code, c.section_name, c.heading, c.ordinal,
              label.effective_date, c.content_hash, c.body) for c in label.chunks],
        )
        current[label.set_id] = (label.version, label.document_id, "active")
        stats.labels_applied += 1

    for event in events:
        if event.kind != "remove":
            continue
        have = current.get(event.set_id)
        if (not have or have[2] != "active") and event.prior:
            # added and removed inside one backfill range. Nothing to delete, but record that
            # the label existed and went away, same as the day by day load would have
            label = spl.parse(event.prior)
            cur.execute(
                """insert into labels (set_id, version, document_id, effective_date, doc_type, drug_name,
                                       generic_name, labeler, status, update_file)
                   values (%s, %s, %s, %s, %s, %s, %s, %s, 'removed', %s)
                   on conflict (set_id) do update set status = 'removed', update_file = excluded.update_file""",
                (label.set_id, label.version, label.document_id, label.effective_date, label.doc_type,
                 label.drug_name, label.generic_name, label.labeler, event.update),
            )
            cur.execute(
                """insert into tombstones (set_id, last_version, removed_in_file, chunks_deleted, vectors_deleted)
                   values (%s, %s, %s, 0, 0) on conflict do nothing""",
                (event.set_id, label.version, event.update),
            )
            stats.tombstoned += 1
            continue
        if not have or have[2] != "active":
            stats.deletes_not_indexed += 1
            continue
        old = [r[0] for r in cur.execute("delete from chunks where set_id = %s returning content_hash",
                                         (event.set_id,)).fetchall()]
        touched_hashes |= set(old)
        cur.execute("update labels set status = 'removed', update_file = %s, updated_at = now() where set_id = %s",
                    (event.update, event.set_id))
        cur.execute("update label_versions set superseded_at = now() where set_id = %s and superseded_at is null",
                    (event.set_id,))
        vectors_gone = _collect_garbage(cur, set(old))
        touched_hashes -= set(old)
        cur.execute(
            """insert into tombstones (set_id, last_version, removed_in_file, chunks_deleted, vectors_deleted)
               values (%s, %s, %s, %s, %s) on conflict do nothing""",
            (event.set_id, have[0], event.update, len(old), vectors_gone),
        )
        current[event.set_id] = (have[0], have[1], "removed")
        stats.tombstoned += 1
        stats.chunks_removed += len(old)

    _collect_garbage(cur, touched_hashes)
    write_quarantine(cur, stats)
    gates = checks.publish_gates(cur, stats)
    if gates:
        raise Blocked("; ".join(gates), stats)
    return stats


def _collect_garbage(cur, hashes: set[str]) -> int:
    """Delete vectors no chunk points at any more. Only the hashes this file touched are checked."""
    if not hashes:
        return 0
    cur.execute(
        """delete from chunk_vectors v where v.content_hash = any(%s)
           and not exists (select 1 from chunks c where c.content_hash = v.content_hash)""",
        (list(hashes),),
    )
    return cur.rowcount


def ledger_status(conn, name: str) -> tuple[str, str] | None:
    row = conn.execute("select source_md5, status from load_ledger where update_file = %s and status = 'loaded' "
                       "order by finished_at desc limit 1", (name,)).fetchone()
    return (row[0], row[1]) if row else None


def write_ledger(conn, update_name: str, md5: str, mode: str, status: str, stats: Stats | None,
                 seconds: float, detail: str = "") -> None:
    s = stats or Stats()
    conn.execute(
        """insert into load_ledger (update_file, source_md5, mode, status, file_date, labels_in, labels_applied,
                                    labels_skipped, quarantined, tombstoned, chunks_added, chunks_removed,
                                    chunks_kept, embedded, seconds, detail)
           values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           on conflict (update_file, source_md5) do update set mode = excluded.mode, status = excluded.status,
             labels_in = excluded.labels_in, labels_applied = excluded.labels_applied,
             labels_skipped = excluded.labels_skipped, quarantined = excluded.quarantined,
             tombstoned = excluded.tombstoned, chunks_added = excluded.chunks_added,
             chunks_removed = excluded.chunks_removed, chunks_kept = excluded.chunks_kept,
             embedded = excluded.embedded, seconds = excluded.seconds, detail = excluded.detail,
             finished_at = now()""",
        (update_name, md5, mode, status, dailymed.file_date(update_name), s.labels_in, s.labels_applied,
         s.labels_skipped, s.quarantined, s.tombstoned, s.chunks_added, s.chunks_removed, s.chunks_kept,
         s.embedded, round(seconds, 2), detail),
    )


def load_file(conn, embedder, updates_dir: Path, name: str, force: bool = False,
              today: date | None = None, cache: EmbeddingCache | None = None) -> tuple[str, Stats | None]:
    """Returns ('skipped' | 'loaded' | 'blocked', stats). A file already in the ledger with the
    same checksum is a no-op. Same name with a new checksum means DailyMed republished it."""
    started = time.monotonic()
    try:
        update = open_update(updates_dir, name)
    except Blocked as e:
        conn.rollback()
        write_ledger(conn, name, "unknown", "daily", "blocked", None, time.monotonic() - started, str(e))
        conn.commit()
        raise
    seen = ledger_status(conn, name)
    if seen and not force:
        if seen[0] == update.md5:
            return "skipped", None
        raise Blocked(f"{name} was loaded with md5 {seen[0]} and is now {update.md5}. Rerun with --force to apply it")
    try:
        stats = apply(conn, embedder, events_for(update), today=today, cache=cache)
        write_ledger(conn, name, update.md5, "daily", "loaded", stats, time.monotonic() - started,
                     json.dumps({"reasons": stats.reasons, "deletes_not_indexed": stats.deletes_not_indexed}))
        conn.commit()
        return "loaded", stats
    except Blocked as e:
        conn.rollback()
        if e.stats:
            write_quarantine(conn.cursor(), e.stats)
        write_ledger(conn, name, update.md5, "daily", "blocked", e.stats, time.monotonic() - started, str(e))
        conn.commit()
        raise
    except Exception as e:
        # a bug, not a data problem. Record it so the run history shows the attempt
        conn.rollback()
        write_ledger(conn, name, update.md5, "daily", "failed", None, time.monotonic() - started,
                     f"{type(e).__name__}: {e}"[:2000])
        conn.commit()
        raise


def backfill(conn, embedder, updates_dir: Path, names: list[str], today: date | None = None,
             cache: EmbeddingCache | None = None) -> Stats:
    """Apply a range of files as one batch, net of intermediate versions, in one transaction."""
    started = time.monotonic()
    updates = [open_update(updates_dir, n) for n in sorted(names, key=dailymed.order_key)]
    todo = [u for u in updates if not (s := ledger_status(conn, u.name)) or s[0] != u.md5]
    if not todo:
        return Stats()
    stats = apply(conn, embedder, net_events(todo, today), today=today, cache=cache)
    elapsed = time.monotonic() - started
    detail = json.dumps({"net_of": [u.name for u in todo], "reasons": stats.reasons,
                         "deletes_not_indexed": stats.deletes_not_indexed})
    for u in todo[:-1]:
        write_ledger(conn, u.name, u.md5, "backfill", "loaded", None, 0, f"applied with {todo[-1].name}")
    write_ledger(conn, todo[-1].name, todo[-1].md5, "backfill", "loaded", stats, elapsed, detail)
    conn.commit()
    return stats
