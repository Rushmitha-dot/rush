"""Integration tests against the Postgres container, with a hashing embedder instead of the model."""

import json

import pytest

from labelfeed import load
from labelfeed.embed import HashEmbedder

from .conftest import LISI, METF, ROSU, spl_xml
from .conftest import make_update as _make

pytestmark = pytest.mark.db

D16 = "dm_spl_daily_update_11162026.zip"
D17 = "dm_spl_daily_update_11172026.zip"
D18 = "dm_spl_daily_update_11182026.zip"


class CountingEmbedder(HashEmbedder):
    def __init__(self):
        self.calls = []

    def encode(self, texts):
        self.calls.append(len(texts))
        return super().encode(texts)


def make(settings, name, added, deleted=(), **kw):
    return _make(settings.updates_dir, name, added, deleted, **kw)


def state(conn) -> dict:
    """Everything a reader can see, in a comparable form."""
    return {
        "labels": conn.execute("select set_id::text, version, status from labels order by 1").fetchall(),
        "chunks": conn.execute("select chunk_id, version, content_hash, body from chunks order by 1").fetchall(),
        "vectors": conn.execute("select content_hash, embedding::text from chunk_vectors order by 1").fetchall(),
        "tombstones": conn.execute("select set_id::text, last_version from tombstones order by 1").fetchall(),
    }


def test_first_load_indexes_labels_and_ignores_other_categories(settings, conn):
    make(settings, D16, {ROSU: spl_xml(ROSU, 6), METF: spl_xml(METF, 3, drug="Metformin")})
    outcome, stats = load.load_file(conn, HashEmbedder(), settings.updates_dir, D16)
    assert outcome == "loaded"
    assert (stats.labels_in, stats.labels_applied, stats.quarantined) == (2, 2, 0)
    assert conn.execute("select count(*) from labels where status = 'active'").fetchone()[0] == 2
    n_chunks = conn.execute("select count(*) from chunks").fetchone()[0]
    assert n_chunks == stats.chunks_added == stats.embedded == 6
    ledger = conn.execute("select status, labels_applied from load_ledger where update_file = %s", (D16,)).fetchone()
    assert ledger == ("loaded", 2)


def test_rerunning_the_same_file_changes_nothing(settings, conn):
    make(settings, D16, {ROSU: spl_xml(ROSU, 6), METF: spl_xml(METF, 3, drug="Metformin")})
    load.load_file(conn, HashEmbedder(), settings.updates_dir, D16)
    before = state(conn)
    outcome, stats = load.load_file(conn, HashEmbedder(), settings.updates_dir, D16)
    assert (outcome, stats) == ("skipped", None)
    outcome, stats = load.load_file(conn, HashEmbedder(), settings.updates_dir, D16, force=True)
    assert outcome == "loaded" and stats.labels_skipped == 2 and stats.embedded == 0
    assert state(conn) == before


def test_new_version_reembeds_only_changed_text_and_supersedes_the_old(settings, conn):
    make(settings, D16, {ROSU: spl_xml(ROSU, 6)})
    load.load_file(conn, HashEmbedder(), settings.updates_dir, D16)
    changed = {
        "34067-9": ("1 INDICATIONS AND USAGE", ["{drug} is indicated to lower LDL cholesterol in adults."]),
        "34070-3": ("4 CONTRAINDICATIONS", ["Acute liver failure or decompensated cirrhosis.",
                                            "Hypersensitivity to {drug} or any excipients."]),
        "34084-4": ("6 ADVERSE REACTIONS", ["The most common adverse reactions were headache, nausea, "
                                            "myalgia, abdominal pain and asthenia."]),
    }
    make(settings, D17, {ROSU: spl_xml(ROSU, 7, sections=changed)}, deleted=[ROSU])
    embedder = CountingEmbedder()
    _, stats = load.load_file(conn, embedder, settings.updates_dir, D17)
    assert embedder.calls == [1]
    assert (stats.chunks_kept, stats.chunks_added, stats.chunks_removed) == (2, 1, 1)
    assert conn.execute("select distinct version from chunks").fetchall() == [(7,)]
    bodies = [r[0] for r in conn.execute("select body from chunks where section_code = '34084-4'")]
    assert bodies == ["The most common adverse reactions were headache, nausea, myalgia, abdominal pain and asthenia."]
    versions = conn.execute("select version, superseded_at is null from label_versions order by 1").fetchall()
    assert versions == [(6, False), (7, True)]
    assert conn.execute("select count(*) from chunk_vectors").fetchone()[0] == 3


def test_removed_label_writes_a_tombstone_and_deletes_its_vectors(settings, conn):
    make(settings, D16, {ROSU: spl_xml(ROSU, 6), METF: spl_xml(METF, 3, drug="Metformin")})
    load.load_file(conn, HashEmbedder(), settings.updates_dir, D16)
    make(settings, D17, {}, deleted=[ROSU, LISI])
    _, stats = load.load_file(conn, HashEmbedder(), settings.updates_dir, D17)
    assert stats.tombstoned == 1 and stats.deletes_not_indexed == 1
    assert conn.execute("select count(*) from chunks where set_id = %s", (ROSU,)).fetchone()[0] == 0
    assert conn.execute("select status from labels where set_id = %s", (ROSU,)).fetchone()[0] == "removed"
    tomb = conn.execute("select last_version, chunks_deleted, vectors_deleted from tombstones").fetchall()
    assert tomb == [(6, 3, 3)]
    assert conn.execute("select count(*) from chunk_vectors").fetchone()[0] == 3  # metformin's


def test_shared_text_keeps_its_vector_while_another_label_uses_it(settings, conn):
    same = {"34070-3": ("4 CONTRAINDICATIONS", ["Hypersensitivity to the drug or any excipients."])}
    make(settings, D16, {ROSU: spl_xml(ROSU, 1, drug="Lisinopril", sections=same),
                         LISI: spl_xml(LISI, 1, drug="Lisinopril", sections=same)})
    _, stats = load.load_file(conn, HashEmbedder(), settings.updates_dir, D16)
    assert stats.embedded == 1 and stats.chunks_added == 2
    make(settings, D17, {}, deleted=[ROSU])
    load.load_file(conn, HashEmbedder(), settings.updates_dir, D17)
    assert conn.execute("select count(*) from chunk_vectors").fetchone()[0] == 1
    assert conn.execute("select vectors_deleted from tombstones").fetchone()[0] == 0


def test_bad_labels_are_quarantined_with_a_reason(settings, conn):
    make(settings, D16, {ROSU: spl_xml(ROSU, 6)})
    load.load_file(conn, HashEmbedder(), settings.updates_dir, D16)
    added = {
        ROSU: spl_xml(ROSU, 5),
        METF: spl_xml(METF, 1, doc_type="34390-5"),
        LISI: spl_xml(METF, 1, drug="Lisinopril"),
    }
    for n in range(30):  # enough good labels that the share stays under the block threshold
        sid = f"00000000-0000-4000-8000-{n:012d}"
        added[sid] = spl_xml(sid, 1, drug=f"Drug{n}")
    make(settings, D17, added)
    _, stats = load.load_file(conn, HashEmbedder(), settings.updates_dir, D17)
    assert stats.reasons == {"older_than_indexed": 1, "not_human_rx": 1, "set_id_mismatch": 1}
    rows = conn.execute("select set_id, version, reason from quarantine order by reason").fetchall()
    assert rows == [(METF, 1, "not_human_rx"), (ROSU, 5, "older_than_indexed"), (LISI, 1, "set_id_mismatch")]
    assert conn.execute("select version from labels where set_id = %s", (ROSU,)).fetchone()[0] == 6


def test_a_mostly_broken_file_is_blocked_and_nothing_is_published(settings, conn):
    make(settings, D16, {ROSU: spl_xml(ROSU, 6)})
    load.load_file(conn, HashEmbedder(), settings.updates_dir, D16)
    before = state(conn)
    broken = {f"00000000-0000-4000-8000-{n:012d}": b"<document>cut off" for n in range(6)}
    make(settings, D17, {METF: spl_xml(METF, 1, drug="Metformin"), **broken}, deleted=[ROSU])
    with pytest.raises(load.Blocked, match="6 of 7 labels quarantined"):
        load.load_file(conn, HashEmbedder(), settings.updates_dir, D17)
    assert state(conn) == before
    assert conn.execute("select status from load_ledger where update_file = %s", (D17,)).fetchone()[0] == "blocked"
    assert conn.execute("select count(*) from quarantine where reason = 'xml_parse_error'").fetchone()[0] == 6


def test_a_republished_file_needs_force(settings, conn):
    make(settings, D16, {ROSU: spl_xml(ROSU, 6)})
    load.load_file(conn, HashEmbedder(), settings.updates_dir, D16)
    make(settings, D16, {ROSU: spl_xml(ROSU, 6), METF: spl_xml(METF, 1, drug="Metformin")})
    with pytest.raises(load.Blocked, match="--force"):
        load.load_file(conn, HashEmbedder(), settings.updates_dir, D16)
    outcome, stats = load.load_file(conn, HashEmbedder(), settings.updates_dir, D16, force=True)
    assert outcome == "loaded" and stats.labels_applied == 1


def test_file_that_does_not_match_its_manifest_is_blocked(settings, conn):
    manifest = make(settings, D16, {ROSU: spl_xml(ROSU, 6)})
    m = json.loads(manifest.read_text())
    m["slim_md5"] = "0" * 32
    manifest.write_text(json.dumps(m))
    with pytest.raises(load.Blocked, match="md5 differs"):
        load.load_file(conn, HashEmbedder(), settings.updates_dir, D16)
    assert conn.execute("select count(*) from labels").fetchone()[0] == 0


def _three_days(settings):
    make(settings, D16, {ROSU: spl_xml(ROSU, 6), METF: spl_xml(METF, 3, drug="Metformin")})
    make(settings, D17, {ROSU: spl_xml(ROSU, 7, sections={"34067-9": ("1 INDICATIONS", ["New wording."])}),
                         LISI: spl_xml(LISI, 1, drug="Lisinopril")}, deleted=[ROSU])
    make(settings, D18, {ROSU: spl_xml(ROSU, 8)}, deleted=[ROSU, METF])
    return [D16, D17, D18]


def test_backfill_of_the_range_matches_loading_day_by_day(settings, conn, tmp_path):
    from labelfeed import store
    from labelfeed.settings import load_settings

    names = _three_days(settings)
    for name in names:
        load.load_file(conn, HashEmbedder(), settings.updates_dir, name)
    daily = state(conn)

    other = load_settings(schema=settings.schema + "_bf", data_dir=settings.data_dir)
    store.drop_schema(other)
    try:
        c2 = store.connect(other)
        store.init_schema(c2)
        embedder = CountingEmbedder()
        stats = load.backfill(c2, embedder, settings.updates_dir, names)
        assert state(c2) == daily
        # v6 and v7 of rosuvastatin were never embedded, only the final v8
        assert stats.labels_applied == 2 and stats.tombstoned == 1
        assert load.backfill(c2, embedder, settings.updates_dir, names).labels_in == 0
        c2.close()
    finally:
        store.drop_schema(other)
    assert daily["tombstones"] == [(METF, 3)]


def test_embedding_work_survives_a_blocked_load(settings, conn):
    from labelfeed import store

    cache = load.EmbeddingCache(store.connect(settings, autocommit=True))
    broken = {f"00000000-0000-4000-8000-{n:012d}": b"<document>cut off" for n in range(6)}
    make(settings, D16, {ROSU: spl_xml(ROSU, 6), **broken})
    with pytest.raises(load.Blocked):
        load.load_file(conn, HashEmbedder(), settings.updates_dir, D16, cache=cache)
    assert conn.execute("select count(*) from chunk_vectors").fetchone()[0] == 0
    make(settings, D16, {ROSU: spl_xml(ROSU, 6)})
    embedder = CountingEmbedder()
    _, stats = load.load_file(conn, embedder, settings.updates_dir, D16, cache=cache)
    assert embedder.calls == [] and (stats.embedded, stats.from_cache) == (0, 3)


def test_labels_with_no_clinical_sections_do_not_block_a_normal_file(settings, conn):
    gas = {"51945-4": ("PACKAGE LABEL", ["Oxygen USP. Rx only."])}
    added = {f"00000000-0000-4000-8000-{n:012d}": spl_xml(f"00000000-0000-4000-8000-{n:012d}", 1, drug="Oxygen",
                                                           sections=gas) for n in range(6)}
    for n in range(20):
        sid = f"00000000-0000-4000-9000-{n:012d}"
        added[sid] = spl_xml(sid, 1, drug=f"Drug{n}")
    make(settings, D16, added)
    outcome, stats = load.load_file(conn, HashEmbedder(), settings.updates_dir, D16)
    assert outcome == "loaded" and stats.reasons == {"no_target_sections": 6} and stats.labels_applied == 20
