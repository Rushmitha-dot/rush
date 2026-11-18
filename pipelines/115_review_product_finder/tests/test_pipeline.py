"""Runner end to end on a few hand-written rows: ledger, quarantine, idempotent
reruns, blocked drops, versioned rescoring and the pointer swap."""
import gzip
import json
from dataclasses import replace

import pytest
from conftest import review, write_drop

from finder import runner, warehouse
from finder.settings import Settings

DAY = "2023-01-02"
TS = 1672660800000  # 2023-01-02 12:00 UTC


@pytest.fixture()
def cfg(tmp_path):
    catalog = tmp_path / "meta.jsonl.gz"
    with gzip.open(catalog, "wt") as f:
        f.write(json.dumps({"parent_asin": "P1", "title": "Hydrating face cream", "store": "A",
                            "main_category": "All Beauty", "average_rating": 4.5, "rating_number": 10,
                            "price": None}) + "\n")
        f.write(json.dumps({"parent_asin": "P2", "title": "Curl shampoo", "store": "B", "main_category": "All Beauty",
                            "average_rating": 4.0, "rating_number": 5, "price": "12.5"}) + "\n")
    base = Settings.from_env()
    c = replace(base, db_path=tmp_path / "w.duckdb", drops=tmp_path / "drops", catalog=catalog,
                serving=tmp_path / "serving", min_reviews=2, alert_webhook=None)
    with warehouse.connect(c.db_path) as con:
        warehouse.init(con)
        warehouse.load_catalog(con, c.catalog)
        con.execute("insert into ops.model_versions (model_version, status) values ('rules-v1', 'active')")
    return c


def rows():
    return [
        review("u1", "P1", TS, "So hydrating, no more dry patches."),
        review("u2", "P1", TS + 1, "Really moisturizing for my dry skin."),
        review("u3", "P1", TS + 2, "Broke me out terribly."),
        review("u3", "P1", TS + 2, "Broke me out terribly."),  # exact repeat, as in the source
        review("u4", "P1", TS + 3, ""),                          # empty text
        review("u5", "P2", TS + 4, "No frizz at all on my curly hair."),
    ] + [review(f"plain{i}", "P2", TS + 10 + i, "Nice, came fast.") for i in range(40)]


def counts(cfg):
    with warehouse.connect(cfg.db_path, read_only=True) as con:
        return con.execute("""select (select count(*) from raw.reviews), (select count(*) from raw.reviews_quarantine),
                                     (select count(*) from nlp.mentions), (select count(*) from nlp.scored_reviews)
                           """).fetchone()


def test_load_quarantines_and_is_idempotent(cfg):
    write_drop(cfg.drops, DAY, rows())
    assert runner.load_day(cfg, DAY, "test", {}) == "ok"  # 2 of 46 quarantined, under the 10 row warn floor
    first = counts(cfg)
    assert first[:2] == (44, 2)
    with warehouse.connect(cfg.db_path, read_only=True) as con:
        reasons = sorted(r[0] for r in con.execute("select reason from raw.reviews_quarantine").fetchall())
    assert reasons == ["duplicate", "empty_text"]

    assert runner.load_day(cfg, DAY, "test", {}) == "skipped"
    assert runner.load_day(cfg, DAY, "test", {}, force=True) == "ok"
    assert counts(cfg) == first


def test_tampered_drop_is_rejected(cfg):
    write_drop(cfg.drops, DAY, rows())
    data = cfg.drops / f"dt={DAY}" / "reviews.jsonl.gz"
    data.write_bytes(data.read_bytes()[:-10])
    assert runner.load_day(cfg, DAY, "test", {}) == "error"
    assert counts(cfg)[0] == 0


def test_misfiled_drop_is_blocked(cfg):
    write_drop(cfg.drops, "2023-01-03", rows())  # timestamps belong to Jan 2
    assert runner.load_day(cfg, "2023-01-03", "test", {}) == "failed_checks"
    assert counts(cfg)[0] == 0


def test_build_rescore_and_swap(cfg):
    write_drop(cfg.drops, DAY, rows())
    assert runner.run_day(cfg, DAY, "test") == "ok"
    meta = json.loads((cfg.serving / "_published.json").read_text())
    assert meta["model_version"] == "rules-v1"

    runner.score_version(cfg, "rules-v2")
    with warehouse.connect(cfg.db_path, read_only=True) as con:
        versions = con.execute("select model_version, count(*) from marts.product_concern_scores "
                               "group by 1 order by 1").fetchall()
        current = con.execute("select distinct model_version from marts.product_scores_current").fetchall()
    assert [v for v, _ in versions] == ["rules-v1", "rules-v2"]
    assert current == [("rules-v1",)]

    assert runner.activate(cfg, "rules-v2")["model_version"] == "rules-v2"
    with warehouse.connect(cfg.db_path, read_only=True) as con:
        current = con.execute("select distinct model_version from marts.product_scores_current").fetchall()
        statuses = dict(con.execute("select model_version, status from ops.model_versions").fetchall())
        # the old version's scores are still there for a rollback
        v1_rows = con.execute("select count(*) from marts.product_concern_scores "
                              "where model_version = 'rules-v1'").fetchone()[0]
    assert current == [("rules-v2",)]
    assert statuses == {"rules-v1": "retired", "rules-v2": "active"}
    assert v1_rows > 0
