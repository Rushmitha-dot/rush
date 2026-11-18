"""Click through the assistant on a tiny hand-written snapshot."""
import json
from pathlib import Path

import duckdb
from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).resolve().parents[1] / "app/chatbot.py")


def publish_fixture(folder: Path):
    folder.mkdir()
    con = duckdb.connect()
    con.execute(f"""copy (select * from (values
        ('rules-v2', 'hair', 'frizz', 'Frizz', 'curly', 'P1', 'Curl cream', 'BrandA', 4.5, 6, 5, 0, 71.0, 1),
        ('rules-v2', 'hair', 'frizz', 'Frizz', 'any', 'P1', 'Curl cream', 'BrandA', 4.5, 9, 7, 1, 69.0, 1))
        t(model_version, domain, concern, concern_label, type_key, parent_asin, title, store, average_rating,
          n_reviews, positive_reviews, negative_reviews, score, rank_in_concern))
        to '{folder}/scores.parquet' (format parquet)""")
    con.execute(f"""copy (select * from (values
        ('rules-v2', 'P1', 'frizz', 'curly', 'r1', 'No frizz at all on my curls.', 0.6, 5.0, 3, 1))
        t(model_version, parent_asin, concern, type_key, review_id, sentence, polarity, rating, helpful_vote,
          evidence_rank)) to '{folder}/evidence.parquet' (format parquet)""")
    con.execute(f"""copy (select * from (values ('frizz', 'hair', 'Frizz', 7)) t(concern, domain, label, sort_order))
        to '{folder}/concerns.parquet' (format parquet)""")
    con.execute(f"""copy (select * from (values ('hair', 'curly', 'Curly hair', 3)) t(type_group, type_key, label,
        sort_order)) to '{folder}/types.parquet' (format parquet)""")
    (folder / "_published.json").write_text(json.dumps({"model_version": "rules-v2", "through_batch": "2023-09-09",
                                                        "score_rows": 2, "published_at": "t"}))


def click(at, label):
    return next(b for b in at.button if b.label == label).click().run()


def test_concern_type_products(tmp_path, monkeypatch):
    publish_fixture(tmp_path / "serving")
    monkeypatch.setenv("FINDER_SERVING", str(tmp_path / "serving"))
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert [b.label for b in at.button] == ["Skin and body", "Hair and scalp"]
    at = click(at, "Hair and scalp")
    at = click(at, "Frizz")
    at = click(at, "Curly hair")
    assert not at.exception
    text = " ".join(m.value for m in at.markdown)
    assert "Curl cream" in text and "No frizz at all on my curls." in text
    assert any("score 71/100 from 6 reviews (5 positive, 0 negative)" in c.value for c in at.caption)
    assert not at.text_input and not at.chat_input  # no free text anywhere


def test_nothing_published(tmp_path, monkeypatch):
    monkeypatch.setenv("FINDER_SERVING", str(tmp_path / "empty"))
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert "Nothing published yet" in at.error[0].value
