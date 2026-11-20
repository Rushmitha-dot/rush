"""Retrieval evaluation on eval/questions.jsonl.

A question has one or more expected spans, each a short exact phrase from a section of a
named drug's label. A retrieved chunk covers a span when it is from that section, from a
label of that drug (or of the exact set id, for version change questions), and contains the
phrase. A question is answered at rank r when every span is covered within the top r.

Version change questions also carry the phrase the old version had. Any chunk of that set id
in the top 10 that still contains it is counted as a stale hit, which should never happen.
"""

import json
import re
import sys
from collections import defaultdict

from . import retrieve, store
from .embed import SentenceEmbedder
from .settings import ROOT, load_settings

K = 10


def norm(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


def covers(hit: retrieve.Hit, q: dict, span: dict) -> bool:
    if hit.section_code not in span["sections"]:
        return False
    if q.get("set_id"):
        if hit.set_id != q["set_id"]:
            return False
    elif norm(q["drug"]) not in norm(hit.generic_name):
        return False
    return norm(span["text"]) in norm(hit.heading + " " + hit.body)


def answered_at(hits: list[retrieve.Hit], q: dict) -> int | None:
    need = list(range(len(q["spans"])))
    for rank, hit in enumerate(hits, 1):
        need = [i for i in need if not covers(hit, q, q["spans"][i])]
        if not need:
            return rank
    return None


def stale_hits(hits: list[retrieve.Hit], q: dict) -> int:
    old = q.get("stale_text")
    if not old:
        return 0
    return sum(1 for h in hits if h.set_id == q["set_id"] and norm(old) in norm(h.heading + " " + h.body))


def run(conn, embedder, questions: list[dict]) -> dict:
    per_method = {}
    for method, fn in retrieve.METHODS.items():
        rows = []
        for q in questions:
            hits = fn(conn, embedder, q["question"], K)
            rank = answered_at(hits, q)
            rows.append({"id": q["id"], "type": q["type"], "rank": rank, "stale": stale_hits(hits, q),
                         "top": [f"{h.generic_name} / {h.section_code} / {h.heading[:60]}" for h in hits[:3]]})
        per_method[method] = rows
    return per_method


def summarise(rows: list[dict]) -> dict:
    n = len(rows)
    ranks = [r["rank"] for r in rows]
    return {
        "n": n,
        "hit@1": sum(1 for r in ranks if r == 1) / n,
        "hit@5": sum(1 for r in ranks if r and r <= 5) / n,
        "hit@10": sum(1 for r in ranks if r and r <= 10) / n,
        "mrr@10": sum(1 / r for r in ranks if r) / n,
        "stale_hits": sum(r["stale"] for r in rows),
    }


def report(per_method: dict) -> dict:
    out = {"overall": {}, "by_type": defaultdict(dict)}
    for method, rows in per_method.items():
        out["overall"][method] = summarise(rows)
        types = sorted({r["type"] for r in rows})
        for t in types:
            out["by_type"][t][method] = summarise([r for r in rows if r["type"] == t])
    hybrid = {r["id"]: r for r in per_method["hybrid"]}
    dense = {r["id"]: r for r in per_method["dense"]}
    out["hybrid_worse_than_dense"] = [
        {"id": i, "dense_rank": dense[i]["rank"], "hybrid_rank": hybrid[i]["rank"], "hybrid_top": hybrid[i]["top"]}
        for i in hybrid
        if dense[i]["rank"] and (hybrid[i]["rank"] is None or hybrid[i]["rank"] > dense[i]["rank"])
    ]
    out["hybrid_missed"] = [{"id": i, "type": r["type"], "top": r["top"]} for i, r in hybrid.items() if not r["rank"]]
    out["by_type"] = dict(out["by_type"])
    return out


def main(argv=None) -> int:
    settings = load_settings()
    path = ROOT / "eval" / "questions.jsonl"
    questions = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    conn = store.connect(settings)
    per_method = run(conn, SentenceEmbedder(settings.model), questions)
    result = report(per_method)
    result["per_question"] = per_method
    (ROOT / "eval" / "results.json").write_text(json.dumps(result, indent=1) + "\n")

    print(f"{len(questions)} questions\n")
    print("| | " + " | ".join(retrieve.METHODS) + " |")
    print("|---|" + "---|" * len(retrieve.METHODS))
    for metric in ["hit@1", "hit@5", "mrr@10", "stale_hits"]:
        cells = [f"{result['overall'][m][metric]:.2f}" if metric != "stale_hits"
                 else str(result["overall"][m][metric]) for m in retrieve.METHODS]
        print(f"| {metric} | " + " | ".join(cells) + " |")
    for t, by in result["by_type"].items():
        print(f"\n{t} ({by['hybrid']['n']}): " + ", ".join(
            f"{m} hit@5 {by[m]['hit@5']:.2f} mrr {by[m]['mrr@10']:.2f}" for m in retrieve.METHODS))
    print(f"\nhybrid worse than dense on {len(result['hybrid_worse_than_dense'])}, "
          f"hybrid missed {len(result['hybrid_missed'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
