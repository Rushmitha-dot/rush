"""Build, label and score the evaluation set for the extractor.

    python -m finder.label sample --n 150          draw the sample from the drops
    python -m finder.label label                   label it in the terminal, resumable
    python -m finder.label evaluate rules-v1       precision / recall on the test split

The sample is drawn from reviews that mention skin, hair, scalp or face and are
40 to 600 characters long. Those words are deliberately not extractor terms, so
the sample isn't picked by the thing it's grading. Short reviews only, so I
could read every one properly. The first 50 are the dev split (allowed to look
at when changing rules), the other 100 are test and only get scored.

Label format, one JSON line per review:
  {"review_id": ..., "split": "dev"|"test", "concerns": {"dryness": "+"}, "skin_types": ["dry"],
   "hair_types": [], "note": ""}
Outcome per concern is + (helped, or the reviewer is happy about it), - (caused it,
made it worse or didn't help), 0 (mentioned with no outcome, or mixed).
"""
import argparse
import gzip
import hashlib
import json
import random
import re
from collections import Counter
from pathlib import Path

from . import taxonomy
from .extract import CONCERN, HAIR_TYPE, SKIN_TYPE, Extractor, clean_text

EVAL = Path(__file__).resolve().parents[2] / "eval"
NEUTRAL = re.compile(r"\b(skin|hair|scalp|face)\b", re.I)


def review_id(r: dict) -> str:
    # same formula as ingest.STAGE_SQL
    return hashlib.md5(f"{r['user_id']}|{r['asin']}|{r['timestamp']}".encode()).hexdigest()


def sample(drops: Path, n: int, seed: int, dev: int) -> list[dict]:
    pool = []
    for f in sorted(drops.glob("dt=*/reviews.jsonl.gz")):
        with gzip.open(f, "rt") as fh:
            for line in fh:
                r = json.loads(line)
                text = clean_text(r.get("title"), r.get("text"))
                if 40 <= len(text) <= 600 and NEUTRAL.search(text) and (r.get("text") or "").strip():
                    pool.append({"review_id": review_id(r), "parent_asin": r["parent_asin"], "rating": r["rating"],
                                 "text": text})
    rng = random.Random(seed)
    picked = rng.sample(pool, n)
    for i, p in enumerate(picked):
        p["split"] = "dev" if i < dev else "test"
    print(f"pool {len(pool)} reviews, sampled {n} ({dev} dev, {n - dev} test)")
    return picked


def ask(prompt: str, valid: set[str]) -> list[str]:
    while True:
        raw = input(prompt).strip()
        keys = [k.strip() for k in raw.split(",") if k.strip()]
        bad = [k for k in keys if k.split("=")[0] not in valid]
        if not bad:
            return keys
        print(f"  unknown: {bad}")


def label(sample_path: Path, labels_path: Path) -> None:
    done = set()
    if labels_path.exists():
        done = {json.loads(line)["review_id"] for line in labels_path.read_text().splitlines() if line.strip()}
    rows = [json.loads(line) for line in sample_path.read_text().splitlines()]
    print("concerns:", ", ".join(taxonomy.concern_keys()), " (write dryness=+ / =- / =0)")
    for i, r in enumerate(rows):
        if r["review_id"] in done:
            continue
        print(f"\n[{i + 1}/{len(rows)}] {r['split']} rating {r['rating']}\n{r['text']}\n")
        concerns = ask("concerns> ", set(taxonomy.CONCERNS))
        skin = ask("skin types> ", set(taxonomy.SKIN_TYPES))
        hair = ask("hair types> ", set(taxonomy.HAIR_TYPES))
        note = input("note> ").strip()
        out = {"review_id": r["review_id"], "split": r["split"],
               "concerns": {c.split("=")[0]: (c.split("=")[1] if "=" in c else "0") for c in concerns},
               "skin_types": skin, "hair_types": hair, "note": note}
        with labels_path.open("a") as fh:
            fh.write(json.dumps(out) + "\n")


def sign(p: float | None) -> str:
    if p is None:
        return "0"
    return "+" if p >= 0.05 else "-" if p <= -0.05 else "0"


def prf(tp: int, fp: int, fn: int) -> dict:
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "precision": round(p, 3), "recall": round(r, 3),
            "f1": round(2 * p * r / (p + r), 3) if p + r else 0.0}


def evaluate(version: str, sample_path: Path, labels_path: Path, split: str) -> dict:
    texts = {json.loads(line)["review_id"]: json.loads(line)["text"] for line in sample_path.read_text().splitlines()}
    gold = [json.loads(line) for line in labels_path.read_text().splitlines() if line.strip()]
    gold = [g for g in gold if split == "all" or g["split"] == split]
    ex = Extractor(version)
    pred = {rid: ms for rid, ms in ex.run((g["review_id"], texts[g["review_id"]]) for g in gold)}

    counts = {"concern": Counter(), "type": Counter()}
    per_concern: dict[str, Counter] = {k: Counter() for k in taxonomy.CONCERNS}
    polarity = Counter()
    errors = []
    for g in gold:
        ms = pred[g["review_id"]]
        p_con: dict[str, list[float]] = {}
        for m in ms:
            if m.label == CONCERN:
                p_con.setdefault(m.key, []).append(m.polarity)
        p_types = {m.key for m in ms if m.label in (SKIN_TYPE, HAIR_TYPE)}
        g_con = g["concerns"]
        g_types = set(g["skin_types"]) | set(g["hair_types"])

        for k in set(p_con) | set(g_con):
            hit = "tp" if k in p_con and k in g_con else "fp" if k in p_con else "fn"
            counts["concern"][hit] += 1
            per_concern[k][hit] += 1
            if hit != "tp":
                errors.append((g["review_id"], hit, k))
        for k in p_types | g_types:
            hit = "tp" if k in p_types and k in g_types else "fp" if k in p_types else "fn"
            counts["type"][hit] += 1
            if hit != "tp":
                errors.append((g["review_id"], hit, k))
        # direction is only graded where both sides agree the concern is there
        for k in set(p_con) & set(g_con):
            mean = sum(p_con[k]) / len(p_con[k])
            polarity["graded"] += 1
            polarity["agree"] += sign(mean) == g_con[k]
            if g_con[k] in "+-":
                polarity["directional"] += 1
                polarity["directional_agree"] += sign(mean) == g_con[k]

    report = {
        "version": version, "split": split, "reviews": len(gold),
        "reviews_with_concern": sum(1 for g in gold if g["concerns"]),
        "concern": prf(counts["concern"]["tp"], counts["concern"]["fp"], counts["concern"]["fn"]),
        "type": prf(counts["type"]["tp"], counts["type"]["fp"], counts["type"]["fn"]),
        "per_concern": {k: prf(c["tp"], c["fp"], c["fn"]) for k, c in per_concern.items() if sum(c.values())},
        "polarity": {
            "graded": polarity["graded"],
            "sign_agreement": round(polarity["agree"] / polarity["graded"], 3) if polarity["graded"] else None,
            "directional_labels": polarity["directional"],
            "directional_agreement": round(polarity["directional_agree"] / polarity["directional"], 3)
            if polarity["directional"] else None,
        },
        "errors": errors,
    }
    return report


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sample")
    s.add_argument("--drops", type=Path, default=Path("data/drops"))
    s.add_argument("--n", type=int, default=150)
    s.add_argument("--dev", type=int, default=50)
    s.add_argument("--seed", type=int, default=115)
    sub.add_parser("label")
    e = sub.add_parser("evaluate")
    e.add_argument("version")
    e.add_argument("--split", default="test", choices=["dev", "test", "all"])
    e.add_argument("--show-errors", action="store_true")
    args = ap.parse_args()

    sample_path, labels_path = EVAL / "sample.jsonl", EVAL / "labels.jsonl"
    if args.cmd == "sample":
        rows = sample(args.drops, args.n, args.seed, args.dev)
        sample_path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    elif args.cmd == "label":
        label(sample_path, labels_path)
    elif args.cmd == "evaluate":
        rep = evaluate(args.version, sample_path, labels_path, args.split)
        errors = rep.pop("errors")
        print(json.dumps(rep, indent=2))
        if args.show_errors:
            for e in errors:
                print(*e, sep="\t")
        out = EVAL / f"results_{args.version}_{args.split}.json"
        out.write_text(json.dumps(rep, indent=2) + "\n")


if __name__ == "__main__":
    main()
