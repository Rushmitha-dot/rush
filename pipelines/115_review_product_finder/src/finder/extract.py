"""Concern and skin/hair type extraction with spaCy rules, plus VADER per mention.

spaCy here is the tokenizer, a rule-based sentencizer and an entity ruler. No
statistical model is loaded, which keeps memory small and makes every hit
explainable (the pattern that fired is the reason).

A model version pins the vocabulary and the rules together. Changing either one
means a new version string, which the pipeline backfills next to the old one
instead of overwriting it.

rules-v1  taxonomy lists, VADER compound of the sentence holding the mention
rules-v2  v1 plus the dev-set fixes in taxonomy.V2_*, coordinated types
          ("sensitive and oily skin"), negated problems scored as good outcomes
          ("no breakouts"), and hair loss / damage dropped in wig and bundle reviews
"""
import html
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

import spacy
from spacy.language import Language
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from . import taxonomy

CONCERN, SKIN_TYPE, HAIR_TYPE, IGNORE = "CONCERN", "SKIN_TYPE", "HAIR_TYPE", "IGNORE"
SNIPPET_CHARS = 280


@dataclass(frozen=True)
class ModelSpec:
    version: str
    v2_vocab: bool = False
    coordinated_types: bool = False
    negation_flip: bool = False
    defect_context: bool = False


VERSIONS = {
    "rules-v1": ModelSpec("rules-v1"),
    "rules-v2": ModelSpec("rules-v2", v2_vocab=True, coordinated_types=True, negation_flip=True,
                          defect_context=True),
}


@dataclass
class Mention:
    review_id: str
    mention_no: int
    label: str
    key: str
    matched: str
    sentence: str
    polarity: float | None


def clean_text(title: str | None, text: str | None) -> str:
    body = re.sub(r"<br\s*/?>", " ", text or "", flags=re.I)
    t = (title or "").strip()
    # titles like "Five Stars" add nothing, but "Great for dry skin" often carries the type
    joined = f"{t}. {body}" if t and not t.endswith((".", "!", "?")) else f"{t} {body}"
    return html.unescape(joined).strip()


def vocabulary(spec: ModelSpec) -> tuple[dict, dict, dict]:
    concerns = {k: list(v["lower"]) for k, v in taxonomy.CONCERNS.items()}
    skin = {k: list(v) for k, v in taxonomy.SKIN_TYPES.items()}
    hair = {k: list(v) for k, v in taxonomy.HAIR_TYPES.items()}
    if spec.v2_vocab:
        for k, removed in taxonomy.V2_CONCERN_REMOVED.items():
            concerns[k] = [p for p in concerns[k] if p not in removed]
        for target, extra in ((concerns, taxonomy.V2_CONCERN_EXTRA), (skin, taxonomy.V2_SKIN_TYPES_EXTRA),
                              (hair, taxonomy.V2_HAIR_TYPES_EXTRA)):
            for k, phrases in extra.items():
                target[k] = target[k] + phrases
    return concerns, skin, hair


def _token_pattern(nlp: Language, phrase: str) -> list[dict]:
    return [{"LOWER": t.text.lower()} for t in nlp.make_doc(phrase)]


def build_patterns(nlp: Language, spec: ModelSpec) -> list[dict]:
    concerns, skin, hair = vocabulary(spec)
    pats = []
    for label, group in ((CONCERN, concerns), (SKIN_TYPE, skin), (HAIR_TYPE, hair)):
        for key, phrases in group.items():
            pats += [{"label": label, "id": key, "pattern": _token_pattern(nlp, p)} for p in phrases]
    pats += [{"label": IGNORE, "id": "ignore", "pattern": _token_pattern(nlp, p)} for p in taxonomy.NOT_CONCERNS]
    return pats


def prefilter_regex(spec: ModelSpec) -> re.Pattern:
    """Cheap first pass so spaCy only sees reviews that could contain a hit.
    Most reviews in this category ("works great", "nice color") never do."""
    concerns, skin, hair = vocabulary(spec)
    words = set()
    for group in (concerns, skin, hair):
        for phrases in group.values():
            for p in phrases:
                words.update(p.replace(",", " ").split())
    if spec.coordinated_types:
        for w in taxonomy.TYPE_WORDS.values():
            words.update(w)
    words -= {"me", "my", "out", "is", "hair", "skin", "the", "very", "super", "to", "type", "on", "in", "a",
              "your", "of", "really", "so", "too", "extremely", "and"}
    alts = sorted((re.escape(w) for w in words if w), key=len, reverse=True)
    return re.compile(r"\b(" + "|".join(alts) + r")\b", re.I)


def load_nlp(spec: ModelSpec) -> Language:
    # blank English tokenizer + rule-based sentences. en_core_web_sm with the tagger
    # was ~4x slower on the same 20k reviews and the patterns only need LOWER.
    nlp = spacy.blank("en")
    nlp.add_pipe("sentencizer")
    ruler = nlp.add_pipe("entity_ruler", config={"overwrite_ents": True})
    ruler.add_patterns(build_patterns(nlp, spec))
    return nlp


def coordinated_type_spans(doc) -> list[tuple[int, str, str]]:
    """(token index, label, key) for type words that reach their noun across filler,
    like "sensitive and oily skin" or "thick, long hair"."""
    out = []
    for i, tok in enumerate(doc):
        word = tok.lower_
        for noun, words in taxonomy.TYPE_WORDS.items():
            if word not in words:
                continue
            j, steps = i + 1, 0
            while j < len(doc) and steps < 4 and (doc[j].lower_ in taxonomy.TYPE_FILLER or doc[j].lower_ in words):
                j, steps = j + 1, steps + 1
            if j < len(doc) and doc[j].lower_ == noun and j > i + 1:
                out.append((i, SKIN_TYPE if noun == "skin" else HAIR_TYPE, words[word]))
    return out


def negated(ent, window: int = 3) -> bool:
    doc = ent.doc
    start = max(ent.sent.start, ent.start - window)
    # reviews mix ' and ’, and spaCy splits "doesn’t" into "does" + "n’t"
    return any(t.lower_.replace("’", "'") in taxonomy.NEGATIONS for t in doc[start:ent.start])


class Extractor:
    def __init__(self, version: str):
        if version not in VERSIONS:
            raise ValueError(f"unknown model version {version}, known: {sorted(VERSIONS)}")
        self.spec = VERSIONS[version]
        self.version = version
        self.nlp = load_nlp(self.spec)
        self.analyzer = SentimentIntensityAnalyzer()
        self.prefilter = prefilter_regex(self.spec)

    def run(self, rows: Iterable[tuple[str, str]], batch_size: int = 256) -> Iterator[tuple[str, list[Mention]]]:
        """rows are (review_id, cleaned text). Yields every review_id, with an empty
        list when nothing matched, so the caller can record it as scored."""
        pending = []
        for review_id, text in rows:
            if text and self.prefilter.search(text):
                pending.append((review_id, text))
            else:
                yield review_id, []
            if len(pending) >= batch_size:
                yield from self._parse(pending, batch_size)
                pending = []
        if pending:
            yield from self._parse(pending, batch_size)

    def _parse(self, pending, batch_size):
        docs = self.nlp.pipe((t for _, t in pending), batch_size=batch_size)
        for (review_id, _), doc in zip(pending, docs, strict=True):
            yield review_id, self.mentions(review_id, doc)

    def polarity(self, sentence: str, ent) -> float:
        compound = self.analyzer.polarity_scores(sentence)["compound"]
        if self.spec.negation_flip and negated(ent):
            # "no breakouts", "doesn't irritate": the problem didn't happen, which is
            # a good outcome even when VADER reads the sentence as neutral or negative
            return max(abs(compound), 0.5)
        return compound

    def mentions(self, review_id: str, doc) -> list[Mention]:
        ents = [e for e in doc.ents if e.label_ != IGNORE]
        if self.spec.defect_context:
            words = {t.lower_ for t in doc}
            if words & taxonomy.DEFECT_CONTEXT:
                ents = [e for e in ents if not (e.label_ == CONCERN and e.ent_id_ in taxonomy.DEFECT_CONCERNS)]
        out = []
        for ent in ents:
            sent = ent.sent.text.strip()
            pol = self.polarity(sent, ent) if ent.label_ == CONCERN else None
            out.append(Mention(review_id, len(out), ent.label_, ent.ent_id_, ent.text, sent[:SNIPPET_CHARS], pol))
        if self.spec.coordinated_types:
            have = {(m.label, m.key) for m in out}
            for i, label, key in coordinated_type_spans(doc):
                if (label, key) not in have:
                    have.add((label, key))
                    tok = doc[i]
                    out.append(Mention(review_id, len(out), label, key, tok.text,
                                       tok.sent.text.strip()[:SNIPPET_CHARS], None))
        return out


