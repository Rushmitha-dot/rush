from finder.extract import clean_text


def found(extractor, text):
    _, ms = next(extractor.run([("r1", text)]))
    return {(m.label, m.key) for m in ms}, ms


def test_concern_and_skin_type(v1):
    hits, _ = found(v1, "I have dry skin and this left my face so hydrated.")
    assert ("SKIN_TYPE", "dry") in hits
    assert ("CONCERN", "dryness") in hits


def test_capitalized_verb_form_matches(v1):
    hits, ms = found(v1, "Broke me out terribly. Never again.")
    assert hits == {("CONCERN", "breakouts")}
    assert ms[0].polarity < 0


def test_not_concern_phrases_are_dropped(v1):
    hits, _ = found(v1, "Best dry shampoo, I blow dry after.")
    assert not hits


def test_review_without_terms_skips_spacy(v1):
    assert next(v1.run([("r1", "Nice color, fast shipping")])) == ("r1", [])


def test_v2_negated_problem_is_good_outcome(v1, v2):
    # from the dev set, VADER scores it -0.128
    text = "I have both sensitive and oily skin and this doesn’t irritate my face or increase oil production."
    p1 = [m.polarity for m in found(v1, text)[1] if m.key == "sensitivity"]
    p2 = [m.polarity for m in found(v2, text)[1] if m.key == "sensitivity"]
    assert p1[0] < 0.05
    assert p2[0] >= 0.5


def test_v2_coordinated_types(v1, v2):
    text = "I have both sensitive and oily skin."
    assert ("SKIN_TYPE", "sensitive") not in found(v1, text)[0]
    hits = found(v2, text)[0]
    assert {("SKIN_TYPE", "sensitive"), ("SKIN_TYPE", "oily")} <= hits


def test_v2_ignores_shedding_in_wig_reviews(v1, v2):
    text = "Beautiful wig but horrible shedding."
    assert ("CONCERN", "hair_loss") in found(v1, text)[0]
    assert ("CONCERN", "hair_loss") not in found(v2, text)[0]


def test_v2_split_phrase_and_rash(v2):
    assert ("CONCERN", "breakouts") in found(v2, "It broke my face out horribly.")[0]
    hits = found(v2, "I was breaking out in a rash from other deodorants.")[0]
    assert hits == {("CONCERN", "sensitivity")}


def test_v2_bare_dry_verb_not_a_concern(v1, v2):
    text = "Works best if I put it in wet hair and dry."
    assert ("CONCERN", "dryness") in found(v1, text)[0]
    assert not found(v2, text)[0]


def test_clean_text_joins_title_and_strips_html():
    assert clean_text("Great for dry skin", "Soft<br />and &quot;light&quot;") == 'Great for dry skin. Soft and "light"'
    assert clean_text("Wow!", "ok") == "Wow! ok"
