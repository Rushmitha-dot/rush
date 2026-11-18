"""Concern and type vocabulary.

The keys here must match dbt/seeds/concern_taxonomy.csv and type_taxonomy.csv.
A unit test checks that, and dbt tests check every extracted key against the seeds,
so a typo in a new term can't reach the marts.

Terms are lowercase phrases matched on the lowercased tokens. Verb forms are
spelled out ("broke out", "breaking out") because lemma matching needs the
tagger, which made extraction about 4x slower for no gain on these phrases.
"""

CONCERNS = {
    "dryness": {
        "domain": "both",
        "lower": ["dry", "dryness", "drying", "dried out", "dries out", "dry out", "flaky", "flaking", "dehydrated",
                  "parched", "chapped", "cracked", "cracking", "dry patches", "dry patch", "tight feeling",
                  "hydrating", "hydrated", "hydration", "moisturizing", "moisturized", "moisturizes",
                  "moisturise", "moisturising", "moisture"],
    },
    "oiliness": {
        "domain": "both",
        "lower": ["oily", "oiliness", "greasy", "greasiness", "sebum", "oil control", "excess oil", "shiny face"],
    },
    "breakouts": {
        "domain": "skin",
        "lower": ["acne", "breakout", "breakouts", "pimple", "pimples", "zit", "zits", "blemish", "blemishes",
                  "clogged pores", "cystic", "whiteheads", "blackheads", "blackhead", "whitehead",
                  "break out", "breaks out", "broke out", "breaking out", "broken out", "break me out",
                  "breaks me out", "broke me out", "breaking me out"],
    },
    "sensitivity": {
        "domain": "both",
        "lower": ["irritation", "irritated", "irritating", "irritate", "irritates", "redness", "burning", "burned",
                  "burns", "stinging", "stings", "stung", "rash", "rashes", "itchy", "itching", "itchiness",
                  "allergic", "allergic reaction", "inflamed", "inflammation", "hives", "bumps"],
    },
    "dark_spots": {
        "domain": "skin",
        "lower": ["dark spots", "dark spot", "hyperpigmentation", "pigmentation", "discoloration", "age spots",
                  "sun spots", "sunspots", "melasma", "acne scars", "scars", "scarring", "uneven skin tone",
                  "uneven tone", "dark marks", "brown spots"],
    },
    "aging": {
        "domain": "skin",
        "lower": ["wrinkle", "wrinkles", "fine lines", "crow's feet", "crows feet", "sagging", "firmness", "firming",
                  "anti-aging", "anti aging", "antiaging", "aging", "elasticity", "saggy"],
    },
    "frizz": {
        "domain": "hair",
        "lower": ["frizz", "frizzy", "frizziness", "flyaways", "fly-aways", "fly aways", "poofy", "puffy hair"],
    },
    "hair_loss": {
        "domain": "hair",
        "lower": ["hair loss", "losing hair", "losing my hair", "thinning hair", "hair thinning", "shedding",
                  "hair fall", "balding", "bald spots", "bald spot", "regrowth", "hair growth", "grow hair",
                  "grow my hair", "hair is thinning", "falling out"],
    },
    "dandruff": {
        "domain": "hair",
        "lower": ["dandruff", "flaky scalp", "itchy scalp", "scalp itch", "scalp flakes", "seborrheic",
                  "dry scalp", "scalp is dry", "flakes"],
    },
    "damage": {
        "domain": "hair",
        "lower": ["split ends", "breakage", "damaged hair", "damaged", "brittle", "heat damage", "broken hair",
                  "hair breakage", "fried hair", "fried"],
    },
}

SKIN_TYPES = {
    "dry": ["dry skin", "dry skinned", "dry complexion"],
    "oily": ["oily skin", "oily complexion", "oily t-zone", "oily t zone"],
    "combination": ["combination skin", "combo skin", "combination/oily skin", "combination to oily skin",
                    "combination to dry skin"],
    "sensitive": ["sensitive skin", "sensitive face", "skin is sensitive", "skin is very sensitive",
                  "skin is super sensitive", "skin is extremely sensitive"],
    "normal": ["normal skin", "normal to dry skin", "normal to oily skin"],
}

HAIR_TYPES = {
    "straight": ["straight hair"],
    "wavy": ["wavy hair", "my waves"],
    "curly": ["curly hair", "my curls", "curly girl", "curly-haired", "curly haired"],
    "coily": ["coily hair", "kinky hair", "4c hair", "4c", "type 4 hair", "natural hair", "kinky curly"],
    "fine": ["fine hair", "thin hair", "baby fine hair", "fine, thin hair", "fine thin hair", "limp hair"],
    "thick": ["thick hair", "coarse hair", "thick, coarse hair", "thick coarse hair", "dense hair"],
    "color_treated": ["color treated hair", "color-treated hair", "colored hair", "dyed hair", "bleached hair",
                      "color treated", "color-treated", "highlighted hair"],
}

# phrases that look like a concern term but aren't one. Matched first and dropped.
NOT_CONCERNS = ["dry shampoo", "blow dry", "blow dryer", "blow drying", "blow-dry", "blow-dryer", "air dry",
                "air dried", "air drying", "towel dry", "let dry", "let it dry", "dry time", "drying time",
                "dries quickly", "dries fast", "dry brush", "dry brushing", "thinning shears", "thinning scissors"]


def concern_keys() -> list[str]:
    return sorted(CONCERNS)


def type_keys() -> list[tuple[str, str]]:
    return sorted([("skin", k) for k in SKIN_TYPES] + [("hair", k) for k in HAIR_TYPES])


# rules-v2. Added after reading the extractor's mistakes on the 50 dev reviews
# (never the test split). Kept apart from the v1 lists so both versions can be
# rebuilt exactly.
V2_CONCERN_EXTRA = {
    "breakouts": ["broke my face out", "broke my skin out", "breaks my face out", "breaks my skin out",
                  "break my face out", "break my skin out", "breaking my face out", "breaking my skin out",
                  "broke my chin out", "broke my forehead out"],
    "sensitivity": ["breaking out in a rash", "broke out in a rash", "break out in a rash", "breaking out in hives",
                    "broke out in hives", "gentle on skin", "gentle on my skin", "gentle on the skin",
                    "gentle on my face", "gentle on my scalp"],
    "oiliness": ["oil production"],
    "damage": ["damage to my hair", "damage to your hair", "damage to the hair", "hair damage"],
    "dryness": ["dry hair", "dry lips", "dry hands", "dry heels", "dry feet", "dry ends", "dry elbows",
                "very dry", "so dry", "too dry", "super dry", "really dry", "extremely dry", "feels dry",
                "felt dry", "left my skin dry", "skin dry"],
}
# bare "dry" matched verbs ("put it in wet hair and dry"). v2 only counts it with
# an intensifier or a body part next to it, see V2_CONCERN_EXTRA["dryness"].
V2_CONCERN_REMOVED = {"dryness": ["dry"]}

V2_SKIN_TYPES_EXTRA = {
    "dry": ["very dry skin", "extremely dry skin", "super dry skin", "really dry skin"],
    "oily": ["very oily skin", "super oily skin", "extremely oily skin"],
    "sensitive": ["very sensitive skin", "super sensitive skin", "extremely sensitive skin"],
}
V2_HAIR_TYPES_EXTRA = {
    "color_treated": ["color treating my hair", "color treat my hair", "dye my hair", "color my hair",
                      "coloring my hair", "dyeing my hair"],
}

# "sensitive and oily skin", "thin but very curly hair", "thick, long hair".
# Type words may sit up to four filler tokens before the noun.
TYPE_WORDS = {
    "skin": {"dry": "dry", "oily": "oily", "combination": "combination", "combo": "combination",
             "sensitive": "sensitive", "normal": "normal"},
    "hair": {"straight": "straight", "wavy": "wavy", "curly": "curly", "coily": "coily", "kinky": "coily",
             "fine": "fine", "thin": "fine", "thick": "thick", "coarse": "thick"},
}
TYPE_FILLER = {",", "and", "but", "very", "really", "super", "extremely", "long", "short", "medium", "semi",
               "-", "/", "naturally", "slightly", "somewhat", "kind", "of", "&", "dense", "length", "shoulder"}

# wig, bundle and extension reviews talk about the product's hair shedding, not the buyer's
DEFECT_CONTEXT = {"wig", "wigs", "bundle", "bundles", "extension", "extensions", "weave", "lace", "closure",
                  "frontal", "install", "installed", "crochet", "braiding", "generated"}
DEFECT_CONCERNS = {"hair_loss", "damage"}

NEGATIONS = {"no", "not", "n't", "never", "without", "zero", "nor", "didn't", "doesn't", "don't", "wasn't",
             "isn't", "won't", "hasn't", "haven't"}
