import csv
from pathlib import Path

from finder import taxonomy

SEEDS = Path(__file__).resolve().parents[1] / "dbt/seeds"


def test_concern_keys_match_dbt_seed():
    with open(SEEDS / "concern_taxonomy.csv") as f:
        rows = {r["concern"]: r["domain"] for r in csv.DictReader(f)}
    assert rows == {k: v["domain"] for k, v in taxonomy.CONCERNS.items()}


def test_type_keys_match_dbt_seed():
    with open(SEEDS / "type_taxonomy.csv") as f:
        rows = sorted((r["type_group"], r["type_key"]) for r in csv.DictReader(f))
    assert rows == taxonomy.type_keys()


def test_v2_extras_only_use_known_keys():
    assert set(taxonomy.V2_CONCERN_EXTRA) <= set(taxonomy.CONCERNS)
    assert set(taxonomy.V2_SKIN_TYPES_EXTRA) <= set(taxonomy.SKIN_TYPES)
    assert set(taxonomy.V2_HAIR_TYPES_EXTRA) <= set(taxonomy.HAIR_TYPES)
    for words in taxonomy.TYPE_WORDS.values():
        assert set(words.values()) <= set(taxonomy.SKIN_TYPES) | set(taxonomy.HAIR_TYPES)
