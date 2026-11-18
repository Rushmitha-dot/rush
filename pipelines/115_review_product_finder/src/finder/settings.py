import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    db_path: Path
    drops: Path
    catalog: Path
    serving: Path
    dbt_dir: Path
    alert_webhook: str | None
    min_reviews: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=Path(os.getenv("FINDER_DB", ROOT / "data/warehouse/finder.duckdb")),
            drops=Path(os.getenv("FINDER_DROPS", ROOT / "data/drops")),
            catalog=Path(os.getenv("FINDER_CATALOG", ROOT / "data/source/meta_All_Beauty.jsonl.gz")),
            serving=Path(os.getenv("FINDER_SERVING", ROOT / "data/serving")),
            dbt_dir=ROOT / "dbt",
            alert_webhook=os.getenv("ALERT_WEBHOOK") or None,
            min_reviews=int(os.getenv("FINDER_MIN_REVIEWS", "5")),
        )
