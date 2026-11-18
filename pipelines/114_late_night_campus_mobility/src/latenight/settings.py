import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]
DBT_DIR = PROJECT_DIR / "dbt"
SEEDS_DIR = DBT_DIR / "seeds"
TZ = "America/New_York"


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    alert_webhook: str | None

    @property
    def bluebikes_dir(self) -> Path:
        return self.data_dir / "bluebikes"

    @property
    def gtfs_dir(self) -> Path:
        return self.data_dir / "gtfs"

    @property
    def warehouse(self) -> Path:
        return self.data_dir / "warehouse" / "latenight.duckdb"

    @property
    def site_dir(self) -> Path:
        return PROJECT_DIR / "site"


def load_settings() -> Settings:
    data_dir = Path(os.getenv("LATENIGHT_DATA_DIR", PROJECT_DIR / "data"))
    if not data_dir.is_absolute():
        data_dir = PROJECT_DIR / data_dir
    return Settings(data_dir=data_dir, alert_webhook=os.getenv("ALERT_WEBHOOK") or None)
