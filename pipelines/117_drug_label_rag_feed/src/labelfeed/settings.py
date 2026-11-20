import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    dsn: str
    schema: str
    data_dir: Path
    model: str

    @property
    def updates_dir(self) -> Path:
        return self.data_dir / "updates"

    @property
    def alerts_log(self) -> Path:
        return self.data_dir / "alerts.log"

    @property
    def lineage_log(self) -> Path:
        return self.data_dir / "lineage" / "events.jsonl"


def load_settings(**overrides) -> Settings:
    values = dict(
        dsn=os.environ.get("LABELFEED_DSN", "postgresql://labelfeed:labelfeed@localhost:55117/labelfeed"),
        schema=os.environ.get("LABELFEED_SCHEMA", "feed"),
        data_dir=Path(os.environ.get("LABELFEED_DATA_DIR", ROOT / "data")),
        model=os.environ.get("LABELFEED_MODEL", "sentence-transformers/all-MiniLM-L6-v2"),
    )
    values.update(overrides)
    return Settings(**values)
