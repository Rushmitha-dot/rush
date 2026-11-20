import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    home: Path = field(default_factory=lambda: Path(os.getenv("CLAIMS_HOME", "data")))
    alert_webhook: str = field(default_factory=lambda: os.getenv("ALERT_WEBHOOK", ""))

    @property
    def landing(self) -> Path:
        return self.home / "landing"

    @property
    def lake(self) -> Path:
        return self.home / "lake"

    @property
    def marts(self) -> Path:
        return self.home / "marts"

    @property
    def scratch(self) -> Path:
        return self.home / "scratch"

    @property
    def eventlogs(self) -> Path:
        return self.home / "eventlogs"

    @property
    def ops_db(self) -> Path:
        return self.home / "ops.db"

    def year_dir(self, root: Path, table: str, year: int) -> Path:
        return root / table / f"year={year}"
