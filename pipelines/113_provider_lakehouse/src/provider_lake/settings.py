import os
from dataclasses import dataclass, field
from pathlib import Path


def _states() -> tuple[str, ...]:
    return tuple(s.strip().upper() for s in os.getenv("SCOPE_STATES", "TX").split(",") if s.strip())


@dataclass(frozen=True)
class Settings:
    home: Path = field(default_factory=lambda: Path(os.getenv("LAKE_HOME", "data")))
    # empty locally (spark_catalog + Hive metastore), a Unity Catalog name on Databricks
    catalog: str = field(default_factory=lambda: os.getenv("LAKE_CATALOG", ""))
    scope_states: tuple[str, ...] = field(default_factory=_states)
    alert_webhook: str = field(default_factory=lambda: os.getenv("ALERT_WEBHOOK", ""))
    nppes_base_url: str = "https://download.cms.gov/nppes/"

    @property
    def downloads(self) -> Path:
        return self.home / "downloads"

    @property
    def landing(self) -> Path:
        # separate so a rebuild into a scratch lake can replay the same landed files
        return Path(os.getenv("LANDING_DIR", str(self.home / "landing")))

    @property
    def warehouse(self) -> Path:
        return self.home / "lake"

    def table(self, schema: str, name: str) -> str:
        return f"{self.catalog}.{schema}.{name}" if self.catalog else f"{schema}.{name}"

    def schema(self, schema: str) -> str:
        return f"{self.catalog}.{schema}" if self.catalog else schema
