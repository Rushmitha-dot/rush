import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name, default):
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    pg_dsn: str = field(default_factory=lambda: _env("HPD_PG_DSN", "postgresql://hpd:hpd@localhost:55116/hpd"))
    kafka: str = field(default_factory=lambda: _env("HPD_KAFKA", "localhost:19116"))
    connect_url: str = field(default_factory=lambda: _env("HPD_CONNECT_URL", "http://localhost:18116"))
    data_dir: Path = field(default_factory=lambda: Path(_env("HPD_DATA_DIR", "data")).resolve())
    lake: str = field(default_factory=lambda: _env("HPD_LAKE", "live"))
    soda_url: str = "https://data.cityofnewyork.us/resource/wvxf-dwi5.json"
    boro: str = field(default_factory=lambda: _env("HPD_BORO", "BRONX"))
    window_start: str = field(default_factory=lambda: _env("HPD_WINDOW_START", "2026-04-01"))
    page_size: int = field(default_factory=lambda: int(_env("HPD_PAGE_SIZE", "5000")))
    page_sleep: float = field(default_factory=lambda: float(_env("HPD_PAGE_SLEEP", "1.0")))
    overlap_minutes: int = field(default_factory=lambda: int(_env("HPD_OVERLAP_MINUTES", "60")))
    extra_columns: tuple = field(
        default_factory=lambda: tuple(c for c in _env("HPD_EXTRA_COLUMNS", "").split(",") if c)
    )
    sla_minutes: int = field(default_factory=lambda: int(_env("HPD_SLA_MINUTES", "15")))
    alert_webhook: str = field(default_factory=lambda: _env("ALERT_WEBHOOK", ""))
    topic: str = "hpd.mirror.violations"
    heartbeat_topic: str = "hpd.mirror.heartbeat"

    @property
    def lake_dir(self) -> Path:
        return self.data_dir / self.lake


def load_settings(**overrides) -> Settings:
    return Settings(**overrides)
