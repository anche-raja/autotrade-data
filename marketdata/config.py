"""Pipeline configuration with Pydantic settings + YAML overlay."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_CONFIG = _PROJECT_ROOT / "configs" / "default.yaml"


class PacingConfig(BaseModel):
    """IBKR pacing / throttle limits."""

    max_requests_per_10min: int = 60
    max_identical_wait_sec: int = 15
    max_same_contract_per_2sec: int = 6
    retry_max_attempts: int = 5
    retry_base_delay_sec: float = 2.0
    retry_max_delay_sec: float = 60.0


class StreamingConfig(BaseModel):
    """Configuration for the real-time streaming service."""

    extended_start: str = "04:00"
    extended_end: str = "20:00"
    flush_interval_sec: int = 60
    symbols: list[str] = Field(default_factory=lambda: ["SPY", "QQQ"])
    bar_sizes: list[str] = Field(default_factory=lambda: ["5sec", "1min"])
    news_symbols: list[str] = Field(
        default_factory=lambda: [
            "SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "GOOGL"
        ]
    )
    news_providers: list[str] = Field(
        default_factory=lambda: ["BZ", "DJ-N", "DJ-RT", "FLY", "BRFG"]
    )
    client_id: int = 20
    reconnect_max_attempts: int = 10
    reconnect_delay_sec: float = 10.0
    reconcile_on_gap: bool = True


class TwitterConfig(BaseModel):
    """Configuration for Twitter/X feed collection.

    Authentication (pick ONE approach):

    **Approach 1 — Browser cookie import (recommended):**
    Log in to x.com in your browser, then from DevTools → Application →
    Cookies → https://x.com, copy ``auth_token`` and ``ct0`` values into
    the config (or env vars ``MD_TWITTER__AUTH_TOKEN``, ``MD_TWITTER__CT0``).
    This bypasses the login flow entirely and avoids Cloudflare blocks.

    **Approach 2 — Username/password login:**
    Set ``username``/``password``/``email``.  May be blocked by Cloudflare.

    On first successful auth the collector persists cookies to
    ``cookies_file`` so subsequent runs skip login.
    """

    accounts: list[str] = Field(
        default_factory=lambda: [
            "elonmusk", "chaaborga", "jimcramer", "CathieDWood",
            "naval", "paulg", "sama",
        ]
    )
    fetch_interval_min: int = 15
    tweets_per_account: int = 20

    # Approach 1: browser cookie import (preferred)
    auth_token: str = ""
    ct0: str = ""

    # Approach 2: login credentials (may hit Cloudflare)
    username: str = ""
    password: str = ""
    email: str = ""

    cookies_file: str = "data/twitter_cookies.json"


class NotificationsConfig(BaseModel):
    """Configuration for notification hooks (future WhatsApp, etc.)."""

    whatsapp_enabled: bool = False
    whatsapp_phone: str = ""


class PipelineConfig(BaseSettings):
    """Top-level configuration for the market-data pipeline.

    Resolution order (highest wins):
      1. Environment variables prefixed ``MD_``
      2. Values in ``configs/default.yaml``
      3. Field defaults below
    """

    model_config = SettingsConfigDict(
        env_prefix="MD_",
        env_nested_delimiter="__",
    )

    ib_host: str = "127.0.0.1"
    ib_port: int = 7497
    ib_client_id: int = 10

    data_dir: str = "data"

    rth_only: bool = True
    session_start: str = "09:30"
    session_end: str = "16:00"
    timezone: str = "America/New_York"

    pacing: PacingConfig = Field(default_factory=PacingConfig)
    streaming: StreamingConfig = Field(default_factory=StreamingConfig)
    twitter: TwitterConfig = Field(default_factory=TwitterConfig)
    notifications: NotificationsConfig = Field(default_factory=NotificationsConfig)

    five_sec_availability_months: int = 6

    # ---- derived helpers ----

    @property
    def data_path(self) -> Path:
        return _PROJECT_ROOT / self.data_dir

    @property
    def parquet_root(self) -> Path:
        return self.data_path / "parquet"

    duckdb_file: str = "metadata.duckdb"

    @property
    def duckdb_path(self) -> Path:
        return self.data_path / self.duckdb_file


def _yaml_values() -> dict[str, Any]:
    """Load default.yaml and return as flat dict."""
    if _DEFAULT_CONFIG.exists():
        with open(_DEFAULT_CONFIG) as fh:
            return yaml.safe_load(fh) or {}
    return {}


def load_config(**overrides: Any) -> PipelineConfig:
    """Build config: YAML defaults -> env vars -> explicit overrides."""
    values: dict[str, Any] = {**_yaml_values(), **overrides}
    return PipelineConfig(**values)
