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
