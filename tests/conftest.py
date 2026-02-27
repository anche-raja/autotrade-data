"""Shared pytest fixtures."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from marketdata.config import PipelineConfig, StreamingConfig

UTC = ZoneInfo("UTC")


@pytest.fixture()
def tmp_data_dir(tmp_path: Path) -> Path:
    """Return a temporary directory for Parquet / DuckDB storage."""
    return tmp_path / "data"


@pytest.fixture()
def sample_1min_df() -> pd.DataFrame:
    """Sorted 1-min bar DataFrame for a typical RTH session (390 bars)."""
    base = dt.datetime(2025, 9, 15, 13, 30, tzinfo=UTC)  # 09:30 ET in UTC (EDT)
    rows = []
    for i in range(390):
        ts = base + dt.timedelta(minutes=i)
        rows.append(
            {
                "ts_utc": ts,
                "open": 450.0 + i * 0.01,
                "high": 450.5 + i * 0.01,
                "low": 449.5 + i * 0.01,
                "close": 450.1 + i * 0.01,
                "volume": 1000.0 + i,
                "wap": 450.05 + i * 0.01,
                "count": 50 + i,
            }
        )
    df = pd.DataFrame(rows)
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    return df


@pytest.fixture()
def sample_5sec_df() -> pd.DataFrame:
    """Sorted 5-sec bar DataFrame for 1 hour (720 bars)."""
    base = dt.datetime(2025, 9, 15, 13, 30, tzinfo=UTC)
    rows = []
    for i in range(720):
        ts = base + dt.timedelta(seconds=i * 5)
        rows.append(
            {
                "ts_utc": ts,
                "open": 450.0,
                "high": 450.5,
                "low": 449.5,
                "close": 450.1,
                "volume": 100.0,
                "wap": 450.05,
                "count": 10,
            }
        )
    df = pd.DataFrame(rows)
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    return df


@pytest.fixture()
def make_pipeline_cfg(tmp_path: Path):
    """Factory fixture: create a PipelineConfig pointing at a temp dir."""

    def _make(**overrides) -> PipelineConfig:
        defaults = {
            "data_dir": str(tmp_path),
            "streaming": StreamingConfig(
                symbols=["SPY"],
                bar_sizes=["5sec", "1min"],
                flush_interval_sec=1,
            ),
        }
        defaults.update(overrides)
        return PipelineConfig(**defaults)

    return _make


@pytest.fixture()
def mock_ibkr_client() -> MagicMock:
    """Create a mock IBKRClient with the methods StreamingCollector needs."""
    client = MagicMock()
    client.subscribe_realtime_bars = MagicMock(return_value=MagicMock(updateEvent=MagicMock()))
    client.cancel_realtime_bars = MagicMock()
    client.register_reconnect_callback = MagicMock()
    client.ib = MagicMock()
    client.ib.disconnectedEvent = MagicMock()
    return client
