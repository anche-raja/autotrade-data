"""Tests for the streaming bar collector (buffer + flush logic)."""

from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from marketdata.config import PipelineConfig, StreamingConfig
from marketdata.streaming.collector import StreamingCollector

UTC = ZoneInfo("UTC")


def _make_cfg(tmp_path: Path) -> PipelineConfig:
    """Create a minimal PipelineConfig pointing at a temp dir."""
    return PipelineConfig(
        data_dir=str(tmp_path),
        streaming=StreamingConfig(
            symbols=["SPY"],
            bar_sizes=["5sec", "1min"],
            flush_interval_sec=1,
        ),
    )


def _mock_client() -> MagicMock:
    """Create a mock IBKRClient with the methods StreamingCollector needs."""
    client = MagicMock()
    client.subscribe_realtime_bars = MagicMock(return_value=MagicMock(updateEvent=MagicMock()))
    client.cancel_realtime_bars = MagicMock()
    client.register_reconnect_callback = MagicMock()
    client.ib = MagicMock()
    client.ib.disconnectedEvent = MagicMock()
    return client


class TestStreamingCollector:
    def test_on_bar_buffers_5sec(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = StreamingCollector(client, cfg)
        collector._running = True

        base = dt.datetime(2025, 6, 15, 14, 0, 0, tzinfo=UTC)

        # Simulate an incoming real-time bar
        mock_rtb = MagicMock()
        mock_rtb.time = base
        mock_rtb.open_ = 500.0
        mock_rtb.high = 501.0
        mock_rtb.low = 499.0
        mock_rtb.close = 500.5
        mock_rtb.volume = 100.0
        mock_rtb.wap = 500.2
        mock_rtb.count = 10

        mock_bars = MagicMock()
        mock_bars.__getitem__ = MagicMock(return_value=mock_rtb)

        collector._on_bar("SPY", mock_bars, True)

        assert len(collector._buffers["SPY"]["5sec"]) == 1
        bar = collector._buffers["SPY"]["5sec"][0]
        assert bar["open"] == 500.0
        assert bar["close"] == 500.5
        assert bar["source"] == "ibkr_stream"

    def test_on_bar_skips_when_not_running(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = StreamingCollector(client, cfg)
        collector._running = False

        mock_bars = MagicMock()
        collector._on_bar("SPY", mock_bars, True)

        assert len(collector._buffers["SPY"]["5sec"]) == 0

    def test_on_bar_skips_when_no_new_bar(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = StreamingCollector(client, cfg)
        collector._running = True

        mock_bars = MagicMock()
        collector._on_bar("SPY", mock_bars, False)

        assert len(collector._buffers["SPY"]["5sec"]) == 0

    def test_on_bar_aggregates_1min(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = StreamingCollector(client, cfg)
        collector._running = True

        base = dt.datetime(2025, 6, 15, 14, 0, 0, tzinfo=UTC)

        # Feed 12 bars for minute :00
        for i in range(12):
            ts = base + dt.timedelta(seconds=i * 5)
            mock_rtb = MagicMock()
            mock_rtb.time = ts
            mock_rtb.open_ = 500.0 + i
            mock_rtb.high = 501.0 + i
            mock_rtb.low = 499.0 + i
            mock_rtb.close = 500.5 + i
            mock_rtb.volume = 100.0
            mock_rtb.wap = 500.0 + i
            mock_rtb.count = 10
            mock_bars = MagicMock()
            mock_bars.__getitem__ = MagicMock(return_value=mock_rtb)
            collector._on_bar("SPY", mock_bars, True)

        # No 1min bar yet (need next minute's bar to trigger)
        assert len(collector._buffers["SPY"]["1min"]) == 0

        # Feed first bar of next minute
        next_ts = base + dt.timedelta(minutes=1)
        mock_rtb = MagicMock()
        mock_rtb.time = next_ts
        mock_rtb.open_ = 520.0
        mock_rtb.high = 521.0
        mock_rtb.low = 519.0
        mock_rtb.close = 520.5
        mock_rtb.volume = 100.0
        mock_rtb.wap = 520.0
        mock_rtb.count = 10
        mock_bars = MagicMock()
        mock_bars.__getitem__ = MagicMock(return_value=mock_rtb)
        collector._on_bar("SPY", mock_bars, True)

        assert len(collector._buffers["SPY"]["1min"]) == 1
        agg = collector._buffers["SPY"]["1min"][0]
        assert agg["open"] == 500.0
        assert agg["volume"] == 1200.0

    @pytest.mark.asyncio
    async def test_flush_all_writes_parquet(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = StreamingCollector(client, cfg)

        base = dt.datetime(2025, 6, 15, 14, 0, 0, tzinfo=UTC)
        bar = {
            "ts_utc": pd.Timestamp(base),
            "open": 500.0,
            "high": 501.0,
            "low": 499.0,
            "close": 500.5,
            "volume": 100.0,
            "wap": 500.2,
            "count": 10,
            "source": "ibkr_stream",
            "quality_flags": "live",
        }
        collector._buffers["SPY"]["5sec"].append(bar)

        await collector._flush_all()

        # Buffer should be cleared
        assert len(collector._buffers["SPY"]["5sec"]) == 0

        # Parquet file should exist
        parquet_dir = tmp_path / "parquet" / "dataset=bars" / "symbol=SPY" / "bar=5sec" / "date=2025-06-15"
        assert parquet_dir.exists()
        files = list(parquet_dir.glob("*.parquet"))
        assert len(files) == 1

        df = pd.read_parquet(files[0])
        assert len(df) == 1
        assert df.iloc[0]["open"] == 500.0

    def test_gap_tracking(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = StreamingCollector(client, cfg)

        assert collector.gaps == []

        # Simulate disconnect
        collector._on_disconnect()
        assert collector._last_disconnect is not None

        # gaps property should still be empty until reconnect
        assert collector.gaps == []

    @pytest.mark.asyncio
    async def test_resubscribe_records_gap(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = StreamingCollector(client, cfg)
        collector._subscriptions["SPY"] = MagicMock()

        # Simulate disconnect
        disconnect_time = dt.datetime(2025, 6, 15, 14, 30, 0, tzinfo=UTC)
        collector._last_disconnect = disconnect_time

        await collector._resubscribe_all()

        assert len(collector.gaps) == 1
        assert collector.gaps[0][0] == disconnect_time
        assert collector._last_disconnect is None
