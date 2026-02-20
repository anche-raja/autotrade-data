"""Tests for the streaming news collector (filter, dedup, flush)."""

from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq
import pytest

from marketdata.config import PipelineConfig, StreamingConfig
from marketdata.streaming.news_collector import NewsCollector

UTC = ZoneInfo("UTC")


def _make_cfg(tmp_path: Path) -> PipelineConfig:
    """Create a minimal PipelineConfig pointing at a temp dir."""
    return PipelineConfig(
        data_dir=str(tmp_path),
        streaming=StreamingConfig(
            news_symbols=["SPY", "AAPL"],
            news_providers=["BZ"],
            flush_interval_sec=1,
        ),
    )


def _mock_client() -> MagicMock:
    client = MagicMock()
    client.subscribe_news = MagicMock(return_value=[MagicMock()])
    client.cancel_news = MagicMock()
    client.register_reconnect_callback = MagicMock()
    client.ib = MagicMock()
    client.ib.tickNewsEvent = MagicMock()
    client.ib.reqNewsArticleAsync = AsyncMock(return_value=None)
    client.ib.RequestTimeout = 45
    return client


class TestNewsCollector:
    def test_dedup_by_article_id(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = NewsCollector(client, cfg)
        collector._running = True

        news1 = MagicMock()
        news1.articleId = "ART001"
        news1.providerCode = "BZ"
        news1.headline = "SPY rallies"
        news1.time = dt.datetime(2025, 6, 15, 14, 0, 0, tzinfo=UTC)
        news1.extraData = "SPY"

        # First call should add to seen_ids
        collector._on_news(news1)
        assert "ART001" in collector._seen_ids

        # Second call with same ID should be skipped
        old_len = len(collector._seen_ids)
        collector._on_news(news1)
        assert len(collector._seen_ids) == old_len

    def test_skip_empty_article_id(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = NewsCollector(client, cfg)
        collector._running = True

        news = MagicMock()
        news.articleId = ""
        collector._on_news(news)
        assert len(collector._seen_ids) == 0

    def test_skip_when_not_running(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = NewsCollector(client, cfg)
        collector._running = False

        news = MagicMock()
        news.articleId = "ART001"
        collector._on_news(news)
        assert len(collector._seen_ids) == 0

    @pytest.mark.asyncio
    async def test_process_article_filters_by_symbol(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = NewsCollector(client, cfg)
        collector._running = True

        # Article for a symbol NOT in our list
        news = MagicMock()
        news.articleId = "ART002"
        news.providerCode = "BZ"
        news.headline = "TSLA drops"
        news.time = dt.datetime(2025, 6, 15, 14, 0, 0, tzinfo=UTC)
        news.extraData = "TSLA"

        # Patch _fetch_body to avoid real API calls
        with patch.object(collector, "_fetch_body", new_callable=AsyncMock, return_value="body"):
            await collector._process_article(news)

        # TSLA is not in configured symbols, should be filtered
        assert len(collector._buffer) == 0

    @pytest.mark.asyncio
    async def test_process_article_adds_matching_symbol(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = NewsCollector(client, cfg)
        collector._running = True

        news = MagicMock()
        news.articleId = "ART003"
        news.providerCode = "BZ"
        news.headline = "SPY hits new high"
        news.time = dt.datetime(2025, 6, 15, 14, 0, 0, tzinfo=UTC)
        news.extraData = "SPY"

        with patch.object(collector, "_fetch_body", new_callable=AsyncMock, return_value="Article body"):
            await collector._process_article(news)

        assert len(collector._buffer) == 1
        assert collector._buffer[0]["article_id"] == "ART003"
        assert collector._buffer[0]["tickers"] == "spy"
        assert collector._buffer[0]["source"] == "ibkr_stream"

    @pytest.mark.asyncio
    async def test_flush_writes_parquet(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = NewsCollector(client, cfg)

        collector._buffer.append({
            "article_id": "ART010",
            "published_utc": pd.Timestamp("2025-06-15 14:00:00", tz="UTC"),
            "provider": "BZ",
            "headline": "Test headline",
            "body": "Test body",
            "sentiment_score": 0.5,
            "confidence": 0.8,
            "language": "en",
            "tickers": "spy",
            "source": "ibkr_stream",
        })

        await collector._flush_all()

        # Buffer should be cleared
        assert len(collector._buffer) == 0

        # Parquet should exist
        parquet_path = tmp_path / "parquet" / "dataset=news" / "symbol=spy" / "date=2025-06-15" / "part.parquet"
        assert parquet_path.exists()

        df = pd.read_parquet(str(parquet_path))
        assert len(df) == 1
        assert df.iloc[0]["article_id"] == "ART010"

    @pytest.mark.asyncio
    async def test_flush_merges_with_existing(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = NewsCollector(client, cfg)

        # First flush
        collector._buffer.append({
            "article_id": "ART020",
            "published_utc": pd.Timestamp("2025-06-15 14:00:00", tz="UTC"),
            "provider": "BZ",
            "headline": "First",
            "body": "First body",
            "sentiment_score": None,
            "confidence": None,
            "language": "en",
            "tickers": "spy",
            "source": "ibkr_stream",
        })
        await collector._flush_all()

        # Second flush with new + duplicate articles
        collector._buffer.append({
            "article_id": "ART020",  # duplicate
            "published_utc": pd.Timestamp("2025-06-15 14:00:00", tz="UTC"),
            "provider": "BZ",
            "headline": "First updated",
            "body": "Updated body",
            "sentiment_score": None,
            "confidence": None,
            "language": "en",
            "tickers": "spy",
            "source": "ibkr_stream",
        })
        collector._buffer.append({
            "article_id": "ART021",  # new
            "published_utc": pd.Timestamp("2025-06-15 14:05:00", tz="UTC"),
            "provider": "BZ",
            "headline": "Second",
            "body": "Second body",
            "sentiment_score": None,
            "confidence": None,
            "language": "en",
            "tickers": "spy",
            "source": "ibkr_stream",
        })
        await collector._flush_all()

        parquet_path = tmp_path / "parquet" / "dataset=news" / "symbol=spy" / "date=2025-06-15" / "part.parquet"
        df = pd.read_parquet(str(parquet_path))
        # Should have 2 articles (ART020 deduped, ART021 new)
        assert len(df) == 2
        assert set(df["article_id"]) == {"ART020", "ART021"}

    @pytest.mark.asyncio
    async def test_flush_empty_buffer_noop(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        client = _mock_client()
        collector = NewsCollector(client, cfg)

        # Flushing empty buffer should not create any files
        await collector._flush_all()

        parquet_dir = tmp_path / "parquet"
        assert not parquet_dir.exists()
