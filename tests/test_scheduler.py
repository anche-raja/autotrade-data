"""Tests for the streaming session scheduler."""

from __future__ import annotations

import datetime as dt
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from marketdata.config import PipelineConfig, StreamingConfig
from marketdata.streaming.scheduler import SessionScheduler

UTC = ZoneInfo("UTC")
ET = ZoneInfo("America/New_York")


def _make_cfg(**overrides) -> PipelineConfig:
    return PipelineConfig(
        streaming=StreamingConfig(**overrides) if overrides else StreamingConfig(),
    )


class TestSessionScheduler:
    def test_session_boundaries(self):
        cfg = _make_cfg(extended_start="04:00", extended_end="20:00")
        sched = SessionScheduler(cfg)

        # A known trading day
        day = dt.date(2025, 6, 16)  # Monday
        start, end = sched.session_boundaries(day)

        # 4:00 AM ET = 08:00 UTC (EDT, -4h)
        assert start.tzinfo is not None
        expected_start = dt.datetime(2025, 6, 16, 4, 0, tzinfo=ET).astimezone(UTC)
        expected_end = dt.datetime(2025, 6, 16, 20, 0, tzinfo=ET).astimezone(UTC)
        assert start == expected_start
        assert end == expected_end

    def test_is_streaming_active_during_hours(self):
        cfg = _make_cfg(extended_start="04:00", extended_end="20:00")
        sched = SessionScheduler(cfg)

        # 10:00 AM ET on a Monday (definitely a trading day)
        now = dt.datetime(2025, 6, 16, 10, 0, tzinfo=ET).astimezone(UTC)

        with patch(
            "marketdata.streaming.scheduler.get_trading_days",
            return_value=[dt.date(2025, 6, 16)],
        ):
            assert sched.is_streaming_active(now) is True

    def test_is_streaming_inactive_outside_hours(self):
        cfg = _make_cfg(extended_start="04:00", extended_end="20:00")
        sched = SessionScheduler(cfg)

        # 9:00 PM ET = outside extended hours
        now = dt.datetime(2025, 6, 16, 21, 0, tzinfo=ET).astimezone(UTC)

        with patch(
            "marketdata.streaming.scheduler.get_trading_days",
            return_value=[dt.date(2025, 6, 16)],
        ):
            assert sched.is_streaming_active(now) is False

    def test_is_streaming_inactive_on_weekend(self):
        cfg = _make_cfg()
        sched = SessionScheduler(cfg)

        # Saturday at 10 AM ET
        now = dt.datetime(2025, 6, 14, 10, 0, tzinfo=ET).astimezone(UTC)

        with patch(
            "marketdata.streaming.scheduler.get_trading_days",
            return_value=[],  # No trading days on weekend
        ):
            assert sched.is_streaming_active(now) is False

    def test_seconds_until_next_session_same_day(self):
        cfg = _make_cfg(extended_start="04:00", extended_end="20:00")
        sched = SessionScheduler(cfg)

        # 3:00 AM ET — 1 hour before extended hours start
        now = dt.datetime(2025, 6, 16, 3, 0, tzinfo=ET).astimezone(UTC)

        with patch(
            "marketdata.streaming.scheduler.get_trading_days",
            return_value=[dt.date(2025, 6, 16)],
        ):
            secs = sched.seconds_until_next_session(now)
            assert 3500 < secs < 3700  # roughly 1 hour = 3600s

    def test_seconds_until_next_session_next_day(self):
        cfg = _make_cfg(extended_start="04:00", extended_end="20:00")
        sched = SessionScheduler(cfg)

        # 9 PM ET Monday — after hours, next session is Tuesday 4 AM
        now = dt.datetime(2025, 6, 16, 21, 0, tzinfo=ET).astimezone(UTC)

        def mock_trading_days(start, end):
            # Monday (today) returns empty since we're past end
            # Tuesday returns a trading day
            if start == dt.date(2025, 6, 16):
                return []  # Already used today's session
            if start == dt.date(2025, 6, 17):
                return [dt.date(2025, 6, 17)]
            return []

        with patch(
            "marketdata.streaming.scheduler.get_trading_days",
            side_effect=mock_trading_days,
        ):
            secs = sched.seconds_until_next_session(now)
            # Should be approximately 7 hours (9 PM to 4 AM)
            assert 24000 < secs < 26000

    def test_seconds_until_next_session_fallback(self):
        cfg = _make_cfg()
        sched = SessionScheduler(cfg)

        now = dt.datetime(2025, 6, 16, 21, 0, tzinfo=ET).astimezone(UTC)

        with patch(
            "marketdata.streaming.scheduler.get_trading_days",
            return_value=[],  # No trading days found
        ):
            secs = sched.seconds_until_next_session(now)
            assert secs == 300.0  # fallback

    def test_is_streaming_active_at_boundary(self):
        cfg = _make_cfg(extended_start="04:00", extended_end="20:00")
        sched = SessionScheduler(cfg)

        # Exactly at start boundary
        start_time = dt.datetime(2025, 6, 16, 4, 0, tzinfo=ET).astimezone(UTC)
        with patch(
            "marketdata.streaming.scheduler.get_trading_days",
            return_value=[dt.date(2025, 6, 16)],
        ):
            assert sched.is_streaming_active(start_time) is True

        # Exactly at end boundary
        end_time = dt.datetime(2025, 6, 16, 20, 0, tzinfo=ET).astimezone(UTC)
        with patch(
            "marketdata.streaming.scheduler.get_trading_days",
            return_value=[dt.date(2025, 6, 16)],
        ):
            assert sched.is_streaming_active(end_time) is True
