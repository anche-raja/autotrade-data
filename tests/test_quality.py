"""Tests for pipeline/quality.py — dedup, sort, gap detection."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd

from marketdata.pipeline.quality import clean_bars, detect_gaps

UTC = ZoneInfo("UTC")


class TestCleanBars:
    """Tests for sort + dedup logic."""

    def test_partition_writer_dedup_and_sort(self, sample_1min_df: pd.DataFrame) -> None:
        """Given unsorted data with duplicates, clean_bars returns sorted, unique rows."""
        # Reverse the df and add duplicate timestamps
        df = sample_1min_df.iloc[::-1].reset_index(drop=True)  # reversed
        dup_row = sample_1min_df.iloc[0:1].copy()
        dup_row["close"] = 999.0  # different value, same timestamp
        df = pd.concat([df, dup_row], ignore_index=True)

        assert len(df) == 391  # 390 + 1 duplicate

        cleaned = clean_bars(df, "1min")

        # Exactly 390 unique rows
        assert len(cleaned) == 390

        # Sorted ascending
        assert cleaned["ts_utc"].is_monotonic_increasing

        # Keep-last: the duplicate row's close=999.0 should be kept
        first_ts = sample_1min_df["ts_utc"].iloc[0]
        row = cleaned.loc[cleaned["ts_utc"] == first_ts]
        assert row["close"].iloc[0] == 999.0

    def test_clean_empty_df(self) -> None:
        """clean_bars handles empty DataFrame gracefully."""
        df = pd.DataFrame(columns=["ts_utc", "open", "high", "low", "close", "volume"])
        result = clean_bars(df, "1min")
        assert result.empty

    def test_already_clean_data(self, sample_1min_df: pd.DataFrame) -> None:
        """Already sorted, no-dupe data passes through unchanged."""
        cleaned = clean_bars(sample_1min_df, "1min")
        assert len(cleaned) == len(sample_1min_df)
        assert cleaned["quality_flags"].iloc[0] == "ok"


class TestDetectGaps:
    """Tests for gap detection in bar time series."""

    def test_gap_detection_for_1min(self) -> None:
        """Detects interior gap when bars are missing."""
        # RTH 09:30–16:00 ET -> 13:30–20:00 UTC (EDT)
        rth_start = dt.datetime(2025, 9, 15, 13, 30, tzinfo=UTC)
        rth_end = dt.datetime(2025, 9, 15, 20, 0, tzinfo=UTC)

        # Build 390 bars but remove 10 bars (minutes 100–109)
        bars = []
        for i in range(390):
            if 100 <= i < 110:
                continue  # gap
            ts = rth_start + dt.timedelta(minutes=i)
            bars.append({"ts_utc": ts, "close": 450.0})

        df = pd.DataFrame(bars)
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)

        gaps = detect_gaps(df, "1min", rth_start, rth_end)

        # Should detect exactly one interior gap
        assert len(gaps) == 1
        gap = gaps[0]
        assert gap.gap_type == "missing_data"
        # Gap should span approximately minute 99 to minute 110
        expected_gap_start = rth_start + dt.timedelta(minutes=99)
        expected_gap_end = rth_start + dt.timedelta(minutes=110)
        assert gap.gap_start_utc == expected_gap_start
        assert gap.gap_end_utc == expected_gap_end

    def test_no_gaps_in_complete_data(self, sample_1min_df: pd.DataFrame) -> None:
        """Complete data has no gaps."""
        rth_start = dt.datetime(2025, 9, 15, 13, 30, tzinfo=UTC)
        rth_end = dt.datetime(2025, 9, 15, 20, 0, tzinfo=UTC)

        gaps = detect_gaps(sample_1min_df, "1min", rth_start, rth_end)
        assert len(gaps) == 0

    def test_empty_df_reports_full_gap(self) -> None:
        """Empty DataFrame should report a full-session gap."""
        rth_start = dt.datetime(2025, 9, 15, 13, 30, tzinfo=UTC)
        rth_end = dt.datetime(2025, 9, 15, 20, 0, tzinfo=UTC)

        df = pd.DataFrame(columns=["ts_utc", "close"])
        gaps = detect_gaps(df, "1min", rth_start, rth_end)

        assert len(gaps) == 1
        assert gaps[0].gap_start_utc == rth_start
        assert gaps[0].gap_end_utc == rth_end

    def test_gap_at_session_start(self) -> None:
        """Gap detected when first bar is significantly after session start."""
        rth_start = dt.datetime(2025, 9, 15, 13, 30, tzinfo=UTC)
        rth_end = dt.datetime(2025, 9, 15, 20, 0, tzinfo=UTC)

        # Start 5 minutes late
        bars = []
        for i in range(5, 390):
            ts = rth_start + dt.timedelta(minutes=i)
            bars.append({"ts_utc": ts, "close": 450.0})

        df = pd.DataFrame(bars)
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)

        gaps = detect_gaps(df, "1min", rth_start, rth_end)
        assert any(g.gap_start_utc == rth_start for g in gaps)
