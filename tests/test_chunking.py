"""Tests for pipeline/chunking.py — chunk plan generation."""

from __future__ import annotations

import datetime as dt

from marketdata.pipeline.chunking import plan_chunks


class TestChunkPlanFor1Min:
    """1-min bar plans should produce a single chunk per day."""

    def test_chunk_plan_for_1min(self) -> None:
        day = dt.date(2025, 9, 15)  # a Monday
        chunks = plan_chunks(day, "1min")

        assert len(chunks) == 1
        chunk = chunks[0]
        assert chunk.bar_size_setting == "1 min"
        assert chunk.duration_str == "1 D"
        # end_dt should correspond to 16:00 ET in UTC
        assert "20:00:00" in chunk.end_dt_ib or "21:00:00" in chunk.end_dt_ib  # EDT or EST


class TestChunkPlanFor5Sec:
    """5-sec bar plans should split RTH into ~7 hourly chunks."""

    def test_chunk_plan_for_5sec_windows(self) -> None:
        day = dt.date(2025, 9, 15)
        chunks = plan_chunks(day, "5sec")

        # RTH = 09:30–16:00 = 6.5 hours = 6 full hours + 30 min
        assert len(chunks) == 7

        # First 6 chunks: 3600 S each
        for c in chunks[:6]:
            assert c.duration_str == "3600 S"
            assert c.bar_size_setting == "5 secs"

        # Last chunk: 1800 S (30 min)
        assert chunks[6].duration_str == "1800 S"

    def test_chunks_cover_full_session(self) -> None:
        day = dt.date(2025, 9, 15)
        chunks = plan_chunks(day, "5sec")

        # Chunks should be contiguous
        for i in range(1, len(chunks)):
            assert chunks[i].start_utc == chunks[i - 1].end_utc

        # Total duration should be 6.5 hours = 23400 seconds
        total_seconds = sum(int(c.duration_str.split()[0]) for c in chunks)
        assert total_seconds == 23400  # 6 * 3600 + 1800

    def test_each_chunk_has_ib_datetime(self) -> None:
        day = dt.date(2025, 9, 15)
        chunks = plan_chunks(day, "5sec")

        for chunk in chunks:
            # IBKR format: YYYYmmdd-HH:MM:SS
            assert len(chunk.end_dt_ib) == 17
            assert "-" in chunk.end_dt_ib


class TestEdgeCases:
    """Edge-case tests for chunk planning."""

    def test_invalid_bar_size_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Unsupported bar_size"):
            plan_chunks(dt.date(2025, 9, 15), "3min")

    def test_winter_time_est(self) -> None:
        """In EST (winter), 16:00 ET = 21:00 UTC."""
        day = dt.date(2025, 12, 15)
        chunks = plan_chunks(day, "1min")
        assert "21:00:00" in chunks[0].end_dt_ib
