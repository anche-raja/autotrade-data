"""Tests for the 5-sec → 1-min bar aggregator."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

from marketdata.streaming.aggregator import BarAggregator

UTC = ZoneInfo("UTC")


def _bar(ts: dt.datetime, price: float, volume: float = 100.0) -> dict:
    return {
        "ts_utc": ts,
        "open": price,
        "high": price + 0.10,
        "low": price - 0.10,
        "close": price + 0.05,
        "volume": volume,
        "wap": price,
        "count": 10,
        "source": "ibkr_stream",
        "quality_flags": "live",
    }


class TestBarAggregator:
    def test_twelve_bars_complete_one_minute(self):
        agg = BarAggregator()
        base = dt.datetime(2025, 6, 15, 14, 0, 0, tzinfo=UTC)

        # Feed 12 bars for minute :00
        results = []
        for i in range(12):
            ts = base + dt.timedelta(seconds=i * 5)
            result = agg.add_bar("SPY", _bar(ts, 500.0 + i))
            if result:
                results.append(result)

        # Should not have emitted yet (no bar from next minute)
        assert len(results) == 0

        # Feed first bar of next minute → triggers flush of previous minute
        next_ts = base + dt.timedelta(minutes=1)
        result = agg.add_bar("SPY", _bar(next_ts, 520.0))
        assert result is not None
        assert result["ts_utc"] == base
        assert result["open"] == 500.0  # first bar's open
        assert result["close"] == 511.05  # last bar's close (500+11+0.05)
        assert result["volume"] == 1200.0  # 12 * 100

    def test_ohlcv_aggregation(self):
        agg = BarAggregator()
        base = dt.datetime(2025, 6, 15, 14, 0, 0, tzinfo=UTC)

        bars = [
            {"ts_utc": base, "open": 100, "high": 105, "low": 98, "close": 102,
             "volume": 50, "wap": 101, "count": 5, "source": "s", "quality_flags": "l"},
            {"ts_utc": base + dt.timedelta(seconds=5), "open": 102, "high": 110, "low": 99, "close": 108,
             "volume": 150, "wap": 106, "count": 15, "source": "s", "quality_flags": "l"},
        ]

        for b in bars:
            agg.add_bar("SPY", b)

        # Trigger flush with next minute bar
        next_ts = base + dt.timedelta(minutes=1)
        result = agg.add_bar("SPY", _bar(next_ts, 108))
        assert result is not None
        assert result["open"] == 100  # first open
        assert result["high"] == 110  # max high
        assert result["low"] == 98  # min low
        assert result["close"] == 108  # last close
        assert result["volume"] == 200  # 50 + 150
        assert result["count"] == 20  # 5 + 15

    def test_flush_all_returns_incomplete(self):
        agg = BarAggregator()
        base = dt.datetime(2025, 6, 15, 14, 0, 0, tzinfo=UTC)

        # Feed 3 bars (incomplete minute)
        for i in range(3):
            ts = base + dt.timedelta(seconds=i * 5)
            agg.add_bar("SPY", _bar(ts, 500.0))

        results = agg.flush_all("SPY")
        assert len(results) == 1
        assert results[0]["ts_utc"] == base
        assert results[0]["volume"] == 300.0

    def test_flush_all_symbols(self):
        agg = BarAggregator()
        base = dt.datetime(2025, 6, 15, 14, 0, 0, tzinfo=UTC)

        agg.add_bar("SPY", _bar(base, 500.0))
        agg.add_bar("QQQ", _bar(base, 400.0))

        results = agg.flush_all_symbols()
        assert len(results) == 2
        symbols = {r["open"] for r in results}
        assert 500.0 in symbols
        assert 400.0 in symbols

    def test_multiple_minutes(self):
        agg = BarAggregator()
        base = dt.datetime(2025, 6, 15, 14, 0, 0, tzinfo=UTC)

        completed = []
        # Feed 3 minutes worth of bars
        for minute in range(3):
            for sec in range(0, 60, 5):
                ts = base + dt.timedelta(minutes=minute, seconds=sec)
                result = agg.add_bar("SPY", _bar(ts, 500.0 + minute))
                if result:
                    completed.append(result)

        # 2 completed minutes (first two), third is still pending
        assert len(completed) == 2
        assert completed[0]["ts_utc"] == base
        assert completed[1]["ts_utc"] == base + dt.timedelta(minutes=1)

    def test_wap_volume_weighted(self):
        agg = BarAggregator()
        base = dt.datetime(2025, 6, 15, 14, 0, 0, tzinfo=UTC)

        bars = [
            {"ts_utc": base, "open": 100, "high": 101, "low": 99, "close": 100,
             "volume": 100, "wap": 100.0, "count": 1, "source": "s", "quality_flags": "l"},
            {"ts_utc": base + dt.timedelta(seconds=5), "open": 100, "high": 101, "low": 99, "close": 100,
             "volume": 300, "wap": 200.0, "count": 1, "source": "s", "quality_flags": "l"},
        ]
        for b in bars:
            agg.add_bar("SPY", b)

        results = agg.flush_all("SPY")
        assert len(results) == 1
        # WAP = (100*100 + 200*300) / (100+300) = 70000/400 = 175.0
        assert results[0]["wap"] == 175.0
