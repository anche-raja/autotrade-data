"""Aggregates 5-second real-time bars into 1-minute OHLCV bars."""

from __future__ import annotations

import datetime as dt
from collections import defaultdict

from marketdata.utils.log import get_logger

log = get_logger(__name__)


class BarAggregator:
    """Collects 5-sec bars and emits completed 1-min bars.

    A minute is considered complete when a bar arrives for the *next* minute.
    Call :meth:`flush_all` at session end to emit any incomplete minute.
    """

    def __init__(self) -> None:
        # symbol -> minute_ts_iso -> list[bar_dict]
        self._pending: dict[str, dict[str, list[dict]]] = defaultdict(dict)

    def add_bar(self, symbol: str, bar: dict) -> dict | None:
        """Add a 5-sec bar.  Returns a completed 1-min bar if the previous
        minute is now done, else ``None``."""
        ts: dt.datetime = bar["ts_utc"]
        minute_ts = ts.replace(second=0, microsecond=0)
        key = minute_ts.isoformat()

        sym_pending = self._pending[symbol]
        if key not in sym_pending:
            sym_pending[key] = []
        sym_pending[key].append(bar)

        # Check if the *previous* minute can be flushed
        prev_minute = minute_ts - dt.timedelta(minutes=1)
        prev_key = prev_minute.isoformat()

        if prev_key in sym_pending:
            bars = sym_pending.pop(prev_key)
            return self._aggregate(prev_minute, bars)
        return None

    def flush_all(self, symbol: str) -> list[dict]:
        """Flush any pending incomplete minutes for *symbol*."""
        sym_pending = self._pending.get(symbol, {})
        results = []
        for key in sorted(sym_pending):
            bars = sym_pending[key]
            minute_ts = dt.datetime.fromisoformat(key)
            results.append(self._aggregate(minute_ts, bars))
        sym_pending.clear()
        return results

    def flush_all_symbols(self) -> list[dict]:
        """Flush incomplete minutes for every symbol."""
        results = []
        for symbol in list(self._pending):
            results.extend(self.flush_all(symbol))
        return results

    @staticmethod
    def _aggregate(minute_ts: dt.datetime, bars: list[dict]) -> dict:
        total_vol = sum(b["volume"] for b in bars)
        wap = (
            sum(b["wap"] * b["volume"] for b in bars) / total_vol
            if total_vol > 0
            else bars[-1]["wap"]
        )
        return {
            "ts_utc": minute_ts,
            "open": bars[0]["open"],
            "high": max(b["high"] for b in bars),
            "low": min(b["low"] for b in bars),
            "close": bars[-1]["close"],
            "volume": total_vol,
            "wap": wap,
            "count": sum(b["count"] for b in bars),
            "source": "ibkr_stream",
            "quality_flags": "live",
        }
