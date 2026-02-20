"""Real-time bar streaming: subscribes, buffers, flushes to Parquet."""

from __future__ import annotations

import asyncio
import datetime as dt
from collections import defaultdict
from zoneinfo import ZoneInfo

import pandas as pd

from marketdata.config import PipelineConfig
from marketdata.ibkr.client import IBKRClient
from marketdata.ibkr.contracts import get_bar_contract
from marketdata.pipeline.storage import (
    _normalize_bar_label,
    bars_partition_path,
    merge_partition,
)
from marketdata.streaming.aggregator import BarAggregator
from marketdata.utils.log import get_logger

log = get_logger(__name__)
UTC = ZoneInfo("UTC")


class StreamingCollector:
    """Subscribes to IBKR real-time 5-sec bars, buffers, and flushes to Parquet."""

    def __init__(self, client: IBKRClient, cfg: PipelineConfig) -> None:
        self._client = client
        self._cfg = cfg
        self._aggregator = BarAggregator()
        self._subscriptions: dict[str, object] = {}  # symbol -> RealTimeBarList

        # Buffers: symbol -> bar_size_label -> list[dict]
        self._buffers: dict[str, dict[str, list[dict]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._lock = asyncio.Lock()
        self._flush_task: asyncio.Task | None = None
        self._running = False

        # Gap tracking: list of (disconnect_utc, reconnect_utc) tuples
        self._gaps: list[tuple[dt.datetime, dt.datetime]] = []
        self._last_disconnect: dt.datetime | None = None

    @property
    def gaps(self) -> list[tuple[dt.datetime, dt.datetime]]:
        """Connection gaps recorded during the session."""
        return list(self._gaps)

    async def start(self, symbols: list[str]) -> None:
        """Subscribe to all symbols and start the periodic flush loop."""
        self._running = True

        for sym in symbols:
            contract = get_bar_contract(sym)
            bars = self._client.subscribe_realtime_bars(contract, use_rth=False)
            bars.updateEvent += lambda b, hasNew, s=sym: self._on_bar(s, b, hasNew)
            self._subscriptions[sym] = bars

        self._client.register_reconnect_callback(self._resubscribe_all)
        self._client.ib.disconnectedEvent += self._on_disconnect
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        """Cancel subscriptions, flush remaining data, stop flush loop."""
        self._running = False

        for bars in self._subscriptions.values():
            try:
                self._client.cancel_realtime_bars(bars)
            except Exception:
                pass
        self._subscriptions.clear()

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        # Flush remaining aggregated 1-min bars
        remaining = self._aggregator.flush_all_symbols()
        for bar in remaining:
            # Determine symbol from context (all remaining are flushed per-symbol)
            for sym in list(self._buffers):
                if "1min" in self._cfg.streaming.bar_sizes:
                    self._buffers[sym]["1min"].append(bar)

        await self._flush_all()

    def _on_disconnect(self) -> None:
        """Track disconnection time for gap detection."""
        self._last_disconnect = dt.datetime.now(UTC)

    def _on_bar(self, symbol: str, bars: object, has_new_bar: bool) -> None:
        """Callback fired by ib_async when a new 5-sec bar arrives."""
        if not has_new_bar or not self._running:
            return

        rtb = bars[-1]  # type: ignore[index]  # RealTimeBar
        bar_dict = {
            "ts_utc": pd.Timestamp(rtb.time).tz_localize(UTC)
            if not hasattr(rtb.time, "tzinfo") or rtb.time.tzinfo is None
            else pd.Timestamp(rtb.time).tz_convert(UTC),
            "open": float(rtb.open_),
            "high": float(rtb.high),
            "low": float(rtb.low),
            "close": float(rtb.close),
            "volume": float(rtb.volume),
            "wap": float(rtb.wap),
            "count": int(rtb.count),
            "source": "ibkr_stream",
            "quality_flags": "live",
        }

        if "5sec" in self._cfg.streaming.bar_sizes:
            self._buffers[symbol]["5sec"].append(bar_dict)

        if "1min" in self._cfg.streaming.bar_sizes:
            completed = self._aggregator.add_bar(symbol, bar_dict)
            if completed:
                self._buffers[symbol]["1min"].append(completed)

    async def _flush_loop(self) -> None:
        """Periodically flush buffered bars to Parquet."""
        interval = self._cfg.streaming.flush_interval_sec
        while self._running:
            await asyncio.sleep(interval)
            try:
                await self._flush_all()
            except Exception:
                log.exception("Error during flush")

    async def _flush_all(self) -> None:
        """Flush all buffered bars to Parquet partitions."""
        async with self._lock:
            for symbol in list(self._buffers):
                for bar_size in list(self._buffers[symbol]):
                    bars = self._buffers[symbol][bar_size]
                    if not bars:
                        continue

                    df = pd.DataFrame(bars)
                    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)

                    # Group by date and write each partition
                    df["_date"] = df["ts_utc"].dt.date
                    for date_val, group in df.groupby("_date"):
                        date_str = date_val.isoformat()
                        bar_label = _normalize_bar_label(bar_size)
                        path = bars_partition_path(
                            self._cfg.parquet_root, symbol, bar_label, date_str,
                        )
                        merge_partition(group.drop(columns=["_date"]), path)
                        log.debug(
                            "Flushed %d %s bars for %s %s",
                            len(group), bar_label, symbol, date_str,
                        )

                    bars.clear()

    async def _resubscribe_all(self) -> None:
        """Re-subscribe after a reconnection.  Records the gap."""
        now = dt.datetime.now(UTC)
        if self._last_disconnect:
            self._gaps.append((self._last_disconnect, now))
            log.warning(
                "Connection gap: %s -> %s (%.0fs)",
                self._last_disconnect.isoformat(),
                now.isoformat(),
                (now - self._last_disconnect).total_seconds(),
            )
            self._last_disconnect = None

        old_symbols = list(self._subscriptions)
        self._subscriptions.clear()

        for sym in old_symbols:
            contract = get_bar_contract(sym)
            bars = self._client.subscribe_realtime_bars(contract, use_rth=False)
            bars.updateEvent += lambda b, hasNew, s=sym: self._on_bar(s, b, hasNew)
            self._subscriptions[sym] = bars

        log.info(
            "Re-subscribed to %d real-time bar streams after reconnect",
            len(self._subscriptions),
        )
