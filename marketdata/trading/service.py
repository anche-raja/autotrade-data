"""Live trading service: monitors ORB breakout signals and logs decisions.

Connects to IBKR, streams real-time bars during RTH, detects opening-range
breakout signals, and logs trade decisions to JSONL.  Actual order placement
is gated behind the ``paper_mode`` flag — when True (default), signals are
logged but no orders are submitted.

Usage (via gateway wrapper):
    uv run python gateway/trading_service.py
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from ib_async import IB, Stock

from marketdata.config import PipelineConfig, TradingConfig
from marketdata.utils.log import get_logger

log = get_logger(__name__)

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


class TradingService:
    """Monitors ORB breakout signals during RTH and logs trade decisions."""

    def __init__(self, cfg: PipelineConfig) -> None:
        self._cfg = cfg
        self._tc: TradingConfig = cfg.trading
        self._ib = IB()
        self._shutdown = asyncio.Event()

        # Opening range state (reset each day)
        self._range_high: float | None = None
        self._range_low: float | None = None
        self._range_bars: list[dict] = []
        self._range_complete = False
        self._today: dt.date | None = None
        self._trades_today = 0

        # Log directory
        self._log_dir = Path(self._tc.log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)

    async def run_forever(self) -> None:
        """Main loop: connect, stream during RTH, sleep outside hours."""
        if not self._tc.enabled:
            log.info("Trading is disabled (trading.enabled=false). Waiting for shutdown signal.")
            await self._shutdown.wait()
            return

        log.info(
            "Trading service starting: symbol=%s range=%dm direction=%s paper=%s",
            self._tc.symbol,
            self._tc.range_minutes,
            self._tc.direction,
            self._tc.paper_mode,
        )

        while not self._shutdown.is_set():
            try:
                now_ny = dt.datetime.now(NY)

                if self._is_rth_active(now_ny):
                    await self._run_session()
                else:
                    sleep_sec = self._seconds_until_rth(now_ny)
                    sleep_sec = min(sleep_sec, 300)
                    log.info("Outside RTH. Sleeping %.0fs.", sleep_sec)
                    try:
                        await asyncio.wait_for(self._shutdown.wait(), timeout=sleep_sec)
                    except asyncio.TimeoutError:
                        pass

            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("Trading service error, restarting in 60s")
                try:
                    await asyncio.wait_for(self._shutdown.wait(), timeout=60)
                except asyncio.TimeoutError:
                    pass

        log.info("Trading service shut down")

    async def _run_session(self) -> None:
        """Run a single RTH trading session."""
        self._reset_day()

        cfg_override = self._cfg.model_copy(
            update={"ib_client_id": self._tc.client_id},
        )
        try:
            await self._ib.connectAsync(
                host=cfg_override.ib_host,
                port=cfg_override.ib_port,
                clientId=cfg_override.ib_client_id,
                readonly=self._tc.paper_mode,
            )
            log.info(
                "Connected to IBKR at %s:%s (clientId=%s, readonly=%s)",
                cfg_override.ib_host,
                cfg_override.ib_port,
                cfg_override.ib_client_id,
                self._tc.paper_mode,
            )
        except Exception:
            log.exception("Failed to connect to IBKR")
            return

        try:
            contract = Stock(self._tc.symbol, "SMART", "USD")
            qualified = await self._ib.qualifyContractsAsync(contract)
            if not qualified:
                log.error("Could not qualify contract: %s", self._tc.symbol)
                return

            # Subscribe to real-time 5-sec bars
            bars = self._ib.reqRealTimeBars(
                contract, barSize=5, whatToShow="TRADES", useRTH=True,
            )
            bars.updateEvent += self._on_bar

            log.info("Streaming 5-sec bars for %s", self._tc.symbol)

            # Wait until RTH ends or shutdown
            while self._is_rth_active(dt.datetime.now(NY)) and not self._shutdown.is_set():
                self._ib.sleep(5)

            self._ib.cancelRealTimeBars(bars)
            log.info("Session ended — cancelled bar subscription")

        except Exception:
            log.exception("Error during trading session")
        finally:
            try:
                self._ib.disconnect()
            except Exception:
                log.debug("Failed to disconnect", exc_info=True)

    def _on_bar(self, bars: list, has_new_bar: bool) -> None:
        """Callback for each new 5-second bar."""
        if not has_new_bar or not bars:
            return

        bar = bars[-1]
        now_ny = dt.datetime.now(NY)
        bar_time = now_ny.time()

        bar_data = {
            "time": bar_time.strftime("%H:%M:%S"),
            "open": float(bar.open_),
            "high": float(bar.high),
            "low": float(bar.low),
            "close": float(bar.close_),
            "volume": int(bar.volume),
        }

        # Phase 1: Build opening range
        rth_start = dt.time(9, 30)
        range_end = (
            dt.datetime.combine(dt.date.today(), rth_start)
            + dt.timedelta(minutes=self._tc.range_minutes)
        ).time()

        if not self._range_complete and rth_start <= bar_time < range_end:
            self._range_bars.append(bar_data)
            high = bar_data["high"]
            low = bar_data["low"]
            self._range_high = max(self._range_high or high, high)
            self._range_low = min(self._range_low or low, low)
            return

        if not self._range_complete and bar_time >= range_end:
            self._range_complete = True
            if self._range_high is not None and self._range_low is not None:
                log.info(
                    "Opening range complete: high=%.2f low=%.2f (range=%.2f)",
                    self._range_high,
                    self._range_low,
                    self._range_high - self._range_low,
                )
                self._log_event("range_complete", {
                    "high": self._range_high,
                    "low": self._range_low,
                    "range": round(self._range_high - self._range_low, 2),
                    "bars": len(self._range_bars),
                })
            else:
                log.warning("Opening range has no data")
                return

        # Phase 2: Check for breakout signals
        if not self._range_complete or self._range_high is None or self._range_low is None:
            return

        # Check entry window
        no_entry = dt.time(*map(int, self._tc.no_entry_before.split(":")))
        time_exit = dt.time(*map(int, self._tc.time_exit.split(":")))

        if bar_time < no_entry:
            return

        if bar_time >= time_exit:
            return

        if self._trades_today >= self._tc.max_trades_per_day:
            return

        close = bar_data["close"]
        direction = self._tc.direction

        # Long breakout
        if direction in ("long", "both") and close > self._range_high:
            self._on_signal("long", close, bar_data)

        # Short breakout
        if direction in ("short", "both") and close < self._range_low:
            self._on_signal("short", close, bar_data)

    def _on_signal(self, side: str, price: float, bar_data: dict) -> None:
        """Handle a breakout signal."""
        assert self._range_high is not None and self._range_low is not None

        range_size = self._range_high - self._range_low
        if side == "long":
            stop = self._range_low - self._tc.stop_buffer
            tp = price + self._tc.tp_spy_dollars
        else:
            stop = self._range_high + self._tc.stop_buffer
            tp = price - self._tc.tp_spy_dollars

        signal = {
            "side": side,
            "entry_price": round(price, 2),
            "stop": round(stop, 2),
            "tp": round(tp, 2),
            "range_size": round(range_size, 2),
            "contracts": self._tc.num_contracts,
            "bar": bar_data,
        }

        self._trades_today += 1

        if self._tc.paper_mode:
            log.info(
                "PAPER SIGNAL: %s %s @ %.2f (stop=%.2f tp=%.2f)",
                side.upper(),
                self._tc.symbol,
                price,
                stop,
                tp,
            )
            self._log_event("paper_signal", signal)
        else:
            log.info(
                "LIVE SIGNAL: %s %s @ %.2f (stop=%.2f tp=%.2f) — order placement not implemented",
                side.upper(),
                self._tc.symbol,
                price,
                stop,
                tp,
            )
            self._log_event("live_signal_skipped", signal)

    def _reset_day(self) -> None:
        """Reset opening range state for a new day."""
        today = dt.date.today()
        if self._today != today:
            self._today = today
            self._range_high = None
            self._range_low = None
            self._range_bars = []
            self._range_complete = False
            self._trades_today = 0
            log.info("Day reset: %s", today.isoformat())

    def _log_event(self, event_type: str, data: dict) -> None:
        """Append an event to the daily JSONL log."""
        today = dt.date.today()
        log_file = self._log_dir / f"{today.isoformat()}.jsonl"
        entry = {
            "ts_utc": dt.datetime.now(UTC).isoformat(),
            "event": event_type,
            **data,
        }
        with open(log_file, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def _is_rth_active(self, now_ny: dt.datetime) -> bool:
        """Check if we're within regular trading hours (9:30-16:00 ET, weekdays)."""
        if now_ny.weekday() >= 5:
            return False
        t = now_ny.time()
        return dt.time(9, 30) <= t < dt.time(16, 0)

    def _seconds_until_rth(self, now_ny: dt.datetime) -> float:
        """Seconds until next RTH open (9:30 ET)."""
        next_open = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
        if now_ny.time() >= dt.time(9, 30):
            next_open += dt.timedelta(days=1)
        # Skip weekends
        while next_open.weekday() >= 5:
            next_open += dt.timedelta(days=1)
        return (next_open - now_ny).total_seconds()

    def request_shutdown(self) -> None:
        """Signal the service to shut down gracefully."""
        log.info("Shutdown requested")
        self._shutdown.set()
