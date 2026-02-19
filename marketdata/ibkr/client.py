"""Thin async wrapper around ib_async for historical-data fetching."""

from __future__ import annotations

import asyncio
from typing import Any

import pandas as pd
from ib_async import IB, BarData, Contract

from marketdata.config import PipelineConfig
from marketdata.ibkr.pacing import PacingEngine
from marketdata.utils.log import get_logger

log = get_logger(__name__)

# Reconnection settings
MAX_RECONNECT_ATTEMPTS = 3
RECONNECT_DELAY_SEC = 5.0


class IBKRClient:
    """Manages an IB Gateway/TWS connection and issues pacing-safe requests."""

    def __init__(self, cfg: PipelineConfig, pacer: PacingEngine | None = None) -> None:
        self._cfg = cfg
        self._ib = IB()
        self.pacer = pacer or PacingEngine(cfg.pacing)
        self._ib.disconnectedEvent += self._on_disconnect

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def _on_disconnect(self) -> None:
        log.warning("Disconnected from IBKR — will reconnect on next request")

    async def _ensure_connected(self) -> None:
        """Reconnect if the connection has dropped."""
        if self._ib.isConnected():
            return
        for attempt in range(MAX_RECONNECT_ATTEMPTS):
            log.warning(
                "Connection lost. Reconnect attempt %d/%d...",
                attempt + 1,
                MAX_RECONNECT_ATTEMPTS,
            )
            try:
                await asyncio.sleep(RECONNECT_DELAY_SEC)
                await self._ib.connectAsync(
                    host=self._cfg.ib_host,
                    port=self._cfg.ib_port,
                    clientId=self._cfg.ib_client_id,
                    readonly=True,
                )
                log.info("Reconnected to IBKR at %s:%s", self._cfg.ib_host, self._cfg.ib_port)
                return
            except Exception as exc:
                log.error("Reconnect attempt %d failed: %s", attempt + 1, exc)
        raise ConnectionError(f"Failed to reconnect after {MAX_RECONNECT_ATTEMPTS} attempts")

    async def connect(self) -> None:
        await self._ib.connectAsync(
            host=self._cfg.ib_host,
            port=self._cfg.ib_port,
            clientId=self._cfg.ib_client_id,
            readonly=True,
        )
        log.info(
            "Connected to IBKR at %s:%s (clientId=%s)",
            self._cfg.ib_host,
            self._cfg.ib_port,
            self._cfg.ib_client_id,
        )

    async def disconnect(self) -> None:
        self._ib.disconnect()
        log.info("Disconnected from IBKR")

    @property
    def ib(self) -> IB:
        return self._ib

    # ------------------------------------------------------------------
    # Historical bars
    # ------------------------------------------------------------------

    async def fetch_historical(
        self,
        contract: Contract,
        end_dt: str,
        duration: str,
        bar_size: str,
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> list[BarData]:
        """Fetch historical bars with pacing compliance, retry, and auto-reconnect.

        Parameters
        ----------
        contract : Contract
            IB contract object.
        end_dt : str
            End datetime in IBKR format ``"YYYYmmdd-HH:MM:SS"`` (UTC) or ``""``.
        duration : str
            IBKR duration string, e.g. ``"1 D"`` or ``"3600 S"``.
        bar_size : str
            IBKR bar-size setting, e.g. ``"1 min"`` or ``"5 secs"``.
        what_to_show : str
            Data type, default ``"TRADES"``.
        use_rth : bool
            Whether to restrict to Regular Trading Hours.

        Returns
        -------
        list[BarData]
            List of BarData objects (may be empty).

        Raises
        ------
        RuntimeError
            After exhausting retry attempts on pacing violations.
        ConnectionError
            After exhausting reconnection attempts.
        """
        last_error: Exception | None = None

        for attempt in range(self.pacer.max_attempts):
            await self._ensure_connected()
            await self.pacer.acquire(contract, end_dt, bar_size)

            try:
                bars = await self._ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=end_dt,
                    durationStr=duration,
                    barSizeSetting=bar_size,
                    whatToShow=what_to_show,
                    useRTH=use_rth,
                    formatDate=2,  # UTC timestamps
                )
                return bars or []
            except Exception as exc:
                msg = str(exc).lower()
                if "pacing" in msg or "historical data request" in msg:
                    log.warning("Pacing violation on attempt %d: %s", attempt + 1, exc)
                    await self.pacer.backoff_sleep(attempt)
                    last_error = exc
                    continue
                if "not connected" in msg or "connection" in msg:
                    log.warning("Connection error on attempt %d: %s", attempt + 1, exc)
                    last_error = exc
                    continue
                raise

        raise RuntimeError(
            f"Exhausted {self.pacer.max_attempts} retries for "
            f"{contract.symbol} {bar_size} ending {end_dt}: {last_error}"
        )

    # ------------------------------------------------------------------
    # Live market-data snapshot (for breadth probing)
    # ------------------------------------------------------------------

    async def snapshot(self, contract: Contract, timeout: float = 5.0) -> dict[str, Any]:
        """Request a live market-data snapshot and return available fields."""
        await self._ensure_connected()
        ticker = self._ib.reqMktData(contract, snapshot=True)
        await asyncio.sleep(timeout)
        self._ib.cancelMktData(contract)

        result: dict[str, Any] = {}
        for attr in ("bid", "ask", "last", "close", "volume"):
            val = getattr(ticker, attr, None)
            if val is not None and val == val:  # not NaN
                result[attr] = val
        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def bars_to_df(bars: list[BarData]) -> pd.DataFrame:
        """Convert IBKR BarData list to a pandas DataFrame."""
        if not bars:
            return pd.DataFrame(
                columns=["ts_utc", "open", "high", "low", "close", "volume", "wap", "count"]
            )

        records = []
        for b in bars:
            records.append(
                {
                    "ts_utc": pd.Timestamp(b.date).tz_convert("UTC")
                    if hasattr(b.date, "tzinfo") and b.date.tzinfo
                    else pd.Timestamp(b.date, tz="UTC"),
                    "open": float(b.open),
                    "high": float(b.high),
                    "low": float(b.low),
                    "close": float(b.close),
                    "volume": float(b.volume),
                    "wap": float(b.average) if hasattr(b, "average") else 0.0,
                    "count": int(b.barCount) if hasattr(b, "barCount") else 0,
                }
            )
        df = pd.DataFrame(records)
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
        return df
