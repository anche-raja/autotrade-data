"""IBKR historical-data pacing engine.

Enforces three rate limits documented by IBKR:
  1. No identical request within 15 seconds.
  2. No more than 60 historical requests in any 10-minute window.
  3. No more than 6 requests for the same contract+exchange+tick_type in 2 seconds.

Also provides exponential-backoff retry logic for pacing-violation errors.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from marketdata.config import PacingConfig
from marketdata.utils.log import get_logger

log = get_logger(__name__)


@dataclass
class _RequestRecord:
    key: str
    contract_key: str
    timestamp: float


class PacingEngine:
    """Thread-safe (asyncio-safe) pacing tracker for IBKR historical requests."""

    def __init__(self, cfg: PacingConfig | None = None) -> None:
        self._cfg = cfg or PacingConfig()
        self._lock = asyncio.Lock()

        # All historical requests (for the 60-per-10-min rule)
        self._all_requests: list[float] = []

        # Per identical-request timestamps (contract+end+bar -> last ts)
        self._identical: dict[str, float] = {}

        # Per contract-key timestamps (for the 6-per-2s rule)
        self._contract_requests: defaultdict[str, list[float]] = defaultdict(list)

    @staticmethod
    def _request_key(contract: Any, end_dt: str, bar_size: str) -> str:
        """Unique key for the identical-request rule."""
        con_id = getattr(contract, "conId", 0)
        symbol = getattr(contract, "symbol", "?")
        exchange = getattr(contract, "exchange", "?")
        return f"{con_id}:{symbol}:{exchange}:{end_dt}:{bar_size}"

    @staticmethod
    def _contract_key(contract: Any) -> str:
        """Key for the per-contract rate limit."""
        symbol = getattr(contract, "symbol", "?")
        exchange = getattr(contract, "exchange", "?")
        sec_type = getattr(contract, "secType", "?")
        return f"{symbol}:{exchange}:{sec_type}"

    def _prune(self, now: float) -> None:
        """Remove stale entries from sliding windows."""
        cutoff_10m = now - 600
        self._all_requests = [t for t in self._all_requests if t > cutoff_10m]

        stale_identical = [k for k, v in self._identical.items() if now - v > 30]
        for k in stale_identical:
            del self._identical[k]

        cutoff_2s = now - 2.0
        for ck in list(self._contract_requests):
            self._contract_requests[ck] = [t for t in self._contract_requests[ck] if t > cutoff_2s]
            if not self._contract_requests[ck]:
                del self._contract_requests[ck]

    async def acquire(self, contract: Any, end_dt: str, bar_size: str) -> None:
        """Wait until it is safe to issue a historical-data request."""
        req_key = self._request_key(contract, end_dt, bar_size)
        con_key = self._contract_key(contract)

        while True:
            async with self._lock:
                now = time.monotonic()
                self._prune(now)
                wait = 0.0

                # Rule 1: identical request within 15 s
                last_identical = self._identical.get(req_key)
                if last_identical is not None:
                    elapsed = now - last_identical
                    if elapsed < self._cfg.max_identical_wait_sec:
                        wait = max(wait, self._cfg.max_identical_wait_sec - elapsed + 0.1)

                # Rule 2: 60 requests per 10 min
                if len(self._all_requests) >= self._cfg.max_requests_per_10min:
                    oldest = self._all_requests[0]
                    wait = max(wait, 600 - (now - oldest) + 0.1)

                # Rule 3: 6 same-contract requests per 2 s
                con_ts = self._contract_requests.get(con_key, [])
                if len(con_ts) >= self._cfg.max_same_contract_per_2sec:
                    oldest_con = con_ts[0]
                    wait = max(wait, 2.0 - (now - oldest_con) + 0.1)

                if wait <= 0:
                    # Safe to proceed: record this request
                    self._all_requests.append(now)
                    self._identical[req_key] = now
                    self._contract_requests[con_key].append(now)
                    return

            # Must wait outside the lock
            log.debug("Pacing: sleeping %.1fs before next request", wait)
            await asyncio.sleep(wait)

    async def backoff_sleep(self, attempt: int) -> float:
        """Exponential backoff after a pacing violation. Returns seconds slept."""
        delay = min(
            self._cfg.retry_base_delay_sec * (2**attempt),
            self._cfg.retry_max_delay_sec,
        )
        log.warning("Pacing violation backoff: attempt %d, sleeping %.1fs", attempt, delay)
        await asyncio.sleep(delay)
        return delay

    @property
    def max_attempts(self) -> int:
        return self._cfg.retry_max_attempts
