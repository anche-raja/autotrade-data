"""Daily data fetch script for IBKR market data.

Fetches recent bars (1min, 5sec) and news for configured tickers.
Designed to run daily after market close via Windows Task Scheduler.
Waits for IB Gateway to be available and retries on connection failures.

Usage:
    uv run python gateway/daily_fetch.py
    uv run python gateway/daily_fetch.py --days 5  # fetch last 5 days
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import logging
import socket
import sys
import time
from pathlib import Path

from marketdata.config import load_config
from marketdata.utils.log import setup_logging

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_FILE = PROJECT_ROOT / "data" / "logs" / "daily_fetch.log"

# --- Configuration ---
BAR_SYMBOLS = ["SPY", "QQQ"]
BAR_SIZES = ["1min", "5sec"]
NEWS_SYMBOLS = ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "GOOGL"]
NEWS_PROVIDERS = "BZ+DJ-N+DJ-RT+FLY+BRFG+BRFUPDN"
DEFAULT_LOOKBACK_DAYS = 3  # fetch last N days to catch gaps from missed runs
GATEWAY_WAIT_TIMEOUT = 300  # seconds to wait for Gateway
GATEWAY_POLL_INTERVAL = 10  # seconds between connectivity checks
MAX_PHASE_RETRIES = 2  # retry entire bars/news phase on failure


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily IBKR data fetch")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Number of calendar days to look back (default: {DEFAULT_LOOKBACK_DAYS})",
    )
    parser.add_argument(
        "--bars-only",
        action="store_true",
        help="Only fetch bars, skip news",
    )
    parser.add_argument(
        "--news-only",
        action="store_true",
        help="Only fetch news, skip bars",
    )
    parser.add_argument(
        "--skip-gateway-check",
        action="store_true",
        help="Skip waiting for Gateway (use if TWS is running manually)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO)",
    )
    return parser.parse_args()


def wait_for_gateway(host: str, port: int, timeout: int = GATEWAY_WAIT_TIMEOUT) -> bool:
    """Poll IB Gateway port until it accepts connections or timeout expires."""
    log = logging.getLogger("daily_fetch")
    log.info("Waiting for IB Gateway at %s:%d (timeout %ds)...", host, port, timeout)
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=5):
                log.info("IB Gateway is accepting connections on %s:%d", host, port)
                return True
        except (ConnectionRefusedError, TimeoutError, OSError):
            remaining = int(deadline - time.monotonic())
            log.debug("Gateway not ready, retrying... (%ds remaining)", remaining)
            time.sleep(GATEWAY_POLL_INTERVAL)

    log.error("Gateway did not become available within %ds", timeout)
    return False


async def fetch_bars(
    cfg, symbols: list[str], bar_sizes: list[str], start: dt.date, end: dt.date
) -> dict[str, int]:
    from marketdata.db.duck import MetadataDB
    from marketdata.ibkr.client import IBKRClient
    from marketdata.pipeline.fetch_bars import fetch_bars as _fetch

    client = IBKRClient(cfg)
    db = MetadataDB(cfg.duckdb_path)

    try:
        await client.connect()
        return await _fetch(client, db, cfg, symbols, bar_sizes, start, end, rth=True)
    finally:
        await client.disconnect()
        db.close()


async def fetch_news(
    cfg, symbols: list[str], providers: str, start: dt.date, end: dt.date
) -> dict[str, int]:
    from marketdata.db.duck import MetadataDB
    from marketdata.ibkr.client import IBKRClient
    from marketdata.pipeline.news import IBKRNewsProvider, resolve_con_id

    client = IBKRClient(cfg)
    db = MetadataDB(cfg.duckdb_path)
    total: dict[str, int] = {}

    try:
        await client.connect()
        news_provider = IBKRNewsProvider(client, cfg)

        for sym in symbols:
            con_id = await resolve_con_id(client, sym)
            result = await news_provider.fetch_news(
                con_id,
                sym,
                providers,
                start,
                end,
                db,
                fetch_bodies=True,
            )
            for k, v in result.items():
                total[f"{sym}_{k}"] = v
    finally:
        await client.disconnect()
        db.close()

    return total


async def run_with_retry(name: str, coro_fn, max_retries: int = MAX_PHASE_RETRIES, **kwargs):
    """Run an async function with retries on failure."""
    log = logging.getLogger("daily_fetch")
    for attempt in range(max_retries + 1):
        try:
            return await coro_fn(**kwargs)
        except Exception:
            if attempt < max_retries:
                delay = 30 * (attempt + 1)
                log.warning(
                    "%s failed (attempt %d/%d), retrying in %ds...",
                    name,
                    attempt + 1,
                    max_retries + 1,
                    delay,
                )
                log.exception("Error details:")
                await asyncio.sleep(delay)
            else:
                log.exception("%s failed after %d attempts", name, max_retries + 1)
                return None


async def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)
    log = logging.getLogger("daily_fetch")

    # Ensure log directory exists
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(str(LOG_FILE), encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logging.getLogger().addHandler(file_handler)

    cfg = load_config()
    today = dt.date.today()
    start = today - dt.timedelta(days=args.days)

    log.info("=" * 60)
    log.info("Daily fetch starting: %s to %s", start, today)
    log.info("Config: host=%s port=%s clientId=%s", cfg.ib_host, cfg.ib_port, cfg.ib_client_id)

    # Wait for Gateway to be ready
    if not args.skip_gateway_check:
        if not wait_for_gateway(cfg.ib_host, cfg.ib_port):
            log.error("Aborting: IB Gateway not available")
            sys.exit(1)

    # Fetch bars with retry
    if not args.news_only:
        log.info("Fetching bars: symbols=%s sizes=%s", BAR_SYMBOLS, BAR_SIZES)
        bar_results = await run_with_retry(
            "Bar fetch",
            fetch_bars,
            cfg=cfg,
            symbols=BAR_SYMBOLS,
            bar_sizes=BAR_SIZES,
            start=start,
            end=today,
        )
        if bar_results:
            for key, rows in bar_results.items():
                log.info("  %s: %d rows", key, rows)

    # Fetch news with retry
    if not args.bars_only:
        log.info("Fetching news: symbols=%s", NEWS_SYMBOLS)
        news_results = await run_with_retry(
            "News fetch",
            fetch_news,
            cfg=cfg,
            symbols=NEWS_SYMBOLS,
            providers=NEWS_PROVIDERS,
            start=start,
            end=today,
        )
        if news_results:
            for key, count in news_results.items():
                log.info("  %s: %d articles", key, count)

    log.info("Daily fetch complete")


if __name__ == "__main__":
    asyncio.run(main())
