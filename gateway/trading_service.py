"""Live trading service for IBKR ORB breakout strategy.

Monitors the opening range for the configured symbol, detects breakout
signals, and logs trade decisions to JSONL.  In paper mode (default),
signals are logged but no orders are placed.

Usage:
    uv run python gateway/trading_service.py
    uv run python gateway/trading_service.py --log-level DEBUG
"""

from __future__ import annotations

import argparse
import asyncio
import signal

from marketdata.config import load_config
from marketdata.trading.service import TradingService
from marketdata.utils.log import setup_logging


def main() -> None:
    parser = argparse.ArgumentParser(description="IBKR live trading service")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args()

    setup_logging(args.log_level)
    cfg = load_config()
    service = TradingService(cfg)

    def _signal_handler(sig: int, frame: object) -> None:
        service.request_shutdown()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    asyncio.run(service.run_forever())


if __name__ == "__main__":
    main()
