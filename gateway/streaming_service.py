"""Streaming data collection service for IBKR market data.

Runs continuously, streaming real-time 5-second bars and news during extended
trading hours (4:00 AM - 8:00 PM ET).  Reconciles with historical data only
if connection gaps are detected.

Usage:
    uv run python gateway/streaming_service.py
    uv run python gateway/streaming_service.py --log-level DEBUG
"""

from __future__ import annotations

import argparse
import asyncio
import signal

from marketdata.config import load_config
from marketdata.streaming.service import StreamingService
from marketdata.utils.log import setup_logging


def main() -> None:
    parser = argparse.ArgumentParser(description="IBKR streaming data service")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args()

    setup_logging(args.log_level)
    cfg = load_config()
    service = StreamingService(cfg)

    def _signal_handler(sig: int, frame: object) -> None:
        service.request_shutdown()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    asyncio.run(service.run_forever())


if __name__ == "__main__":
    main()
