"""Twitter/X feed collection service.

Fetches tweets from configured accounts on a regular interval and stores
them as Parquet partitions.

Usage:
    uv run python gateway/twitter_service.py
    uv run python gateway/twitter_service.py --log-level DEBUG
"""

from __future__ import annotations

import argparse
import asyncio
import signal

from marketdata.config import load_config
from marketdata.twitter.collector import TweetCollector
from marketdata.utils.log import setup_logging


def main() -> None:
    parser = argparse.ArgumentParser(description="Twitter/X feed collection service")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args()

    setup_logging(args.log_level)
    cfg = load_config()
    collector = TweetCollector(cfg)

    def _signal_handler(sig: int, frame: object) -> None:
        collector._running = False

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    async def _run() -> None:
        await collector.start()
        try:
            await collector.run_forever()
        finally:
            await collector.stop()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
