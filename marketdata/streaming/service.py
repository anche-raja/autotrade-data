"""Top-level streaming service orchestrator.

Runs 24/7: streams bars + news during extended hours, optionally reconciles
after close if connection gaps were detected, then sleeps until next session.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from zoneinfo import ZoneInfo

from marketdata.config import PipelineConfig, load_config
from marketdata.ibkr.client import IBKRClient
from marketdata.streaming.collector import StreamingCollector
from marketdata.streaming.news_collector import NewsCollector
from marketdata.streaming.scheduler import SessionScheduler
from marketdata.utils.log import get_logger

log = get_logger(__name__)
UTC = ZoneInfo("UTC")


class StreamingService:
    """Streams bars + news during extended hours, reconciles on gaps, sleeps between sessions."""

    def __init__(self, cfg: PipelineConfig) -> None:
        self._cfg = cfg
        self._scheduler = SessionScheduler(cfg)
        self._shutdown = asyncio.Event()

    async def run_forever(self) -> None:
        """Main loop: stream during market hours, reconcile on gaps, sleep, repeat."""
        log.info(
            "Streaming service starting (bar_symbols=%s, news_symbols=%s, hours=%s-%s ET)",
            self._cfg.streaming.symbols,
            self._cfg.streaming.news_symbols,
            self._cfg.streaming.extended_start,
            self._cfg.streaming.extended_end,
        )

        while not self._shutdown.is_set():
            try:
                if self._scheduler.is_streaming_active():
                    await self._run_session()
                else:
                    sleep_sec = min(
                        self._scheduler.seconds_until_next_session(),
                        300,  # Check at least every 5 min
                    )
                    log.info(
                        "Outside trading hours. Sleeping %.0fs until next session.",
                        sleep_sec,
                    )
                    try:
                        await asyncio.wait_for(
                            self._shutdown.wait(), timeout=sleep_sec,
                        )
                    except asyncio.TimeoutError:
                        pass
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("Streaming service error, restarting in 60s")
                try:
                    await asyncio.wait_for(self._shutdown.wait(), timeout=60)
                except asyncio.TimeoutError:
                    pass

        log.info("Streaming service shut down")

    async def _run_session(self) -> None:
        """Run a single streaming session (one day's extended hours)."""
        # Use streaming-specific client_id to avoid conflicts with batch
        cfg_override = self._cfg.model_copy(
            update={"ib_client_id": self._cfg.streaming.client_id},
        )
        client = IBKRClient(cfg_override)
        bar_collector = StreamingCollector(client, self._cfg)
        news_collector = NewsCollector(client, self._cfg)

        try:
            await client.connect()
            log.info("Session started — streaming bars and news")

            await bar_collector.start(self._cfg.streaming.symbols)

            try:
                await news_collector.start()
            except Exception:
                log.exception("News collector failed to start — continuing with bars only")

            # Wait until session ends or shutdown signal
            while (
                self._scheduler.is_streaming_active()
                and not self._shutdown.is_set()
            ):
                await asyncio.sleep(10)

            log.info("Session ending — stopping collectors")
            await bar_collector.stop()
            await news_collector.stop()

            # Reconcile only if gaps were detected
            gaps = bar_collector.gaps
            if gaps and self._cfg.streaming.reconcile_on_gap:
                log.info(
                    "Detected %d connection gap(s) — running reconciliation",
                    len(gaps),
                )
                await self._run_reconciliation()
            else:
                log.info("No connection gaps — skipping reconciliation")

        except Exception:
            log.exception("Error during streaming session")
            try:
                await bar_collector.stop()
            except Exception:
                pass
            try:
                await news_collector.stop()
            except Exception:
                pass
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass

    async def _run_reconciliation(self) -> None:
        """Run historical fetch to fill gaps from the current day."""
        log.info("Starting post-session reconciliation")
        today = dt.datetime.now(ZoneInfo(self._cfg.timezone)).date()

        # Use the default batch client_id for historical fetch
        batch_client = IBKRClient(self._cfg)

        try:
            await batch_client.connect()

            from marketdata.db.duck import MetadataDB
            from marketdata.pipeline.fetch_bars import fetch_bars

            db = MetadataDB(self._cfg.duckdb_path)
            try:
                await fetch_bars(
                    batch_client,
                    db,
                    self._cfg,
                    self._cfg.streaming.symbols,
                    self._cfg.streaming.bar_sizes,
                    today,
                    today,
                    rth=False,  # Include extended hours
                )
            finally:
                db.close()
        except Exception:
            log.exception("Reconciliation failed")
        finally:
            try:
                await batch_client.disconnect()
            except Exception:
                pass

        log.info("Reconciliation complete")

    def request_shutdown(self) -> None:
        """Signal the service to shut down gracefully."""
        log.info("Shutdown requested")
        self._shutdown.set()
