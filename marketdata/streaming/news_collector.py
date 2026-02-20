"""Real-time news streaming via BroadTape feeds."""

from __future__ import annotations

import asyncio
import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from marketdata.config import PipelineConfig
from marketdata.ibkr.client import IBKRClient
from marketdata.pipeline.news import NEWS_SCHEMA, _parse_headline, news_partition_path
from marketdata.utils.log import get_logger

log = get_logger(__name__)
UTC = ZoneInfo("UTC")

# Max concurrent article body fetches
_BODY_CONCURRENCY = 5


class NewsCollector:
    """Subscribes to BroadTape news and flushes articles to Parquet."""

    def __init__(self, client: IBKRClient, cfg: PipelineConfig) -> None:
        self._client = client
        self._cfg = cfg
        self._tickers: list[object] = []
        self._buffer: list[dict] = []
        self._seen_ids: set[str] = set()
        self._lock = asyncio.Lock()
        self._flush_task: asyncio.Task | None = None
        self._running = False
        self._body_sem = asyncio.Semaphore(_BODY_CONCURRENCY)
        self._symbols_lower = {
            s.lower() for s in cfg.streaming.news_symbols
        }

    async def start(self) -> None:
        """Subscribe to news via stock contracts with tick 292 and start flush loop."""
        self._running = True
        self._tickers = await self._client.subscribe_news(
            self._cfg.streaming.news_symbols,
        )
        self._client.ib.tickNewsEvent += self._on_news
        self._client.register_reconnect_callback(self._resubscribe)
        self._flush_task = asyncio.create_task(self._flush_loop())
        log.info(
            "News collector started (symbols=%s, subscribed=%d)",
            self._cfg.streaming.news_symbols,
            len(self._tickers),
        )

    async def stop(self) -> None:
        """Cancel subscriptions and flush remaining articles."""
        self._running = False
        self._client.ib.tickNewsEvent -= self._on_news

        try:
            self._client.cancel_news(self._tickers)
        except Exception:
            pass
        self._tickers.clear()

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        await self._flush_all()

    def _on_news(self, news: object) -> None:
        """Callback from ib_async tickNewsEvent."""
        if not self._running:
            return

        article_id = getattr(news, "articleId", "")
        if not article_id or article_id in self._seen_ids:
            return
        self._seen_ids.add(article_id)

        # Schedule async body fetch + buffering
        asyncio.ensure_future(self._process_article(news))

    async def _process_article(self, news: object) -> None:
        """Fetch article body and add to buffer."""
        article_id = getattr(news, "articleId", "")
        provider = getattr(news, "providerCode", "")
        headline_raw = getattr(news, "headline", "")
        timestamp = getattr(news, "time", "")

        # Parse IBKR headline metadata
        parsed = _parse_headline(headline_raw)

        # Determine the tickers this article relates to
        extra_data = getattr(news, "extraData", "")
        # extraData contains the symbol; also check headline for ticker matches
        tickers_str = extra_data if extra_data else ""

        # Filter: only keep articles for our configured symbols
        article_tickers = tickers_str.lower()
        matching_symbols = [
            s for s in self._symbols_lower if s in article_tickers
        ]
        if not matching_symbols and tickers_str:
            # No match on our symbol list — skip
            return
        if not matching_symbols:
            # If we can't determine the symbol, store under the first configured symbol
            matching_symbols = [self._cfg.streaming.news_symbols[0].lower()]

        # Fetch article body
        body = ""
        if provider and article_id:
            body = await self._fetch_body(provider, article_id)

        if timestamp:
            _ts = pd.Timestamp(timestamp)
            ts = _ts if _ts.tzinfo is not None else _ts.tz_localize("UTC")
        else:
            ts = pd.Timestamp.now(tz="UTC")

        for sym in matching_symbols:
            record = {
                "article_id": article_id,
                "published_utc": ts,
                "provider": provider,
                "headline": parsed["headline"],
                "body": body,
                "sentiment_score": parsed["sentiment_score"],
                "confidence": parsed["confidence"],
                "language": parsed["language"],
                "tickers": sym,
                "source": "ibkr_stream",
            }
            async with self._lock:
                self._buffer.append(record)

    async def _fetch_body(self, provider_code: str, article_id: str) -> str:
        """Fetch a single article body with concurrency limit."""
        async with self._body_sem:
            ib = self._client.ib
            prev_timeout = ib.RequestTimeout
            ib.RequestTimeout = 30
            try:
                art = await ib.reqNewsArticleAsync(provider_code, article_id)
                if art and art.articleText:
                    return art.articleText
            except Exception as exc:
                log.debug("Body fetch failed for %s/%s: %s", provider_code, article_id, exc)
            finally:
                ib.RequestTimeout = prev_timeout
        return ""

    async def _flush_loop(self) -> None:
        """Periodically flush buffered articles to Parquet."""
        interval = self._cfg.streaming.flush_interval_sec
        while self._running:
            await asyncio.sleep(interval)
            try:
                await self._flush_all()
            except Exception:
                log.exception("Error during news flush")

    async def _flush_all(self) -> None:
        """Flush buffered articles to Parquet partitions."""
        async with self._lock:
            if not self._buffer:
                return
            records = list(self._buffer)
            self._buffer.clear()

        df = pd.DataFrame(records)
        if df.empty:
            return

        df["published_utc"] = pd.to_datetime(df["published_utc"], utc=True)
        df = df.drop_duplicates(subset=["article_id"], keep="last")

        # Group by (symbol, date) and write each partition
        df["_date"] = df["published_utc"].dt.date
        for (sym, date_val), group in df.groupby(["tickers", "_date"]):
            date_str = date_val.isoformat()
            path = news_partition_path(
                self._cfg.parquet_root, str(sym), date_str,
            )

            write_df = group.drop(columns=["_date"])

            # Merge with existing partition if present
            if path.exists():
                existing = pd.read_parquet(str(path))
                combined = pd.concat([existing, write_df], ignore_index=True)
                combined = combined.drop_duplicates(subset=["article_id"], keep="last")
                combined = combined.sort_values("published_utc").reset_index(drop=True)
                write_df = combined

            path.parent.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pandas(write_df, schema=NEWS_SCHEMA, preserve_index=False)
            pq.write_table(table, str(path), compression="zstd")
            log.debug("Flushed %d news articles for %s %s", len(write_df), sym, date_str)

    async def _resubscribe(self) -> None:
        """Re-subscribe to news after reconnection."""
        try:
            self._client.cancel_news(self._tickers)
        except Exception:
            pass
        self._tickers = await self._client.subscribe_news(
            self._cfg.streaming.news_symbols,
        )
        self._client.ib.tickNewsEvent += self._on_news
        log.info("Re-subscribed to news after reconnect")
