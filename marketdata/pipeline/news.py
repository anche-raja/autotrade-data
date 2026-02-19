"""IBKR news fetcher and storage.

Fetches news headlines and article bodies through Interactive Brokers' API
(which provides Benzinga, Dow Jones, Briefing.com, The Fly, etc. depending
on your subscription).

IBKR API calls used:
  - reqNewsProviders()      -- list available providers
  - reqHistoricalNews()     -- fetch headlines for a contract + date range
  - reqNewsArticle()        -- fetch full article body by articleId
"""

from __future__ import annotations

import asyncio
import datetime as dt
import re
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rich.progress import BarColumn, Progress, TextColumn, TimeRemainingColumn

from marketdata.config import PipelineConfig
from marketdata.utils.log import get_console, get_logger

log = get_logger(__name__)

NEWS_SCHEMA = pa.schema(
    [
        pa.field("article_id", pa.string()),
        pa.field("published_utc", pa.timestamp("us", tz="UTC")),
        pa.field("provider", pa.string()),
        pa.field("headline", pa.string()),
        pa.field("body", pa.string()),
        pa.field("sentiment_score", pa.float64()),
        pa.field("confidence", pa.float64()),
        pa.field("language", pa.string()),
        pa.field("tickers", pa.string()),
        pa.field("source", pa.string()),
    ]
)

# Regex to parse the IBKR headline metadata prefix:
# {A:conId:L:lang:K:sentiment:C:confidence}headline text
_META_RE = re.compile(
    r"\{A:(?P<conid>[^:]*):L:(?P<lang>[^:]*)"
    r":K:(?P<sent>[^:]*):C:(?P<conf>[^}]*)\}"
    r"(?P<text>.*)",
    re.DOTALL,
)


def news_partition_path(root: Path, symbol: str, date: str) -> Path:
    """Hive-style path for a news partition."""
    return root / f"dataset=news/symbol={symbol}/date={date}/part.parquet"


def _parse_headline(raw: str) -> dict[str, Any]:
    """Parse IBKR headline metadata and return structured fields."""
    m = _META_RE.match(raw)
    if m:
        sent_str = m.group("sent")
        conf_str = m.group("conf")
        return {
            "language": m.group("lang"),
            "sentiment_score": _safe_float(sent_str),
            "confidence": _safe_float(conf_str),
            "headline": m.group("text").strip(),
        }
    return {
        "language": "",
        "sentiment_score": None,
        "confidence": None,
        "headline": raw.strip(),
    }


def _safe_float(s: str) -> float | None:
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


class IBKRNewsProvider:
    """Fetches news through IBKR's API (Benzinga, DJ, Fly, etc.).

    Uses concurrent body fetching (semaphore-limited) for ~10x speedup
    over sequential fetching.
    """

    BODY_CONCURRENCY = 10  # max simultaneous reqNewsArticle calls

    def __init__(self, client: Any, cfg: PipelineConfig) -> None:
        self._client = client
        self._cfg = cfg
        self._body_sem = asyncio.Semaphore(self.BODY_CONCURRENCY)

    async def list_providers(self) -> list[dict[str, str]]:
        """Return available news providers from IBKR."""
        providers = await self._client.ib.reqNewsProvidersAsync()
        return [{"code": p.code, "name": p.name} for p in providers]

    async def fetch_news(
        self,
        con_id: int,
        symbol: str,
        providers: str,
        start_date: dt.date,
        end_date: dt.date,
        db: Any,
        fetch_bodies: bool = True,
        max_per_day: int = 300,
    ) -> dict[str, int]:
        """Fetch news headlines day-by-day with concurrent body fetching.

        Parameters
        ----------
        con_id : int
            IB contract ID for the instrument (e.g. SPY=756733).
        symbol : str
            Symbol label for storage (e.g. "SPY").
        providers : str
            Plus-separated provider codes (e.g. "BZ+DJ-N+FLY").
        start_date, end_date : date
            Inclusive date range.
        db : MetadataDB
            For tracking ingestion.
        fetch_bodies : bool
            Whether to also fetch full article text.
        max_per_day : int
            Max headlines to request per day.

        Returns
        -------
        dict with total article count.
        """
        console = get_console()

        all_days: list[dt.date] = []
        cursor = start_date
        while cursor <= end_date:
            all_days.append(cursor)
            cursor += dt.timedelta(days=1)

        done_dates = db.get_news_ingested_dates(source=f"ibkr_{symbol}")
        pending_days = [d for d in all_days if d not in done_dates]

        if not pending_days:
            log.info(
                "[%s news] All %d days already ingested",
                symbol,
                len(all_days),
            )
            return {"news": 0}

        log.info(
            "[%s news] %d/%d days to fetch, providers=%s",
            symbol,
            len(pending_days),
            len(all_days),
            providers,
        )

        total = 0

        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"{symbol} news",
                total=len(pending_days),
            )

            for day in pending_days:
                count = await self._fetch_day(
                    con_id,
                    symbol,
                    providers,
                    day,
                    db,
                    fetch_bodies,
                    max_per_day,
                )
                total += count
                progress.advance(task)

        return {"news": total}

    async def _fetch_body(self, provider_code: str, article_id: str) -> str:
        """Fetch a single article body, respecting the concurrency limit."""
        async with self._body_sem:
            ib = self._client.ib
            prev_timeout = ib.RequestTimeout
            ib.RequestTimeout = 30
            try:
                art = await ib.reqNewsArticleAsync(
                    provider_code,
                    article_id,
                )
                if art and art.articleText:
                    return art.articleText
            except Exception:
                pass
            finally:
                ib.RequestTimeout = prev_timeout
            return ""

    async def _req_headlines_one_provider(
        self,
        con_id: int,
        provider: str,
        start_dt: str,
        end_dt: str,
        max_results: int,
        retries: int = 2,
    ) -> list[Any]:
        """Fetch headlines for a single provider with retry on None/timeout."""
        ib = self._client.ib
        for attempt in range(1, retries + 1):
            prev_timeout = ib.RequestTimeout
            ib.RequestTimeout = 60
            try:
                result = await ib.reqHistoricalNewsAsync(
                    conId=con_id,
                    providerCodes=provider,
                    startDateTime=start_dt,
                    endDateTime=end_dt,
                    totalResults=max_results,
                )
                if result is not None:
                    return list(result)
                # None = IBKR-side timeout, retry
                log.debug(
                    "%s headlines returned None (attempt %d/%d)",
                    provider,
                    attempt,
                    retries,
                )
            except Exception as exc:
                log.debug(
                    "%s headlines error (attempt %d/%d): %s",
                    provider,
                    attempt,
                    retries,
                    exc,
                )
            finally:
                ib.RequestTimeout = prev_timeout
            await asyncio.sleep(1.0 * attempt)
        return []

    async def _req_headlines_all(
        self,
        con_id: int,
        providers: str,
        start_dt: str,
        end_dt: str,
        max_results: int,
    ) -> list[Any]:
        """Query each provider individually, merge and de-duplicate.

        Individual provider queries are far more reliable than a combined
        multi-provider request, which frequently times out on IBKR.
        """
        provider_list = providers.split("+")
        all_headlines: list[Any] = []
        seen_ids: set[str] = set()

        for prov in provider_list:
            results = await self._req_headlines_one_provider(
                con_id,
                prov,
                start_dt,
                end_dt,
                max_results,
            )
            for h in results:
                if h.articleId not in seen_ids:
                    seen_ids.add(h.articleId)
                    all_headlines.append(h)
            if results:
                log.debug(
                    "  %s: %d headlines",
                    prov,
                    len(results),
                )

        return all_headlines

    async def _fetch_day(
        self,
        con_id: int,
        symbol: str,
        providers: str,
        day: dt.date,
        db: Any,
        fetch_bodies: bool,
        max_per_day: int,
    ) -> int:
        """Fetch headlines + bodies for a single day."""
        start_dt = f"{day.isoformat()} 00:00:00"
        end_dt = f"{(day + dt.timedelta(days=1)).isoformat()} 00:00:00"

        try:
            headlines = await self._req_headlines_all(
                con_id,
                providers,
                start_dt,
                end_dt,
                max_per_day,
            )
        except Exception as exc:
            log.warning("[%s news %s] Headlines failed: %s", symbol, day, exc)
            db.upsert_news_ingestion(
                f"ibkr_{symbol}",
                day,
                0,
                "error",
                str(exc)[:500],
            )
            return 0

        if not headlines:
            db.upsert_news_ingestion(f"ibkr_{symbol}", day, 0, "empty")
            return 0

        # Parse all headlines first
        parsed_headlines = []
        for h in headlines:
            parsed = _parse_headline(h.headline)
            parsed_headlines.append((h, parsed))

        # Fetch all bodies concurrently (10 at a time via semaphore)
        bodies: list[str] = [""] * len(parsed_headlines)
        if fetch_bodies:
            body_tasks = [
                self._fetch_body(h.providerCode, h.articleId) for h, _ in parsed_headlines
            ]
            bodies = await asyncio.gather(*body_tasks)

        # Build records
        records: list[dict] = []
        for i, (h, parsed) in enumerate(parsed_headlines):
            records.append(
                {
                    "article_id": h.articleId,
                    "published_utc": pd.Timestamp(h.time, tz="UTC"),
                    "provider": h.providerCode,
                    "headline": parsed["headline"],
                    "body": bodies[i],
                    "sentiment_score": parsed["sentiment_score"],
                    "confidence": parsed["confidence"],
                    "language": parsed["language"],
                    "tickers": symbol,
                    "source": "ibkr",
                }
            )

        df = pd.DataFrame(records)
        df["published_utc"] = pd.to_datetime(df["published_utc"], utc=True)
        df = df.drop_duplicates(subset=["article_id"], keep="last")

        path = news_partition_path(
            self._cfg.parquet_root,
            symbol.lower(),
            day.isoformat(),
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(
            df,
            schema=NEWS_SCHEMA,
            preserve_index=False,
        )
        pq.write_table(table, str(path), compression="zstd")

        db.upsert_news_ingestion(f"ibkr_{symbol}", day, len(df), "ok")
        log.info(
            "[%s news %s] %d articles stored (%d bodies)",
            symbol,
            day,
            len(df),
            sum(1 for b in bodies if b),
        )

        return len(df)


async def resolve_con_id(client: Any, symbol: str) -> int:
    """Resolve a symbol to its IBKR conId."""
    from ib_async import Stock

    contract = Stock(symbol, "SMART", "USD")
    details = await client.ib.reqContractDetailsAsync(contract)
    if details:
        return details[0].contract.conId
    raise ValueError(f"Could not resolve conId for {symbol}")
