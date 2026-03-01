"""Finnhub events calendar: earnings, economic, IPO, dividend, split data.

Fetches calendar data from Finnhub's free API (60 calls/min) and stores
as Hive-partitioned Parquet under ``dataset=events/date=YYYY-MM-DD/``.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
from collections import defaultdict
from typing import Any

import httpx
import pandas as pd

from marketdata.config import PipelineConfig
from marketdata.pipeline.storage import (
    EVENTS_SCHEMA,
    events_partition_path,
    merge_partition,
)
from marketdata.utils.log import get_logger

log = get_logger(__name__)

FINNHUB_BASE = "https://finnhub.io/api/v1"


class FinnhubEventsProvider:
    """Fetch and store calendar events from Finnhub API."""

    def __init__(self, cfg: PipelineConfig) -> None:
        self._cfg = cfg
        self._api_key = cfg.events.finnhub_api_key
        self._rate_delay = cfg.events.rate_limit_delay_sec
        if not self._api_key:
            raise ValueError(
                "Finnhub API key not configured. "
                "Set MD_EVENTS__FINNHUB_API_KEY or events.finnhub_api_key in config."
            )

    async def fetch_all(
        self,
        start: dt.date | None = None,
        end: dt.date | None = None,
    ) -> dict[str, int]:
        """Fetch all event types and store as Parquet. Returns counts per type."""
        today = dt.date.today()
        if start is None:
            start = today - dt.timedelta(days=self._cfg.events.lookback_days)
        if end is None:
            end = today + dt.timedelta(days=self._cfg.events.lookahead_days)

        start_str = start.isoformat()
        end_str = end.isoformat()
        now = dt.datetime.now(dt.timezone.utc)
        counts: dict[str, int] = {}

        async with httpx.AsyncClient(timeout=30.0) as client:
            # 1. Earnings calendar
            earnings = await self._fetch_earnings(client, start_str, end_str, now)
            counts["earnings"] = len(earnings)
            await asyncio.sleep(self._rate_delay)

            # 2. Economic calendar
            economic = await self._fetch_economic(client, start_str, end_str, now)
            counts["economic"] = len(economic)
            await asyncio.sleep(self._rate_delay)

            # 3. IPO calendar
            ipos = await self._fetch_ipos(client, start_str, end_str, now)
            counts["ipo"] = len(ipos)
            await asyncio.sleep(self._rate_delay)

            # 4. Dividends (per-symbol)
            all_dividends: list[dict[str, Any]] = []
            for sym in self._cfg.events.dividend_symbols:
                divs = await self._fetch_dividends(client, sym, start_str, end_str, now)
                all_dividends.extend(divs)
                await asyncio.sleep(self._rate_delay)
            counts["dividend"] = len(all_dividends)

            # 5. Splits (per-symbol)
            all_splits: list[dict[str, Any]] = []
            for sym in self._cfg.events.dividend_symbols:
                splits = await self._fetch_splits(client, sym, start_str, end_str, now)
                all_splits.extend(splits)
                await asyncio.sleep(self._rate_delay)
            counts["split"] = len(all_splits)

        # Combine and write
        all_records = earnings + economic + ipos + all_dividends + all_splits
        if all_records:
            self._write_partitions(all_records)

        log.info("Events fetch complete: %s", counts)
        return counts

    # ── API helpers ──

    async def _api_get(
        self,
        client: httpx.AsyncClient,
        path: str,
        params: dict[str, Any],
    ) -> Any:
        """GET a Finnhub endpoint with API key, return parsed JSON."""
        params["token"] = self._api_key
        url = f"{FINNHUB_BASE}{path}"
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            log.error("Finnhub API error %s for %s: %s", e.response.status_code, path, e)
            return None
        except Exception as e:
            log.error("Finnhub request failed for %s: %s", path, e)
            return None

    # ── Earnings ──

    async def _fetch_earnings(
        self,
        client: httpx.AsyncClient,
        start: str,
        end: str,
        now: dt.datetime,
    ) -> list[dict[str, Any]]:
        data = await self._api_get(client, "/calendar/earnings", {"from": start, "to": end})
        if not data:
            return []
        entries = data.get("earningsCalendar", [])
        log.info("Fetched %d earnings entries", len(entries))
        records = []
        for e in entries:
            date = e.get("date", "")
            symbol = e.get("symbol", "")
            if not date or not symbol:
                continue
            records.append(
                {
                    "event_id": f"earnings-{symbol}-{date}",
                    "event_type": "earnings",
                    "event_date": date,
                    "event_time": e.get("hour", ""),
                    "symbol": symbol,
                    "title": f"{symbol} Q{e.get('quarter', '?')} {e.get('year', '')} Earnings",
                    "country": "US",
                    "impact": "high",
                    "estimate": _safe_float(e.get("epsEstimate")),
                    "actual": _safe_float(e.get("epsActual")),
                    "previous": None,
                    "currency": "USD",
                    "unit": "",
                    "details_json": json.dumps(
                        {
                            "revenueEstimate": e.get("revenueEstimate"),
                            "revenueActual": e.get("revenueActual"),
                            "quarter": e.get("quarter"),
                            "year": e.get("year"),
                        }
                    ),
                    "source": "finnhub",
                    "fetched_at": now,
                }
            )
        return records

    # ── Economic ──

    async def _fetch_economic(
        self,
        client: httpx.AsyncClient,
        start: str,
        end: str,
        now: dt.datetime,
    ) -> list[dict[str, Any]]:
        data = await self._api_get(client, "/calendar/economic", {"from": start, "to": end})
        if not data:
            return []
        entries = data.get("economicCalendar", [])
        log.info("Fetched %d economic entries", len(entries))
        records = []
        for e in entries:
            event_name = e.get("event", "")
            date = e.get("time", "")[:10] if e.get("time") else ""
            time_str = e.get("time", "")[11:16] if e.get("time") else ""
            if not date:
                continue
            # Synthetic ID from event name + date + time
            id_source = f"{event_name}-{date}-{time_str}"
            event_id = f"econ-{hashlib.md5(id_source.encode()).hexdigest()[:12]}"
            impact_val = e.get("impact", "")
            impact_map = {1: "low", 2: "medium", 3: "high"}
            impact = impact_map.get(impact_val, str(impact_val) if impact_val else "")
            records.append(
                {
                    "event_id": event_id,
                    "event_type": "economic",
                    "event_date": date,
                    "event_time": time_str,
                    "symbol": "",
                    "title": event_name,
                    "country": e.get("country", ""),
                    "impact": impact,
                    "estimate": _safe_float(e.get("estimate")),
                    "actual": _safe_float(e.get("actual")),
                    "previous": _safe_float(e.get("prev")),
                    "currency": e.get("currency", ""),
                    "unit": e.get("unit", ""),
                    "details_json": "{}",
                    "source": "finnhub",
                    "fetched_at": now,
                }
            )
        return records

    # ── IPO ──

    async def _fetch_ipos(
        self,
        client: httpx.AsyncClient,
        start: str,
        end: str,
        now: dt.datetime,
    ) -> list[dict[str, Any]]:
        data = await self._api_get(client, "/calendar/ipo", {"from": start, "to": end})
        if not data:
            return []
        entries = data.get("ipoCalendar", [])
        log.info("Fetched %d IPO entries", len(entries))
        records = []
        for e in entries:
            date = e.get("date", "")
            symbol = e.get("symbol", "")
            name = e.get("name", symbol)
            if not date:
                continue
            records.append(
                {
                    "event_id": f"ipo-{symbol or name[:20]}-{date}",
                    "event_type": "ipo",
                    "event_date": date,
                    "event_time": "",
                    "symbol": symbol,
                    "title": f"{name} IPO",
                    "country": "US",
                    "impact": "",
                    "estimate": _safe_float(e.get("price")),
                    "actual": None,
                    "previous": None,
                    "currency": "USD",
                    "unit": "",
                    "details_json": json.dumps(
                        {
                            "exchange": e.get("exchange", ""),
                            "numberOfShares": e.get("numberOfShares"),
                            "totalSharesValue": e.get("totalSharesValue"),
                            "status": e.get("status", ""),
                        }
                    ),
                    "source": "finnhub",
                    "fetched_at": now,
                }
            )
        return records

    # ── Dividends ──

    async def _fetch_dividends(
        self,
        client: httpx.AsyncClient,
        symbol: str,
        start: str,
        end: str,
        now: dt.datetime,
    ) -> list[dict[str, Any]]:
        data = await self._api_get(
            client, "/stock/dividend", {"symbol": symbol, "from": start, "to": end}
        )
        if not data or not isinstance(data, list):
            return []
        records = []
        for e in data:
            date = e.get("exDate", e.get("date", ""))
            amount = _safe_float(e.get("amount"))
            if not date:
                continue
            records.append(
                {
                    "event_id": f"div-{symbol}-{date}-{amount}",
                    "event_type": "dividend",
                    "event_date": date,
                    "event_time": "",
                    "symbol": symbol,
                    "title": f"{symbol} Dividend",
                    "country": "US",
                    "impact": "",
                    "estimate": None,
                    "actual": amount,
                    "previous": None,
                    "currency": e.get("currency", "USD"),
                    "unit": "",
                    "details_json": json.dumps(
                        {
                            "payDate": e.get("payDate", ""),
                            "recordDate": e.get("recordDate", ""),
                            "declarationDate": e.get("declarationDate", ""),
                            "adjDividend": e.get("adjustedAmount"),
                            "frequency": e.get("freq", ""),
                        }
                    ),
                    "source": "finnhub",
                    "fetched_at": now,
                }
            )
        return records

    # ── Splits ──

    async def _fetch_splits(
        self,
        client: httpx.AsyncClient,
        symbol: str,
        start: str,
        end: str,
        now: dt.datetime,
    ) -> list[dict[str, Any]]:
        data = await self._api_get(
            client, "/stock/split", {"symbol": symbol, "from": start, "to": end}
        )
        if not data or not isinstance(data, list):
            return []
        records = []
        for e in data:
            date = e.get("date", "")
            if not date:
                continue
            from_factor = e.get("fromFactor", 1)
            to_factor = e.get("toFactor", 1)
            records.append(
                {
                    "event_id": f"split-{symbol}-{date}",
                    "event_type": "split",
                    "event_date": date,
                    "event_time": "",
                    "symbol": symbol,
                    "title": f"{symbol} {to_factor}:{from_factor} Stock Split",
                    "country": "US",
                    "impact": "",
                    "estimate": None,
                    "actual": None,
                    "previous": None,
                    "currency": "",
                    "unit": "",
                    "details_json": json.dumps(
                        {"fromFactor": from_factor, "toFactor": to_factor}
                    ),
                    "source": "finnhub",
                    "fetched_at": now,
                }
            )
        return records

    # ── Write ──

    def _write_partitions(self, records: list[dict[str, Any]]) -> None:
        """Group records by event_date and merge into Parquet partitions."""
        by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in records:
            by_date[r["event_date"]].append(r)

        root = self._cfg.parquet_root
        for date, date_records in by_date.items():
            df = pd.DataFrame(date_records)
            path = events_partition_path(root, date)
            merge_partition(df, path, schema=EVENTS_SCHEMA, dedup_col="event_id")
            log.info("Wrote %d events to %s", len(date_records), path)


def _safe_float(val: Any) -> float | None:
    """Convert a value to float, returning None for non-numeric."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
