"""NYSE Market Breadth: probe, historical fetch, and live collector.

Attempts to discover which IBKR breadth-related contracts actually return
data (live snapshot and/or historical bars).  Falls back gracefully if
nothing works — the rest of the pipeline still succeeds.
"""

from __future__ import annotations

import asyncio
import datetime as dt

import pandas as pd
from rich.table import Table

from marketdata.config import PipelineConfig
from marketdata.db.duck import MetadataDB
from marketdata.ibkr.client import IBKRClient
from marketdata.ibkr.contracts import BREADTH_CANDIDATES, breadth_contract_by_name
from marketdata.pipeline.storage import BREADTH_SCHEMA, breadth_partition_path, write_partition
from marketdata.utils.log import get_console, get_logger
from marketdata.utils.time import rth_boundaries, to_ib_datetime

log = get_logger(__name__)


class BreadthProvider:
    """Probe, collect, and store NYSE breadth data from IBKR."""

    def __init__(self, client: IBKRClient, db: MetadataDB, cfg: PipelineConfig) -> None:
        self._client = client
        self._db = db
        self._cfg = cfg

    # ------------------------------------------------------------------
    # Probe
    # ------------------------------------------------------------------

    async def probe(self) -> list[dict]:
        """Probe all breadth candidates for live + historical data availability.

        Results are stored in ``breadth_probe_results`` and also returned.
        """
        console = get_console()
        results: list[dict] = []

        table = Table(title="Breadth Probe Results")
        table.add_column("Name")
        table.add_column("Contract")
        table.add_column("Live?")
        table.add_column("Hist?")
        table.add_column("Notes")

        for name, contract in BREADTH_CANDIDATES:
            log.info("Probing %s (%s)…", name, contract)
            worked_live = False
            worked_hist = False
            notes_parts: list[str] = []

            # Live snapshot
            try:
                snap = await self._client.snapshot(contract, timeout=5.0)
                if snap:
                    worked_live = True
                    notes_parts.append(f"live_fields={list(snap.keys())}")
                else:
                    notes_parts.append("live=empty")
            except Exception as exc:
                notes_parts.append(f"live_err={str(exc)[:80]}")

            # Short historical request
            try:
                bars = await self._client.fetch_historical(
                    contract=contract,
                    end_dt="",
                    duration="1 D",
                    bar_size="1 min",
                    what_to_show="TRADES",
                    use_rth=True,
                )
                if bars:
                    worked_hist = True
                    notes_parts.append(f"hist_bars={len(bars)}")
                else:
                    notes_parts.append("hist=empty")
            except Exception as exc:
                notes_parts.append(f"hist_err={str(exc)[:80]}")

            notes = "; ".join(notes_parts)
            contract_desc = f"{contract.symbol}@{getattr(contract, 'exchange', '?')}"

            self._db.insert_probe_result(name, contract_desc, worked_live, worked_hist, notes)

            result = {
                "name": name,
                "contract_desc": contract_desc,
                "worked_live": worked_live,
                "worked_hist": worked_hist,
                "notes": notes,
            }
            results.append(result)

            live_mark = "[green]YES[/green]" if worked_live else "[red]NO[/red]"
            hist_mark = "[green]YES[/green]" if worked_hist else "[red]NO[/red]"
            table.add_row(name, contract_desc, live_mark, hist_mark, notes)

        console.print(table)

        if not any(r["worked_live"] or r["worked_hist"] for r in results):
            console.print(
                "[bold yellow]No breadth contracts returned data. "
                "Breadth is marked unavailable — bar pipeline unaffected.[/bold yellow]"
            )

        return results

    # ------------------------------------------------------------------
    # Live collector
    # ------------------------------------------------------------------

    async def collect(
        self,
        name: str,
        interval_sec: float = 5.0,
        duration_min: float = 60.0,
    ) -> int:
        """Subscribe to a breadth contract's live data and sample at fixed intervals.

        Writes samples to Parquet when collection ends.
        Returns number of samples collected.
        """
        contract = breadth_contract_by_name(name)
        if contract is None:
            log.error("Unknown breadth contract name: %s", name)
            return 0

        log.info(
            "Starting live collection for %s: interval=%ss, duration=%smin",
            name,
            interval_sec,
            duration_min,
        )

        ib = self._client.ib
        ticker = ib.reqMktData(contract)

        samples: list[dict] = []
        loop = asyncio.get_running_loop()
        end_time = loop.time() + duration_min * 60

        try:
            while loop.time() < end_time:
                await asyncio.sleep(interval_sec)
                now_utc = dt.datetime.now(dt.timezone.utc)

                # Try to get a meaningful value
                value = None
                for attr in ("last", "close", "bid", "ask"):
                    v = getattr(ticker, attr, None)
                    if v is not None and v == v:  # not NaN
                        value = float(v)
                        break

                if value is not None:
                    samples.append(
                        {
                            "ts_utc": now_utc,
                            "value": value,
                            "source": "ibkr",
                            "quality_flags": "live",
                        }
                    )
        finally:
            ib.cancelMktData(contract)

        if not samples:
            log.warning("No samples collected for %s", name)
            return 0

        df = pd.DataFrame(samples)
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)

        # Group by date and write partitions
        df["_date"] = df["ts_utc"].dt.date
        for date_val, group in df.groupby("_date"):
            date_str = date_val.isoformat()
            breadth_name = _breadth_storage_name(name)
            path = breadth_partition_path(self._cfg.parquet_root, breadth_name, date_str)
            write_partition(group.drop(columns=["_date"]), path, schema=BREADTH_SCHEMA)

        log.info("Collected %d samples for %s", len(samples), name)
        return len(samples)

    # ------------------------------------------------------------------
    # Historical fetch (if probe showed hist=True)
    # ------------------------------------------------------------------

    async def fetch_historical(
        self,
        name: str,
        day: dt.date,
    ) -> int:
        """Fetch 1-day of historical breadth data and store. Returns row count."""
        contract = breadth_contract_by_name(name)
        if contract is None:
            log.error("Unknown breadth contract: %s", name)
            return 0

        rth_start, rth_end = rth_boundaries(
            day,
            self._cfg.session_start,
            self._cfg.session_end,
            self._cfg.timezone,
        )

        try:
            bars = await self._client.fetch_historical(
                contract=contract,
                end_dt=to_ib_datetime(rth_end),
                duration="1 D",
                bar_size="1 min",
                what_to_show="TRADES",
                use_rth=True,
            )
        except Exception as exc:
            log.warning("Historical fetch for %s on %s failed: %s", name, day, exc)
            return 0

        if not bars:
            return 0

        df = IBKRClient.bars_to_df(bars)
        if df.empty:
            return 0

        # For breadth, store close as the "value"
        breadth_df = pd.DataFrame(
            {
                "ts_utc": df["ts_utc"],
                "value": df["close"],
                "source": "ibkr",
                "quality_flags": "ok",
            }
        )

        breadth_name = _breadth_storage_name(name)
        date_str = day.isoformat()
        path = breadth_partition_path(self._cfg.parquet_root, breadth_name, date_str)
        write_partition(breadth_df, path, schema=BREADTH_SCHEMA)

        return len(breadth_df)


def _breadth_storage_name(name: str) -> str:
    """Convert display name to storage partition name.

    e.g. ``"AD-NYSE"`` -> ``"nyse_adv"``, ``"TICK-NYSE"`` -> ``"nyse_tick"``.
    """
    mapping = {
        "AD-NYSE": "nyse_adv",
        "AD-NASD": "nasd_adv",
        "TICK-NYSE": "nyse_tick",
        "TRIN-NYSE": "nyse_trin",
        "ADVN": "nyse_advn",
        "DECN": "nyse_decn",
    }
    return mapping.get(name.upper(), name.lower().replace("-", "_"))
