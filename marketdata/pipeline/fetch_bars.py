"""Main fetch orchestrator for IBKR historical bars.

Iterates trading-calendar days, generates chunk plans, fetches with
pacing compliance, stitches chunks, runs quality checks, writes
Parquet partitions, and records metadata in DuckDB.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd
from rich.progress import BarColumn, Progress, TextColumn, TimeRemainingColumn

from marketdata.config import PipelineConfig
from marketdata.db.duck import MetadataDB
from marketdata.ibkr.client import IBKRClient
from marketdata.ibkr.contracts import get_bar_contract
from marketdata.pipeline.chunking import plan_chunks
from marketdata.pipeline.quality import Gap, validate_partition
from marketdata.pipeline.storage import _normalize_bar_label, bars_partition_path, write_partition
from marketdata.utils.log import get_console, get_logger
from marketdata.utils.time import get_trading_days, is_within_availability, rth_boundaries

log = get_logger(__name__)


async def fetch_bars(
    client: IBKRClient,
    db: MetadataDB,
    cfg: PipelineConfig,
    symbols: list[str],
    bar_sizes: list[str],
    start_date: dt.date,
    end_date: dt.date,
    rth: bool = True,
) -> dict[str, int]:
    """Fetch historical bars for symbols x bar_sizes x trading days.

    Returns a dict of ``{symbol_barsize: rows_fetched}``.
    """
    console = get_console()
    trading_days = get_trading_days(start_date, end_date)
    results: dict[str, int] = {}

    for symbol in symbols:
        contract = get_bar_contract(symbol)

        for bar_size in bar_sizes:
            bar_key = _normalize_bar_label(bar_size)
            result_key = f"{symbol}_{bar_key}"
            results[result_key] = 0

            # Already-fetched dates
            done_dates = db.get_ingested_dates(symbol, bar_key, status="ok")

            # Filter days we still need
            pending_days = [d for d in trading_days if d not in done_dates]

            if not pending_days:
                log.info(
                    "[%s %s] All %d days already ingested — skipping",
                    symbol,
                    bar_key,
                    len(trading_days),
                )
                continue

            log.info(
                "[%s %s] %d/%d days to fetch (start=%s end=%s)",
                symbol,
                bar_key,
                len(pending_days),
                len(trading_days),
                pending_days[0],
                pending_days[-1],
            )

            with Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                TextColumn("{task.completed}/{task.total}"),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                task = progress.add_task(f"{symbol} {bar_key}", total=len(pending_days))

                for day in pending_days:
                    rows = await _fetch_day(
                        client,
                        db,
                        cfg,
                        contract,
                        symbol,
                        bar_key,
                        bar_size,
                        day,
                        rth,
                    )
                    results[result_key] += rows
                    progress.advance(task)

    return results


async def _fetch_day(
    client: IBKRClient,
    db: MetadataDB,
    cfg: PipelineConfig,
    contract,
    symbol: str,
    bar_key: str,
    bar_size: str,
    day: dt.date,
    rth: bool,
) -> int:
    """Fetch and store a single day of data. Returns row count."""
    rth_start, rth_end = rth_boundaries(day, cfg.session_start, cfg.session_end, cfg.timezone)
    chunks = plan_chunks(day, bar_size, cfg.session_start, cfg.session_end, cfg.timezone)

    all_frames: list[pd.DataFrame] = []

    for chunk in chunks:
        try:
            bars = await client.fetch_historical(
                contract=contract,
                end_dt=chunk.end_dt_ib,
                duration=chunk.duration_str,
                bar_size=chunk.bar_size_setting,
                use_rth=rth,
            )
            df = IBKRClient.bars_to_df(bars)
            if not df.empty:
                all_frames.append(df)
        except RuntimeError as exc:
            # Exhausted retries — pacing violation
            log.warning(
                "[%s %s %s] Pacing exhausted for chunk %s: %s",
                symbol,
                bar_key,
                day,
                chunk.label,
                exc,
            )
        except Exception as exc:
            msg = str(exc).lower()
            if "no data" in msg or "no historical data" in msg or "no market data" in msg:
                # 5-sec older-than-6mo or genuinely no data
                if not is_within_availability(day, bar_size, cfg.five_sec_availability_months):
                    log.info(
                        "[%s %s %s] Beyond availability window",
                        symbol,
                        bar_key,
                        day,
                    )
                    db.upsert_ingestion(
                        symbol,
                        bar_key,
                        day,
                        rth_start,
                        rth_end,
                        0,
                        status="unavailable",
                        error_msg="beyond_ibkr_limit",
                    )
                    db.insert_gaps(
                        symbol, bar_key, day, [Gap(rth_start, rth_end, "unavailable_by_ibkr_limit")]
                    )
                    return 0
                log.warning("[%s %s %s] No data returned: %s", symbol, bar_key, day, exc)
            else:
                log.error("[%s %s %s] Unexpected error: %s", symbol, bar_key, day, exc)
                db.upsert_ingestion(
                    symbol,
                    bar_key,
                    day,
                    rth_start,
                    rth_end,
                    0,
                    status="error",
                    error_msg=str(exc)[:500],
                )
                return 0

    if not all_frames:
        # No data at all for this day
        beyond = not is_within_availability(
            day,
            bar_size,
            cfg.five_sec_availability_months,
        )
        gap_type = "unavailable_by_ibkr_limit" if beyond else "missing_data"
        db.upsert_ingestion(symbol, bar_key, day, rth_start, rth_end, 0, status="empty")
        db.insert_gaps(symbol, bar_key, day, [Gap(rth_start, rth_end, gap_type)])
        return 0

    # Stitch chunks
    combined = pd.concat(all_frames, ignore_index=True)

    # Quality: sort, dedup, gap detection
    cleaned, gaps = validate_partition(combined, bar_size, rth_start, rth_end, day)

    # Write Parquet
    date_str = day.isoformat()
    path = bars_partition_path(cfg.parquet_root, symbol, bar_key, date_str)
    checksum = write_partition(cleaned, path)

    # Metadata
    start_ts = cleaned["ts_utc"].min()
    end_ts = cleaned["ts_utc"].max()
    db.upsert_ingestion(
        symbol,
        bar_key,
        day,
        start_ts,
        end_ts,
        rows=len(cleaned),
        status="ok",
        checksum=checksum,
    )
    if gaps:
        db.insert_gaps(symbol, bar_key, day, gaps)

    return len(cleaned)
