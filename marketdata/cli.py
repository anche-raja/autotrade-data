"""Typer CLI for the IBKR market-data pipeline.

Commands:
  fetch-bars       Fetch historical bars from IBKR
  probe-breadth    Probe breadth contracts for data availability
  collect-breadth  Collect live breadth data
  validate         Validate ingested data and report gaps
"""

from __future__ import annotations

import asyncio
import datetime as dt

import typer
from rich.table import Table

from marketdata.config import load_config
from marketdata.utils.log import get_console, get_logger, setup_logging

app = typer.Typer(
    name="marketdata",
    help="IBKR market-data pipeline — fetch, store, and validate SPY/QQQ bars + NYSE breadth.",
    no_args_is_help=True,
)


# ------------------------------------------------------------------
# fetch-bars
# ------------------------------------------------------------------


@app.command()
def fetch_bars(
    symbols: str = typer.Option("SPY,QQQ", help="Comma-separated symbols"),
    bar_sizes: str = typer.Option("1min,5sec", help="Comma-separated bar sizes: 1min, 5sec"),
    start: str = typer.Option(..., help="Start date YYYY-MM-DD"),
    end: str = typer.Option(..., help="End date YYYY-MM-DD"),
    rth: bool = typer.Option(True, "--rth/--no-rth", help="Regular Trading Hours only"),
    client_id: int = typer.Option(0, "--client-id", help="IBKR client ID (0=use config default)"),
    duckdb_file: str = typer.Option(
        "",
        "--duckdb-file",
        help="DuckDB filename override",
    ),
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level"),
) -> None:
    """Fetch historical bars from IBKR and store as Parquet."""
    setup_logging(log_level)
    log = get_logger(__name__)
    overrides: dict = {}
    if client_id > 0:
        overrides["ib_client_id"] = client_id
    if duckdb_file:
        overrides["duckdb_file"] = duckdb_file
    cfg = load_config(**overrides)

    sym_list = [s.strip().upper() for s in symbols.split(",")]
    bs_list = [b.strip() for b in bar_sizes.split(",")]
    start_date = dt.date.fromisoformat(start)
    end_date = dt.date.fromisoformat(end)

    log.info(
        "Fetching bars: symbols=%s bar_sizes=%s range=%s..%s rth=%s clientId=%s",
        sym_list,
        bs_list,
        start_date,
        end_date,
        rth,
        cfg.ib_client_id,
    )

    async def _run() -> dict[str, int]:
        from marketdata.db.duck import MetadataDB
        from marketdata.ibkr.client import IBKRClient
        from marketdata.pipeline.fetch_bars import fetch_bars as _fetch

        client = IBKRClient(cfg)
        db = MetadataDB(cfg.duckdb_path)

        try:
            await client.connect()
            return await _fetch(client, db, cfg, sym_list, bs_list, start_date, end_date, rth)
        finally:
            await client.disconnect()
            db.close()

    results = asyncio.run(_run())
    console = get_console()
    console.print("\n[bold green]Fetch complete![/bold green]")
    for key, rows in results.items():
        console.print(f"  {key}: {rows:,} rows")


# ------------------------------------------------------------------
# probe-breadth
# ------------------------------------------------------------------


@app.command()
def probe_breadth(
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level"),
) -> None:
    """Probe IBKR breadth contracts for live + historical data availability."""
    setup_logging(log_level)
    cfg = load_config()

    async def _run() -> None:
        from marketdata.db.duck import MetadataDB
        from marketdata.ibkr.client import IBKRClient
        from marketdata.pipeline.breadth import BreadthProvider

        client = IBKRClient(cfg)
        db = MetadataDB(cfg.duckdb_path)

        try:
            await client.connect()
            provider = BreadthProvider(client, db, cfg)
            await provider.probe()
        finally:
            await client.disconnect()
            db.close()

    asyncio.run(_run())


# ------------------------------------------------------------------
# collect-breadth
# ------------------------------------------------------------------


@app.command()
def collect_breadth(
    name: str = typer.Option("AD-NYSE", help="Breadth contract name (e.g. AD-NYSE, TICK-NYSE)"),
    interval_seconds: float = typer.Option(5.0, help="Sampling interval in seconds"),
    duration_minutes: float = typer.Option(60.0, help="Collection duration in minutes"),
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level"),
) -> None:
    """Collect live breadth data from IBKR at fixed intervals."""
    setup_logging(log_level)
    log = get_logger(__name__)
    cfg = load_config()

    log.info(
        "Collecting breadth: name=%s interval=%ss duration=%smin",
        name,
        interval_seconds,
        duration_minutes,
    )

    async def _run() -> int:
        from marketdata.db.duck import MetadataDB
        from marketdata.ibkr.client import IBKRClient
        from marketdata.pipeline.breadth import BreadthProvider

        client = IBKRClient(cfg)
        db = MetadataDB(cfg.duckdb_path)

        try:
            await client.connect()
            provider = BreadthProvider(client, db, cfg)
            return await provider.collect(name, interval_seconds, duration_minutes)
        finally:
            await client.disconnect()
            db.close()

    count = asyncio.run(_run())
    console = get_console()
    console.print(f"\n[bold green]Collection complete![/bold green] {count} samples stored.")


# ------------------------------------------------------------------
# fetch-news
# ------------------------------------------------------------------


@app.command()
def fetch_news(
    symbols: str = typer.Option("SPY,QQQ", help="Comma-separated symbols"),
    providers: str = typer.Option(
        "BZ+DJ-N+DJ-RT+FLY+BRFG+BRFUPDN",
        help="Plus-separated IBKR news provider codes",
    ),
    start: str = typer.Option(..., help="Start date YYYY-MM-DD"),
    end: str = typer.Option(..., help="End date YYYY-MM-DD"),
    fetch_bodies: bool = typer.Option(True, "--bodies/--no-bodies", help="Fetch full article text"),
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level"),
) -> None:
    """Fetch news from IBKR (Benzinga, DJ, Fly, etc.) and store as Parquet."""
    setup_logging(log_level)
    log = get_logger(__name__)
    cfg = load_config()

    sym_list = [s.strip().upper() for s in symbols.split(",")]
    start_date = dt.date.fromisoformat(start)
    end_date = dt.date.fromisoformat(end)

    log.info(
        "Fetching news: symbols=%s providers=%s range=%s..%s",
        sym_list,
        providers,
        start_date,
        end_date,
    )

    async def _run() -> dict[str, int]:
        from marketdata.db.duck import MetadataDB
        from marketdata.ibkr.client import IBKRClient
        from marketdata.pipeline.news import IBKRNewsProvider, resolve_con_id

        client = IBKRClient(cfg)
        db = MetadataDB(cfg.duckdb_path)
        total: dict[str, int] = {}

        try:
            await client.connect()
            news_provider = IBKRNewsProvider(client, cfg)

            # Show available providers
            avail = await news_provider.list_providers()
            log.info(
                "Available providers: %s",
                ", ".join(f"{p['code']}" for p in avail),
            )

            for sym in sym_list:
                con_id = await resolve_con_id(client, sym)
                log.info("Resolved %s -> conId %d", sym, con_id)
                result = await news_provider.fetch_news(
                    con_id,
                    sym,
                    providers,
                    start_date,
                    end_date,
                    db,
                    fetch_bodies=fetch_bodies,
                )
                for k, v in result.items():
                    total[f"{sym}_{k}"] = v
        finally:
            await client.disconnect()
            db.close()

        return total

    results = asyncio.run(_run())
    console = get_console()
    console.print("\n[bold green]News fetch complete![/bold green]")
    for key, count in results.items():
        console.print(f"  {key}: {count:,} articles")


# ------------------------------------------------------------------
# stream
# ------------------------------------------------------------------


@app.command()
def stream(
    symbols: str = typer.Option("", help="Comma-separated bar symbols (empty=use config)"),
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level"),
) -> None:
    """Start the real-time streaming service (bars + news)."""
    import signal as _signal

    setup_logging(log_level)
    log = get_logger(__name__)
    cfg = load_config()

    if symbols:
        cfg.streaming.symbols = [s.strip().upper() for s in symbols.split(",")]

    log.info(
        "Starting streaming service: bars=%s news=%s",
        cfg.streaming.symbols,
        cfg.streaming.news_symbols,
    )

    from marketdata.streaming.service import StreamingService

    service = StreamingService(cfg)
    _signal.signal(_signal.SIGINT, lambda *_: service.request_shutdown())
    _signal.signal(_signal.SIGTERM, lambda *_: service.request_shutdown())

    asyncio.run(service.run_forever())


# ------------------------------------------------------------------
# tweets
# ------------------------------------------------------------------


@app.command()
def tweets(
    accounts: str = typer.Option("", help="Comma-separated accounts (empty=use config)"),
    once: bool = typer.Option(False, "--once", help="Run once and exit instead of looping"),
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level"),
) -> None:
    """Collect tweets from configured Twitter/X accounts."""
    import signal as _signal

    setup_logging(log_level)
    log = get_logger(__name__)
    cfg = load_config()

    if accounts:
        cfg.twitter.accounts = [a.strip() for a in accounts.split(",")]

    log.info("Starting tweet collection: accounts=%s", cfg.twitter.accounts)

    from marketdata.twitter.collector import TweetCollector

    collector = TweetCollector(cfg)
    _signal.signal(_signal.SIGINT, lambda *_: setattr(collector, "_running", False))
    _signal.signal(_signal.SIGTERM, lambda *_: setattr(collector, "_running", False))

    async def _run() -> int:
        await collector.start()
        try:
            if once:
                return await collector.collect_all()
            else:
                await collector.run_forever()
                return 0
        finally:
            await collector.stop()

    result = asyncio.run(_run())
    if once:
        console = get_console()
        console.print(f"\n[bold green]Tweet collection complete![/bold green] {result} new tweets.")


# ------------------------------------------------------------------
# validate
# ------------------------------------------------------------------


@app.command()
def validate(
    symbols: str = typer.Option("SPY,QQQ", help="Comma-separated symbols"),
    bar_sizes: str = typer.Option("1min,5sec", help="Comma-separated bar sizes"),
    log_level: str = typer.Option("INFO", "--log-level", help="Logging level"),
) -> None:
    """Validate ingested data and report coverage + gaps."""
    setup_logging(log_level)
    cfg = load_config()

    from marketdata.db.duck import MetadataDB

    db = MetadataDB(cfg.duckdb_path)
    console = get_console()

    sym_list = [s.strip().upper() for s in symbols.split(",")]
    bs_list = [b.strip() for b in bar_sizes.split(",")]

    # Ingestion summary
    summary_table = Table(title="Ingestion Summary")
    summary_table.add_column("Symbol")
    summary_table.add_column("Bar Size")
    summary_table.add_column("Status")
    summary_table.add_column("Days", justify="right")
    summary_table.add_column("Total Rows", justify="right")

    for sym in sym_list:
        for bs in bs_list:
            rows = db.ingestion_summary(sym, bs)
            if not rows:
                summary_table.add_row(sym, bs, "[yellow]no data[/yellow]", "0", "0")
            for r in rows:
                color_map = {
                    "ok": "green",
                    "error": "red",
                    "empty": "yellow",
                    "unavailable": "dim",
                }
                status_color = color_map.get(r["status"], "white")
                summary_table.add_row(
                    r["symbol"],
                    r["bar_size"],
                    f"[{status_color}]{r['status']}[/{status_color}]",
                    str(r["days"]),
                    f"{r['total_rows']:,}",
                )

    console.print(summary_table)

    # Gap summary
    gap_table = Table(title="Gap Summary")
    gap_table.add_column("Symbol")
    gap_table.add_column("Bar Size")
    gap_table.add_column("Date")
    gap_table.add_column("Gap Start")
    gap_table.add_column("Gap End")
    gap_table.add_column("Type")

    for sym in sym_list:
        for bs in bs_list:
            gaps_data = db.get_gaps_summary(sym, bs)
            for g in gaps_data[:50]:  # limit display
                gap_table.add_row(
                    g["symbol"],
                    g["bar_size"],
                    str(g["date"]),
                    str(g["gap_start_utc"]),
                    str(g["gap_end_utc"]),
                    g["gap_type"],
                )
            if len(gaps_data) > 50:
                gap_table.add_row("", "", "", "", "", f"... and {len(gaps_data) - 50} more")

    console.print(gap_table)

    # News summary
    news_rows = db.news_summary()
    if news_rows:
        news_table = Table(title="News Ingestion Summary")
        news_table.add_column("Source")
        news_table.add_column("Status")
        news_table.add_column("Days", justify="right")
        news_table.add_column("Articles", justify="right")
        for r in news_rows:
            color_map = {"ok": "green", "error": "red", "empty": "yellow"}
            sc = color_map.get(r["status"], "white")
            news_table.add_row(
                r["source"],
                f"[{sc}]{r['status']}[/{sc}]",
                str(r["days"]),
                f"{r['total_articles']:,}",
            )
        console.print(news_table)

    # Breadth probe results
    probes = db.get_probe_results()
    if probes:
        probe_table = Table(title="Breadth Probe Results")
        probe_table.add_column("Name")
        probe_table.add_column("Contract")
        probe_table.add_column("Live?")
        probe_table.add_column("Hist?")
        probe_table.add_column("Notes")
        for p in probes:
            probe_table.add_row(
                p["name"],
                p["contract_desc"],
                "[green]Y[/green]" if p["worked_live"] else "[red]N[/red]",
                "[green]Y[/green]" if p["worked_hist"] else "[red]N[/red]",
                p["notes"][:80],
            )
        console.print(probe_table)

    db.close()


