# AutoTrade-Data

IBKR market data collection pipeline. Connects to Interactive Brokers via `ib_async`, fetches historical bars, news, and market breadth data, streams real-time bars/news, collects Twitter/X feeds, and stores everything as Hive-partitioned Parquet with DuckDB metadata tracking.

## Stack

- **Python 3.12**, `uv` package manager, `hatchling` build
- **IBKR**: `ib_async` for Interactive Brokers API
- **Data**: pyarrow (Parquet), duckdb (metadata tracking), pandas
- **CLI**: typer with rich progress bars
- **Config**: pydantic-settings + YAML overlay (`configs/default.yaml`)
- **Twitter**: twikit (authenticated client with cookie persistence)
- **Quality**: ruff linter, pytest tests

## Main directories

| Directory | Purpose |
|-----------|---------|
| `marketdata/` | Main Python package (CLI, config, pipeline, streaming, twitter) |
| `marketdata/ibkr/` | IBKR client, contract definitions, pacing engine |
| `marketdata/pipeline/` | Fetch orchestrator, chunking, storage, quality checks, news, breadth |
| `marketdata/streaming/` | Real-time bar/news collection with aggregation and gap tracking |
| `marketdata/twitter/` | Twitter/X feed collector (twikit) |
| `marketdata/db/` | DuckDB metadata helpers |
| `marketdata/utils/` | Trading calendar, logging |
| `gateway/` | IB Gateway automation scripts (daily fetch, streaming, twitter services) |
| `configs/` | YAML configuration |
| `tests/` | pytest test suite |
| `data/` | Runtime output (gitignored): Parquet files, DuckDB, logs |

## Commands

```bash
# Install dependencies
uv sync

# Fetch historical bars (requires IB Gateway/TWS running)
uv run marketdata fetch-bars --symbols SPY,QQQ --bar-sizes 1min,5sec --start 2024-01-01 --end 2024-12-31 --rth

# Fetch news
uv run marketdata fetch-news --symbols SPY,QQQ --providers BZ+DJ-N+DJ-RT+FLY+BRFG --start 2024-01-01 --end 2024-12-31

# Validate data coverage and gaps
uv run marketdata validate --symbols SPY,QQQ --bar-sizes 1min,5sec

# Start real-time streaming service (bars + news)
uv run marketdata stream

# Collect tweets from configured accounts
uv run marketdata tweets
uv run marketdata tweets --once  # single run

# Probe breadth contracts for data availability
uv run marketdata probe-breadth

# Collect live breadth data
uv run marketdata collect-breadth --name AD-NYSE --duration-minutes 60

# Daily automated fetch (via Task Scheduler)
uv run python gateway/daily_fetch.py --days 3

# Run tests
uv run pytest

# Lint with auto-fix
uv run ruff check . --fix && uv run ruff format .
```

## Configuration

All config uses Pydantic `BaseSettings` with env prefix `MD_`. Resolution order (highest wins):
1. Environment variables prefixed `MD_` (nested: `MD_STREAMING__SYMBOLS`)
2. Values in `configs/default.yaml`
3. Field defaults in `marketdata/config.py`

Key env vars:
- `MD_IB_HOST` — IBKR host (default: `127.0.0.1`)
- `MD_IB_PORT` — IBKR port (paper: `7497`, live: `7496`, gateway: `4002`)
- `MD_IB_CLIENT_ID` — IBKR client ID (default: `10`)
- `MD_DATA_DIR` — Data output directory (default: `data`)
- `MD_STREAMING__SYMBOLS` — Comma-separated streaming symbols
- `MD_TWITTER__AUTH_TOKEN` / `MD_TWITTER__CT0` — Twitter auth cookies

## Data output layout

```
data/parquet/
  dataset=bars/symbol=SPY/bar=1min/date=YYYY-MM-DD/part.parquet
  dataset=bars/symbol=SPY/bar=5sec/date=YYYY-MM-DD/part.parquet
  dataset=news/symbol=spy/date=YYYY-MM-DD/part.parquet
  dataset=breadth/name=nyse_adv/date=YYYY-MM-DD/part.parquet
  dataset=tweets/account=elonmusk/date=YYYY-MM-DD/part.parquet
```

## Key conventions

- `from __future__ import annotations` in every file.
- Use `%s` format strings in log calls (not f-strings) to avoid eager evaluation.
- Config values come from `PipelineConfig` via `load_config()`, never hardcoded.
- All async entry points use `asyncio.run()` at CLI level only.
- Parquet schemas are defined as module-level `pa.schema` constants in `marketdata/pipeline/storage.py`.
- DuckDB metadata tracks ingestion status, checksums, and gaps per partition.
- The companion repo [autotrade-spy](../autotrade-spy) reads this data via `PARQUET_ROOT`.

## Related repos

- **[autotrade-spy](../autotrade-spy)** — Analysis, backtesting, AI agent, and web UI. Reads the Parquet data that this repo produces.
