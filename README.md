# AutoTrade-Data

IBKR market data collection pipeline. Fetches historical bars, news, and breadth data from Interactive Brokers and stores in Hive-partitioned Parquet with DuckDB metadata tracking.

## Structure

```
autotrade-data/
├── marketdata/                    # IBKR data pipeline
│   ├── cli.py                     # Typer CLI: fetch-bars, fetch-news, validate
│   ├── config.py                  # PipelineConfig (YAML + env vars)
│   ├── ibkr/                      # Client, contracts, pacing
│   ├── pipeline/                  # Fetch, chunk, store, quality, news, breadth
│   ├── db/                        # DuckDB metadata tracking
│   └── utils/                     # Trading calendar, logging
│
├── gateway/                       # IB Gateway automation (IBC)
│   ├── daily_fetch.py             # Daily data fetch (bars + news)
│   ├── start_gateway.bat          # IBC gateway launcher
│   └── README.md                  # Full setup guide
│
├── configs/
│   └── default.yaml               # IBKR connection, pacing, RTH settings
│
├── tests/                         # pytest suite
└── data/                          # Runtime output (gitignored)
    └── parquet/                   # Hive-partitioned: bars, news, breadth
```

## Quick Start

```bash
# Install dependencies
uv sync

# Fetch bars (requires IB Gateway/TWS running)
uv run marketdata fetch-bars --symbols SPY,QQQ --bar-sizes 1min --start 2024-01-01 --end 2024-12-31 --rth

# Fetch news
uv run marketdata fetch-news --symbols SPY,QQQ --providers BZ+DJ-N+DJ-RT+FLY+BRFG --start 2024-01-01 --end 2024-12-31 --bodies

# Validate data coverage
uv run marketdata validate --symbols SPY,QQQ --bar-sizes 1min

# Daily automated fetch (via Task Scheduler)
uv run python gateway/daily_fetch.py --days 3

# Run tests
uv run pytest
```

## Configuration

Edit `configs/default.yaml` or use `MD_` env vars:

```yaml
ib_host: "127.0.0.1"
ib_port: 7497          # paper: 7497, live: 7496
ib_client_id: 10
data_dir: "data"
rth_only: true
```

## Data Output

```
data/parquet/
├── dataset=bars/symbol=SPY/bar=1min/date=YYYY-MM-DD/part.parquet
├── dataset=news/symbol=spy/date=YYYY-MM-DD/part.parquet
└── dataset=breadth/name=nyse_adv/date=YYYY-MM-DD/part.parquet
```

The companion repo [autotrade-spy](../autotrade-spy) reads this data for analysis, backtesting, and the web UI. Point it here via:

```
PARQUET_ROOT=C:\Users\claw\autotrade-data\data\parquet
```

## IB Gateway Setup

See [gateway/README.md](gateway/README.md) for full IBC setup, scheduled tasks, and troubleshooting.
