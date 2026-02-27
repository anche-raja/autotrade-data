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
│   ├── streaming/                 # Real-time bar/news collection
│   ├── twitter/                   # Twitter/X feed collector
│   ├── db/                        # DuckDB metadata tracking
│   └── utils/                     # Trading calendar, logging
│
├── gateway/                       # Docker-based IB Gateway + services
│   ├── docker-compose.yml         # IB Gateway + streaming + twitter + daily-fetch
│   ├── .env.docker                # IBKR credentials (gitignored)
│   ├── crontab                    # Daily fetch schedule (5 PM ET weekdays)
│   └── README.md                  # Full setup guide
│
├── Dockerfile                     # Service image (Python 3.12 + uv + supercronic)
├── configs/
│   └── default.yaml               # IBKR connection, pacing, RTH settings
│
├── tests/                         # pytest suite
└── data/                          # Runtime output (gitignored)
    └── parquet/                   # Hive-partitioned: bars, news, breadth, tweets
```

## Quick Start (Docker)

```bash
# 1. Configure IBKR credentials
edit gateway/.env.docker

# 2. Start everything (IB Gateway + all services)
cd gateway && docker-compose up -d --build

# 3. Verify
docker-compose ps
docker-compose logs -f
```

This starts 4 containers:
- **ib-gateway** — IB Gateway (paper trading, port 4002)
- **streaming** — Real-time 5sec bars + news (4 AM - 8 PM ET)
- **twitter** — Tweet collection every 15 min
- **daily-fetch** — Cron: bars + news at 5 PM ET weekdays

See [gateway/README.md](gateway/README.md) for full setup guide.

## Manual CLI Usage

```bash
# Install dependencies
uv sync

# Fetch bars (requires IB Gateway running — Docker or local)
uv run marketdata fetch-bars --symbols SPY,QQQ --bar-sizes 1min --start 2024-01-01 --end 2024-12-31 --rth

# Fetch news
uv run marketdata fetch-news --symbols SPY,QQQ --providers BZ+DJ-N+DJ-RT+FLY+BRFG --start 2024-01-01 --end 2024-12-31 --bodies

# Validate data coverage
uv run marketdata validate --symbols SPY,QQQ --bar-sizes 1min

# Run tests
uv run pytest
```

## Configuration

Edit `configs/default.yaml` or use `MD_` env vars:

```yaml
ib_host: "127.0.0.1"
ib_port: 4002          # Docker: 4002, local paper: 7497, local live: 7496
ib_client_id: 10
data_dir: "data"
rth_only: true
```

## Data Output

```
data/parquet/
├── dataset=bars/symbol=SPY/bar=1min/date=YYYY-MM-DD/part.parquet
├── dataset=bars/symbol=SPY/bar=5sec/date=YYYY-MM-DD/part.parquet
├── dataset=news/symbol=spy/date=YYYY-MM-DD/part.parquet
├── dataset=breadth/name=nyse_adv/date=YYYY-MM-DD/part.parquet
└── dataset=tweets/account=elonmusk/date=YYYY-MM-DD/part.parquet
```

The companion repo [autotrade-spy](../autotrade-spy) reads this data for analysis, backtesting, and the web UI. Point it here via:

```
PARQUET_ROOT=../autotrade-data/data/parquet
```
