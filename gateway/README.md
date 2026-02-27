# Gateway

IB Gateway management and automated data collection for AutoTrade.

## Overview

Everything runs via **Docker Compose** — no local IB Gateway install or Windows scheduled tasks needed.

```
docker-compose up -d
    ├── ib-gateway        IB Gateway (paper trading, port 4002)
    ├── streaming         Real-time 5sec bars + news (4 AM - 8 PM ET)
    ├── twitter           Tweet collection every 15 min
    └── daily-fetch       Cron: bars + news at 5 PM ET weekdays
```

Docker Desktop auto-starts on Windows login. All containers have `restart: unless-stopped`, so they come back after reboots automatically.

## Prerequisites

| Component | Notes |
|-----------|-------|
| Docker Desktop | [Install](https://www.docker.com/products/docker-desktop/) — set to start on login |
| IBKR account | Paper or live trading account |
| Python + uv | Only needed for manual CLI usage outside Docker |

## Setup

### 1. Configure credentials

Edit `gateway/.env.docker` with your IBKR credentials:

```env
TWS_USERID=your_username
TWS_PASSWORD=your_password
TRADING_MODE=paper
READ_ONLY_API=yes
VNC_SERVER_PASSWORD=autotrade
TWOFA_TIMEOUT_ACTION=restart
TIME_ZONE=America/New_York
```

### 2. Start everything

```bash
cd autotrade-data/gateway
docker-compose up -d --build
```

The first run pulls the IB Gateway image (~400MB) and builds the service image (~200MB). Subsequent starts are fast.

### 3. Verify

```bash
# Check all containers are running
docker-compose ps

# Check gateway is healthy
docker-compose logs ib-gateway --tail 10

# Check services
docker-compose logs streaming --tail 10
docker-compose logs twitter --tail 10
docker-compose logs daily-fetch --tail 5
```

## Architecture

### Docker Compose Services

| Service | Image | Purpose | Depends On |
|---------|-------|---------|------------|
| `ib-gateway` | `ghcr.io/gnzsnz/ib-gateway:stable` | IB Gateway with auto-login via IBC | — |
| `streaming` | Custom (`Dockerfile`) | Real-time bars + news during market hours | Gateway healthy |
| `twitter` | Custom (`Dockerfile`) | Tweet collection from 40+ accounts | — |
| `daily-fetch` | Custom (`Dockerfile`) | Cron job: fetch bars + news after market close | Gateway healthy |

### Networking

Services connect to the gateway via Docker's internal network (`ib-gateway:4002`). The gateway also exposes port 4002 to the host for manual CLI usage:

```bash
# Manual CLI fetch (from host, uses localhost:4002)
uv run marketdata fetch-bars --symbols SPY --bar-sizes 1min --start 2025-01-01 --end 2025-03-01 --rth
```

### Data Persistence

- **Parquet data**: Bind-mounted from `../data` so autotrade-spy can read it
- **Gateway settings**: Docker named volume `ib-gateway-settings` persists between restarts

## Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | All 4 services: gateway, streaming, twitter, daily-fetch |
| `.env.docker` | IBKR credentials for gateway container |
| `crontab` | Supercronic schedule for daily fetch (5 PM ET weekdays) |
| `../Dockerfile` | Service image: Python 3.12 + uv + supercronic + project deps |
| `daily_fetch.py` | Fetch script: bars (1min, 5sec) + news with retry and gateway polling |
| `streaming_service.py` | Real-time bar/news streamer (4 AM - 8 PM ET) |
| `twitter_service.py` | Tweet collector (15-min intervals) |
| `gateway_manager.py` | Gateway lifecycle tool (status, start, stop) |

### Legacy files (Windows IBC setup, no longer used)

| File | Purpose |
|------|---------|
| `start_gateway.bat` | IBC gateway launcher (Windows) |
| `ibc_config.ini` | Template IBC config |
| `setup_scheduled_task.ps1` | Daily fetch Task Scheduler setup |
| `setup_ibc_task.ps1` | Gateway Task Scheduler setup |
| `setup_streaming_task.ps1` | Streaming Task Scheduler setup |
| `setup_twitter_task.ps1` | Twitter Task Scheduler setup |
| `gateway_manager.ps1` | PowerShell gateway manager |

## Daily Fetch

### What it fetches

| Data Type | Symbols | Details |
|-----------|---------|---------|
| 1min bars | SPY, QQQ | RTH only (09:30-16:00 ET) |
| 5sec bars | SPY, QQQ | RTH only, ~6 months history available |
| News | SPY, QQQ, AAPL, MSFT, NVDA, TSLA, AMZN, META, GOOGL | Headlines + bodies, providers: BZ, DJ-N, DJ-RT, FLY, BRFG |

### Resilience

- **Gateway health check**: Docker waits for port 4002 before starting dependent services
- **Phase retry**: Retries the bars or news phase up to 2 times with 30s backoff
- **Client auto-reconnect**: `IBKRClient` detects dropped connections and reconnects (3 attempts, 5s delay)
- **Idempotent**: Pipeline skips already-ingested days via DuckDB metadata tracking
- **3-day lookback**: Default catches gaps from missed runs or weekends

### Manual run

```bash
# Run fetch manually inside the container
docker-compose exec daily-fetch .venv/bin/python gateway/daily_fetch.py --days 7

# Or from host (requires uv and IB Gateway port exposed)
uv run python gateway/daily_fetch.py --days 3
```

## Common Operations

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# Restart just the gateway
docker-compose restart ib-gateway

# Rebuild after code changes
docker-compose up -d --build

# View live logs
docker-compose logs -f

# View specific service logs
docker-compose logs streaming --tail 50

# Check gateway health
docker inspect --format='{{.State.Health.Status}}' ib-gateway
```

## Weekly Operation

| Day | What Happens |
|-----|-------------|
| **Sunday ~1 AM ET** | IB expires weekly session. Gateway container restarts. Approve 2FA on IBKR Mobile app once. |
| **Mon-Fri** | Gateway auto-restarts daily (no 2FA). Daily fetch runs at 5 PM ET. Streaming runs 4 AM - 8 PM ET. |
| **Weekends** | Gateway stays connected. No market data to fetch (markets closed). Twitter collection continues. |

> **Note**: You only need to manually approve 2FA once per week (Sunday night). All other restarts are handled automatically.

## Troubleshooting

### Gateway won't authenticate
- Check credentials in `.env.docker`
- View gateway logs: `docker-compose logs ib-gateway --tail 30`
- Connect via VNC (`localhost:5900`, password: `autotrade`) to see the Gateway UI

### Services won't start (waiting for gateway)
- Services with `depends_on: service_healthy` wait for gateway health check
- Health check has 180s start period — give it time after first boot
- Check: `docker inspect --format='{{.State.Health.Status}}' ib-gateway`

### Streaming connects but disconnects
- Check if market is open (4 AM - 8 PM ET)
- View logs: `docker-compose logs streaming --tail 30`
- Streaming auto-reconnects (up to 10 attempts with 10s delay)

### Daily fetch not running
- Verify cron schedule: `docker-compose exec daily-fetch cat /app/gateway/crontab`
- Check supercronic logs: `docker-compose logs daily-fetch --tail 20`
- Run manually: `docker-compose exec daily-fetch .venv/bin/python gateway/daily_fetch.py --days 1`

### Rebuilding after code changes
If you modify `marketdata/`, `configs/`, or `gateway/` scripts:
```bash
docker-compose up -d --build
```
