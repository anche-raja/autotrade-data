# Gateway

IB Gateway management and daily data collection for AutoTrade-SPY.

## Overview

Two automated systems work together to keep data flowing without manual intervention:

1. **IB Gateway (via IBC)** — Starts IB Gateway at login, handles authentication, auto-restarts on crashes
2. **Daily Data Fetch** — Runs at 5 PM ET after market close, pulls bars and news

```
System Boot / Login
    └─► IB Gateway starts via IBC (Startup folder)
            └─► Gateway accepts API connections on port 7497

5:00 PM ET (Task Scheduler)
    └─► daily_fetch.bat
            └─► Waits for Gateway connectivity
            └─► Fetches 1min + 5sec bars (SPY, QQQ)
            └─► Fetches news (SPY, QQQ, AAPL, MSFT, NVDA, TSLA, AMZN, META, GOOGL)
            └─► Logs to data/logs/daily_fetch.log
```

## Prerequisites

| Component | Location | Source |
|-----------|----------|--------|
| IB Gateway | `C:\Jts` | [IBKR Downloads](https://www.interactivebrokers.com/en/trading/tws-updateable-latest.php) |
| IBC v3.23+ | `C:\IBC` | [IBC Releases](https://github.com/IbcAlpha/IBC/releases/latest) |
| IBC Config | `%USERPROFILE%\Documents\IBC\config.ini` | Copy from `gateway/ibc_config.ini` |
| Python + uv | On PATH | [uv](https://docs.astral.sh/uv/) |

## Setup

### 1. Install IB Gateway

Download and install IB Gateway from IBKR. Default install location is `C:\Jts`.

### 2. Install IBC

1. Download the latest IBC release ZIP from [GitHub](https://github.com/IbcAlpha/IBC/releases/latest)
2. Right-click the ZIP > Properties > check "Unblock" > Apply
3. Extract to `C:\IBC`

### 3. Configure IBC

Copy the template config and fill in your IBKR credentials:

```powershell
mkdir "$env:USERPROFILE\Documents\IBC" -Force
copy gateway\ibc_config.ini "$env:USERPROFILE\Documents\IBC\config.ini"
```

Edit `%USERPROFILE%\Documents\IBC\config.ini`:

```ini
IbLoginId=your_username
IbPassword=your_password
TradingMode=paper          # or "live"
OverrideTwsApiPort=7497    # paper: 7497, live: 7496
```

> **Important**: The real `config.ini` with credentials lives outside the repo and is never committed to git. Only the template (`gateway/ibc_config.ini`) is tracked.

### 4. Update Gateway Version

Check your installed IB Gateway version: run Gateway manually > Help > About IB Gateway.
For example, "Build 10.44.1t" means version **1044**.

Update the version in two places:

- `gateway/start_gateway.bat` — line `set TWS_MAJOR_VRSN=1044`
- `C:\IBC\StartGateway.bat` — line `set TWS_MAJOR_VRSN=1044`

### 5. Register Scheduled Tasks

**IB Gateway auto-start** — uses a Windows Startup shortcut (no admin required):

The Startup shortcut is at:
```
%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\AutoTrade-SPY IB Gateway.lnk
```
Target: `C:\Users\claw\auto-trade\autotrade-data\gateway\start_gateway.bat`

Alternatively, if you have admin access, use the Task Scheduler script:
```powershell
powershell -ExecutionPolicy Bypass -File gateway\setup_ibc_task.ps1
```

**Daily data fetch** — runs at 5 PM ET via Task Scheduler:

```powershell
powershell -ExecutionPolicy Bypass -File gateway\setup_scheduled_task.ps1
```

## Files

| File | Purpose |
|------|---------|
| `start_gateway.bat` | Launches IB Gateway through IBC. Sets version, calls `C:\IBC\StartGateway.bat` |
| `daily_fetch.py` | Main fetch script: bars (1min, 5sec) + news with retry and gateway polling |
| `daily_fetch.bat` | Batch wrapper for Task Scheduler, calls `daily_fetch.py` with 3-day lookback |
| `ibc_config.ini` | Template IBC config — copy to `%USERPROFILE%\Documents\IBC\config.ini` |
| `setup_scheduled_task.ps1` | Creates Task Scheduler entry for daily fetch at 5 PM ET |
| `setup_ibc_task.ps1` | Creates Task Scheduler entry for Gateway at logon (requires admin) |

## Daily Fetch Script

### Usage

```bash
# Default: fetch last 3 days of bars + news
uv run python gateway/daily_fetch.py

# Custom lookback
uv run python gateway/daily_fetch.py --days 7

# Bars only (skip news)
uv run python gateway/daily_fetch.py --bars-only

# News only (skip bars)
uv run python gateway/daily_fetch.py --news-only

# Skip Gateway connectivity check (if TWS is running manually)
uv run python gateway/daily_fetch.py --skip-gateway-check
```

### What it fetches

| Data Type | Symbols | Details |
|-----------|---------|---------|
| 1min bars | SPY, QQQ | RTH only (09:30-16:00 ET) |
| 5sec bars | SPY, QQQ | RTH only, ~6 months history available |
| News | SPY, QQQ, AAPL, MSFT, NVDA, TSLA, AMZN, META, GOOGL | Headlines + bodies, providers: BZ, DJ-N, DJ-RT, FLY, BRFG, BRFUPDN |

### Resilience

- **Gateway polling**: Waits up to 5 minutes for IB Gateway to accept connections before starting
- **Phase retry**: Retries the entire bars or news phase up to 2 times with 30s backoff
- **Client auto-reconnect**: `IBKRClient` detects dropped connections and reconnects (3 attempts, 5s delay)
- **Idempotent**: Pipeline skips already-ingested days via DuckDB metadata tracking
- **3-day lookback**: Default catches gaps from missed runs or weekends

### Logs

- Console: stdout during manual runs
- File: `data/logs/daily_fetch.log` (appended each run)
- Errors: `data/logs/daily_fetch_errors.log` (batch wrapper, exit code only)

## Client Auto-Reconnect

The `IBKRClient` in `marketdata/ibkr/client.py` handles dropped connections:

- Registers a `disconnectedEvent` handler to log disconnections
- `_ensure_connected()` checks connection state before every API call
- On disconnect: retries up to 3 times with 5-second delays
- Raises `ConnectionError` if all reconnect attempts fail

## IBC Configuration Reference

Key settings in `%USERPROFILE%\Documents\IBC\config.ini`:

| Setting | Value | Purpose |
|---------|-------|---------|
| `TradingMode` | `paper` / `live` | Which account to log into |
| `OverrideTwsApiPort` | `7497` (paper) / `7496` (live) | API socket port, must match `configs/default.yaml` |
| `ReadOnlyApi` | `yes` | Safe for data-only fetching |
| `AcceptIncomingConnectionAction` | `accept` | Allow API connections without manual approval |
| `ExistingSessionDetectedAction` | `primaryoverride` | Take over if logged in elsewhere (e.g., mobile) |
| `ReloginAfterSecondFactorAuthenticationTimeout` | `yes` | Auto-retry login after 2FA timeout |
| `SecondFactorAuthenticationExitInterval` | `180` | Seconds to wait for 2FA approval |
| `AcceptNonBrokerageAccountWarning` | `yes` | Auto-dismiss paper trading dialog |

## Weekly Operation

| Day | What Happens |
|-----|-------------|
| **Sunday ~1 AM ET** | IB expires weekly session. Gateway stops. IBC restarts it. Approve 2FA on IBKR Mobile app once. |
| **Mon-Fri** | Gateway auto-restarts daily (no 2FA). Daily fetch runs at 5 PM ET. |
| **Weekends** | Gateway stays connected. No market data to fetch (markets closed). |

> **Note**: You only need to manually approve 2FA once per week (Sunday night). All other restarts are handled automatically.

## Troubleshooting

### Gateway won't start
- Check IBC logs: `C:\IBC\Logs\`
- Verify `TWS_MAJOR_VRSN` matches installed version in both `gateway/start_gateway.bat` and `C:\IBC\StartGateway.bat`
- Run `gateway/start_gateway.bat` manually from a command prompt to see errors

### Daily fetch fails
- Check `data/logs/daily_fetch.log` for details
- Verify Gateway is running: `netstat -an | findstr 7497`
- Test manually: `uv run python gateway/daily_fetch.py --days 1`

### 2FA not working
- Ensure IBKR Mobile app is installed and notifications are enabled
- Check `SecondFactorAuthenticationExitInterval` is long enough (default 180s)
- IBC will retry automatically if `ReloginAfterSecondFactorAuthenticationTimeout=yes`

### Task Scheduler issues
- View task status: `Get-ScheduledTask -TaskName "AutoTrade-SPY*"`
- Run manually: `Start-ScheduledTask -TaskName "AutoTrade-SPY Daily Fetch"`
- Check task history: Task Scheduler > right-click task > "View History"

### Port mismatch
The API port must match in three places:
1. `configs/default.yaml` — `ib_port: 7497`
2. `%USERPROFILE%\Documents\IBC\config.ini` — `OverrideTwsApiPort=7497`
3. IB Gateway settings — Configure > API > Settings > Socket port
