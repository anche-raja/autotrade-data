@echo off
REM Daily IBKR data fetch — called by Windows Task Scheduler
REM Runs after market close to pull today's bars and news
REM
REM Prerequisites: TWS must be running with API enabled on port 7497

cd /d "C:\Users\claw\autotrade-data"
uv run python gateway/daily_fetch.py --days 3 --log-level INFO

if %ERRORLEVEL% NEQ 0 (
    echo [%date% %time%] Daily fetch failed with exit code %ERRORLEVEL% >> data\logs\daily_fetch_errors.log
)
