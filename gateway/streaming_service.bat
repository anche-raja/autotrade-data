@echo off
REM Streaming data collection service — runs 24/7
REM Streams real-time bars + news during extended hours (4 AM - 8 PM ET)
REM
REM Prerequisites: IB Gateway must be running with API enabled on port 7497

cd /d "C:\Users\claw\auto-trade\autotrade-data"
uv run python gateway/streaming_service.py --log-level INFO

if %ERRORLEVEL% NEQ 0 (
    echo [%date% %time%] Streaming service exited with code %ERRORLEVEL% >> data\logs\streaming_errors.log
)
