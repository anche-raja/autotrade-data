@echo off
cd /d "C:\Users\claw\auto-trade\autotrade-data"
uv run python gateway/twitter_service.py --log-level INFO
