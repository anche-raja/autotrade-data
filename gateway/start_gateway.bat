@echo off
REM Start IB Gateway via IBC for AutoTrade-SPY
REM ============================================
REM
REM Prerequisites:
REM   1. IB Gateway installed (C:\Jts)
REM   2. IBC installed (C:\IBC)
REM   3. config.ini at %USERPROFILE%\Documents\IBC\config.ini
REM
REM Edit TWS_MAJOR_VRSN below to match your installed Gateway version.
REM Find version: run Gateway manually > Help > About IB Gateway
REM   e.g. "Build 10.30.1t" -> TWS_MAJOR_VRSN=1030
REM
REM For Task Scheduler, use: start_gateway.bat /INLINE

set TWS_MAJOR_VRSN=1037
set IBC_PATH=C:\IBC
set TWOFA_TIMEOUT_ACTION=restart

call "%IBC_PATH%\StartGateway.bat" %*
