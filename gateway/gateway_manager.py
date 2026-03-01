"""IB Gateway lifecycle manager — check, start, wait, and verify.

Single script to ensure IB Gateway is running and accepting connections.
Can be called standalone or imported by other scripts (daily_fetch, streaming).

Usage:
    # Check status only
    uv run python gateway/gateway_manager.py status

    # Start Gateway if not running, wait until ready
    uv run python gateway/gateway_manager.py start

    # Start Gateway + run daily fetch after it's ready
    uv run python gateway/gateway_manager.py start --fetch

    # Stop Gateway
    uv run python gateway/gateway_manager.py stop
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GATEWAY_DIR = Path(__file__).resolve().parent
LOG_DIR = PROJECT_ROOT / "data" / "logs"

# Defaults — read from config if available, fallback to these
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7497
WAIT_TIMEOUT = 120  # seconds to wait for Gateway to accept connections
POLL_INTERVAL = 5  # seconds between port checks
STARTUP_GRACE = 15  # seconds to wait after launching before first port check

log = logging.getLogger("gateway_manager")


def _load_port() -> tuple[str, int]:
    """Load host/port from marketdata config, fallback to defaults."""
    try:
        from marketdata.config import load_config

        cfg = load_config()
        return cfg.ib_host, cfg.ib_port
    except Exception:
        return DEFAULT_HOST, DEFAULT_PORT


def is_port_open(host: str, port: int, timeout: float = 3.0) -> bool:
    """Check if a TCP port is accepting connections."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (ConnectionRefusedError, TimeoutError, OSError):
        return False


def find_gateway_process() -> int | None:
    """Find running IB Gateway Java process, return PID or None."""
    if sys.platform == "win32":
        return _find_gateway_windows()
    return _find_gateway_unix()


def _find_gateway_windows() -> int | None:
    """Find Gateway process on Windows via PowerShell."""
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-Process java -ErrorAction SilentlyContinue | "
             "Where-Object { $_.MainWindowTitle -match 'IBKR|Gateway' -or "
             "$_.Path -match 'ibgateway' } | "
             "Select-Object -ExpandProperty Id"],
            capture_output=True, text=True, timeout=10,
        )
        if result.stdout.strip():
            return int(result.stdout.strip().splitlines()[0])
    except Exception:
        pass

    # Fallback: check for any java process with ibgateway in command line
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" | "
             "Where-Object { $_.CommandLine -match 'ibgateway' } | "
             "Select-Object -ExpandProperty ProcessId"],
            capture_output=True, text=True, timeout=10,
        )
        if result.stdout.strip():
            return int(result.stdout.strip().splitlines()[0])
    except Exception:
        pass

    return None


def _find_gateway_unix() -> int | None:
    """Find Gateway process on macOS/Linux via pgrep."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "ibgateway"],
            capture_output=True, text=True, timeout=10,
        )
        if result.stdout.strip():
            return int(result.stdout.strip().splitlines()[0])
    except Exception:
        pass
    return None


def get_status(host: str, port: int) -> dict:
    """Get comprehensive Gateway status."""
    pid = find_gateway_process()
    port_open = is_port_open(host, port)
    return {
        "running": pid is not None,
        "pid": pid,
        "port_open": port_open,
        "host": host,
        "port": port,
        "ready": pid is not None and port_open,
    }


def start_gateway() -> bool:
    """Launch IB Gateway via IBC start script. Returns True if process launched."""
    if sys.platform == "win32":
        script = GATEWAY_DIR / "start_gateway.bat"
        if not script.exists():
            log.error("Start script not found: %s", script)
            return False
        log.info("Launching IB Gateway via %s", script.name)
        try:
            subprocess.Popen(
                ["cmd.exe", "/c", str(script)],
                cwd=str(PROJECT_ROOT),
                creationflags=subprocess.CREATE_NEW_CONSOLE,
            )
            return True
        except Exception:
            log.exception("Failed to launch Gateway")
            return False
    else:
        script = GATEWAY_DIR / "start_gateway.sh"
        if not script.exists():
            log.error("Start script not found: %s", script)
            return False
        log.info("Launching IB Gateway via %s", script.name)
        try:
            subprocess.Popen(
                ["bash", str(script)],
                cwd=str(PROJECT_ROOT),
                start_new_session=True,
            )
            return True
        except Exception:
            log.exception("Failed to launch Gateway")
            return False


def stop_gateway() -> bool:
    """Stop running IB Gateway process."""
    import signal as _signal

    pid = find_gateway_process()
    if pid is None:
        log.info("No Gateway process found")
        return True

    log.info("Stopping Gateway process (PID %d)", pid)
    try:
        if sys.platform == "win32":
            subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 f"Stop-Process -Id {pid} -Force -ErrorAction SilentlyContinue"],
                timeout=15,
            )
        else:
            os.kill(pid, _signal.SIGTERM)
        time.sleep(2)
        if find_gateway_process() is None:
            log.info("Gateway stopped successfully")
            return True
        log.warning("Gateway process still running after stop attempt")
        return False
    except Exception:
        log.exception("Error stopping Gateway")
        return False


def wait_for_ready(
    host: str, port: int, timeout: int = WAIT_TIMEOUT, grace: int = STARTUP_GRACE
) -> bool:
    """Wait for Gateway to accept connections on the API port."""
    log.info("Waiting for Gateway to be ready on %s:%d (timeout %ds)...", host, port, timeout)

    if grace > 0:
        log.info("Startup grace period: %ds", grace)
        time.sleep(grace)

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if is_port_open(host, port):
            log.info("Gateway is ready and accepting connections on %s:%d", host, port)
            return True

        remaining = int(deadline - time.monotonic())
        if remaining > 0:
            log.debug("Port not open yet, retrying in %ds... (%ds remaining)", POLL_INTERVAL, remaining)
            time.sleep(POLL_INTERVAL)

    log.error("Gateway did not become ready within %ds", timeout)
    return False


def ensure_running(host: str, port: int) -> bool:
    """Ensure Gateway is running and ready. Start if needed. Returns True if ready."""
    status = get_status(host, port)

    if status["ready"]:
        log.info("Gateway is already running (PID %d) and accepting connections", status["pid"])
        return True

    if status["running"] and not status["port_open"]:
        log.info("Gateway process found (PID %d) but port not open yet, waiting...", status["pid"])
        return wait_for_ready(host, port, grace=0)

    # Need to start Gateway
    log.info("Gateway is not running, starting...")
    if not start_gateway():
        return False

    return wait_for_ready(host, port)


def run_daily_fetch(days: int = 3) -> int:
    """Run the daily fetch script after Gateway is confirmed ready."""
    fetch_script = GATEWAY_DIR / "daily_fetch.py"
    log.info("Running daily fetch (--days %d --skip-gateway-check)...", days)
    result = subprocess.run(
        [sys.executable, str(fetch_script), "--days", str(days), "--skip-gateway-check"],
        cwd=str(PROJECT_ROOT),
    )
    return result.returncode


def setup_logging(level: str = "INFO") -> None:
    """Configure logging to console and optionally file."""
    fmt = "%(asctime)s %(levelname)-5s %(name)s: %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper()), format=fmt)

    # Also log to file
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(str(LOG_DIR / "gateway_manager.log"), encoding="utf-8")
    fh.setFormatter(logging.Formatter(fmt))
    logging.getLogger().addHandler(fh)


def main() -> None:
    parser = argparse.ArgumentParser(description="IB Gateway lifecycle manager")
    sub = parser.add_subparsers(dest="command", required=True)

    # status
    sub.add_parser("status", help="Check if Gateway is running and port is open")

    # start
    start_p = sub.add_parser("start", help="Ensure Gateway is running, start if needed")
    start_p.add_argument("--fetch", action="store_true", help="Run daily_fetch after Gateway is ready")
    start_p.add_argument("--fetch-days", type=int, default=3, help="Days to look back for fetch (default: 3)")
    start_p.add_argument("--timeout", type=int, default=WAIT_TIMEOUT, help=f"Seconds to wait for ready (default: {WAIT_TIMEOUT})")

    # stop
    sub.add_parser("stop", help="Stop the Gateway process")

    # restart
    restart_p = sub.add_parser("restart", help="Stop then start Gateway")
    restart_p.add_argument("--timeout", type=int, default=WAIT_TIMEOUT, help=f"Seconds to wait for ready (default: {WAIT_TIMEOUT})")

    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args()

    setup_logging(args.log_level)
    host, port = _load_port()

    if args.command == "status":
        status = get_status(host, port)
        if status["ready"]:
            print(f"READY — Gateway running (PID {status['pid']}), port {port} open")
        elif status["running"]:
            print(f"STARTING — Gateway running (PID {status['pid']}), port {port} not yet open")
        else:
            print(f"STOPPED — No Gateway process, port {port} closed")
        sys.exit(0 if status["ready"] else 1)

    elif args.command == "start":
        ok = ensure_running(host, port)
        if not ok:
            log.error("Failed to start Gateway")
            sys.exit(1)
        if args.fetch:
            rc = run_daily_fetch(args.fetch_days)
            sys.exit(rc)

    elif args.command == "stop":
        ok = stop_gateway()
        sys.exit(0 if ok else 1)

    elif args.command == "restart":
        stop_gateway()
        time.sleep(5)
        ok = ensure_running(host, port)
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
