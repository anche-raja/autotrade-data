"""Rich-powered structured logging with daily rotating file output."""

from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler

_console = Console(stderr=True)

_DEFAULT_LOG_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "logs"


def setup_logging(level: str = "INFO", log_dir: Path | None = None) -> None:
    """Configure root logger with Rich console + daily rotating file handler.

    Logs are written to ``data/logs/pipeline.log`` with daily rotation,
    keeping the last 30 days of log files.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Rich console handler (colored, to stderr)
    console_handler = RichHandler(
        console=_console,
        show_path=False,
        rich_tracebacks=True,
        tracebacks_show_locals=False,
        markup=True,
    )
    console_handler.setLevel(log_level)

    # File handler (plain text, daily rotation)
    dest = log_dir or _DEFAULT_LOG_DIR
    dest.mkdir(parents=True, exist_ok=True)
    log_file = dest / "pipeline.log"

    file_handler = TimedRotatingFileHandler(
        filename=str(log_file),
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
        utc=True,
    )
    file_handler.setLevel(log_level)
    file_handler.suffix = "%Y-%m-%d"
    file_formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)

    logging.basicConfig(
        level=log_level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[console_handler, file_handler],
        force=True,
    )

    logging.getLogger(__name__).debug("Logging initialized: file=%s level=%s", log_file, level)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger."""
    return logging.getLogger(name)


def get_console() -> Console:
    """Return the shared Rich console (for progress bars, tables, etc.)."""
    return _console
