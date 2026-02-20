"""Trading session lifecycle management for the streaming service."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

from marketdata.config import PipelineConfig
from marketdata.utils.time import extended_hours_boundaries, get_trading_days

UTC = ZoneInfo("UTC")


class SessionScheduler:
    """Determines when to start/stop streaming based on extended-hours calendar."""

    def __init__(self, cfg: PipelineConfig) -> None:
        self._cfg = cfg
        self._tz = ZoneInfo(cfg.timezone)

    def is_streaming_active(self, now_utc: dt.datetime | None = None) -> bool:
        """Check if we should be streaming right now."""
        now = now_utc or dt.datetime.now(UTC)
        today = now.astimezone(self._tz).date()

        trading_days = get_trading_days(today, today)
        if not trading_days:
            return False

        start, end = self.session_boundaries(today)
        return start <= now <= end

    def session_boundaries(self, day: dt.date) -> tuple[dt.datetime, dt.datetime]:
        """Return (start_utc, end_utc) for the streaming session on *day*."""
        return extended_hours_boundaries(
            day,
            self._cfg.streaming.extended_start,
            self._cfg.streaming.extended_end,
            self._cfg.timezone,
        )

    def seconds_until_next_session(self, now_utc: dt.datetime | None = None) -> float:
        """Seconds until the next streaming session begins.

        Searches up to 7 days ahead to handle weekends and holidays.
        """
        now = now_utc or dt.datetime.now(UTC)
        today = now.astimezone(self._tz).date()

        for offset in range(8):
            day = today + dt.timedelta(days=offset)
            trading_days = get_trading_days(day, day)
            if not trading_days:
                continue
            start, _end = self.session_boundaries(day)
            if start > now:
                return (start - now).total_seconds()

        # Fallback: check again in 5 minutes
        return 300.0
