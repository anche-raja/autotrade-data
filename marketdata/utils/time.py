"""Trading-calendar helpers, RTH boundaries, and UTC conversions."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as mcal

NYSE_TZ = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def get_trading_days(
    start: dt.date,
    end: dt.date,
    exchange: str = "NYSE",
) -> list[dt.date]:
    """Return sorted list of trading days between *start* and *end* (inclusive)."""
    cal = mcal.get_calendar(exchange)
    schedule = cal.schedule(
        start_date=pd.Timestamp(start),
        end_date=pd.Timestamp(end),
    )
    return [ts.date() for ts in schedule.index]


def rth_boundaries(
    day: dt.date,
    session_start: str = "09:30",
    session_end: str = "16:00",
    tz: str = "America/New_York",
) -> tuple[dt.datetime, dt.datetime]:
    """Return (start_utc, end_utc) for a given trading day's RTH session."""
    local_tz = ZoneInfo(tz)
    sh, sm = (int(x) for x in session_start.split(":"))
    eh, em = (int(x) for x in session_end.split(":"))

    start_local = dt.datetime(day.year, day.month, day.day, sh, sm, tzinfo=local_tz)
    end_local = dt.datetime(day.year, day.month, day.day, eh, em, tzinfo=local_tz)

    return start_local.astimezone(UTC), end_local.astimezone(UTC)


def to_ib_datetime(d: dt.datetime) -> str:
    """Format a tz-aware datetime to IBKR's ``YYYYmmdd-HH:MM:SS`` UTC string."""
    utc_dt = d.astimezone(UTC)
    return utc_dt.strftime("%Y%m%d-%H:%M:%S")


def is_within_availability(day: dt.date, bar_size: str, months: int = 6) -> bool:
    """Check whether *day* falls within the IBKR availability window for *bar_size*.

    For bar sizes <= 30 sec, IBKR only keeps ~*months* of history.
    """
    if bar_size not in ("5 secs", "5sec", "5s"):
        return True  # 1-min and larger have ~1-year history
    cutoff = dt.date.today() - dt.timedelta(days=months * 30)
    return day >= cutoff


def expected_cadence_seconds(bar_size: str) -> int:
    """Return expected seconds between consecutive bars."""
    mapping = {
        "1 min": 60,
        "1min": 60,
        "5 secs": 5,
        "5sec": 5,
        "5s": 5,
    }
    return mapping.get(bar_size, 60)
