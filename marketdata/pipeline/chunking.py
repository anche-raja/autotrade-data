"""Chunk-plan generator for IBKR historical-data requests.

IBKR imposes small ``durationStr`` windows for sub-30-second bars.
This module splits a trading day into appropriately sized request chunks.

For **1-min bars**, a single ``"1 D"`` request suffices per day.
For **5-sec bars**, RTH (6.5 h) is split into 1-hour windows, each with
``durationStr = "3600 S"``, plus a final 30-min tail (``"1800 S"``).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from marketdata.utils.time import rth_boundaries, to_ib_datetime


@dataclass(frozen=True)
class ChunkPlan:
    """A single IBKR historical-data request specification."""

    start_utc: dt.datetime
    end_utc: dt.datetime
    duration_str: str
    bar_size_setting: str
    end_dt_ib: str  # formatted for IBKR API

    @property
    def label(self) -> str:
        return (
            f"{self.bar_size_setting} "
            f"{self.start_utc.strftime('%H:%M')}-{self.end_utc.strftime('%H:%M')} UTC"
        )


def plan_chunks(
    day: dt.date,
    bar_size: str,
    session_start: str = "09:30",
    session_end: str = "16:00",
    tz: str = "America/New_York",
) -> list[ChunkPlan]:
    """Generate a list of :class:`ChunkPlan` for a single trading day.

    Parameters
    ----------
    day : date
        The trading day.
    bar_size : str
        Canonical bar size: ``"1min"`` or ``"5sec"``.
    session_start, session_end : str
        RTH boundaries in ``"HH:MM"`` format.
    tz : str
        Timezone for session logic.

    Returns
    -------
    list[ChunkPlan]
    """
    rth_start, rth_end = rth_boundaries(day, session_start, session_end, tz)

    if bar_size in ("1min", "1 min"):
        return [
            ChunkPlan(
                start_utc=rth_start,
                end_utc=rth_end,
                duration_str="1 D",
                bar_size_setting="1 min",
                end_dt_ib=to_ib_datetime(rth_end),
            )
        ]

    if bar_size in ("5sec", "5 secs", "5s"):
        return _plan_5sec_chunks(rth_start, rth_end)

    raise ValueError(f"Unsupported bar_size: {bar_size!r}")


def _plan_5sec_chunks(
    rth_start: dt.datetime,
    rth_end: dt.datetime,
    chunk_seconds: int = 3600,
) -> list[ChunkPlan]:
    """Split the RTH window into fixed-length chunks for 5-sec bars."""
    chunks: list[ChunkPlan] = []
    cursor = rth_start

    while cursor < rth_end:
        chunk_end = min(cursor + dt.timedelta(seconds=chunk_seconds), rth_end)
        remaining = int((chunk_end - cursor).total_seconds())

        chunks.append(
            ChunkPlan(
                start_utc=cursor,
                end_utc=chunk_end,
                duration_str=f"{remaining} S",
                bar_size_setting="5 secs",
                end_dt_ib=to_ib_datetime(chunk_end),
            )
        )
        cursor = chunk_end

    return chunks
