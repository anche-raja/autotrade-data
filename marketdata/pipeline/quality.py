"""Data-quality checks: sort, dedup, gap detection, quality flags.

All cleaning is non-destructive to raw data: no forward-filling of prices.
Only timestamps are sorted/deduped, and quality_flags are annotated.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import pandas as pd

from marketdata.utils.log import get_logger
from marketdata.utils.time import expected_cadence_seconds, is_within_availability

log = get_logger(__name__)


@dataclass(frozen=True)
class Gap:
    """Represents a detected gap in a time series."""

    gap_start_utc: dt.datetime
    gap_end_utc: dt.datetime
    gap_type: str  # "missing_data" | "unavailable_by_ibkr_limit"


def clean_bars(df: pd.DataFrame, bar_size: str) -> pd.DataFrame:
    """Sort by ts_utc, de-duplicate (keep last), ensure monotonic.

    Returns a new DataFrame.
    """
    if df.empty:
        return df.copy()

    out = df.copy()
    out = out.sort_values("ts_utc", kind="stable").reset_index(drop=True)
    n_before = len(out)
    out = out.drop_duplicates(subset=["ts_utc"], keep="last").reset_index(drop=True)
    n_dupes = n_before - len(out)
    if n_dupes:
        log.info("Removed %d duplicate timestamps", n_dupes)

    # Assign quality flags
    out["quality_flags"] = "ok"
    if n_dupes:
        # Mark all rows as having come from a deduped set
        out["quality_flags"] = "deduped"

    # Verify monotonic
    if not out["ts_utc"].is_monotonic_increasing:
        log.error("ts_utc is NOT monotonic after sort+dedup — this should never happen")

    return out


def detect_gaps(
    df: pd.DataFrame,
    bar_size: str,
    rth_start_utc: dt.datetime,
    rth_end_utc: dt.datetime,
    day: dt.date | None = None,
) -> list[Gap]:
    """Detect gaps in the time series within the RTH window.

    Parameters
    ----------
    df : DataFrame
        Must have a ``ts_utc`` column, already sorted.
    bar_size : str
        Canonical bar size for cadence calculation.
    rth_start_utc, rth_end_utc : datetime
        RTH boundaries in UTC.
    day : date, optional
        Trading day (used for availability check on 5-sec data).

    Returns
    -------
    list[Gap]
    """
    cadence = expected_cadence_seconds(bar_size)
    # Allow 50% tolerance (e.g., 90 s gap for 1 min bars is ok)
    threshold = cadence * 1.5

    gaps: list[Gap] = []

    if df.empty:
        gap_type = "missing_data"
        if day and not is_within_availability(day, bar_size):
            gap_type = "unavailable_by_ibkr_limit"
        gaps.append(Gap(rth_start_utc, rth_end_utc, gap_type))
        return gaps

    timestamps = pd.to_datetime(df["ts_utc"], utc=True)

    # Gap at session start?
    first_ts = timestamps.iloc[0].to_pydatetime()
    if (first_ts - rth_start_utc).total_seconds() > threshold:
        gaps.append(Gap(rth_start_utc, first_ts, "missing_data"))

    # Interior gaps
    diffs = timestamps.diff().dt.total_seconds()
    for i in range(1, len(diffs)):
        if diffs.iloc[i] > threshold:
            gap_start = timestamps.iloc[i - 1].to_pydatetime()
            gap_end = timestamps.iloc[i].to_pydatetime()
            gaps.append(Gap(gap_start, gap_end, "missing_data"))

    # Gap at session end?
    last_ts = timestamps.iloc[-1].to_pydatetime()
    if (rth_end_utc - last_ts).total_seconds() > threshold:
        gaps.append(Gap(last_ts, rth_end_utc, "missing_data"))

    return gaps


def validate_partition(
    df: pd.DataFrame,
    bar_size: str,
    rth_start_utc: dt.datetime,
    rth_end_utc: dt.datetime,
    day: dt.date | None = None,
) -> tuple[pd.DataFrame, list[Gap]]:
    """Clean bars and detect gaps in one pass.

    Returns
    -------
    (cleaned_df, gaps)
    """
    cleaned = clean_bars(df, bar_size)
    gaps = detect_gaps(cleaned, bar_size, rth_start_utc, rth_end_utc, day)
    if gaps:
        log.info("Detected %d gap(s) for %s on %s", len(gaps), bar_size, day)
    return cleaned, gaps
