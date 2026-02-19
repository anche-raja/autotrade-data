"""Parquet partition writer / reader with Hive-style layout.

Layout::

    data/parquet/
      dataset=bars/symbol=SPY/bar=1min/date=2025-06-15/part.parquet
      dataset=breadth/name=nyse_adv/date=2025-06-15/part.parquet
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from marketdata.utils.log import get_logger

log = get_logger(__name__)

# ------------------------------------------------------------------
# Schema
# ------------------------------------------------------------------

BAR_SCHEMA = pa.schema(
    [
        pa.field("ts_utc", pa.timestamp("us", tz="UTC")),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.float64()),
        pa.field("wap", pa.float64()),
        pa.field("count", pa.int32()),
        pa.field("source", pa.string()),
        pa.field("quality_flags", pa.string()),
    ]
)

BREADTH_SCHEMA = pa.schema(
    [
        pa.field("ts_utc", pa.timestamp("us", tz="UTC")),
        pa.field("value", pa.float64()),
        pa.field("source", pa.string()),
        pa.field("quality_flags", pa.string()),
    ]
)


# ------------------------------------------------------------------
# Path helpers
# ------------------------------------------------------------------


def bars_partition_path(
    root: Path,
    symbol: str,
    bar_size: str,
    date: str,
) -> Path:
    """Build Hive-style path for a bar partition.

    Returns something like:
        ``root/dataset=bars/symbol=SPY/bar=1min/date=2025-06-15/part.parquet``
    """
    bar_label = _normalize_bar_label(bar_size)
    return root / f"dataset=bars/symbol={symbol}/bar={bar_label}/date={date}/part.parquet"


def breadth_partition_path(
    root: Path,
    name: str,
    date: str,
) -> Path:
    return root / f"dataset=breadth/name={name}/date={date}/part.parquet"


def _normalize_bar_label(bar_size: str) -> str:
    mapping = {"1 min": "1min", "1min": "1min", "5 secs": "5sec", "5sec": "5sec", "5s": "5sec"}
    return mapping.get(bar_size, bar_size)


# ------------------------------------------------------------------
# Write / Read
# ------------------------------------------------------------------


def write_partition(df: pd.DataFrame, path: Path, schema: pa.Schema = BAR_SCHEMA) -> str:
    """Write a DataFrame to a single Parquet file, returning its SHA-256 checksum."""
    path.parent.mkdir(parents=True, exist_ok=True)

    # Ensure required columns exist with defaults
    if "source" not in df.columns or "quality_flags" not in df.columns:
        df = df.copy()
        if "source" not in df.columns:
            df["source"] = "ibkr"
        if "quality_flags" not in df.columns:
            df["quality_flags"] = "ok"
    if "count" in df.columns:
        df["count"] = df["count"].fillna(0).astype("int32")

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, str(path), compression="zstd")

    checksum = _file_sha256(path)
    log.debug("Wrote %d rows to %s (sha256=%s…)", len(df), path, checksum[:12])
    return checksum


def read_partition(path: Path) -> pd.DataFrame:
    """Read a Parquet partition back into a DataFrame."""
    if not path.exists():
        return pd.DataFrame()
    table = pq.read_table(str(path))
    return table.to_pandas()


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()
