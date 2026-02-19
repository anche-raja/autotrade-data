"""DuckDB metadata database for ingestion tracking, gap logging, and views."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import duckdb

from marketdata.utils.log import get_logger

log = get_logger(__name__)

_SCHEMA_SQL = """
-- Ingestion tracking: one row per (symbol, bar_size, date) fetch attempt
CREATE TABLE IF NOT EXISTS ingestions (
    symbol        VARCHAR NOT NULL,
    bar_size      VARCHAR NOT NULL,
    date          DATE NOT NULL,
    start_ts_utc  TIMESTAMPTZ,
    end_ts_utc    TIMESTAMPTZ,
    rows          INTEGER DEFAULT 0,
    status        VARCHAR NOT NULL DEFAULT 'pending',
    error_msg     VARCHAR,
    fetched_at_utc TIMESTAMPTZ DEFAULT now(),
    checksum      VARCHAR,
    PRIMARY KEY (symbol, bar_size, date)
);

-- Gap records detected during quality checks
CREATE TABLE IF NOT EXISTS gaps (
    symbol       VARCHAR NOT NULL,
    bar_size     VARCHAR NOT NULL,
    date         DATE NOT NULL,
    gap_start_utc TIMESTAMPTZ NOT NULL,
    gap_end_utc  TIMESTAMPTZ NOT NULL,
    gap_type     VARCHAR NOT NULL
);

-- News ingestion tracking: one row per (source, date)
CREATE TABLE IF NOT EXISTS news_ingestions (
    source         VARCHAR NOT NULL,
    date           DATE NOT NULL,
    articles       INTEGER DEFAULT 0,
    status         VARCHAR NOT NULL DEFAULT 'pending',
    error_msg      VARCHAR,
    fetched_at_utc TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (source, date)
);

-- Breadth-probe results
CREATE TABLE IF NOT EXISTS breadth_probe_results (
    name           VARCHAR NOT NULL,
    contract_desc  VARCHAR,
    worked_live    BOOLEAN,
    worked_hist    BOOLEAN,
    notes          VARCHAR,
    checked_at_utc TIMESTAMPTZ DEFAULT now()
);

-- View: continuous 1-min time index for SPY RTH, left-joined with actual data.
-- Reads Parquet files via DuckDB's glob reader and flags missing bars.
CREATE OR REPLACE VIEW v_spy_1min_rth_continuous AS
WITH trading_dates AS (
    SELECT DISTINCT date FROM ingestions
    WHERE symbol = 'SPY' AND bar_size = '1min' AND status = 'ok'
),
minute_series AS (
    SELECT
        d.date,
        generate_series(
            (d.date::TIMESTAMP + INTERVAL '14 hours 30 minutes'),  -- 09:30 ET ≈ 14:30 UTC (EST)
            (d.date::TIMESTAMP + INTERVAL '21 hours') - INTERVAL '1 minute',
            INTERVAL '1 minute'
        ) AS ts_utc
    FROM trading_dates d
)
SELECT
    ms.date,
    ms.ts_utc,
    b.open,
    b.high,
    b.low,
    b.close,
    b.volume,
    CASE WHEN b.ts_utc IS NULL THEN true ELSE false END AS is_gap
FROM minute_series ms
LEFT JOIN (
    SELECT * FROM read_parquet('data/parquet/dataset=bars/symbol=SPY/bar=1min/date=*/part.parquet',
                               hive_partitioning=true, union_by_name=true)
) b ON ms.ts_utc = b.ts_utc
ORDER BY ms.ts_utc;
"""


class MetadataDB:
    """Manages the DuckDB metadata database."""

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._path = db_path
        self._con = duckdb.connect(str(db_path))
        self._init_schema()

    def _init_schema(self) -> None:
        for stmt in _SCHEMA_SQL.split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    self._con.execute(stmt)
                except Exception as exc:
                    log.debug("Schema statement skipped: %s", exc)

    def close(self) -> None:
        self._con.close()

    # ----------------------------------------------------------------
    # Ingestion CRUD
    # ----------------------------------------------------------------

    def upsert_ingestion(
        self,
        symbol: str,
        bar_size: str,
        date: dt.date,
        start_ts_utc: dt.datetime | None = None,
        end_ts_utc: dt.datetime | None = None,
        rows: int = 0,
        status: str = "ok",
        error_msg: str | None = None,
        checksum: str | None = None,
    ) -> None:
        """Insert or update an ingestion record."""
        self._con.execute(
            """
            INSERT INTO ingestions (symbol, bar_size, date, start_ts_utc, end_ts_utc,
                                    rows, status, error_msg, fetched_at_utc, checksum)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, now(), ?)
            ON CONFLICT (symbol, bar_size, date)
            DO UPDATE SET
                start_ts_utc = EXCLUDED.start_ts_utc,
                end_ts_utc   = EXCLUDED.end_ts_utc,
                rows         = EXCLUDED.rows,
                status       = EXCLUDED.status,
                error_msg    = EXCLUDED.error_msg,
                fetched_at_utc = now(),
                checksum     = EXCLUDED.checksum
            """,
            [symbol, bar_size, date, start_ts_utc, end_ts_utc, rows, status, error_msg, checksum],
        )

    def get_ingested_dates(self, symbol: str, bar_size: str, status: str = "ok") -> set[dt.date]:
        """Return the set of dates already ingested for a symbol+bar_size."""
        rows = self._con.execute(
            "SELECT date FROM ingestions WHERE symbol = ? AND bar_size = ? AND status = ?",
            [symbol, bar_size, status],
        ).fetchall()
        return {r[0] for r in rows}

    # ----------------------------------------------------------------
    # Gaps
    # ----------------------------------------------------------------

    def insert_gaps(
        self,
        symbol: str,
        bar_size: str,
        date: dt.date,
        gaps: list[Any],
    ) -> None:
        """Write gap records (delete existing for this day first)."""
        self._con.execute(
            "DELETE FROM gaps WHERE symbol = ? AND bar_size = ? AND date = ?",
            [symbol, bar_size, date],
        )
        for g in gaps:
            self._con.execute(
                """INSERT INTO gaps (symbol, bar_size, date, gap_start_utc, gap_end_utc, gap_type)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [symbol, bar_size, date, g.gap_start_utc, g.gap_end_utc, g.gap_type],
            )

    def get_gaps_summary(
        self, symbol: str | None = None, bar_size: str | None = None
    ) -> list[dict[str, Any]]:
        """Return gap summary rows."""
        sql = (
            "SELECT symbol, bar_size, date, gap_start_utc, "
            "gap_end_utc, gap_type FROM gaps WHERE 1=1"
        )
        params: list[Any] = []
        if symbol:
            sql += " AND symbol = ?"
            params.append(symbol)
        if bar_size:
            sql += " AND bar_size = ?"
            params.append(bar_size)
        sql += " ORDER BY date, gap_start_utc"
        rows = self._con.execute(sql, params).fetchall()
        cols = ["symbol", "bar_size", "date", "gap_start_utc", "gap_end_utc", "gap_type"]
        return [dict(zip(cols, r)) for r in rows]

    # ----------------------------------------------------------------
    # Breadth probe results
    # ----------------------------------------------------------------

    def insert_probe_result(
        self,
        name: str,
        contract_desc: str,
        worked_live: bool,
        worked_hist: bool,
        notes: str = "",
    ) -> None:
        self._con.execute(
            """INSERT INTO breadth_probe_results
                (name, contract_desc, worked_live, worked_hist, notes, checked_at_utc)
               VALUES (?, ?, ?, ?, ?, now())""",
            [name, contract_desc, worked_live, worked_hist, notes],
        )

    def get_probe_results(self) -> list[dict[str, Any]]:
        rows = self._con.execute(
            "SELECT name, contract_desc, worked_live, worked_hist, notes, checked_at_utc "
            "FROM breadth_probe_results ORDER BY checked_at_utc DESC"
        ).fetchall()
        cols = ["name", "contract_desc", "worked_live", "worked_hist", "notes", "checked_at_utc"]
        return [dict(zip(cols, r)) for r in rows]

    # ----------------------------------------------------------------
    # News ingestion
    # ----------------------------------------------------------------

    def upsert_news_ingestion(
        self,
        source: str,
        date: dt.date,
        articles: int = 0,
        status: str = "ok",
        error_msg: str | None = None,
    ) -> None:
        self._con.execute(
            """
            INSERT INTO news_ingestions (source, date, articles, status,
                                         error_msg, fetched_at_utc)
            VALUES (?, ?, ?, ?, ?, now())
            ON CONFLICT (source, date)
            DO UPDATE SET
                articles   = EXCLUDED.articles,
                status     = EXCLUDED.status,
                error_msg  = EXCLUDED.error_msg,
                fetched_at_utc = now()
            """,
            [source, date, articles, status, error_msg],
        )

    def get_news_ingested_dates(self, source: str, status: str = "ok") -> set[dt.date]:
        rows = self._con.execute(
            "SELECT date FROM news_ingestions WHERE source = ? AND status = ?",
            [source, status],
        ).fetchall()
        return {r[0] for r in rows}

    def news_summary(self, source: str | None = None) -> list[dict[str, Any]]:
        sql = """
            SELECT source, status, count(*) as days,
                   sum(articles) as total_articles
            FROM news_ingestions WHERE 1=1
        """
        params: list[Any] = []
        if source:
            sql += " AND source = ?"
            params.append(source)
        sql += " GROUP BY source, status ORDER BY source, status"
        rows = self._con.execute(sql, params).fetchall()
        cols = ["source", "status", "days", "total_articles"]
        return [dict(zip(cols, r)) for r in rows]

    # ----------------------------------------------------------------
    # Validation helpers
    # ----------------------------------------------------------------

    def ingestion_summary(
        self, symbol: str | None = None, bar_size: str | None = None
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT symbol, bar_size, status, count(*) as days, sum(rows) as total_rows
            FROM ingestions WHERE 1=1
        """
        params: list[Any] = []
        if symbol:
            sql += " AND symbol = ?"
            params.append(symbol)
        if bar_size:
            sql += " AND bar_size = ?"
            params.append(bar_size)
        sql += " GROUP BY symbol, bar_size, status ORDER BY symbol, bar_size, status"
        rows = self._con.execute(sql, params).fetchall()
        cols = ["symbol", "bar_size", "status", "days", "total_rows"]
        return [dict(zip(cols, r)) for r in rows]

    def execute(self, sql: str, params: list[Any] | None = None) -> Any:
        """Run arbitrary SQL (for the continuous-time view, etc.)."""
        return self._con.execute(sql, params or [])
