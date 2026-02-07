"""
Storage module — atomic writes to Parquet and SQLite.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiosqlite
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src import config

logger = logging.getLogger("data_fetcher.storage")


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------
async def save_parquet(
    df: pd.DataFrame,
    symbol: str = config.SYMBOL,
    timeframe: str = config.TIMEFRAME,
    directory: Path = config.PARQUET_DIR,
) -> Path:
    """
    Write DataFrame to a Parquet file with atomic rename to prevent corruption.
    File is named <symbol>_<timeframe>.parquet (e.g. BTC_USDT_1m.parquet).
    """
    safe_symbol = symbol.replace("/", "_")
    target = directory / f"{safe_symbol}_{timeframe}.parquet"

    table = pa.Table.from_pandas(df, preserve_index=False)

    # Atomic write: write to temp file then rename
    tmp_fd = tempfile.NamedTemporaryFile(
        dir=directory,
        suffix=".parquet.tmp",
        delete=False,
    )
    tmp_path = Path(tmp_fd.name)
    tmp_fd.close()

    try:
        pq.write_table(table, tmp_path, compression="snappy")
        tmp_path.rename(target)
        logger.info("Saved Parquet → %s (%d rows)", target, len(df))
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    # Daily archive: data/archive/BTC_USDT_1m_2026-02-07.parquet
    _create_daily_archive(df, safe_symbol, timeframe, directory)

    return target


def _create_daily_archive(
    df: pd.DataFrame,
    safe_symbol: str,
    timeframe: str,
    base_dir: Path,
    retention_days: int = 30,
) -> None:
    """Create a daily Parquet snapshot and clean up archives older than retention_days."""
    archive_dir = base_dir / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    archive_path = archive_dir / f"{safe_symbol}_{timeframe}_{today}.parquet"

    if not archive_path.exists():
        try:
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table, archive_path, compression="snappy")
            logger.info("Daily archive → %s (%d rows)", archive_path.name, len(df))
        except Exception as exc:
            logger.warning("Failed to create daily archive: %s", exc)

    # Cleanup archives older than retention_days
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    for old_file in archive_dir.glob(f"{safe_symbol}_{timeframe}_*.parquet"):
        try:
            date_str = old_file.stem.rsplit("_", 1)[-1]
            file_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if file_date < cutoff:
                old_file.unlink()
                logger.info("Deleted old archive: %s", old_file.name)
        except (ValueError, OSError):
            pass


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------
_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS ohlcv (
    timestamp     INTEGER PRIMARY KEY,
    datetime      TEXT,
    open          REAL,
    high          REAL,
    low           REAL,
    close         REAL,
    volume        REAL,
    log_return    REAL,
    atr           REAL,
    relative_volatility REAL,
    funding_rate  REAL,
    best_bid      REAL,
    best_ask      REAL,
    spread        REAL,
    spread_bps    REAL,
    symbol        TEXT,
    timeframe     TEXT
);
"""

_UPSERT = """
INSERT OR REPLACE INTO ohlcv (
    timestamp, datetime, open, high, low, close, volume,
    log_return, atr, relative_volatility,
    funding_rate, best_bid, best_ask, spread, spread_bps,
    symbol, timeframe
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


async def save_sqlite(
    df: pd.DataFrame,
    symbol: str = config.SYMBOL,
    timeframe: str = config.TIMEFRAME,
    db_path: Path = config.SQLITE_DB,
) -> int:
    """
    Upsert DataFrame rows into SQLite with WAL mode for concurrent reads.
    Returns the number of rows written.
    """
    df = df.copy()
    df["symbol"] = symbol
    df["timeframe"] = timeframe

    # Convert datetime to ISO string for SQLite
    if "datetime" in df.columns:
        df["datetime"] = df["datetime"].astype(str)

    cols = [
        "timestamp", "datetime", "open", "high", "low", "close", "volume",
        "log_return", "atr", "relative_volatility",
        "funding_rate", "best_bid", "best_ask", "spread", "spread_bps",
        "symbol", "timeframe",
    ]

    # Fill missing columns with None
    for c in cols:
        if c not in df.columns:
            df[c] = None

    rows = df[cols].values.tolist()

    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute(_CREATE_TABLE)
        await db.executemany(_UPSERT, rows)
        await db.commit()

    logger.info("Saved SQLite → %s (%d rows upserted)", db_path, len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Read helpers (useful for downstream consumers)
# ---------------------------------------------------------------------------
async def load_parquet(
    symbol: str = config.SYMBOL,
    timeframe: str = config.TIMEFRAME,
    directory: Path = config.PARQUET_DIR,
) -> pd.DataFrame:
    """Load a previously saved Parquet file back into a DataFrame."""
    safe_symbol = symbol.replace("/", "_")
    path = directory / f"{safe_symbol}_{timeframe}.parquet"
    return pd.read_parquet(path)


async def load_sqlite(
    symbol: str = config.SYMBOL,
    timeframe: str = config.TIMEFRAME,
    db_path: Path = config.SQLITE_DB,
    limit: int | None = None,
) -> pd.DataFrame:
    """Query stored OHLCV rows from SQLite."""
    query = "SELECT * FROM ohlcv WHERE symbol = ? AND timeframe = ? ORDER BY timestamp"
    params: list = [symbol, timeframe]
    if limit:
        query += " LIMIT ?"
        params.append(limit)

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            if not rows:
                return pd.DataFrame()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame([dict(zip(columns, r)) for r in rows])
