"""
Configuration module — loads .env and exposes typed settings.
"""

import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load .env from project root
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Exchange
# ---------------------------------------------------------------------------
EXCHANGE_ID: str = os.getenv("EXCHANGE_ID", "binance")
EXCHANGE_API_KEY: str = os.getenv("EXCHANGE_API_KEY", "")
EXCHANGE_SECRET: str = os.getenv("EXCHANGE_SECRET", "")

# ---------------------------------------------------------------------------
# Market
# ---------------------------------------------------------------------------
SYMBOL: str = os.getenv("SYMBOL", "BTC/USDT")
TIMEFRAME: str = os.getenv("TIMEFRAME", "1m")

# Map timeframe strings to milliseconds for gap detection
TIMEFRAME_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}

# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------
FETCH_LIMIT: int = int(os.getenv("FETCH_LIMIT", "1000"))
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
RATE_LIMIT_MS: int = int(os.getenv("RATE_LIMIT_MS", "100"))

# ---------------------------------------------------------------------------
# Storage paths
# ---------------------------------------------------------------------------
DATA_DIR: Path = _PROJECT_ROOT / os.getenv("DATA_DIR", "data")
SQLITE_DB: Path = _PROJECT_ROOT / os.getenv("SQLITE_DB", "data/ohlcv.db")
PARQUET_DIR: Path = _PROJECT_ROOT / os.getenv("PARQUET_DIR", "data")

DATA_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()


def setup_logging() -> logging.Logger:
    """Configure and return the root project logger."""
    logger = logging.getLogger("data_fetcher")
    logger.setLevel(LOG_LEVEL)

    if not logger.handlers:
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        # Console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

        # File handler (optional, inside data/)
        fh = logging.FileHandler(DATA_DIR / "fetcher.log")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger
