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
# Redis
# ---------------------------------------------------------------------------
REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "")
REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
REDIS_MAX_CONNECTIONS: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "10"))
REDIS_URL: str = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"

# ---------------------------------------------------------------------------
# Hyperliquid DEX
# ---------------------------------------------------------------------------
HL_PRIVATE_KEY: str = os.getenv("HL_PRIVATE_KEY", "")
HL_WALLET_ADDRESS: str = os.getenv("HL_WALLET_ADDRESS", "")
HL_MAINNET: bool = os.getenv("HL_MAINNET", "false").lower() == "true"
HL_DEFAULT_LEVERAGE: int = int(os.getenv("HL_DEFAULT_LEVERAGE", "1"))
HL_POSITION_FRAC: float = float(os.getenv("HL_POSITION_FRAC", "0.10"))
HL_LIMIT_OFFSET_BPS: float = float(os.getenv("HL_LIMIT_OFFSET_BPS", "5"))
HL_LIMIT_WAIT_SEC: int = int(os.getenv("HL_LIMIT_WAIT_SEC", "10"))
HL_SIGNAL_CHANNEL: str = os.getenv("HL_SIGNAL_CHANNEL", "trading_signals")
HL_STATE_POLL_SEC: int = int(os.getenv("HL_STATE_POLL_SEC", "10"))

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

        # Rotating file handler (daily, 7-day retention)
        from src.log_rotation import create_rotating_handler

        fh = create_rotating_handler(DATA_DIR / "fetcher.log")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger
