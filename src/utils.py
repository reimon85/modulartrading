"""
Data cleaning, normalization, and feature engineering utilities.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger("data_fetcher.utils")


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------
def normalize_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Convert ms-epoch timestamps to a proper UTC datetime column."""
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df


# ---------------------------------------------------------------------------
# Feature engineering — IA-ready primitives
# ---------------------------------------------------------------------------
def add_log_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Add log-return column: ln(close_t / close_{t-1})."""
    df = df.copy()
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))
    return df


def add_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """
    Add Average True Range (ATR) over *period* bars.

    ATR captures volatility in price-unit terms and is a standard feature
    for HFT and ML-based trading models.
    """
    df = df.copy()
    high_low = df["high"] - df["low"]
    high_prev = (df["high"] - df["close"].shift(1)).abs()
    low_prev = (df["low"] - df["close"].shift(1)).abs()
    true_range = pd.concat([high_low, high_prev, low_prev], axis=1).max(axis=1)
    df["atr"] = true_range.rolling(window=period, min_periods=1).mean()
    return df


def add_relative_volatility(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """
    Relative volatility = rolling std(log_return) / rolling mean(|log_return|).

    Values > 1 indicate fat-tailed movements; useful for regime detection.
    """
    df = df.copy()
    if "log_return" not in df.columns:
        df = add_log_returns(df)
    roll_std = df["log_return"].rolling(window=window, min_periods=1).std()
    roll_mean_abs = df["log_return"].abs().rolling(window=window, min_periods=1).mean()
    df["relative_volatility"] = roll_std / roll_mean_abs.replace(0, np.nan)
    return df


def add_metadata(
    df: pd.DataFrame,
    funding_rate: float | None = None,
    spread_info: dict | None = None,
) -> pd.DataFrame:
    """Attach exchange-level metadata columns."""
    df = df.copy()
    df["funding_rate"] = funding_rate
    if spread_info:
        df["best_bid"] = spread_info["best_bid"]
        df["best_ask"] = spread_info["best_ask"]
        df["spread"] = spread_info["spread"]
        df["spread_bps"] = spread_info["spread_bps"]
    else:
        for col in ("best_bid", "best_ask", "spread", "spread_bps"):
            df[col] = np.nan
    return df


# ---------------------------------------------------------------------------
# Full enrichment pipeline
# ---------------------------------------------------------------------------
def enrich(
    df: pd.DataFrame,
    funding_rate: float | None = None,
    spread_info: dict | None = None,
    atr_period: int = 14,
    vol_window: int = 20,
) -> pd.DataFrame:
    """
    Run the complete enrichment pipeline:
      1. Normalize timestamps to UTC datetime
      2. Log returns
      3. ATR
      4. Relative volatility
      5. Exchange metadata (funding rate, spread)
    """
    df = normalize_timestamps(df)
    df = add_log_returns(df)
    df = add_atr(df, period=atr_period)
    df = add_relative_volatility(df, window=vol_window)
    df = add_metadata(df, funding_rate=funding_rate, spread_info=spread_info)
    logger.info("Enrichment complete — %d rows, %d columns", len(df), len(df.columns))
    return df
