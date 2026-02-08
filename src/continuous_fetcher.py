"""
Continuous Data Fetcher — Fetches latest OHLCV data, funding rate, and spread
from a ccxt-compatible exchange, enriches it, and pushes to Redis and Parquet/SQLite.

Usage:
    python -m src.continuous_fetcher
    python -m src.continuous_fetcher --symbol ETH/USDT --timeframe 5m
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

import pandas as pd

# Add project root to sys.path for imports
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from src.config import TIMEFRAME_MS, SYMBOL, TIMEFRAME
from src.config import setup_logging
from src.database import RedisManager
from src.fetcher import ExchangeClient, fetch_funding_rate, fetch_ohlcv, fetch_spread
from src.storage import save_parquet, save_sqlite
from src.utils import enrich

logger = setup_logging() # Uses 'data_fetcher' logger name


async def run_continuous_fetcher(args: argparse.Namespace) -> None:
    client = ExchangeClient()
    redis_mgr = RedisManager()
    
    symbol = args.symbol
    timeframe = args.timeframe
    interval_ms = TIMEFRAME_MS.get(timeframe, 60_000)
    interval_sec = interval_ms / 1000.0

    if interval_ms is None:
        logger.error(f"Unsupported timeframe: {timeframe}")
        return

    try:
        await client.connect()
        await redis_mgr.connect()
        logger.info(f"Starting continuous fetcher for {symbol}@{timeframe} (interval: {interval_sec}s)")

        while True:
            start_time = time.time()
            now_ms = int(pd.Timestamp.now("UTC").timestamp() * 1000)
            
            # Fetch latest candle (limit=1)
            # Fetch starting from the last known timestamp if possible,
            # otherwise, fetch the very latest bar.
            # For continuous fetching, we always want the most recent bar
            # and let the exchange handle partial bar updates or new bar creation.
            df_ohlcv = await fetch_ohlcv(
                client,
                symbol=symbol,
                timeframe=timeframe,
                limit=1,
                since=now_ms - interval_ms * 2 # Fetch last 2 intervals to be safe
            )

            if df_ohlcv.empty:
                logger.warning("No OHLCV data returned in this cycle. Retrying...")
                await asyncio.sleep(interval_sec)
                continue
            
            # Ensure we only have the latest complete bar (or partial if still forming)
            latest_bar = df_ohlcv.iloc[-1]
            
            # Supplementary data
            funding_rate = await fetch_funding_rate(client, symbol=symbol)
            spread_info = await fetch_spread(client, symbol=symbol)

            # Enrich
            df_enriched = enrich(
                pd.DataFrame([latest_bar]), # Pass as DataFrame for enrich function
                funding_rate=funding_rate,
                spread_info=spread_info
            )
            latest_enriched = df_enriched.iloc[-1] # Get back the enriched Series

            # Persist to Parquet + SQLite (consider if this is efficient for single bars)
            # For now, append to existing or create new file
            await save_parquet(df_enriched, symbol=symbol, timeframe=timeframe)
            await save_sqlite(df_enriched, symbol=symbol, timeframe=timeframe)

            # Push to Redis
            # 5a. Store last 100 candles in Sorted Set (will only have 1 if always limit=1)
            # It should ideally add the new bar to the ZSET and trim old ones.
            # For simplicity, I'll push the latest enriched bar.
            # The existing store_candles expects a list of dicts.
            candles_to_store = df_enriched[["timestamp", "open", "high", "low", "close", "volume"]].to_dict("records")
            await redis_mgr.store_candles(candles_to_store, timeframe=timeframe)

            # 5b. Publish latest tick via Hash + Pub/Sub
            tick = {
                "symbol": symbol,
                "price": float(latest_enriched["close"]),
                "open": float(latest_enriched["open"]),
                "high": float(latest["high"]), # Use original high/low for tick
                "low": float(latest["low"]),
                "close": float(latest_enriched["close"]),
                "volume": float(latest_enriched["volume"]),
                "funding_rate": funding_rate,
                "timestamp": int(latest_enriched["timestamp"]),
            }
            if spread_info:
                tick.update(spread_info)
            await redis_mgr.publish_tick(tick)

            logger.info(
                f"Fetched latest {symbol}@{timeframe} bar ending "
                f"{pd.Timestamp(latest_enriched['timestamp'], unit='ms', tz='UTC')} | "
                f"Close: {latest_enriched['close']:.2f} | Pushed to Redis."
            )

            # Sleep until next interval
            elapsed_time = time.time() - start_time
            sleep_duration = max(0, interval_sec - elapsed_time)
            if sleep_duration > 0:
                await asyncio.sleep(sleep_duration)
            else:
                logger.warning(f"Fetcher cycle for {symbol}@{timeframe} took longer than interval ({elapsed_time:.2f}s vs {interval_sec}s)")

    except asyncio.CancelledError:
        logger.info("Continuous fetcher stopped by cancellation.")
    except Exception as e:
        logger.exception("Continuous fetcher encountered an error.")
    finally:
        await client.close()
        await redis_mgr.disconnect()
        logger.info("Continuous fetcher gracefully shut down.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Continuous Data Fetcher Pro — Real-time OHLCV downloader",
    )
    parser.add_argument("--symbol", default=SYMBOL, help=f"Trading pair (default: {SYMBOL})")
    parser.add_argument("--timeframe", default=TIMEFRAME, help=f"Candle timeframe (default: {TIMEFRAME})")

    args = parser.parse_args()
    asyncio.run(run_continuous_fetcher(args))


if __name__ == "__main__":
    main()
