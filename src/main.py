"""
Data Fetcher Pro — entry point.

Downloads BTC/USDT OHLCV data from a ccxt-compatible exchange, enriches it
with IA-ready features (ATR, log returns, volatility, funding rate, spread),
and persists to Parquet + SQLite.

Usage:
    python -m src.main                         # last 7 days, 1m bars
    python -m src.main --days 30               # last 30 days
    python -m src.main --timeframe 5m --days 7 # 5-minute bars
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from src.config import setup_logging
from src.database import RedisManager
from src.fetcher import ExchangeClient, fetch_funding_rate, fetch_ohlcv, fetch_spread
from src.storage import save_parquet, save_sqlite
from src.utils import enrich

import pandas as pd

logger = setup_logging()


async def run(args: argparse.Namespace) -> None:
    client = ExchangeClient()
    redis_mgr = RedisManager()

    try:
        await client.connect()
        await redis_mgr.connect()

        # Time range
        now_ms = int(pd.Timestamp.now("UTC").timestamp() * 1000)
        since_ms = now_ms - (args.days * 86_400_000)

        # 1. Fetch OHLCV -------------------------------------------------
        df = await fetch_ohlcv(
            client,
            symbol=args.symbol,
            timeframe=args.timeframe,
            since=since_ms,
            until=now_ms,
        )

        if df.empty:
            logger.error("No data returned — exiting")
            return

        # 2. Supplementary data ------------------------------------------
        funding_rate = await fetch_funding_rate(client, symbol=args.symbol)
        spread_info = await fetch_spread(client, symbol=args.symbol)

        # 3. Enrich with IA-ready features -------------------------------
        df = enrich(df, funding_rate=funding_rate, spread_info=spread_info)

        # 4. Persist to Parquet + SQLite ---------------------------------
        await save_parquet(df, symbol=args.symbol, timeframe=args.timeframe)
        await save_sqlite(df, symbol=args.symbol, timeframe=args.timeframe)

        # 5. Push to Redis -----------------------------------------------
        # 5a. Store last 100 candles in Sorted Set
        last_100 = df.tail(100)
        candles = last_100[["timestamp", "open", "high", "low", "close", "volume"]].to_dict("records")
        await redis_mgr.store_candles(candles, timeframe=args.timeframe)

        # 5b. Publish latest tick via Hash + Pub/Sub
        latest = df.iloc[-1]
        tick = {
            "symbol": args.symbol,
            "price": float(latest["close"]),
            "open": float(latest["open"]),
            "high": float(latest["high"]),
            "low": float(latest["low"]),
            "close": float(latest["close"]),
            "volume": float(latest["volume"]),
            "funding_rate": funding_rate,
            "timestamp": int(latest["timestamp"]),
        }
        if spread_info:
            tick.update(spread_info)
        await redis_mgr.publish_tick(tick)

        logger.info("Pipeline complete — %d enriched bars stored + Redis updated", len(df))

    finally:
        await client.close()
        await redis_mgr.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Data Fetcher Pro — BTC/USDT OHLCV downloader",
    )
    parser.add_argument("--symbol", default="BTC/USDT", help="Trading pair (default: BTC/USDT)")
    parser.add_argument("--timeframe", default="1m", help="Candle timeframe (default: 1m)")
    parser.add_argument("--days", type=int, default=7, help="Number of days to fetch (default: 7)")

    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
