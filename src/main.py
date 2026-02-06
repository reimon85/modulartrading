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
from src.fetcher import ExchangeClient, fetch_funding_rate, fetch_ohlcv, fetch_spread
from src.storage import save_parquet, save_sqlite
from src.utils import enrich

import pandas as pd

logger = setup_logging()


async def run(args: argparse.Namespace) -> None:
    client = ExchangeClient()

    try:
        await client.connect()

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

        # 4. Persist -----------------------------------------------------
        await save_parquet(df, symbol=args.symbol, timeframe=args.timeframe)
        await save_sqlite(df, symbol=args.symbol, timeframe=args.timeframe)

        logger.info("Pipeline complete — %d enriched bars stored", len(df))

    finally:
        await client.close()


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
