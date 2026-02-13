"""
Multi-Timeframe Continuous Fetcher — Keeps Redis updated with latest candles
for multiple timeframes simultaneously.

Usage:
    python -m src.continuous_fetcher --symbol BTC/USDT --timeframes 1m,15m,1h,1d
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

import pandas as pd

# Add project root to sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from src.config import TIMEFRAME_MS, SYMBOL, setup_logging
from src.database import RedisManager
from src.fetcher import ExchangeClient, fetch_funding_rate, fetch_ohlcv, fetch_spread
from src.storage import save_parquet, save_sqlite
from src.utils import enrich

logger = setup_logging()

class MultiTimeframeFetcher:
    def __init__(self, symbol: str, timeframes: list[str]):
        self.symbol = symbol
        self.timeframes = timeframes
        self.client = ExchangeClient()
        self.redis = RedisManager()
        self._running = True

    async def start(self):
        await self.client.connect()
        await self.redis.connect()
        logger.info(f"🚀 Multi-Fetcher started for {self.symbol} | TFs: {self.timeframes}")

        # Task for each timeframe
        tasks = [self._fetch_loop(tf) for tf in self.timeframes]
        # Task for global data (funding, spread)
        tasks.append(self._global_data_loop())

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Multi-Fetcher stopping...")
        finally:
            self._running = False
            await self.client.close()
            await self.redis.disconnect()

    async def _fetch_loop(self, timeframe: str):
        """Individual loop for a specific timeframe."""
        interval_ms = TIMEFRAME_MS.get(timeframe, 60_000)
        # We poll more frequently than the timeframe to ensure Redis is "live"
        # For 1d, polling every 30s is fine to see the candle grow.
        poll_sec = min(30, interval_ms / 1000.0) 
        
        logger.info(f"  [{timeframe}] loop started (poll every {poll_sec}s)")

        while self._running:
            try:
                now_ms = int(pd.Timestamp.now("UTC").timestamp() * 1000)
                
                # Fetch last 2 candles to handle the "current" open one and the last closed one
                df = await fetch_ohlcv(
                    self.client, self.symbol, timeframe, limit=2, since=now_ms - interval_ms * 3
                )

                if not df.empty:
                    # Store in Redis
                    candles = df[["timestamp", "open", "high", "low", "close", "volume"]].to_dict("records")
                    await self.redis.store_candles(candles, timeframe=timeframe)
                    
                    # Update latest tick only for the smallest timeframe (usually 1m) to avoid noise
                    # or just for all to keep them synced. We'll use 1m as the master tick.
                    if timeframe == "1m" or len(self.timeframes) == 1:
                        latest = df.iloc[-1]
                        tick = {
                            "symbol": self.symbol,
                            "price": float(latest["close"]),
                            "timestamp": int(latest["timestamp"]),
                        }
                        await self.redis.publish_tick(tick)
                
                await asyncio.sleep(poll_sec)
            except Exception as e:
                logger.error(f"Error in {timeframe} loop: {e}")
                await asyncio.sleep(10)

    async def _global_data_loop(self):
        """Update funding rate and spread every 1 minute."""
        while self._running:
            try:
                funding = await fetch_funding_rate(self.client, self.symbol)
                spread = await fetch_spread(self.client, self.symbol)
                
                # Store in Redis (we can use a dedicated key or add to the 1m tick)
                # For now, just logging or storing in a simple key is fine
                if funding is not None:
                    await self.redis.client.set(f"ticker:{self.symbol.lower()}:funding", funding)
                
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Error in global data loop: {e}")
                await asyncio.sleep(30)

async def run(args: argparse.Namespace):
    timeframes = args.timeframes.split(",")
    fetcher = MultiTimeframeFetcher(args.symbol, timeframes)
    await fetcher.start()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default=SYMBOL)
    parser.add_argument("--timeframes", default="1m,15m,1d", help="Comma separated timeframes")
    args = parser.parse_args()
    asyncio.run(run(args))

if __name__ == "__main__":
    main()