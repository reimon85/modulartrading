"""
Redis Manager — bridge between the Data Fetcher and trading bots.

Architecture:
    ┌──────────┐       ┌─────────────────┐       ┌──────────┐
    │  Fetcher  │──────▶│  Redis Manager   │◀──────│   Bots   │
    │ (1 conn)  │  push │  - Hash (tick)   │  read │ (N conn) │
    └──────────┘       │  - ZSET (OHLCV)  │       └──────────┘
                       │  - Pub/Sub       │
                       └─────────────────┘

Features:
    - Singleton connection pool (one pool per process)
    - Redis Hash for real-time ticker snapshot
    - Sorted Set (ZSET) for last MAX_CANDLES OHLCV bars (score = timestamp ms)
    - Pub/Sub publish on every tick update
    - Automatic reconnection with exponential backoff
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import redis.asyncio as aioredis

from src import config

logger = logging.getLogger("data_fetcher.redis")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TICKER_HASH_KEY = "ticker:btc_usdt:latest"
OHLCV_ZSET_KEY = "ohlcv:btc_usdt:{timeframe}"
PUBSUB_CHANNEL = "ticker:btc_usdt"
MAX_CANDLES = 2000


# ---------------------------------------------------------------------------
# Singleton connection pool
# ---------------------------------------------------------------------------
class RedisManager:
    """
    Async Redis manager with Singleton connection pool.

    Usage:
        manager = RedisManager()
        await manager.connect()
        await manager.publish_tick({...})
        await manager.disconnect()
    """

    _instance: RedisManager | None = None
    _pool: aioredis.ConnectionPool | None = None
    _client: aioredis.Redis | None = None
    _lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls) -> RedisManager:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    # -- Connection lifecycle -----------------------------------------------

    async def connect(self) -> None:
        """Create or reuse the singleton connection pool."""
        async with self._lock:
            if self._client is not None:
                try:
                    await self._client.ping()
                    logger.debug("Redis connection already alive")
                    return
                except Exception:
                    logger.warning("Stale Redis connection detected — reconnecting")
                    await self._close_internal()

            self._pool = aioredis.ConnectionPool.from_url(
                config.REDIS_URL,
                max_connections=config.REDIS_MAX_CONNECTIONS,
                decode_responses=True,
                retry_on_timeout=True,
            )
            self._client = aioredis.Redis(connection_pool=self._pool)
            await self._client.ping()
            logger.info("Redis connected → %s", config.REDIS_URL.replace(config.REDIS_PASSWORD, "***"))

    async def disconnect(self) -> None:
        """Gracefully close the pool."""
        async with self._lock:
            await self._close_internal()
            RedisManager._instance = None
            logger.info("Redis disconnected")

    async def _close_internal(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
        if self._pool:
            await self._pool.aclose()
            self._pool = None

    @property
    def client(self) -> aioredis.Redis:
        assert self._client is not None, "Call connect() first"
        return self._client

    # -- Resilient command execution ----------------------------------------

    async def _execute(self, coro_factory, *args, **kwargs) -> Any:
        """
        Execute a Redis command with automatic reconnection.

        On connection failure, retries up to MAX_RETRIES times
        with exponential backoff before re-raising.
        """
        for attempt in range(1, config.MAX_RETRIES + 1):
            try:
                return await coro_factory(*args, **kwargs)
            except (
                aioredis.ConnectionError,
                aioredis.TimeoutError,
                ConnectionResetError,
                OSError,
            ) as exc:
                logger.warning(
                    "Redis error (attempt %d/%d): %s",
                    attempt,
                    config.MAX_RETRIES,
                    exc,
                )
                if attempt == config.MAX_RETRIES:
                    raise
                await asyncio.sleep(min(2 ** attempt, 30))
                await self.connect()  # reconnect

    # -- Real-time ticker (Hash) -------------------------------------------

    async def publish_tick(self, tick: dict) -> None:
        """
        Store latest tick in a Hash AND publish via Pub/Sub.

        Expected tick dict keys:
            symbol, price, bid, ask, spread, spread_bps,
            volume_24h, funding_rate, timestamp
        """
        payload = {k: str(v) for k, v in tick.items()}

        async def _do():
            pipe = self.client.pipeline(transaction=True)
            # 1) Update the Hash with the latest snapshot
            pipe.hset(TICKER_HASH_KEY, mapping=payload)
            pipe.expire(TICKER_HASH_KEY, 60)  # TTL: stale guard
            # 2) Publish to Pub/Sub channel
            pipe.publish(PUBSUB_CHANNEL, json.dumps(tick))
            await pipe.execute()

        await self._execute(_do)
        logger.debug("Tick published → %s | price=%s", PUBSUB_CHANNEL, tick.get("price"))

    async def get_tick(self) -> dict | None:
        """Read the latest ticker snapshot from the Hash."""
        async def _do():
            data = await self.client.hgetall(TICKER_HASH_KEY)
            return data if data else None
        return await self._execute(_do)

    # -- OHLCV candle history (Sorted Set) ---------------------------------

    async def store_candles(
        self,
        candles: list[dict],
        timeframe: str = config.TIMEFRAME,
    ) -> int:
        """
        Store OHLCV candles in a Sorted Set (score = timestamp ms).

        Each member is a JSON string of the candle dict.
        Only the last MAX_CANDLES candles are kept.

        Returns the number of candles stored.
        """
        if not candles:
            return 0

        key = OHLCV_ZSET_KEY.format(timeframe=timeframe)

        async def _do():
            pipe = self.client.pipeline(transaction=True)
            for candle in candles:
                score = int(candle["timestamp"])
                # Use sort_keys=True for deterministic JSON
                member = json.dumps(candle, sort_keys=True)
                # Ensure one member per score (timestamp)
                pipe.zremrangebyscore(key, score, score)
                pipe.zadd(key, {member: score})

            # Trim to keep only the latest MAX_CANDLES
            pipe.zremrangebyrank(key, 0, -(MAX_CANDLES + 1))
            pipe.expire(key, 3600)  # TTL: 1 hour safety net
            await pipe.execute()

        await self._execute(_do)
        logger.info("Stored %d candles in %s (max %d)", len(candles), key, MAX_CANDLES)
        return len(candles)

    async def get_candles(
        self,
        timeframe: str = config.TIMEFRAME,
        limit: int = MAX_CANDLES,
    ) -> list[dict]:
        """
        Retrieve the latest N candles from the Sorted Set (newest last).

        Returns a list of candle dicts sorted by timestamp ascending.
        """
        key = OHLCV_ZSET_KEY.format(timeframe=timeframe)

        async def _do():
            raw = await self.client.zrange(key, -limit, -1)
            return [json.loads(item) for item in raw]

        return await self._execute(_do) or []

    async def get_candles_since(
        self,
        since_ms: int,
        timeframe: str = config.TIMEFRAME,
    ) -> list[dict]:
        """Retrieve candles with timestamp >= since_ms."""
        key = OHLCV_ZSET_KEY.format(timeframe=timeframe)

        async def _do():
            raw = await self.client.zrangebyscore(key, since_ms, "+inf")
            return [json.loads(item) for item in raw]

        return await self._execute(_do) or []

    # -- Health check ------------------------------------------------------

    async def health_check(self) -> dict:
        """Return connection status and key counts."""
        try:
            await self.client.ping()
            tick = await self.client.hlen(TICKER_HASH_KEY)
            candle_count = await self.client.zcard(
                OHLCV_ZSET_KEY.format(timeframe=config.TIMEFRAME)
            )
            return {
                "status": "healthy",
                "ticker_fields": tick,
                "candle_count": candle_count,
                "timestamp": int(time.time() * 1000),
            }
        except Exception as exc:
            return {"status": "unhealthy", "error": str(exc)}


# ---------------------------------------------------------------------------
# Subscriber helper (for bots to consume)
# ---------------------------------------------------------------------------
async def subscribe_ticks(callback, channel: str = PUBSUB_CHANNEL) -> None:
    """
    Subscribe to real-time tick updates.

    Usage from a bot:
        async def on_tick(tick: dict):
            print(f"New price: {tick['price']}")

        await subscribe_ticks(on_tick)

    This function blocks indefinitely, reconnecting on failure.
    """
    manager = RedisManager()
    await manager.connect()

    while True:
        try:
            pubsub = manager.client.pubsub()
            await pubsub.subscribe(channel)
            logger.info("Subscribed to %s — waiting for ticks...", channel)

            async for message in pubsub.listen():
                if message["type"] == "message":
                    tick = json.loads(message["data"])
                    await callback(tick)

        except (
            aioredis.ConnectionError,
            aioredis.TimeoutError,
            ConnectionResetError,
            OSError,
        ) as exc:
            logger.warning("Subscriber lost connection: %s — reconnecting in 3s", exc)
            await asyncio.sleep(3)
            await manager.connect()
        finally:
            try:
                await pubsub.unsubscribe(channel)
                await pubsub.aclose()
            except Exception:
                pass
