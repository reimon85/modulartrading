"""
Async data fetcher — downloads OHLCV + funding rate + order-book spreads
from any ccxt-compatible exchange with automatic gap detection and retry.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

import ccxt.async_support as ccxt
import pandas as pd

from src import config

logger = logging.getLogger("data_fetcher.fetcher")


# ---------------------------------------------------------------------------
# Token-bucket rate limiter
# ---------------------------------------------------------------------------
class TokenBucket:
    """Simple async token-bucket to respect exchange rate limits."""

    def __init__(self, rate: float, capacity: int = 1):
        self._rate = rate            # tokens per second
        self._capacity = capacity
        self._tokens = float(capacity)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last = now
            if self._tokens < 1:
                wait = (1 - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._tokens = 0
            else:
                self._tokens -= 1


# ---------------------------------------------------------------------------
# Exchange wrapper
# ---------------------------------------------------------------------------
@dataclass
class ExchangeClient:
    """Thin async wrapper around a ccxt exchange instance."""

    exchange_id: str = config.EXCHANGE_ID
    api_key: str = config.EXCHANGE_API_KEY
    secret: str = config.EXCHANGE_SECRET
    _exchange: ccxt.Exchange | None = field(default=None, init=False, repr=False)
    _bucket: TokenBucket = field(default=None, init=False, repr=False)

    async def connect(self) -> None:
        cls = getattr(ccxt, self.exchange_id)
        params: dict = {
            "enableRateLimit": True,
            "options": {"defaultType": "future"},
        }
        if self.api_key:
            params["apiKey"] = self.api_key
        if self.secret:
            params["secret"] = self.secret

        self._exchange = cls(params)
        await self._exchange.load_markets()

        # Token bucket: 1 request per RATE_LIMIT_MS
        tokens_per_sec = 1000.0 / max(config.RATE_LIMIT_MS, 1)
        self._bucket = TokenBucket(rate=tokens_per_sec, capacity=5)
        logger.info("Connected to %s (futures)", self.exchange_id)

    async def close(self) -> None:
        if self._exchange:
            await self._exchange.close()
            logger.info("Exchange connection closed")

    @property
    def exchange(self) -> ccxt.Exchange:
        assert self._exchange is not None, "Call connect() first"
        return self._exchange

    @property
    def bucket(self) -> TokenBucket:
        assert self._bucket is not None, "Call connect() first"
        return self._bucket


# ---------------------------------------------------------------------------
# OHLCV fetcher with gap detection and retry
# ---------------------------------------------------------------------------
async def fetch_ohlcv(
    client: ExchangeClient,
    symbol: str = config.SYMBOL,
    timeframe: str = config.TIMEFRAME,
    since: int | None = None,
    until: int | None = None,
    limit: int = config.FETCH_LIMIT,
) -> pd.DataFrame:
    """
    Fetch OHLCV data in paginated batches.

    Returns a DataFrame with columns:
        timestamp, open, high, low, close, volume
    All timestamps are UTC milliseconds.
    """
    ex = client.exchange
    tf_ms = config.TIMEFRAME_MS.get(timeframe, 60_000)

    all_rows: list[list] = []
    cursor = since or int((pd.Timestamp.now("UTC") - pd.Timedelta(days=7)).timestamp() * 1000)
    end = until or int(pd.Timestamp.now("UTC").timestamp() * 1000)

    logger.info(
        "Fetching OHLCV %s %s from %s to %s",
        symbol,
        timeframe,
        pd.Timestamp(cursor, unit="ms", tz="UTC"),
        pd.Timestamp(end, unit="ms", tz="UTC"),
    )

    while cursor < end:
        for attempt in range(1, config.MAX_RETRIES + 1):
            try:
                await client.bucket.acquire()
                bars = await ex.fetch_ohlcv(symbol, timeframe, since=cursor, limit=limit)
                break
            except (ccxt.NetworkError, ccxt.ExchangeNotAvailable) as exc:
                logger.warning("Attempt %d/%d failed: %s", attempt, config.MAX_RETRIES, exc)
                if attempt == config.MAX_RETRIES:
                    raise
                await asyncio.sleep(2 ** attempt)
        else:
            break

        if not bars:
            break

        all_rows.extend(bars)
        last_ts = bars[-1][0]
        cursor = last_ts + tf_ms

        if len(bars) < limit:
            break

    if not all_rows:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)

    # --- Gap detection and fill ---
    df = _detect_and_fill_gaps(df, tf_ms, client, symbol, timeframe)

    logger.info("Fetched %d bars for %s %s", len(df), symbol, timeframe)
    return df


def _detect_and_fill_gaps(
    df: pd.DataFrame,
    tf_ms: int,
    client: ExchangeClient,
    symbol: str,
    timeframe: str,
) -> pd.DataFrame:
    """Detect time-gaps and forward-fill missing bars."""
    diffs = df["timestamp"].diff()
    gaps = diffs[diffs > tf_ms]

    if gaps.empty:
        return df

    logger.warning("Detected %d gap(s) in OHLCV data — forward-filling", len(gaps))

    # Build a complete time index and reindex
    full_index = range(
        int(df["timestamp"].iloc[0]),
        int(df["timestamp"].iloc[-1]) + tf_ms,
        tf_ms,
    )
    df = df.set_index("timestamp").reindex(full_index)
    df.index.name = "timestamp"

    # Forward-fill price columns, zero-fill volume
    price_cols = ["open", "high", "low", "close"]
    df[price_cols] = df[price_cols].ffill()
    df["volume"] = df["volume"].fillna(0)

    df = df.reset_index()
    return df


# ---------------------------------------------------------------------------
# Supplementary data (funding rate & order-book spread)
# ---------------------------------------------------------------------------
async def fetch_funding_rate(
    client: ExchangeClient,
    symbol: str = config.SYMBOL,
) -> float | None:
    """Fetch the latest funding rate (futures only). Returns None if unavailable."""
    try:
        await client.bucket.acquire()
        result = await client.exchange.fetch_funding_rate(symbol)
        rate = result.get("fundingRate")
        logger.debug("Funding rate for %s: %s", symbol, rate)
        return rate
    except Exception as exc:
        logger.debug("Could not fetch funding rate: %s", exc)
        return None


async def fetch_spread(
    client: ExchangeClient,
    symbol: str = config.SYMBOL,
) -> dict | None:
    """Fetch current best bid/ask and compute spread."""
    try:
        await client.bucket.acquire()
        book = await client.exchange.fetch_order_book(symbol, limit=1)
        best_bid = book["bids"][0][0] if book["bids"] else None
        best_ask = book["asks"][0][0] if book["asks"] else None
        if best_bid and best_ask:
            spread = best_ask - best_bid
            spread_bps = (spread / best_bid) * 10_000
            return {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
                "spread_bps": spread_bps,
            }
    except Exception as exc:
        logger.debug("Could not fetch order book: %s", exc)
    return None
