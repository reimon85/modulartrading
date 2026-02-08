"""
Trade Journal — SQLite persistence for all trade operations.

Stores opens, re-entries, closes, and PnL in data/trades.db.
Singleton pattern matching RedisManager.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time

import aiosqlite
import pandas as pd

from src import config

logger = logging.getLogger("trade_journal")

TRADE_DB = config.TRADE_DB

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange TEXT NOT NULL,
    coin TEXT NOT NULL,
    action TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL,
    size REAL NOT NULL,
    pnl REAL,
    sl_price REAL,
    tp_type TEXT,
    tp_value REAL,
    status TEXT DEFAULT 'OPEN',
    open_ts INTEGER NOT NULL,
    close_ts INTEGER,
    close_reason TEXT,
    entries_json TEXT DEFAULT '[]'
);
"""


class TradeJournal:
    """Async SQLite journal for trade persistence."""

    _instance: TradeJournal | None = None
    _lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls) -> TradeJournal:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._db = None
        return cls._instance

    async def connect(self) -> None:
        async with self._lock:
            if self._db is not None:
                return
            TRADE_DB.parent.mkdir(parents=True, exist_ok=True)
            self._db = await aiosqlite.connect(str(TRADE_DB))
            self._db.row_factory = aiosqlite.Row
            await self._db.execute(_CREATE_TABLE)
            await self._db.commit()
            logger.info("Trade journal connected → %s", TRADE_DB)

    async def disconnect(self) -> None:
        async with self._lock:
            if self._db:
                await self._db.close()
                self._db = None
            TradeJournal._instance = None
            logger.info("Trade journal disconnected")

    # ── Write operations ──────────────────────────────────────

    async def log_open(
        self,
        exchange: str,
        coin: str,
        action: str,
        entry_price: float,
        size: float,
        sl_price: float,
        tp_type: str,
        tp_value: float,
    ) -> int:
        """Record a new trade open. Returns the trade id."""
        now = int(time.time() * 1000)
        entries = [{"price": entry_price, "size": size, "ts": now}]
        cursor = await self._db.execute(
            """INSERT INTO trades
               (exchange, coin, action, entry_price, size, sl_price,
                tp_type, tp_value, status, open_ts, entries_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?)""",
            (exchange, coin, action, entry_price, size, sl_price,
             tp_type, tp_value, now, json.dumps(entries)),
        )
        await self._db.commit()
        trade_id = cursor.lastrowid
        logger.info("Journal OPEN #%d: %s %s %s size=%.6f @ $%.2f",
                     trade_id, exchange, action, coin, size, entry_price)
        return trade_id

    async def log_reentry(self, trade_id: int, price: float, size: float) -> None:
        """Add a re-entry to an existing trade."""
        row = await self._db.execute_fetchall(
            "SELECT entry_price, size, entries_json FROM trades WHERE id = ?",
            (trade_id,),
        )
        if not row:
            logger.warning("Journal: trade #%d not found for re-entry", trade_id)
            return

        old_entry_price = float(row[0][0])
        old_size = float(row[0][1])
        entries = json.loads(row[0][2] or "[]")
        entries.append({"price": price, "size": size, "ts": int(time.time() * 1000)})

        new_size = old_size + size
        total_cost = old_entry_price * old_size + price * size
        new_entry_price = total_cost / new_size if new_size > 0 else price

        await self._db.execute(
            """UPDATE trades
               SET entry_price = ?, size = ?, entries_json = ?
               WHERE id = ?""",
            (new_entry_price, new_size, json.dumps(entries), trade_id),
        )
        await self._db.commit()
        logger.info("Journal REENTRY #%d: +%.6f @ $%.2f → total=%.6f @ $%.2f",
                     trade_id, size, price, new_size, new_entry_price)

    async def log_close(self, trade_id: int, exit_price: float, reason: str) -> None:
        """Close a trade and compute PnL."""
        row = await self._db.execute_fetchall(
            "SELECT action, entry_price, size FROM trades WHERE id = ?",
            (trade_id,),
        )
        if not row:
            logger.warning("Journal: trade #%d not found for close", trade_id)
            return

        action = row[0][0]
        entry_price = float(row[0][1])
        size = float(row[0][2])

        if action == "BUY":
            pnl = (exit_price - entry_price) * size
        else:
            pnl = (entry_price - exit_price) * size

        now = int(time.time() * 1000)
        await self._db.execute(
            """UPDATE trades
               SET exit_price = ?, pnl = ?, status = 'CLOSED',
                   close_ts = ?, close_reason = ?
               WHERE id = ?""",
            (exit_price, pnl, now, reason, trade_id),
        )
        await self._db.commit()
        logger.info("Journal CLOSE #%d: exit=$%.2f | PnL=$%.2f | reason=%s",
                     trade_id, exit_price, pnl, reason)

    # ── Read operations ───────────────────────────────────────

    async def get_open_trades(self, exchange: str | None = None) -> list[dict]:
        """Get all open trades, optionally filtered by exchange."""
        if exchange:
            rows = await self._db.execute_fetchall(
                "SELECT * FROM trades WHERE status = 'OPEN' AND exchange = ?",
                (exchange,),
            )
        else:
            rows = await self._db.execute_fetchall(
                "SELECT * FROM trades WHERE status = 'OPEN'",
            )
        return [dict(r) for r in rows]

    async def get_trade_history(self, limit: int = 50) -> list[dict]:
        """Get recent trades (closed first, then open)."""
        rows = await self._db.execute_fetchall(
            """SELECT * FROM trades
               ORDER BY CASE WHEN status='CLOSED' THEN close_ts ELSE open_ts END DESC
               LIMIT ?""",
            (limit,),
        )
        return [dict(r) for r in rows]


# ── Sync read operations (for dashboard) ────────────────────

def get_all_trades_df() -> pd.DataFrame:
    """Get all trades from the journal as a DataFrame."""
    if not TRADE_DB.exists():
        return pd.DataFrame()
    try:
        con = sqlite3.connect(f"file:{TRADE_DB}?mode=ro", uri=True)
        df = pd.read_sql_query("SELECT * FROM trades", con)
        con.close()
        return df
    except sqlite3.Error:
        return pd.DataFrame()
