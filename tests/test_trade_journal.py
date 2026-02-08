import asyncio
import pytest
import aiosqlite
import os
from pathlib import Path
from unittest.mock import patch

# Adjust the path to import from src
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database.trade_journal import TradeJournal, TRADE_DB, get_all_trades_df


@pytest.fixture
async def in_memory_trade_journal():
    """
    Fixture for an in-memory TradeJournal instance for testing.
    Uses a mock TRADE_DB to ensure in-memory database usage.
    """
    # Patch TRADE_DB to use an in-memory database for aiosqlite operations
    with patch('src.database.trade_journal.TRADE_DB', Path(":memory:")):
        journal = TradeJournal()
        await journal.connect()
        yield journal
        await journal.disconnect()
    # No need to patch.stopall() here, as `with patch` handles it.


@pytest.mark.asyncio
async def test_log_open(in_memory_trade_journal: TradeJournal):
    journal = in_memory_trade_journal
    trade_id = await journal.log_open(
        exchange="binance",
        coin="BTC",
        action="BUY",
        entry_price=100.0,
        size=0.1,
        sl_price=90.0,
        tp_type="price",
        tp_value=110.0,
    )
    assert trade_id is not None and trade_id > 0

    open_trades = await journal.get_open_trades()
    assert len(open_trades) == 1
    trade = open_trades[0]
    assert trade["id"] == trade_id
    assert trade["exchange"] == "binance"
    assert trade["coin"] == "BTC"
    assert trade["status"] == "OPEN"


@pytest.mark.asyncio
async def test_log_reentry(in_memory_trade_journal: TradeJournal):
    journal = in_memory_trade_journal
    trade_id = await journal.log_open(
        exchange="binance", coin="BTC", action="BUY", entry_price=100.0, size=0.1,
        sl_price=90.0, tp_type="price", tp_value=110.0,
    )

    await journal.log_reentry(trade_id, price=95.0, size=0.05)

    open_trades = await journal.get_open_trades()
    assert len(open_trades) == 1
    trade = open_trades[0]
    assert abs(trade["entry_price"] - 98.33333333333333) < 0.001  # (100*0.1 + 95*0.05) / (0.1+0.05)
    assert abs(trade["size"] - 0.15) < 0.001


@pytest.mark.asyncio
async def test_log_reentry_non_existent_trade(in_memory_trade_journal: TradeJournal):
    journal = in_memory_trade_journal
    # Should not raise an error, just log a warning
    await journal.log_reentry(999, price=95.0, size=0.05)
    open_trades = await journal.get_open_trades()
    assert len(open_trades) == 0


@pytest.mark.asyncio
async def test_log_close(in_memory_trade_journal: TradeJournal):
    journal = in_memory_trade_journal
    trade_id = await journal.log_open(
        exchange="binance", coin="BTC", action="BUY", entry_price=100.0, size=0.1,
        sl_price=90.0, tp_type="price", tp_value=110.0,
    )

    await journal.log_close(trade_id, exit_price=105.0, reason="TP Hit")

    open_trades = await journal.get_open_trades()
    assert len(open_trades) == 0

    trade_history = await journal.get_trade_history()
    assert len(trade_history) == 1
    closed_trade = trade_history[0]
    assert closed_trade["id"] == trade_id
    assert closed_trade["status"] == "CLOSED"
    assert closed_trade["exit_price"] == 105.0
    assert abs(closed_trade["pnl"] - ((105.0 - 100.0) * 0.1)) < 0.001  # BUY: (exit - entry) * size
    assert closed_trade["close_reason"] == "TP Hit"


@pytest.mark.asyncio
async def test_log_close_non_existent_trade(in_memory_trade_journal: TradeJournal):
    journal = in_memory_trade_journal
    # Should not raise an error, just log a warning
    await journal.log_close(999, exit_price=105.0, reason="Test")
    open_trades = await journal.get_open_trades()
    assert len(open_trades) == 0


@pytest.mark.asyncio
async def test_get_trade_history(in_memory_trade_journal: TradeJournal):
    journal = in_memory_trade_journal
    # Use explicit timestamps to control order for deterministic testing
    ts_counter = 1678886400000 # Start with a fixed timestamp

    # BTC opened first, remains open
    with patch('time.time', return_value=ts_counter / 1000):
        await journal.log_open("binance", "BTC", "BUY", 100.0, 0.1, 90.0, "price", 110.0)
    ts_counter += 1000 # Increment timestamp for next operation

    # ETH opened second, closed soon after
    with patch('time.time', return_value=ts_counter / 1000):
        trade_id_eth = await journal.log_open("binance", "ETH", "SELL", 2000.0, 0.01, 2100.0, "price", 1900.0)
    ts_counter += 1000
    with patch('time.time', return_value=ts_counter / 1000):
        await journal.log_close(trade_id_eth, 1950.0, "Manual")
    ts_counter += 1000

    # ADA opened third, closed even later
    with patch('time.time', return_value=ts_counter / 1000):
        trade_id_ada = await journal.log_open("binance", "ADA", "BUY", 0.5, 100, 0.4, "price", 0.6)
    ts_counter += 1000
    with patch('time.time', return_value=ts_counter / 1000):
        await journal.log_close(trade_id_ada, 0.55, "TP")
    ts_counter += 1000

    history = await journal.get_trade_history(limit=10)
    assert len(history) == 3
    # Expected order: ADA (most recent close_ts), ETH (second recent close_ts), BTC (open, with its open_ts)
    assert history[0]["coin"] == "ADA"
    assert history[0]["status"] == "CLOSED"
    assert history[1]["coin"] == "ETH"
    assert history[1]["status"] == "CLOSED"
    assert history[2]["coin"] == "BTC"
    assert history[2]["status"] == "OPEN"

    history_limit_1 = await journal.get_trade_history(limit=1)
    assert len(history_limit_1) == 1
    assert history_limit_1[0]["coin"] == "ADA"


@pytest.mark.asyncio
async def test_get_all_trades_df_sync(): # Renamed to clarify it's testing the sync function
    # We need a separate test case for get_all_trades_df (sync)
    # as the fixture creates an aiosqlite in-memory db which won't be seen by sqlite3.connect.
    
    # Temporary file setup for sync get_all_trades_df
    temp_db_path = Path("./temp_test_trades.db")
    try:
        # Manually create some data in the temporary DB for the sync reader
        conn = await aiosqlite.connect(str(temp_db_path)) # AWAIT ADDED HERE
        await conn.execute("""
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
        """)
        await conn.execute(
            """INSERT INTO trades (exchange, coin, action, entry_price, size, sl_price, tp_type, tp_value, open_ts)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", # 9 columns
            ("binance", "BTC", "BUY", 100.0, 0.1, 90.0, "price", 110.0, 1678886400000) # 9 values
        )
        await conn.execute(
            """INSERT INTO trades (exchange, coin, action, entry_price, size, pnl, status, open_ts, close_ts)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", # 9 columns
            ("binance", "ETH", "SELL", 2000.0, 0.01, 0.5, "CLOSED", 1678886400001, 1678886500001) # 9 values
        )
        await conn.commit()
        await conn.close()

        # Patch TRADE_DB in config and trade_journal to point to the temp_db_path
        with patch('src.database.trade_journal.TRADE_DB', temp_db_path):
            # The import needs to happen *after* the patch is active for get_all_trades_df
            # to pick up the mocked TRADE_DB path, otherwise it will use the original.
            # Using a local import here to ensure this.
            from src.database.trade_journal import get_all_trades_df as get_all_trades_df_patched
            df = get_all_trades_df_patched()
            assert not df.empty
            assert len(df) == 2
            assert "coin" in df.columns
            assert "pnl" in df.columns
            assert df["coin"].iloc[0] == "BTC"
            assert df["status"].iloc[1] == "CLOSED"
    finally:
        if temp_db_path.exists():
            os.remove(temp_db_path)
