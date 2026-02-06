# Trading Data Fetcher Pro

High-performance async data fetcher for **BTC/USDT** OHLCV data, designed for quantitative research and ML/AI model training.

## Features

- **Async architecture** — built on `asyncio` + `ccxt` for non-blocking I/O
- **Gap detection** — automatically identifies missing bars and forward-fills them
- **Rate limiting** — token-bucket algorithm respects exchange limits
- **Dual storage** — Parquet (columnar, fast for ML) + SQLite (relational, concurrent-safe with WAL)
- **Atomic writes** — temp-file + rename pattern prevents data corruption
- **IA-ready columns** — log returns, ATR, relative volatility, funding rate, bid-ask spread

## Project Structure

```
trading-data-fetcher/
├── .env                 # API keys & config (gitignored)
├── .gitignore
├── requirements.txt
├── README.md
├── src/
│   ├── main.py          # Entry point & CLI
│   ├── config.py        # Settings from .env + logging
│   ├── fetcher.py       # Async OHLCV download, gap detection, retry
│   ├── storage.py       # Parquet & SQLite persistence
│   └── utils.py         # Feature engineering & data cleaning
└── data/                # Output directory (gitignored)
    ├── BTC_USDT_1m.parquet
    └── ohlcv.db
```

## Quick Start

```bash
# 1. Clone & enter the project
cd trading-data-fetcher

# 2. Create a virtual environment
python -m venv .venv && source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure (optional — public endpoints work without API keys)
cp .env .env.local   # edit API keys if needed

# 5. Run
python -m src.main                          # last 7 days, 1-minute bars
python -m src.main --days 30                # last 30 days
python -m src.main --timeframe 5m --days 14 # 5-min bars, 14 days
```

## Configuration (.env)

| Variable | Default | Description |
|---|---|---|
| `EXCHANGE_ID` | `binance` | Any ccxt-supported exchange |
| `SYMBOL` | `BTC/USDT` | Trading pair |
| `TIMEFRAME` | `1m` | Candle interval (`1m`, `5m`, `15m`, `1h`, `4h`, `1d`) |
| `FETCH_LIMIT` | `1000` | Bars per API request |
| `MAX_RETRIES` | `3` | Retry count on network errors |
| `RATE_LIMIT_MS` | `100` | Min interval between requests (ms) |
| `LOG_LEVEL` | `INFO` | Logging verbosity |

## Output Schema

Each row in the Parquet/SQLite output contains:

| Column | Type | Description |
|---|---|---|
| `timestamp` | int | Unix epoch (ms) |
| `datetime` | datetime | UTC timestamp |
| `open` | float | Bar open price |
| `high` | float | Bar high price |
| `low` | float | Bar low price |
| `close` | float | Bar close price |
| `volume` | float | Traded volume |
| `log_return` | float | ln(close_t / close_{t-1}) |
| `atr` | float | Average True Range (14-period) |
| `relative_volatility` | float | Rolling σ(returns) / mean(\|returns\|) |
| `funding_rate` | float | Perpetual funding rate (if available) |
| `best_bid` | float | Best bid at fetch time |
| `best_ask` | float | Best ask at fetch time |
| `spread` | float | Bid-ask spread (absolute) |
| `spread_bps` | float | Spread in basis points |

## Data Quality

- **Gap handling**: Missing bars are detected via timestamp diffs and forward-filled (price) / zero-filled (volume)
- **Deduplication**: Duplicate timestamps are removed automatically
- **UTC normalization**: All timestamps are stored in UTC
- **Atomic writes**: Parquet files are written to a temp file first, then renamed — no partial files on crash

## License

MIT
