# ModularTrading

**Automated trading infrastructure for Hyperliquid DEX.**
Async data pipeline, rule-based strategy engine, and smart order execution with dynamic risk management.

```
                    Market Data                  Signals                  Orders
   Binance ──────► Data Fetcher ──────► Redis ──────► Strategy ──────► Executor ──────► Hyperliquid
    (CCXT)          (async)            (pub/sub)    (EMA + RSI)       (limit/market)      (DEX)
```

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              MODULARTRADES                                          │
│                                                                                     │
│  ┌──────────────┐    ┌──────────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │ Data Fetcher │    │      Redis       │    │   Strategy   │    │   Executor   │  │
│  │              │    │                  │    │              │    │              │  │
│  │ CCXT async   │───►│ Hash  (tick)     │───►│ EMA crossover│───►│ Limit entry  │  │
│  │ Rate limiter │    │ ZSET  (OHLCV)   │    │ RSI filter   │    │ Trigger SL   │  │
│  │ Gap detect   │    │ Pub/Sub (stream) │    │ Alert system │    │ TP price/time│  │
│  │ Enrichment   │    │ State (executor) │    │              │    │ Auto-resume  │  │
│  └──────────────┘    └────────┬─────────┘    └──────────────┘    └──────────────┘  │
│                               │                                                     │
│                    ┌──────────▼─────────┐                                           │
│                    │    Dashboard       │                                            │
│                    │    (Streamlit)     │                                            │
│                    │                   │                                            │
│                    │ Live chart         │                                            │
│                    │ Position monitor   │                                            │
│                    │ Kill switch        │                                            │
│                    └───────────────────┘                                            │
│                                                                                     │
│  ┌──────────────┐    ┌──────────────┐                                              │
│  │  Backtester  │    │   Launcher   │                                              │
│  │              │    │              │                                              │
│  │ Bar-by-bar   │    │ Orchestrator │                                              │
│  │ Grid search  │    │ Auto-restart │                                              │
│  │ Plotly chart │    │ Graceful stop│                                              │
│  └──────────────┘    └──────────────┘                                              │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
1. FETCH     Binance API ──► OHLCV + ticker + funding rate
2. ENRICH    Raw bars ──► ATR, log returns, relative volatility, spread (bps)
3. PERSIST   Enriched data ──► Parquet + SQLite + Redis (ZSET + Hash + Pub/Sub)
4. ANALYZE   Strategy reads Redis ──► computes EMA/RSI ──► emits BUY/SELL/WAIT
5. EXECUTE   Executor subscribes to signal channel ──► places orders on Hyperliquid
6. MONITOR   Dashboard polls Redis every 5s ──► renders chart, positions, health LEDs
```

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| **Runtime** | Python 3.10+ / asyncio |
| **Exchange Data** | CCXT (async) |
| **Message Broker** | Redis 7 (Pub/Sub + ZSET + Hash) |
| **Order Execution** | Hyperliquid Python SDK |
| **Backtesting** | NumPy + Pandas (pure, no pandas_ta) |
| **Dashboard** | Streamlit + Plotly |
| **Storage** | Parquet (columnar) + SQLite (WAL) |
| **Containers** | Docker Compose (Redis) |
| **Alerts** | Discord Webhooks + Telegram Bot API |

---

## Project Structure

```
modulartrading/
├── launcher.py              # Master orchestrator — starts everything
├── my_strategy.py           # Strategy engine — your trading rules
├── docker-compose.yml       # Redis 7 Alpine
├── requirements.txt
├── .env                     # Configuration (gitignored)
│
├── src/
│   ├── config.py            # Typed settings from .env
│   ├── main.py              # Data Fetcher CLI entry point
│   ├── fetcher.py           # Async OHLCV download + gap detection
│   ├── storage.py           # Parquet + SQLite persistence
│   ├── utils.py             # Feature engineering (ATR, log returns)
│   ├── log_rotation.py      # Daily rotating log handler
│   └── database/
│       └── redis_client.py  # Singleton async Redis manager
│
├── backtesting/
│   ├── engine.py            # Universal backtesting engine + optimizer
│   └── example_btc.py       # EMA+RSI strategy example
│
├── executor/
│   └── hl_smart_executor.py # Hyperliquid order executor
│
├── dashboard/
│   └── app.py               # Real-time Streamlit dashboard
│
└── data/                    # Parquet, SQLite, charts (gitignored)
```

---

## Quick Start

```bash
# Clone
git clone https://github.com/reimon85/modulartrading.git
cd modulartrading

# Dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Redis
docker compose up -d

# Configure
cp .env.example .env    # edit with your keys

# Launch everything
python launcher.py
```

### Individual Components

```bash
# Data Fetcher only (one-shot, downloads last 7 days)
python -m src.main --days 7

# Strategy in live mode (reads from Redis)
python my_strategy.py live --poll 10

# Strategy in backtest mode
python my_strategy.py backtest --file data/BTC_USDT_1m.parquet

# Backtesting engine with grid optimization
python -m backtesting.example_btc --optimize grid

# Executor in dry-run mode (no real orders)
python -m executor.hl_smart_executor --dry-run

# Dashboard
streamlit run dashboard/app.py --server.port 8501

# System status check
python launcher.py --status
```

### Launcher Modes

| Flag | Behavior |
|------|----------|
| *(no flags)* | Full pipeline: Fetcher + Strategy + Executor |
| `--dry-run` | Executor simulates orders (no real trades) |
| `--no-executor` | Data + Strategy only |
| `--no-strategy` | Data + Executor only |
| `--fetcher-only` | One-shot data download |
| `--status` | Print system health and exit |

---

## Configuration

All settings are loaded from `.env` via `src/config.py`. Sensitive values are **never** committed to the repository.

### Core

| Variable | Default | Description |
|----------|---------|-------------|
| `SYMBOL` | `BTC/USDT` | Trading pair |
| `TIMEFRAME` | `1m` | Candle interval (`1m`, `5m`, `15m`, `1h`, `4h`, `1d`) |
| `EXCHANGE_ID` | `binance` | Data source (any ccxt-supported exchange) |

### Redis

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis server host |
| `REDIS_PORT` | `6379` | Redis server port |
| `REDIS_PASSWORD` | *(empty)* | Redis auth password |

### Hyperliquid

| Variable | Default | Description |
|----------|---------|-------------|
| `HL_PRIVATE_KEY` | *(empty)* | Wallet private key. **Empty = automatic dry-run** |
| `HL_WALLET_ADDRESS` | *(empty)* | Wallet public address |
| `HL_MAINNET` | `false` | `true` for mainnet, `false` for testnet |
| `HL_POSITION_FRAC` | `0.10` | Fraction of balance per trade (10%) |
| `HL_LIMIT_OFFSET_BPS` | `5` | Limit order offset from mid price (basis points) |
| `HL_LIMIT_WAIT_SEC` | `10` | Seconds to wait for limit fill before market fallback |
| `HL_SIGNAL_CHANNEL` | `trading_signals` | Redis Pub/Sub channel for trade signals |

### Alerts

| Variable | Default | Description |
|----------|---------|-------------|
| `DISCORD_WEBHOOK_URL` | *(empty)* | Discord channel webhook |
| `TELEGRAM_BOT_TOKEN` | *(empty)* | Telegram bot token |
| `TELEGRAM_CHAT_ID` | *(empty)* | Telegram chat/group ID |

> **Security**: Never share your `HL_PRIVATE_KEY`. If the key is missing or empty, the executor automatically activates **dry-run mode** — no real orders are sent.

---

## Monitoring Dashboard

The Streamlit dashboard (`dashboard/app.py`) provides a real-time control panel with **5-second auto-refresh**:

```
┌─────────────────────────────────────────────────────────────┐
│  MODULARTRADES                          2025-02-07 12:00 UTC│
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  BTC/USDT  $97,432.10     System Health                    │
│                            ● Redis  ● Fetcher  ● Strategy  │
│                            ● Executor                       │
│                                                             │
│  24h Change   Volume     Spread    Funding    Positions     │
│   +2.41%      $28.3B    0.42 bps   0.0100%      1          │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│  Open Positions                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ BTC  LONG  Entry: $96,100  SL: $95,200             │   │
│  │ TP: TIME (4320 min)  Countdown: 2d 14h  PnL: +1.38%│   │
│  │ ████████████████░░░░░░░░░░░░░░░  42%               │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│  Price Chart (candlestick + volume + signal arrows)         │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           ╱╲                                        │   │
│  │      ╱╲  ╱  ╲    ▲ BUY                             │   │
│  │  ╱╲ ╱  ╲╱    ╲╱╲                                   │   │
│  │ ╱  ╲          ▼ SELL                                │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         EMERGENCY CLOSE ALL                         │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**Key features**:
- **Health LEDs** — green/yellow/red per component (Redis, Fetcher, Strategy, Executor)
- **Position cards** — real-time unrealized PnL, TP countdown with progress bar
- **TP Time tracking** — supports minutes, hours, days; displays `Xd Xh` or `H:MM:SS` countdown
- **Signal arrows** — BUY/SELL markers overlaid on the candlestick chart
- **Kill Switch** — two-step confirmation emergency close, publishes `CLOSE` signal via Redis

---

## Backtesting

The backtesting engine (`backtesting/engine.py`) runs **bar-by-bar simulation** with realistic commission modeling.

```bash
# Run with default EMA(9/21) + RSI(14) strategy
python -m backtesting.example_btc

# Grid search optimization
python -m backtesting.example_btc --optimize grid

# Random search (50 trials)
python -m backtesting.example_btc --optimize random --trials 50
```

**Metrics computed**: Net Profit, Win Rate, Sharpe Ratio, Sortino Ratio, Calmar Ratio, Max Drawdown, Profit Factor, Drawdown Periods.

**Output**: Text report (AI/LLM-ready) + interactive Plotly HTML chart with equity curve, drawdown, and trade markers.

---

## Signal Protocol

The executor listens to Redis Pub/Sub channel `trading_signals` for JSON signals:

```json
// Open long with 1.5x leverage, SL at $95000, TP by price at $100000
{"action": "BUY", "pair": "BTC/USDT", "multiplier": 1.5, "sl_price": 95000, "tp_type": "PRICE", "tp_value": 100000}

// Open short, TP by time (3 hours = 180 minutes)
{"action": "SELL", "pair": "BTC/USDT", "multiplier": 1.0, "sl_price": 99000, "tp_type": "TIME", "tp_value": 180}

// Emergency close
{"action": "CLOSE", "pair": "BTC/USDT"}
```

| Field | Type | Description |
|-------|------|-------------|
| `action` | string | `BUY`, `SELL`, or `CLOSE` |
| `pair` | string | Trading pair (e.g., `BTC/USDT`) |
| `multiplier` | float | Leverage and position sizing multiplier |
| `sl_price` | float | Stop-loss trigger price |
| `tp_type` | string | `PRICE` (trigger order) or `TIME` (auto-close after N minutes) |
| `tp_value` | float | Target price or duration in **minutes** (180 = 3h, 1440 = 1d, 10080 = 7d) |

---

## Safety & Compliance

> **Warning**: Cryptocurrency trading involves substantial risk of loss. This software is provided as-is for educational and research purposes. Past performance does not guarantee future results. You are solely responsible for your trading decisions.

### Built-in Safeguards

| Safeguard | Description |
|-----------|-------------|
| **Automatic Dry-Run** | Empty `HL_PRIVATE_KEY` activates simulation mode — zero real orders |
| **Kill Switch** | Dashboard button publishes `CLOSE` signal, cancels all open orders and market-closes positions |
| **Position Limits** | `HL_POSITION_FRAC` caps each trade to a fraction of total balance (default: 10%) |
| **State Persistence** | Executor state survives restarts via Redis — resumes TP timers and monitors positions |
| **Crash Recovery** | Launcher auto-restarts crashed components (max 5 attempts per process) |
| **Graceful Shutdown** | SIGTERM/SIGINT propagates cleanly — cancels orders, closes connections |
| **Entry Fallback** | Limit order with timeout → automatic market order fallback if not filled |
| **SL Monitor** | Background loop detects externally filled stop-losses and cleans up internal state |

---

## License

MIT
