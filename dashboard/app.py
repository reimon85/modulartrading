"""
ModularTrades — Real-time monitoring dashboard.

Run:
    streamlit run dashboard/app.py --server.port 8501 --server.headless true
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ---------------------------------------------------------------------------
# Project paths — resolve relative to this file
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(_PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Page config (MUST be first Streamlit call)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="ModularTrades",
    page_icon="chart_with_upwards_trend",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Auto-refresh every 5 seconds
st_autorefresh(interval=5_000, limit=None, key="auto_refresh")

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    /* Header bar */
    .header-bar {
        background: linear-gradient(135deg, #0e1117 0%, #1a1a2e 100%);
        border-bottom: 3px solid #e94560;
        padding: 0.8rem 1.5rem;
        margin: -1rem -1rem 1rem -1rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .header-title {
        font-size: 1.6rem;
        font-weight: 700;
        letter-spacing: 4px;
        color: #e94560;
        font-family: monospace;
    }
    .header-clock {
        font-size: 0.9rem;
        color: #888;
        font-family: monospace;
    }

    /* LED indicators */
    .led {
        display: inline-block;
        width: 10px;
        height: 10px;
        border-radius: 50%;
        margin-right: 6px;
        vertical-align: middle;
    }
    .led-green { background: #00ff88; box-shadow: 0 0 6px #00ff88; }
    .led-red { background: #ff4444; box-shadow: 0 0 6px #ff4444; }
    .led-yellow { background: #ffaa00; box-shadow: 0 0 6px #ffaa00; }

    /* Position cards */
    .pos-card {
        background: #1a1a2e;
        border: 1px solid #2a2a4e;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 0.5rem;
    }

    /* Emergency button */
    .stButton > button[kind="primary"] {
        background-color: #ff4444 !important;
        color: white !important;
        border: none !important;
        font-weight: 700 !important;
    }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* Metric styling */
    [data-testid="stMetricValue"] {
        font-family: monospace;
        font-size: 1.3rem;
    }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Redis connection (sync, cached)
# ---------------------------------------------------------------------------
@st.cache_resource
def get_redis():
    """Create a sync Redis connection shared across reruns."""
    import redis

    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    password = os.getenv("REDIS_PASSWORD", "")
    db = int(os.getenv("REDIS_DB", "0"))

    return redis.Redis(
        host=host,
        port=port,
        password=password or None,
        db=db,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    )


def safe_redis_call(func, *args, default=None, **kwargs):
    """Execute a Redis command, returning default on any failure."""
    try:
        return func(*args, **kwargs)
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------
def get_tick(r) -> dict | None:
    """Read latest tick snapshot from Redis hash."""
    return safe_redis_call(r.hgetall, "ticker:btc_usdt:latest", default=None)


def get_candles(r, timeframe: str = "1m", limit: int = 100) -> list[dict]:
    """Read OHLCV candles from Redis ZSET."""
    key = f"ohlcv:btc_usdt:{timeframe}"
    raw = safe_redis_call(r.zrange, key, -limit, -1, default=[])
    if not raw:
        return []
    return [json.loads(item) for item in raw]


def get_active_positions(r) -> list[dict]:
    """Read all active positions from Redis."""
    pairs = safe_redis_call(r.smembers, "executor:active_pairs", default=set())
    positions = []
    for pair in pairs:
        coin = pair.replace("/", "_").replace("USDT", "").replace("_", "").upper()
        data = safe_redis_call(r.hgetall, f"executor:position:{coin}", default=None)
        if data:
            data["coin"] = coin
            positions.append(data)
    return positions


def check_health(r) -> dict[str, str]:
    """Check health of each component. Returns status per component."""
    health = {}

    # Redis
    pong = safe_redis_call(r.ping, default=False)
    health["Redis"] = "green" if pong else "red"

    # Fetcher: tick timestamp freshness < 120s
    tick = get_tick(r)
    if tick and tick.get("timestamp"):
        try:
            ts = float(tick["timestamp"]) / 1000
            age = time.time() - ts
            health["Fetcher"] = "green" if age < 120 else "yellow"
        except (ValueError, TypeError):
            health["Fetcher"] = "red"
    else:
        health["Fetcher"] = "red"

    # Strategy: candle data exists + tick data exists
    candle_count = safe_redis_call(
        r.zcard, "ohlcv:btc_usdt:1m", default=0
    )
    if candle_count and candle_count > 0 and tick:
        health["Strategy"] = "green"
    elif candle_count and candle_count > 0:
        health["Strategy"] = "yellow"
    else:
        health["Strategy"] = "red"

    # Executor: check if subscribed to trading_signals
    numsub = safe_redis_call(r.pubsub_numsub, "trading_signals", default=[])
    if numsub:
        # numsub returns list of (channel, count) tuples
        count = numsub[0][1] if numsub else 0
        health["Executor"] = "green" if count > 0 else "red"
    else:
        health["Executor"] = "red"

    return health


def format_countdown(entry_ts: float, tp_target_ts: float) -> tuple[str, float]:
    """Format TP countdown and return (text, progress 0-1)."""
    now = time.time()
    total = tp_target_ts - entry_ts
    elapsed = now - entry_ts
    remaining = tp_target_ts - now

    if total <= 0:
        return "N/A", 0.0

    progress = max(0.0, min(1.0, elapsed / total))

    if remaining <= 0:
        return "EXPIRED", 1.0
    elif remaining > 86400:
        days = int(remaining // 86400)
        hours = int((remaining % 86400) // 3600)
        return f"{days}d {hours}h", progress
    else:
        hours = int(remaining // 3600)
        mins = int((remaining % 3600) // 60)
        secs = int(remaining % 60)
        return f"{hours}:{mins:02d}:{secs:02d}", progress


def load_signals_log() -> pd.DataFrame | None:
    """Load signals log Parquet if it exists."""
    path = _PROJECT_ROOT / "data" / "signals_log.parquet"
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception:
            return None
    return None


def load_parquet_fallback(timeframe: str = "1m") -> pd.DataFrame | None:
    """Load main Parquet file as fallback for chart data."""
    path = _PROJECT_ROOT / "data" / f"BTC_USDT_{timeframe}.parquet"
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception:
            return None
    return None


# ---------------------------------------------------------------------------
# LED helper
# ---------------------------------------------------------------------------
def led_html(label: str, status: str) -> str:
    """Return HTML for a colored LED indicator."""
    return f'<span class="led led-{status}"></span>{label}'


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════

r = get_redis()

# ---------------------------------------------------------------------------
# 1. Header
# ---------------------------------------------------------------------------
utc_now = datetime.now(timezone.utc)
st.markdown(f"""
<div class="header-bar">
    <span class="header-title">MODULARTRADES</span>
    <span class="header-clock">{utc_now:%Y-%m-%d %H:%M:%S} UTC</span>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# 2. Top row: Price + Health LEDs
# ---------------------------------------------------------------------------
tick = get_tick(r)
health = check_health(r)

col_price, col_health = st.columns([1, 2])

with col_price:
    if tick and tick.get("price"):
        price = float(tick["price"])
        st.metric("BTC/USDT", f"${price:,.2f}")
    else:
        st.metric("BTC/USDT", "N/A")

with col_health:
    leds = " &nbsp;&nbsp; ".join(
        led_html(name, status) for name, status in health.items()
    )
    st.markdown(f"**System Health** &nbsp;&nbsp; {leds}", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# 3. Metrics row
# ---------------------------------------------------------------------------
col1, col2, col3, col4, col5 = st.columns(5)

if tick:
    # 24h Change
    with col1:
        change = tick.get("change_24h")
        if change:
            val = float(change)
            st.metric("24h Change", f"{val:+.2f}%")
        else:
            st.metric("24h Change", "N/A")

    # Volume
    with col2:
        vol = tick.get("volume_24h")
        if vol:
            v = float(vol)
            if v >= 1_000_000_000:
                st.metric("24h Volume", f"${v / 1e9:.2f}B")
            elif v >= 1_000_000:
                st.metric("24h Volume", f"${v / 1e6:.1f}M")
            else:
                st.metric("24h Volume", f"${v:,.0f}")
        else:
            st.metric("24h Volume", "N/A")

    # Spread
    with col3:
        spread = tick.get("spread_bps")
        if spread:
            st.metric("Spread", f"{float(spread):.2f} bps")
        else:
            st.metric("Spread", "N/A")

    # Funding Rate
    with col4:
        funding = tick.get("funding_rate")
        if funding:
            st.metric("Funding Rate", f"{float(funding) * 100:.4f}%")
        else:
            st.metric("Funding Rate", "N/A")

    # Open Positions
    with col5:
        positions = get_active_positions(r)
        st.metric("Open Positions", len(positions))
else:
    for c in [col1, col2, col3, col4, col5]:
        with c:
            st.metric(c._provided_label if hasattr(c, '_provided_label') else "-", "N/A")
    positions = []

# ---------------------------------------------------------------------------
# 4. Open Positions
# ---------------------------------------------------------------------------
if not tick:
    positions = get_active_positions(r)

if positions:
    st.markdown("### Open Positions")
    for pos in positions:
        coin = pos.get("coin", "?")
        side = pos.get("side", "?")
        entry_price = pos.get("entry_price", "0")
        size = pos.get("size", "0")
        sl = pos.get("sl_price", "N/A")
        tp_type = pos.get("tp_type", "N/A")
        tp_value = pos.get("tp_value", "N/A")

        side_color = "#00ff88" if side == "BUY" else "#ff4444"

        # Unrealized PnL
        pnl_text = "N/A"
        if tick and tick.get("price") and entry_price != "0":
            try:
                current = float(tick["price"])
                entry = float(entry_price)
                if side == "BUY":
                    pnl_pct = ((current - entry) / entry) * 100
                else:
                    pnl_pct = ((entry - current) / entry) * 100
                pnl_color = "#00ff88" if pnl_pct >= 0 else "#ff4444"
                pnl_text = f'<span style="color:{pnl_color}">{pnl_pct:+.2f}%</span>'
            except (ValueError, ZeroDivisionError):
                pass

        # TP Time countdown
        countdown_html = ""
        progress_val = None
        entry_ts = pos.get("entry_ts")
        tp_target_ts = pos.get("tp_target_ts")
        if tp_type == "time" and entry_ts and tp_target_ts:
            try:
                text, progress_val = format_countdown(float(entry_ts), float(tp_target_ts))
                countdown_html = f" | Countdown: <b>{text}</b>"
            except (ValueError, TypeError):
                pass

        st.markdown(f"""
<div class="pos-card">
    <b>{coin}</b> &nbsp;
    <span style="color:{side_color}; font-weight:700">{side}</span> &nbsp;
    Entry: ${float(entry_price):,.2f} &nbsp;
    Size: {size} &nbsp;
    SL: {sl} &nbsp;
    TP: {tp_type} ({tp_value}){countdown_html} &nbsp;
    PnL: {pnl_text}
</div>
""", unsafe_allow_html=True)

        if progress_val is not None:
            st.progress(progress_val)

# ---------------------------------------------------------------------------
# 5. Candlestick Chart
# ---------------------------------------------------------------------------
st.markdown("### Price Chart")

candles = get_candles(r)
df_chart = None

if candles:
    df_chart = pd.DataFrame(candles)
else:
    df_chart = load_parquet_fallback()

if df_chart is not None and not df_chart.empty:
    # Ensure numeric types
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df_chart.columns:
            df_chart[col] = pd.to_numeric(df_chart[col], errors="coerce")

    if "timestamp" in df_chart.columns:
        df_chart["dt"] = pd.to_datetime(df_chart["timestamp"], unit="ms", utc=True)
    elif "datetime" in df_chart.columns:
        df_chart["dt"] = pd.to_datetime(df_chart["datetime"], utc=True)
    else:
        df_chart["dt"] = range(len(df_chart))

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.75, 0.25],
    )

    # Candlestick
    fig.add_trace(
        go.Candlestick(
            x=df_chart["dt"],
            open=df_chart["open"],
            high=df_chart["high"],
            low=df_chart["low"],
            close=df_chart["close"],
            name="BTC/USDT",
            increasing_line_color="#00d4aa",
            decreasing_line_color="#e94560",
        ),
        row=1, col=1,
    )

    # Volume bars
    if "volume" in df_chart.columns:
        colors = [
            "#00d4aa" if c >= o else "#e94560"
            for c, o in zip(df_chart["close"], df_chart["open"])
        ]
        fig.add_trace(
            go.Bar(
                x=df_chart["dt"],
                y=df_chart["volume"],
                name="Volume",
                marker_color=colors,
                opacity=0.5,
            ),
            row=2, col=1,
        )

    # Signal arrows from signals_log.parquet
    signals_df = load_signals_log()
    if signals_df is not None and not signals_df.empty:
        signals_df["dt"] = pd.to_datetime(signals_df["timestamp"], unit="ms", utc=True)

        # Filter to chart time range
        if "dt" in df_chart.columns:
            chart_start = df_chart["dt"].min()
            signals_df = signals_df[signals_df["dt"] >= chart_start]

        buys = signals_df[signals_df["signal"] == "BUY"]
        sells = signals_df[signals_df["signal"] == "SELL"]

        if not buys.empty:
            fig.add_trace(
                go.Scatter(
                    x=buys["dt"],
                    y=buys["price"],
                    mode="markers",
                    marker=dict(
                        symbol="triangle-up",
                        size=14,
                        color="#00ff88",
                        line=dict(width=1, color="white"),
                    ),
                    name="BUY",
                ),
                row=1, col=1,
            )

        if not sells.empty:
            fig.add_trace(
                go.Scatter(
                    x=sells["dt"],
                    y=sells["price"],
                    mode="markers",
                    marker=dict(
                        symbol="triangle-down",
                        size=14,
                        color="#ff4444",
                        line=dict(width=1, color="white"),
                    ),
                    name="SELL",
                ),
                row=1, col=1,
            )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        height=500,
        margin=dict(l=50, r=20, t=30, b=30),
        xaxis_rangeslider_visible=False,
        showlegend=False,
        font=dict(family="monospace", color="#ffffff"),
    )
    fig.update_xaxes(gridcolor="#1a1a2e")
    fig.update_yaxes(gridcolor="#1a1a2e")

    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No candle data available. Start the Data Fetcher to populate Redis.")

# ---------------------------------------------------------------------------
# 6. Emergency Close
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("### Emergency Close")

if "confirm_close" not in st.session_state:
    st.session_state.confirm_close = False

col_btn, col_status = st.columns([1, 3])

with col_btn:
    if not st.session_state.confirm_close:
        if st.button("EMERGENCY CLOSE ALL", type="primary"):
            st.session_state.confirm_close = True
            st.rerun()
    else:
        st.warning("Are you sure? This will close ALL positions.")
        col_yes, col_no = st.columns(2)
        with col_yes:
            if st.button("YES, CLOSE ALL", type="primary"):
                signal = json.dumps({
                    "action": "CLOSE",
                    "pair": "BTC/USDT",
                    "source": "dashboard",
                    "timestamp": int(time.time() * 1000),
                })
                result = safe_redis_call(
                    r.publish, "trading_signals", signal, default=0
                )
                st.session_state.confirm_close = False
                if result:
                    st.success(f"Close signal published (received by {result} subscriber(s))")
                else:
                    st.error("Failed to publish signal — check Redis connection")
                st.rerun()
        with col_no:
            if st.button("Cancel"):
                st.session_state.confirm_close = False
                st.rerun()

# ---------------------------------------------------------------------------
# 7. System Info (expandable)
# ---------------------------------------------------------------------------
with st.expander("System Info"):
    info_cols = st.columns(3)

    with info_cols[0]:
        st.markdown("**Redis**")
        redis_info = safe_redis_call(r.info, "server", default={})
        if redis_info:
            st.text(f"Version: {redis_info.get('redis_version', 'N/A')}")
            st.text(f"Uptime: {redis_info.get('uptime_in_seconds', 0) // 3600}h")
        else:
            st.text("Disconnected")

    with info_cols[1]:
        st.markdown("**Data**")
        candle_count = safe_redis_call(r.zcard, "ohlcv:btc_usdt:1m", default=0)
        st.text(f"Candles in Redis: {candle_count or 0}")
        parquet_path = _PROJECT_ROOT / "data" / "BTC_USDT_1m.parquet"
        if parquet_path.exists():
            size_mb = parquet_path.stat().st_size / (1024 * 1024)
            st.text(f"Parquet: {size_mb:.1f} MB")
        else:
            st.text("Parquet: not found")

    with info_cols[2]:
        st.markdown("**Config**")
        st.text(f"Symbol: {os.getenv('SYMBOL', 'BTC/USDT')}")
        st.text(f"Timeframe: {os.getenv('TIMEFRAME', '1m')}")
        st.text(f"Refresh: 5s")
