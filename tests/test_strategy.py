import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

# Adjust the path to import from src
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from my_strategy import StrategyConfig, CustomStrategy

# Fixture for a default StrategyConfig
@pytest.fixture
def default_config():
    # Use config values that make indicators warm up faster for tests
    return StrategyConfig(
        ema_fast_period=5,
        ema_slow_period=10,
        ema_mid_period=7,
        rsi_period=7,
        poll_interval=1, # Not relevant for unit tests but good practice
    )

# Fixture for CustomStrategy instance
@pytest.fixture
def custom_strategy(default_config):
    return CustomStrategy(default_config)

# Helper function to create a basic DataFrame
def create_df(closes, opens=None, highs=None, lows=None, volumes=None, start_timestamp=None, timeframe="2h"):
    n_bars = len(closes)
    # Default opens/highs/lows relative to closes, ensuring some movement
    if opens is None: opens = [c - (1 if i % 2 == 0 else -1) for i, c in enumerate(closes)]
    if highs is None: highs = [max(c, o) + 1 for c, o in zip(closes, opens)]
    if lows is None: lows = [min(c, o) - 1 for c, o in zip(closes, opens)]
    if volumes is None: volumes = [100] * n_bars
    
    if start_timestamp is None:
        start_timestamp = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    
    tf_ms = 0
    if timeframe == "1m": tf_ms = 60 * 1000
    elif timeframe == "5m": tf_ms = 5 * 60 * 1000
    elif timeframe == "1h": tf_ms = 60 * 60 * 1000
    elif timeframe == "2h": tf_ms = 2 * 60 * 60 * 1000
    elif timeframe == "4h": tf_ms = 4 * 60 * 60 * 1000
    elif timeframe == "1d": tf_ms = 24 * 60 * 60 * 1000
    else: raise ValueError(f"Unsupported timeframe '{timeframe}' for test DF generation")
    
    timestamps = [(start_timestamp + timedelta(milliseconds=i * tf_ms)).timestamp() * 1000 for i in range(n_bars)]

    df = pd.DataFrame({
        "timestamp": timestamps,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": volumes,
    })
    df['dt'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df['date'] = df['dt'].dt.date
    return df






# --- Test CustomStrategy.compute_indicators ---
def test_compute_indicators_basic_emas_rsi(custom_strategy: CustomStrategy):
    # StrategyConfig uses ema_fast=5, ema_slow=10, rsi=7, min_bars=12
    # Create a DataFrame with enough bars for all indicators to warm up
    closes = [100 + i for i in range(30)] # More than min_bars
    df = create_df(closes, timeframe="2h")

    df_with_indicators = custom_strategy.compute_indicators(df)

    # Check if new columns are added
    expected_cols = [
        "ema_fast", "ema_slow", "ema_mid", "rsi", "ema_gap", "daily_ema34_slope",
        "last_2h_close", "last_2h_open", "last_2h_size", "last_2h_color",
        "pivot_bias", "unhit_pivot_val", "unhit_pivot_days", "unhit_pivot_side",
    ]
    for col in expected_cols:
        assert col in df_with_indicators.columns

    # Check that indicators are not NaN for the last row
    last_row = df_with_indicators.iloc[-1]
    assert not pd.isna(last_row["ema_fast"])
    assert not pd.isna(last_row["ema_slow"])
    assert not pd.isna(last_row["rsi"])

    # Check some indicator values for an upward trend
    assert last_row["ema_fast"] > last_row["ema_mid"]
    assert last_row["ema_mid"] > last_row["ema_slow"]
    assert last_row["rsi"] == pytest.approx(100.0, abs=0.1) # Purely increasing should give 100

    # Check ema_gap for an upward trend
    assert last_row["ema_gap"] == "BULLISH"

    # Check daily_ema34_slope for an upward trend (assuming default data makes this true)
    assert last_row["daily_ema34_slope"] == 1


def test_compute_indicators_pivot_bias(custom_strategy: CustomStrategy):
    # Simulate data across multiple days to test daily pivot calculations
    # Day 1: Closes high, pivot up (relative to prev day, if any)
    # Day 2: Closes low, pivot down (relative to prev day)
    # Day 3: Closes high, pivot up (relative to prev day)
    #
    # To ensure clear pivot bias, we need to carefully craft daily high/low/close
    # such that the (H+L+C)/3 pivot value has a desired trend.

    # Day 1: Start low, end high. Pivot around 120.
    closes_d1 = [100, 105, 110, 115, 120, 125] # 12h, close=125
    df_d1 = create_df(closes_d1, start_timestamp=datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc), timeframe="2h",
                      highs=[127]*6, lows=[98]*6) # Ensure high/low for pivot

    # Day 2: Start high, end low. close=80. Pivot lower than Day 1.
    closes_d2 = [130, 120, 110, 100, 90, 80] # 12h, close=80
    df_d2 = create_df(closes_d2, start_timestamp=datetime(2023, 1, 2, 0, 0, 0, tzinfo=timezone.utc), timeframe="2h",
                      highs=[132]*6, lows=[78]*6)

    # Day 3: Start low, end high. close=100. Pivot lower than Day 2.
    closes_d3 = [75, 80, 85, 90, 95, 100] # 12h, close=100
    df_d3 = create_df(closes_d3, start_timestamp=datetime(2023, 1, 3, 0, 0, 0, tzinfo=timezone.utc), timeframe="2h",
                      highs=[102]*6, lows=[73]*6)

    df = pd.concat([df_d1, df_d2, df_d3], ignore_index=True)
    df_with_indicators = custom_strategy.compute_indicators(df)

    # Check pivot_bias for last bar of Day 2 (compared to Day 1)
    # Day 1 Pivot: ~116.66. Day 2 Pivot: ~96.66. -> bias -1
    last_bar_d2_idx = len(df_d1) + len(df_d2) - 1
    assert df_with_indicators.loc[last_bar_d2_idx, "pivot_bias"] == -1

    # Check pivot_bias for last bar of Day 3 (compared to Day 2)
    # Day 2 Pivot: ~96.66. Day 3 Pivot: ~91.66. -> bias -1
    last_bar_d3_idx = len(df) - 1
    assert df_with_indicators.loc[last_bar_d3_idx, "pivot_bias"] == -1


# --- Test CustomStrategy.check_signals ---
def test_check_signals_wait_insufficient_data(custom_strategy: CustomStrategy):
    df = create_df([100])
    signal, reason = custom_strategy.check_signals(df)
    assert signal == "WAIT"
    assert "Datos insuficientes" in reason

def test_check_signals_wait_indicators_warming_up(custom_strategy: CustomStrategy):
    # Create a DataFrame with fewer bars than the smallest indicator period (ema_fast=5).
    # This ensures that 'ema_fast' will be NaN at the last row, triggering the guard.
    closes = [100, 101, 102, 103] # Length 4, less than ema_fast_period=5
    df = create_df(closes, timeframe="2h")
    df_processed = custom_strategy.compute_indicators(df)
    
    signal, reason = custom_strategy.check_signals(df_processed)
    assert signal == "WAIT"
    assert "Indicadores calentando" in reason


# Helper to create a fully processed DF for check_signals
def create_processed_df(signal_type, config: StrategyConfig):
    # Ensure enough bars for indicators to be non-NaN before the trigger bar
    base_len = max(config.ema_slow_period, config.rsi_period) + 5 # Sufficient length for indicators to warm up
    closes_base = [10000 + i * 10 for i in range(base_len)]
    df_base = create_df(closes_base, timeframe=config.timeframe)
    df_processed_base = CustomStrategy(config).compute_indicators(df_base)

    # Prepare the last bar with explicit values to trigger the signal
    last_bar_data = {
        "timestamp": df_processed_base.iloc[-1]["timestamp"] + (2 * 60 * 60 * 1000), # Next bar
        "open": 0, "high": 0, "low": 0, "close": 0, "volume": 100, # Placeholder OHLCV
        "dt": datetime.now(timezone.utc), "date": datetime.now(timezone.utc).date(), # Placeholder dates
        "ema_fast": 0.0, "ema_slow": 0.0, "ema_mid": 0.0, "rsi": 0.0, # Placeholder indicator values
        "ema_gap": "", "daily_ema34_slope": 0,
        "last_2h_close": 0, "last_2h_open": 0, "last_2h_size": 0, "last_2h_color": 0,
        "pivot_bias": 0, "unhit_pivot_val": np.nan, "unhit_pivot_days": 0, "unhit_pivot_side": "NONE",
    }
    last_bar_series = pd.Series(last_bar_data, name=df_processed_base.index[-1] + 1)
    
    if signal_type == "BUY":
        # Context: pivot_bias=1, unhit_pivot_side="UPPER", ema_gap="BULLISH", daily_ema34_slope=1
        # Trigger: last_2h_size < -200 (pullback)
        last_bar_series["pivot_bias"] = 1
        last_bar_series["unhit_pivot_side"] = "UPPER"
        last_bar_series["ema_gap"] = "BULLISH"
        last_bar_series["daily_ema34_slope"] = 1
        last_bar_series["last_2h_size"] = -250 # Pullback
        last_bar_series["ema_fast"] = 11000
        last_bar_series["ema_mid"] = 10900
        last_bar_series["ema_slow"] = 10800
        last_bar_series["rsi"] = 60 # Not overbought/oversold
        last_bar_series["close"] = 10900 # Arbitrary to match EMAs
    elif signal_type == "SELL":
        # Context: pivot_bias=-1, unhit_pivot_side="LOWER", ema_gap="BEARISH", daily_ema34_slope=-1
        # Trigger: last_2h_size > 200 (rally)
        last_bar_series["pivot_bias"] = -1
        last_bar_series["unhit_pivot_side"] = "LOWER"
        last_bar_series["ema_gap"] = "BEARISH"
        last_bar_series["daily_ema34_slope"] = -1
        last_bar_series["last_2h_size"] = 250 # Rally
        last_bar_series["ema_fast"] = 9000
        last_bar_series["ema_mid"] = 9100
        last_bar_series["ema_slow"] = 9200
        last_bar_series["rsi"] = 40 # Not overbought/oversold
        last_bar_series["close"] = 9100 # Arbitrary to match EMAs
    else: # NO_SIGNAL
        # Neutral conditions
        last_bar_series["pivot_bias"] = 0
        last_bar_series["unhit_pivot_side"] = "NONE"
        last_bar_series["ema_gap"] = "NEUTRAL"
        last_bar_series["daily_ema34_slope"] = 0
        last_bar_series["last_2h_size"] = 0
        last_bar_series["ema_fast"] = 10000
        last_bar_series["ema_mid"] = 10000
        last_bar_series["ema_slow"] = 10000
        last_bar_series["rsi"] = 50

    # Ensure last_2h_close and last_2h_open are set for curr and prev bars.
    # For check_signals, it needs curr and prev values.
    # We will append the last_bar_series to a base DataFrame that has all indicators filled.
    df_final = pd.concat([df_processed_base, pd.DataFrame([last_bar_series])], ignore_index=True)
    
    # Ensure prev bar also has valid indicators
    prev_bar_idx = len(df_final) - 2
    if prev_bar_idx >= 0:
        for col in ["ema_fast", "ema_slow", "ema_mid", "rsi", "ema_gap", "daily_ema34_slope",
                    "pivot_bias", "unhit_pivot_side", "last_2h_size"]:
            if pd.isna(df_final.loc[prev_bar_idx, col]):
                df_final.loc[prev_bar_idx, col] = df_processed_base.iloc[-1][col] # Fill from last processed base

    return df_final


def test_check_signals_buy_condition(custom_strategy: CustomStrategy):
    df_processed = create_processed_df("BUY", custom_strategy.cfg)
    signal, reason = custom_strategy.check_signals(df_processed)
    
    assert signal == "BUY"
    assert "Pullback 2H" in reason

def test_check_signals_sell_condition(custom_strategy: CustomStrategy):
    df_processed = create_processed_df("SELL", custom_strategy.cfg)
    signal, reason = custom_strategy.check_signals(df_processed)
    
    assert signal == "SELL"
    assert "Rally 2H" in reason

def test_check_signals_wait_no_signal_match(custom_strategy: CustomStrategy):
    df_processed = create_processed_df("NO_SIGNAL", custom_strategy.cfg)
    signal, reason = custom_strategy.check_signals(df_processed)
    assert signal == "WAIT"
    assert reason == ""