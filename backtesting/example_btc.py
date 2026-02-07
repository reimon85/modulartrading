"""
example_btc.py — Ejemplo completo del Universal Backtesting Engine.

Usa la misma estrategia EMA crossover + RSI de my_strategy.py
empaquetada como BaseStrategy para el engine de backtesting.

Uso:
    python -m backtesting.example_btc                          # Backtest simple
    python -m backtesting.example_btc --optimize grid           # Grid Search
    python -m backtesting.example_btc --optimize random --trials 50
    python -m backtesting.example_btc --start 2025-01-01 --end 2025-06-30
    python -m backtesting.example_btc --file data/BTC_USDT_1m.parquet
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Asegurar que el proyecto raíz esté en el path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backtesting.engine import (
    BacktestEngine,
    BaseStrategy,
    DataLoader,
    Optimizer,
    ReportGenerator,
    generate_chart,
    get_optimization_suggestions,
)


# ═══════════════════════════════════════════════════════════════════
#  INDICADORES (pandas puro — sin dependencias externas)
# ═══════════════════════════════════════════════════════════════════

def _calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """RSI de Wilder con media exponencial."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


# ═══════════════════════════════════════════════════════════════════
#  ESTRATEGIA: EMA Crossover + RSI
# ═══════════════════════════════════════════════════════════════════

class EmaRsiStrategy(BaseStrategy):
    """
    Cruce de Medias Exponenciales + filtro RSI.

    Parámetros esperados:
        ema_fast       (int)   — periodo EMA rápida
        ema_slow       (int)   — periodo EMA lenta
        rsi_period     (int)   — periodo RSI
        rsi_oversold   (float) — umbral de sobreventa
        rsi_overbought (float) — umbral de sobrecompra

    Reglas:
        BUY  → EMA fast cruza ARRIBA de EMA slow  AND  RSI < oversold
        SELL → EMA fast cruza ABAJO de EMA slow    AND  RSI > overbought
    """

    @property
    def min_bars(self) -> int:
        return max(
            self.params.get("ema_fast", 9),
            self.params.get("ema_slow", 21),
            self.params.get("rsi_period", 14),
        ) + 2

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["ema_fast"] = df["close"].ewm(
            span=self.params["ema_fast"], adjust=False,
        ).mean()
        df["ema_slow"] = df["close"].ewm(
            span=self.params["ema_slow"], adjust=False,
        ).mean()
        df["rsi"] = _calc_rsi(df["close"], period=self.params["rsi_period"])
        return df

    def check_signal(self, df: pd.DataFrame, i: int) -> str:
        if i < 1:
            return "WAIT"

        curr = df.iloc[i]
        prev = df.iloc[i - 1]

        for col in ("ema_fast", "ema_slow", "rsi"):
            if pd.isna(curr[col]) or pd.isna(prev[col]):
                return "WAIT"

        oversold = self.params["rsi_oversold"]
        overbought = self.params["rsi_overbought"]

        # BUY: cruce alcista + RSI en sobreventa
        if (prev["ema_fast"] <= prev["ema_slow"]
                and curr["ema_fast"] > curr["ema_slow"]
                and curr["rsi"] < oversold):
            return "BUY"

        # SELL: cruce bajista + RSI en sobrecompra
        if (prev["ema_fast"] >= prev["ema_slow"]
                and curr["ema_fast"] < curr["ema_slow"]
                and curr["rsi"] > overbought):
            return "SELL"

        return "WAIT"


# ═══════════════════════════════════════════════════════════════════
#  ESPACIO DE PARAMETROS PARA OPTIMIZACION
# ═══════════════════════════════════════════════════════════════════

PARAM_SPACE = {
    "ema_fast":       [5, 7, 9, 12, 15, 20],
    "ema_slow":       [15, 21, 30, 40, 50],
    "rsi_period":     [10, 14, 20],
    "rsi_oversold":   [20, 25, 30, 35],
    "rsi_overbought": [65, 70, 75, 80],
}

DEFAULT_PARAMS = {
    "ema_fast": 9,
    "ema_slow": 21,
    "rsi_period": 14,
    "rsi_oversold": 30,
    "rsi_overbought": 70,
}


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="BTC EMA+RSI Backtesting Example",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
ejemplos:
  python -m backtesting.example_btc                           Single backtest
  python -m backtesting.example_btc --optimize grid           Grid search
  python -m backtesting.example_btc --optimize random         Random search
  python -m backtesting.example_btc --start 2025-01-01        Time slice
        """,
    )
    parser.add_argument("--file", default=None, help="Ruta al .parquet")
    parser.add_argument("--start", default=None, help="Fecha inicio (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="Fecha fin (YYYY-MM-DD)")
    parser.add_argument("--optimize", choices=["grid", "random"], default=None)
    parser.add_argument("--trials", type=int, default=50, help="Trials para random search")
    parser.add_argument("--capital", type=float, default=10_000, help="Capital inicial USDT")
    parser.add_argument("--timeframe", default="1m")

    args = parser.parse_args()

    # ── Buscar archivo Parquet ─────────────────────────────────
    if args.file:
        parquet_path = args.file
    else:
        base = Path(__file__).resolve().parent.parent
        parquet_path = base / "data" / f"BTC_USDT_{args.timeframe}.parquet"
        if not Path(parquet_path).exists():
            print(f"\n  Parquet no encontrado: {parquet_path}")
            print("  Primero ejecuta el Data Fetcher:")
            print("    python -m src.main --days 7")
            print("  O especifica: --file ruta/al/archivo.parquet\n")
            return

    # ── Cargar datos ───────────────────────────────────────────
    print("\n  Cargando datos...")
    data = DataLoader.from_parquet(parquet_path, start=args.start, end=args.end)
    print(f"  {len(data):,} barras cargadas\n")

    # ── Crear engine ───────────────────────────────────────────
    engine = BacktestEngine(
        strategy_cls=EmaRsiStrategy,
        data=data,
        initial_capital=args.capital,
        commission_pct=0.1,
        timeframe=args.timeframe,
    )

    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)

    if args.optimize:
        # ── MODO OPTIMIZACIÓN ──────────────────────────────────
        optimizer = Optimizer(engine)

        if args.optimize == "grid":
            results = optimizer.grid_search(PARAM_SPACE)
        else:
            results = optimizer.random_search(PARAM_SPACE, n_trials=args.trials)

        # Tabla resumen
        table = ReportGenerator.optimization_table(results)
        print(table)

        # Reporte completo del mejor resultado
        report = ReportGenerator.single(
            results[0],
            strategy_name="EMA+RSI (Best)",
            symbol="BTC/USDT",
            timeframe=args.timeframe,
        )
        print(report)

        # Guardar reporte
        report_path = output_dir / "optimization_report.txt"
        report_path.write_text(table + "\n\n" + report, encoding="utf-8")
        print(f"\n  Reporte guardado: {report_path}")

        # Gráfico del mejor resultado
        generate_chart(
            results[0], data,
            output_path=output_dir / "best_strategy_chart.html",
            strategy_name="EMA+RSI (Best)",
        )

        # LLM feedback prompt
        suggestions = get_optimization_suggestions(
            results, PARAM_SPACE,
            strategy_name="EMA Crossover + RSI",
            symbol="BTC/USDT",
            timeframe=args.timeframe,
        )
        prompt_path = output_dir / "llm_optimization_prompt.txt"
        prompt_path.write_text(suggestions, encoding="utf-8")
        print(f"  LLM prompt guardado: {prompt_path}")
        print("  Copia el contenido y pegalo en Claude/ChatGPT para sugerencias.\n")

    else:
        # ── MODO SINGLE BACKTEST ───────────────────────────────
        print("  Ejecutando backtest con parametros default...")
        result = engine.run(DEFAULT_PARAMS)

        report = ReportGenerator.single(
            result,
            strategy_name="EMA+RSI",
            symbol="BTC/USDT",
            timeframe=args.timeframe,
        )
        print(report)

        # Guardar reporte
        report_path = output_dir / "backtest_report.txt"
        report_path.write_text(report, encoding="utf-8")
        print(f"\n  Reporte guardado: {report_path}")

        # Gráfico
        generate_chart(
            result, data,
            output_path=output_dir / "backtest_chart.html",
            strategy_name="EMA+RSI",
        )


if __name__ == "__main__":
    main()
