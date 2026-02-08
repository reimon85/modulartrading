"""
Universal Backtesting Engine — optimizado para workflows con LLM.

Componentes:
    1. DataLoader        — carga Parquet con time-slicing
    2. BaseStrategy      — interfaz abstracta para cualquier estrategia
    3. BacktestEngine    — simulador bar-por-bar con comisiones
    4. Optimizer         — Grid Search + Random Search
    5. ReportGenerator   — reporte en texto plano (AI-ready)
    6. generate_chart    — gráfico interactivo Plotly (HTML)
    7. get_optimization_suggestions — prompt formateado para LLM

Uso rápido:
    engine = BacktestEngine(MyStrategy, data)
    result = engine.run({"ema_fast": 9, "ema_slow": 21})
    print(ReportGenerator.single(result))
"""

from __future__ import annotations

import itertools
import logging
import random
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger("backtesting")


# ═══════════════════════════════════════════════════════════════════
#  1. DATA LOADER
# ═══════════════════════════════════════════════════════════════════

class DataLoader:
    """
    Carga archivos Parquet con time-slicing eficiente.

    Uso:
        df = DataLoader.from_parquet("data/BTC_USDT_1m.parquet")
        df = DataLoader.from_parquet("data/BTC_USDT_1m.parquet",
                                     start="2025-01-01", end="2025-06-30")
    """

    REQUIRED = {"timestamp", "open", "high", "low", "close", "volume"}

    @staticmethod
    def from_parquet(
        path: str | Path,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """
        Carga un Parquet y opcionalmente recorta por rango de fechas.

        Args:
            path:  ruta al archivo .parquet
            start: fecha inicio inclusive (str: "2025-01-01" o "2025-03-15 08:00")
            end:   fecha fin inclusive
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {path}")

        df = pd.read_parquet(path)

        missing = DataLoader.REQUIRED - set(df.columns)
        if missing:
            raise ValueError(f"Columnas faltantes: {missing}")

        for col in ("open", "high", "low", "close", "volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # ── Time slicing ───────────────────────────────────────
        if start:
            start_ms = int(pd.Timestamp(start, tz="UTC").timestamp() * 1000)
            df = df[df["timestamp"] >= start_ms]
        if end:
            end_ms = int(pd.Timestamp(end, tz="UTC").timestamp() * 1000)
            df = df[df["timestamp"] <= end_ms]

        df = df.sort_values("timestamp").reset_index(drop=True)

        if df.empty:
            raise ValueError("Sin datos después de aplicar filtros de tiempo")

        t0 = datetime.fromtimestamp(df["timestamp"].iloc[0] / 1000, tz=timezone.utc)
        t1 = datetime.fromtimestamp(df["timestamp"].iloc[-1] / 1000, tz=timezone.utc)
        logger.info("Loaded %d bars from %s (%s → %s)", len(df), path.name, t0.date(), t1.date())
        return df


# ═══════════════════════════════════════════════════════════════════
#  2. STRATEGY INTERFACE (clase base abstracta)
# ═══════════════════════════════════════════════════════════════════

class BaseStrategy(ABC):
    """
    Clase base que cualquier estrategia debe heredar.

    Subclase mínima:
        class MiEstrategia(BaseStrategy):
            def compute_indicators(self, df):
                df = df.copy()
                df["sma"] = df["close"].rolling(self.params["period"]).mean()
                return df

            def check_signal(self, df, i):
                if df.iloc[i]["close"] > df.iloc[i]["sma"]:
                    return "BUY"
                return "WAIT"
    """

    def __init__(self, params: dict):
        self.params = params

    @abstractmethod
    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Añadir columnas de indicadores al DataFrame. Debe retornar copia."""
        ...

    @abstractmethod
    def check_signal(self, df: pd.DataFrame, i: int) -> str:
        """
        Evaluar señal en la barra i.
        El DataFrame ya tiene indicadores calculados.
        Debe retornar "BUY", "SELL" o "WAIT".
        """
        ...

    @property
    def min_bars(self) -> int:
        """Override para especificar periodo de calentamiento de indicadores."""
        return 50


# ═══════════════════════════════════════════════════════════════════
#  3. MODELOS DE DATOS
# ═══════════════════════════════════════════════════════════════════

@dataclass
class Trade:
    """Registro de una operación completa (entrada + salida)."""
    entry_bar: int
    entry_timestamp: int           # epoch ms
    entry_price: float
    exit_bar: int = 0
    exit_timestamp: int = 0        # epoch ms
    exit_price: float = 0.0
    size: float = 0.0              # unidades (ej: BTC)
    invested: float = 0.0          # USDT totales invertidos
    pnl: float = 0.0              # ganancia/pérdida neta
    pnl_pct: float = 0.0          # % sobre capital invertido
    commission_paid: float = 0.0
    bars_held: int = 0


@dataclass
class BacktestResult:
    """Resultado completo de un backtest."""
    params: dict
    trades: list[Trade]
    equity_curve: list[float]
    timestamps: list[int]
    metrics: dict = field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════════
#  4. MOTOR DE BACKTESTING
# ═══════════════════════════════════════════════════════════════════

# Barras por año para anualización de Sharpe/Sortino
_BARS_PER_YEAR = {
    "1m": 525_600, "3m": 175_200, "5m": 105_120, "15m": 35_040,
    "30m": 17_520, "1h": 8_760, "4h": 2_190, "1d": 365,
}


class BacktestEngine:
    """
    Simulador universal bar-por-bar.

    Uso:
        engine = BacktestEngine(MiEstrategia, data, initial_capital=10_000)
        result = engine.run({"ema_fast": 9, "ema_slow": 21})
    """

    def __init__(
        self,
        strategy_cls: type[BaseStrategy],
        data: pd.DataFrame,
        initial_capital: float = 10_000.0,
        commission_pct: float = 0.1,
        position_frac: float = 1.0,
        timeframe: str = "1m",
        tp_points: float = 0.0,
        sl_points: float = 0.0,
    ):
        """
        Args:
            strategy_cls:    clase que hereda BaseStrategy
            data:            DataFrame con OHLCV
            initial_capital: capital inicial en USDT
            commission_pct:  comisión por operación en % (0.1 = Binance spot)
            position_frac:   fracción del cash a invertir por operación (1.0 = 100%)
            timeframe:       para anualizar Sharpe/Sortino
            tp_points:       Take Profit en puntos de precio (0 = desactivado)
            sl_points:       Stop Loss en puntos de precio (0 = desactivado)
        """
        self.strategy_cls = strategy_cls
        self.data = data.copy()
        self.initial_capital = initial_capital
        self.commission = commission_pct / 100.0
        self.position_frac = position_frac
        self.timeframe = timeframe
        self.tp_points = tp_points
        self.sl_points = sl_points

    def run(self, params: dict) -> BacktestResult:
        """Ejecutar un backtest con los parámetros dados."""
        strategy = self.strategy_cls(params)
        df = strategy.compute_indicators(self.data)
        trades, equity, timestamps = self._simulate(df, strategy)
        metrics = _compute_metrics(
            trades, equity, self.initial_capital,
            self.timeframe, timestamps,
        )
        return BacktestResult(
            params=params, trades=trades,
            equity_curve=equity, timestamps=timestamps,
            metrics=metrics,
        )

    def _simulate(
        self,
        df: pd.DataFrame,
        strategy: BaseStrategy,
    ) -> tuple[list[Trade], list[float], list[int]]:
        """Simulación bar-por-bar con tracking de posición y equity."""
        cash = self.initial_capital
        holdings = 0.0
        active_trade: Trade | None = None

        trades: list[Trade] = []
        equity_curve: list[float] = []
        timestamps: list[int] = []
        start = strategy.min_bars

        for i in range(len(df)):
            row = df.iloc[i]
            price = float(row["close"])
            ts = int(row["timestamp"])

            if i >= start:
                # ── TP / SL CHECK (prioridad sobre señales) ───
                if active_trade is not None:
                    hi = float(row["high"])
                    lo = float(row["low"])

                    tp_hit = self.tp_points > 0 and hi >= active_trade.entry_price + self.tp_points
                    sl_hit = self.sl_points > 0 and lo <= active_trade.entry_price - self.sl_points

                    if tp_hit or sl_hit:
                        exit_px = (
                            (active_trade.entry_price + self.tp_points) if tp_hit
                            else (active_trade.entry_price - self.sl_points)
                        )
                        revenue = holdings * exit_px
                        fee = revenue * self.commission
                        net = revenue - fee

                        active_trade.exit_bar = i
                        active_trade.exit_timestamp = ts
                        active_trade.exit_price = exit_px
                        active_trade.pnl = net - active_trade.invested
                        active_trade.pnl_pct = (net / active_trade.invested - 1) * 100
                        active_trade.commission_paid += fee
                        active_trade.bars_held = i - active_trade.entry_bar

                        trades.append(active_trade)
                        cash += net
                        holdings = 0.0
                        active_trade = None

                        equity_curve.append(cash)
                        timestamps.append(ts)
                        continue

                signal = strategy.check_signal(df, i)

                # ── ABRIR LONG ─────────────────────────────────
                if signal == "BUY" and active_trade is None and cash > 0:
                    invest = cash * self.position_frac
                    fee = invest * self.commission
                    units = (invest - fee) / price

                    cash -= invest
                    holdings = units
                    active_trade = Trade(
                        entry_bar=i, entry_timestamp=ts,
                        entry_price=price, size=units,
                        invested=invest, commission_paid=fee,
                    )

                # ── CERRAR LONG ────────────────────────────────
                elif signal == "SELL" and active_trade is not None:
                    revenue = holdings * price
                    fee = revenue * self.commission
                    net = revenue - fee

                    active_trade.exit_bar = i
                    active_trade.exit_timestamp = ts
                    active_trade.exit_price = price
                    active_trade.pnl = net - active_trade.invested
                    active_trade.pnl_pct = (net / active_trade.invested - 1) * 100
                    active_trade.commission_paid += fee
                    active_trade.bars_held = i - active_trade.entry_bar

                    trades.append(active_trade)
                    cash += net
                    holdings = 0.0
                    active_trade = None

            equity_curve.append(cash + holdings * price)
            timestamps.append(ts)

        # ── Cierre forzado si hay posición abierta al final ────
        if active_trade is not None and len(df) > 0:
            last_price = float(df.iloc[-1]["close"])
            last_ts = int(df.iloc[-1]["timestamp"])
            revenue = holdings * last_price
            fee = revenue * self.commission
            net = revenue - fee

            active_trade.exit_bar = len(df) - 1
            active_trade.exit_timestamp = last_ts
            active_trade.exit_price = last_price
            active_trade.pnl = net - active_trade.invested
            active_trade.pnl_pct = (net / active_trade.invested - 1) * 100
            active_trade.commission_paid += fee
            active_trade.bars_held = len(df) - 1 - active_trade.entry_bar
            trades.append(active_trade)
            equity_curve[-1] = cash + net

        return trades, equity_curve, timestamps


# ═══════════════════════════════════════════════════════════════════
#  5. MÉTRICAS
# ═══════════════════════════════════════════════════════════════════

def _compute_metrics(
    trades: list[Trade],
    equity_curve: list[float],
    initial_capital: float,
    timeframe: str = "1m",
    timestamps: list[int] | None = None,
) -> dict:
    """Calcula todas las métricas estándar de un backtest."""
    if not equity_curve:
        return _empty_metrics()

    final = equity_curve[-1]
    net_profit = final - initial_capital
    net_profit_pct = (net_profit / initial_capital) * 100
    n = len(trades)

    if n == 0:
        m = _empty_metrics()
        m["net_profit"] = round(net_profit, 2)
        m["net_profit_pct"] = round(net_profit_pct, 2)
        return m

    # ── Win / Loss ─────────────────────────────────────────────
    winners = [t for t in trades if t.pnl > 0]
    losers = [t for t in trades if t.pnl <= 0]
    win_rate = len(winners) / n * 100

    avg_win = float(np.mean([t.pnl for t in winners])) if winners else 0.0
    avg_loss = float(np.mean([t.pnl for t in losers])) if losers else 0.0
    avg_win_pct = float(np.mean([t.pnl_pct for t in winners])) if winners else 0.0
    avg_loss_pct = float(np.mean([t.pnl_pct for t in losers])) if losers else 0.0

    gross_profit = sum(t.pnl for t in winners) if winners else 0.0
    gross_loss = abs(sum(t.pnl for t in losers)) if losers else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    best = max(trades, key=lambda t: t.pnl)
    worst = min(trades, key=lambda t: t.pnl)
    avg_held = float(np.mean([t.bars_held for t in trades]))
    total_comm = sum(t.commission_paid for t in trades)

    # ── Sharpe & Sortino (anualizado) ──────────────────────────
    eq = pd.Series(equity_curve)
    returns = eq.pct_change().dropna()
    ann = _BARS_PER_YEAR.get(timeframe, 525_600)

    if len(returns) > 1 and returns.std() > 0:
        sharpe = float(returns.mean() / returns.std() * np.sqrt(ann))
    else:
        sharpe = 0.0

    downside = returns[returns < 0]
    if len(downside) > 1 and downside.std() > 0:
        sortino = float(returns.mean() / downside.std() * np.sqrt(ann))
    else:
        sortino = 0.0

    # ── Drawdown ───────────────────────────────────────────────
    running_max = eq.cummax()
    drawdown = (eq - running_max) / running_max
    max_dd = float(drawdown.min() * 100)

    is_dd = eq < running_max
    if is_dd.any():
        groups = (~is_dd).cumsum()
        max_dd_dur = int(is_dd.groupby(groups).sum().max())
    else:
        max_dd_dur = 0

    calmar = float(net_profit_pct / abs(max_dd)) if max_dd != 0 else 0.0
    dd_periods = _find_drawdown_periods(equity_curve, timestamps)

    return {
        "net_profit": round(net_profit, 2),
        "net_profit_pct": round(net_profit_pct, 2),
        "total_trades": n,
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": round(win_rate, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "avg_win_pct": round(avg_win_pct, 2),
        "avg_loss_pct": round(avg_loss_pct, 2),
        "profit_factor": round(profit_factor, 3),
        "best_trade_pnl": round(best.pnl, 2),
        "best_trade_pnl_pct": round(best.pnl_pct, 2),
        "worst_trade_pnl": round(worst.pnl, 2),
        "worst_trade_pnl_pct": round(worst.pnl_pct, 2),
        "avg_bars_held": round(avg_held, 1),
        "total_commission": round(total_comm, 2),
        "sharpe_ratio": round(sharpe, 3),
        "sortino_ratio": round(sortino, 3),
        "calmar_ratio": round(calmar, 3),
        "max_drawdown_pct": round(max_dd, 2),
        "max_dd_duration_bars": max_dd_dur,
        "drawdown_periods": dd_periods,
    }


def _empty_metrics() -> dict:
    keys = [
        "net_profit", "net_profit_pct", "total_trades", "winners", "losers",
        "win_rate", "avg_win", "avg_loss", "avg_win_pct", "avg_loss_pct",
        "profit_factor", "best_trade_pnl", "best_trade_pnl_pct",
        "worst_trade_pnl", "worst_trade_pnl_pct", "avg_bars_held",
        "total_commission", "sharpe_ratio", "sortino_ratio", "calmar_ratio",
        "max_drawdown_pct", "max_dd_duration_bars",
    ]
    m = {k: 0 for k in keys}
    m["drawdown_periods"] = []
    return m


def _find_drawdown_periods(
    equity_curve: list[float],
    timestamps: list[int] | None = None,
) -> list[dict]:
    """Identifica los peores periodos de drawdown."""
    eq = pd.Series(equity_curve)
    running_max = eq.cummax()
    dd = (eq - running_max) / running_max
    is_dd = dd < -0.001

    if not is_dd.any():
        return []

    groups = (~is_dd).cumsum()
    periods = []

    for gid in groups[is_dd].unique():
        mask = groups == gid
        dd_slice = dd[mask]
        s, e = int(dd_slice.index[0]), int(dd_slice.index[-1])
        periods.append({
            "start_bar": s,
            "end_bar": e,
            "start_ts": timestamps[s] if timestamps and s < len(timestamps) else None,
            "end_ts": timestamps[e] if timestamps and e < len(timestamps) else None,
            "max_dd_pct": round(float(dd_slice.min() * 100), 2),
            "duration_bars": e - s + 1,
        })

    periods.sort(key=lambda x: x["max_dd_pct"])
    return periods[:5]


# ═══════════════════════════════════════════════════════════════════
#  6. OPTIMIZER (Grid Search + Random Search)
# ═══════════════════════════════════════════════════════════════════

class Optimizer:
    """
    Búsqueda de parámetros óptimos.

    Uso:
        opt = Optimizer(engine)
        results = opt.grid_search({
            "ema_fast": [5, 9, 12],
            "ema_slow": [21, 30, 50],
        })
    """

    def __init__(self, engine: BacktestEngine):
        self.engine = engine

    def grid_search(
        self,
        param_space: dict[str, list],
        sort_by: str = "sharpe_ratio",
    ) -> list[BacktestResult]:
        """Prueba TODAS las combinaciones posibles."""
        keys = list(param_space.keys())
        values = list(param_space.values())
        combos = list(itertools.product(*values))
        total = len(combos)
        step = max(1, total // 20)

        print(f"\n  Grid Search: {total} combinaciones")
        print(f"  {'─' * 50}")

        results: list[BacktestResult] = []
        for idx, combo in enumerate(combos, 1):
            params = dict(zip(keys, combo))
            result = self.engine.run(params)
            results.append(result)

            if idx % step == 0 or idx == total:
                best = max(results, key=lambda r: r.metrics.get(sort_by, 0))
                print(
                    f"  [{idx:>{len(str(total))}}/{total}] "
                    f"Best {sort_by}: {best.metrics.get(sort_by, 0):>8.3f} | "
                    f"Profit: ${best.metrics['net_profit']:>+10,.2f}"
                )

        results.sort(key=lambda r: r.metrics.get(sort_by, 0), reverse=True)
        print(f"  {'─' * 50}")
        print(f"  Completado. Mejor {sort_by}: {results[0].metrics.get(sort_by, 0):.3f}\n")
        return results

    def random_search(
        self,
        param_space: dict[str, list],
        n_trials: int = 100,
        sort_by: str = "sharpe_ratio",
        seed: int | None = None,
    ) -> list[BacktestResult]:
        """Muestreo aleatorio del espacio de parámetros."""
        if seed is not None:
            random.seed(seed)

        step = max(1, n_trials // 20)
        print(f"\n  Random Search: {n_trials} trials")
        print(f"  {'─' * 50}")

        results: list[BacktestResult] = []
        seen: set[tuple] = set()

        for idx in range(1, n_trials + 1):
            for _ in range(100):
                params = {k: random.choice(v) for k, v in param_space.items()}
                key = tuple(sorted(params.items()))
                if key not in seen:
                    seen.add(key)
                    break

            result = self.engine.run(params)
            results.append(result)

            if idx % step == 0 or idx == n_trials:
                best = max(results, key=lambda r: r.metrics.get(sort_by, 0))
                print(
                    f"  [{idx:>{len(str(n_trials))}}/{n_trials}] "
                    f"Best {sort_by}: {best.metrics.get(sort_by, 0):>8.3f}"
                )

        results.sort(key=lambda r: r.metrics.get(sort_by, 0), reverse=True)
        print(f"  {'─' * 50}")
        print(f"  Completado. Mejor {sort_by}: {results[0].metrics.get(sort_by, 0):.3f}\n")
        return results


# ═══════════════════════════════════════════════════════════════════
#  7. REPORT GENERATOR (AI-Ready)
# ═══════════════════════════════════════════════════════════════════

class ReportGenerator:
    """Genera reportes en texto plano optimizados para consumo por LLM."""

    @staticmethod
    def single(
        result: BacktestResult,
        strategy_name: str = "Strategy",
        symbol: str = "BTC/USDT",
        timeframe: str = "1m",
    ) -> str:
        """Reporte ejecutivo de un solo backtest."""
        m = result.metrics
        ts = result.timestamps

        if ts:
            t0 = datetime.fromtimestamp(ts[0] / 1000, tz=timezone.utc)
            t1 = datetime.fromtimestamp(ts[-1] / 1000, tz=timezone.utc)
            period = f"{t0:%Y-%m-%d %H:%M} -> {t1:%Y-%m-%d %H:%M} UTC"
        else:
            period = "N/A"

        params_str = ", ".join(f"{k}={v}" for k, v in result.params.items())

        lines = [
            "=" * 64,
            f"  BACKTEST REPORT — {strategy_name}",
            f"  Generated: {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC}",
            "=" * 64,
            "",
            "[CONFIGURATION]",
            f"  Strategy:       {strategy_name}",
            f"  Symbol:         {symbol}",
            f"  Timeframe:      {timeframe}",
            f"  Period:         {period}",
            f"  Bars:           {len(ts):,}",
            f"  Parameters:     {params_str}",
            "",
            "[PERFORMANCE]",
            f"  Net Profit:     ${m['net_profit']:+,.2f} ({m['net_profit_pct']:+.2f}%)",
            f"  Total Trades:   {m['total_trades']}",
            f"  Win Rate:       {m['win_rate']:.1f}%  ({m['winners']}W / {m['losers']}L)",
            f"  Profit Factor:  {m['profit_factor']:.3f}",
            f"  Avg Win:        ${m['avg_win']:+,.2f} ({m['avg_win_pct']:+.2f}%)",
            f"  Avg Loss:       ${m['avg_loss']:+,.2f} ({m['avg_loss_pct']:+.2f}%)",
            f"  Best Trade:     ${m['best_trade_pnl']:+,.2f} ({m['best_trade_pnl_pct']:+.2f}%)",
            f"  Worst Trade:    ${m['worst_trade_pnl']:+,.2f} ({m['worst_trade_pnl_pct']:+.2f}%)",
            f"  Avg Duration:   {m['avg_bars_held']:.0f} bars",
            f"  Commission:     ${m['total_commission']:,.2f}",
            "",
            "[RISK METRICS]",
            f"  Max Drawdown:   {m['max_drawdown_pct']:.2f}%",
            f"  Max DD Duration: {m['max_dd_duration_bars']} bars",
            f"  Sharpe Ratio:   {m['sharpe_ratio']:.3f}",
            f"  Sortino Ratio:  {m['sortino_ratio']:.3f}",
            f"  Calmar Ratio:   {m['calmar_ratio']:.3f}",
        ]

        # ── Drawdown periods ───────────────────────────────────
        dd_periods = m.get("drawdown_periods", [])
        if dd_periods:
            lines += ["", "[DRAWDOWN PERIODS (top 5 worst)]"]
            for idx, dd in enumerate(dd_periods, 1):
                if dd.get("start_ts") and dd.get("end_ts"):
                    s = datetime.fromtimestamp(dd["start_ts"] / 1000, tz=timezone.utc)
                    e = datetime.fromtimestamp(dd["end_ts"] / 1000, tz=timezone.utc)
                    lines.append(
                        f"  {idx}. {s:%m-%d %H:%M} -> {e:%m-%d %H:%M} | "
                        f"{dd['max_dd_pct']:.2f}% | {dd['duration_bars']} bars"
                    )
                else:
                    lines.append(
                        f"  {idx}. Bars {dd['start_bar']}-{dd['end_bar']} | "
                        f"{dd['max_dd_pct']:.2f}% | {dd['duration_bars']} bars"
                    )

        # ── Best/Worst trades ──────────────────────────────────
        if result.trades:
            by_pnl = sorted(result.trades, key=lambda t: t.pnl, reverse=True)

            lines += ["", "[TOP 5 WINNING TRADES]"]
            for t in by_pnl[:5]:
                if t.pnl <= 0:
                    break
                ed = datetime.fromtimestamp(t.entry_timestamp / 1000, tz=timezone.utc)
                xd = datetime.fromtimestamp(t.exit_timestamp / 1000, tz=timezone.utc)
                lines.append(
                    f"  {ed:%m-%d %H:%M} -> {xd:%m-%d %H:%M} | "
                    f"${t.entry_price:,.2f} -> ${t.exit_price:,.2f} | "
                    f"PnL: ${t.pnl:+,.2f} ({t.pnl_pct:+.2f}%) | "
                    f"{t.bars_held} bars"
                )

            lines += ["", "[TOP 5 LOSING TRADES]"]
            for t in reversed(by_pnl[-5:]):
                if t.pnl > 0:
                    continue
                ed = datetime.fromtimestamp(t.entry_timestamp / 1000, tz=timezone.utc)
                xd = datetime.fromtimestamp(t.exit_timestamp / 1000, tz=timezone.utc)
                lines.append(
                    f"  {ed:%m-%d %H:%M} -> {xd:%m-%d %H:%M} | "
                    f"${t.entry_price:,.2f} -> ${t.exit_price:,.2f} | "
                    f"PnL: ${t.pnl:+,.2f} ({t.pnl_pct:+.2f}%) | "
                    f"{t.bars_held} bars"
                )

        lines += ["", "=" * 64]
        return "\n".join(lines)

    @staticmethod
    def optimization_table(
        results: list[BacktestResult],
        top_n: int = 20,
    ) -> str:
        """Tabla resumen de optimización ordenada por Sharpe."""
        lines = [
            "=" * 90,
            "  OPTIMIZATION RESULTS (sorted by Sharpe Ratio)",
            "=" * 90,
            "",
        ]

        header = (
            f"{'#':>4} | {'Sharpe':>7} | {'Sortino':>7} | "
            f"{'Profit':>11} | {'Win%':>5} | {'Trades':>6} | "
            f"{'MaxDD':>7} | Parameters"
        )
        lines.append(header)
        lines.append("-" * len(header))

        for idx, r in enumerate(results[:top_n], 1):
            m = r.metrics
            p = ", ".join(f"{k}={v}" for k, v in r.params.items())
            lines.append(
                f"{idx:>4} | {m['sharpe_ratio']:>7.3f} | {m['sortino_ratio']:>7.3f} | "
                f"${m['net_profit']:>+9,.2f} | {m['win_rate']:>4.1f}% | "
                f"{m['total_trades']:>6} | {m['max_drawdown_pct']:>6.2f}% | {p}"
            )

        lines += [
            "",
            f"  Total configuraciones: {len(results)}",
            "=" * 90,
        ]
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
#  8. LLM FEEDBACK LOOP
# ═══════════════════════════════════════════════════════════════════

def get_optimization_suggestions(
    results: list[BacktestResult],
    param_space: dict[str, list],
    strategy_name: str = "Strategy",
    symbol: str = "BTC/USDT",
    timeframe: str = "1m",
) -> str:
    """
    Genera un prompt estructurado para que un LLM sugiera
    nuevos parámetros basándose en los resultados de optimización.

    Uso:
        prompt = get_optimization_suggestions(results, PARAM_SPACE)
        # Copiar y pegar el prompt en Claude/ChatGPT
    """
    if not results:
        return "No hay resultados para analizar."

    ranked = sorted(results, key=lambda r: r.metrics.get("sharpe_ratio", 0), reverse=True)
    top = ranked[:10]
    bottom = ranked[-5:]
    sensitivity = _param_sensitivity(results)

    lines = [
        "You are a quantitative trading researcher. Analyze the following",
        "backtesting optimization results and suggest improved parameters.",
        "",
        "[CONTEXT]",
        f"  Strategy:         {strategy_name}",
        f"  Symbol:           {symbol}",
        f"  Timeframe:        {timeframe}",
        f"  Combinations:     {len(results)}",
        "",
        "[PARAMETER SPACE TESTED]",
    ]
    for k, v in param_space.items():
        lines.append(f"  {k}: {v}")

    lines += [
        "",
        "[TOP 10 RESULTS (by Sharpe Ratio)]",
        f"{'#':>3} | {'Sharpe':>7} | {'Profit%':>8} | {'WinRate':>7} | "
        f"{'MaxDD':>7} | {'Trades':>6} | Parameters",
        "-" * 85,
    ]
    for i, r in enumerate(top, 1):
        m = r.metrics
        p = ", ".join(f"{k}={v}" for k, v in r.params.items())
        lines.append(
            f"{i:>3} | {m['sharpe_ratio']:>7.3f} | {m['net_profit_pct']:>+7.2f}% | "
            f"{m['win_rate']:>6.1f}% | {m['max_drawdown_pct']:>6.2f}% | "
            f"{m['total_trades']:>6} | {p}"
        )

    lines += ["", "[BOTTOM 5 RESULTS (worst performers)]"]
    for i, r in enumerate(bottom, 1):
        m = r.metrics
        p = ", ".join(f"{k}={v}" for k, v in r.params.items())
        lines.append(
            f"  {i}. Sharpe={m['sharpe_ratio']:.3f} | "
            f"Profit={m['net_profit_pct']:+.2f}% | MaxDD={m['max_drawdown_pct']:.2f}% | {p}"
        )

    if sensitivity:
        lines += ["", "[PARAMETER SENSITIVITY (correlation with Sharpe Ratio)]"]
        for param, corr in sensitivity:
            hint = "higher is better" if corr > 0 else "lower is better"
            lines.append(f"  {param}: r={corr:+.3f} ({hint})")

    lines += [
        "",
        "[REQUEST]",
        "Based on this analysis:",
        "1. Which parameter ranges should be explored more finely?",
        "2. Suggest 5-10 specific new parameter combinations to test.",
        "3. Are there any patterns indicating overfitting risk?",
        "4. What additional indicators or filters would improve this strategy?",
        "5. Recommend a risk-adjusted position sizing rule.",
        "",
        "Respond with concrete values in JSON:",
        '  [{"ema_fast": X, "ema_slow": Y, "rsi_period": Z, ...}, ...]',
    ]
    return "\n".join(lines)


def _param_sensitivity(results: list[BacktestResult]) -> list[tuple[str, float]]:
    """Correlación de cada parámetro con Sharpe Ratio."""
    if len(results) < 5:
        return []

    sharpe = [r.metrics.get("sharpe_ratio", 0) for r in results]
    keys = list(results[0].params.keys())
    corrs = []

    for k in keys:
        try:
            vals = [float(r.params[k]) for r in results]
            if len(set(vals)) < 2:
                continue
            c = float(np.corrcoef(vals, sharpe)[0, 1])
            if not np.isnan(c):
                corrs.append((k, round(c, 3)))
        except (ValueError, TypeError):
            continue

    corrs.sort(key=lambda x: abs(x[1]), reverse=True)
    return corrs


# ═══════════════════════════════════════════════════════════════════
#  9. VISUALIZATION (Plotly)
# ═══════════════════════════════════════════════════════════════════

def generate_chart(
    result: BacktestResult,
    data: pd.DataFrame,
    output_path: str | Path = "backtest_chart.html",
    strategy_name: str = "Strategy",
) -> Path:
    """
    Genera gráfico interactivo en HTML con 3 paneles:
        1. Precio + señales de entrada/salida
        2. Curva de equity
        3. Drawdown %
    """
    try:
        from plotly.subplots import make_subplots
        import plotly.graph_objects as go
    except ImportError:
        print("  [WARN] Plotly no instalado. Ejecuta: pip install plotly")
        return Path(output_path)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Eje temporal
    n = len(result.equity_curve)
    ts_dt = [
        datetime.fromtimestamp(t / 1000, tz=timezone.utc)
        for t in result.timestamps[:n]
    ]
    prices = data["close"].values[:n]

    # Drawdown
    eq = pd.Series(result.equity_curve)
    rmax = eq.cummax()
    dd_pct = ((eq - rmax) / rmax * 100).values

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.40, 0.35, 0.25],
        subplot_titles=(
            f"Price & Signals — {strategy_name}",
            "Equity Curve",
            "Drawdown %",
        ),
    )

    # ── Panel 1: Precio ────────────────────────────────────────
    fig.add_trace(
        go.Scatter(
            x=ts_dt, y=prices,
            name="Price", line=dict(color="#636EFA", width=1),
        ),
        row=1, col=1,
    )

    for t in result.trades:
        buy_dt = datetime.fromtimestamp(t.entry_timestamp / 1000, tz=timezone.utc)
        fig.add_trace(
            go.Scatter(
                x=[buy_dt], y=[t.entry_price], mode="markers",
                marker=dict(symbol="triangle-up", size=10, color="#00CC96"),
                name="BUY", showlegend=False,
                hovertext=f"BUY @ ${t.entry_price:,.2f}",
            ),
            row=1, col=1,
        )
        sell_dt = datetime.fromtimestamp(t.exit_timestamp / 1000, tz=timezone.utc)
        color = "#EF553B" if t.pnl < 0 else "#00CC96"
        fig.add_trace(
            go.Scatter(
                x=[sell_dt], y=[t.exit_price], mode="markers",
                marker=dict(symbol="triangle-down", size=10, color=color),
                name="SELL", showlegend=False,
                hovertext=f"SELL @ ${t.exit_price:,.2f} | PnL: ${t.pnl:+,.2f}",
            ),
            row=1, col=1,
        )

    # ── Panel 2: Equity ───────────────────────────────────────
    fig.add_trace(
        go.Scatter(
            x=ts_dt, y=result.equity_curve,
            name="Equity", line=dict(color="#AB63FA", width=1.5),
            fill="tozeroy", fillcolor="rgba(171,99,250,0.1)",
        ),
        row=2, col=1,
    )

    # ── Panel 3: Drawdown ─────────────────────────────────────
    fig.add_trace(
        go.Scatter(
            x=ts_dt, y=dd_pct,
            name="Drawdown", line=dict(color="#EF553B", width=1),
            fill="tozeroy", fillcolor="rgba(239,85,59,0.15)",
        ),
        row=3, col=1,
    )

    m = result.metrics
    fig.update_layout(
        title=dict(
            text=(
                f"{strategy_name} | "
                f"Profit: ${m['net_profit']:+,.2f} ({m['net_profit_pct']:+.2f}%) | "
                f"Sharpe: {m['sharpe_ratio']:.3f} | "
                f"MaxDD: {m['max_drawdown_pct']:.2f}%"
            ),
            x=0.5,
        ),
        height=900, template="plotly_dark",
        showlegend=False, hovermode="x unified",
    )
    fig.update_yaxes(title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="Equity ($)", row=2, col=1)
    fig.update_yaxes(title_text="DD %", row=3, col=1)

    fig.write_html(str(output_path))
    print(f"  Chart saved: {output_path}")
    return output_path
