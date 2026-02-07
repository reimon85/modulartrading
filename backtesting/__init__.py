"""Universal Backtesting Engine — optimizado para workflows con LLM."""

from backtesting.engine import (
    BacktestEngine,
    BacktestResult,
    BaseStrategy,
    DataLoader,
    Optimizer,
    ReportGenerator,
    Trade,
    generate_chart,
    get_optimization_suggestions,
)

__all__ = [
    "BacktestEngine",
    "BacktestResult",
    "BaseStrategy",
    "DataLoader",
    "Optimizer",
    "ReportGenerator",
    "Trade",
    "generate_chart",
    "get_optimization_suggestions",
]
