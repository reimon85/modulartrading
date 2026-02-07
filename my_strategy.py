"""
my_strategy.py — Motor de reglas manual para trading algorítmico.

Lee datos en vivo de Redis o procesa Parquet para backtesting.
TU defines las reglas.  El motor solo las ejecuta.

Uso:
    python my_strategy.py live                          # Tiempo real (Redis)
    python my_strategy.py live --poll 5                 # Polling cada 5s
    python my_strategy.py backtest                      # Parquet por defecto
    python my_strategy.py backtest --file data/x.parquet

Estructura del archivo (4 secciones):
    1. StrategyConfig  — tus parámetros (edita libremente)
    2. CustomStrategy   — tus indicadores y reglas de compra/venta
    3. AlertManager     — alertas a Discord / Telegram / consola
    4. StrategyRunner   — motor de ejecución (NO tocar)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import numpy as np
import pandas as pd

# ── Imports del proyecto ────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
from src import config  # noqa: E402
from src.database import RedisManager  # noqa: E402

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("strategy")


# ═══════════════════════════════════════════════════════════════════
#  SECCION 1: TUS PARAMETROS
# ───────────────────────────────────────────────────────────────────
#  Cambia cualquier valor aquí.  Estos son los UNICOS controles
#  que necesitas tocar para un barrido rápido de parámetros.
# ═══════════════════════════════════════════════════════════════════

@dataclass
class StrategyConfig:
    # ── Indicadores ────────────────────────────────────────────
    ema_fast_period: int = 9       # EMA rápida (señal)
    ema_slow_period: int = 21      # EMA lenta  (tendencia)
    rsi_period: int = 14           # Periodo RSI
    rsi_oversold: float = 30.0     # Umbral de sobreventa
    rsi_overbought: float = 70.0   # Umbral de sobrecompra

    # ── Mercado ────────────────────────────────────────────────
    symbol: str = "BTC/USDT"
    timeframe: str = "1m"

    # ── Motor en vivo ──────────────────────────────────────────
    poll_interval: int = 10        # Segundos entre lecturas de Redis

    # ── Alertas (dejar vacío para desactivar) ──────────────────
    discord_webhook: str = os.getenv("DISCORD_WEBHOOK_URL", "")
    telegram_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")

    @property
    def min_bars(self) -> int:
        """Mínimo de velas antes de que los indicadores sean válidos."""
        return max(self.ema_fast_period, self.ema_slow_period, self.rsi_period) + 2


# ═══════════════════════════════════════════════════════════════════
#  SECCION 2: TUS REGLAS DE TRADING
# ───────────────────────────────────────────────────────────────────
#  - compute_indicators() → añade columnas de indicadores al DF
#  - check_signals()      → evalúa BUY / SELL / WAIT
#
#  Puedes editar ambos métodos sin tocar el resto del archivo.
# ═══════════════════════════════════════════════════════════════════

class CustomStrategy:
    """
    Motor de reglas.  Modifica check_signals() con tus condiciones.

    El motor llama a process_bar() cada ciclo de polling.
    check_signals() debe devolver:  ("BUY"|"SELL"|"WAIT", "razón")
    """

    def __init__(self, cfg: StrategyConfig):
        self.cfg = cfg

    # ── Tus indicadores ────────────────────────────────────────

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Añade indicadores técnicos al DataFrame de velas.

        Usa pandas/numpy puro (sin dependencias externas).
        Para agregar uno nuevo, usa los helpers de abajo como ejemplo:

            df["sma_50"]  = df["close"].rolling(50).mean()
            df["bbu"]     = df["close"].rolling(20).mean() + 2 * df["close"].rolling(20).std()
            df["atr"]     = _calc_atr(df, period=14)
        """
        df = df.copy()
        df["ema_fast"] = df["close"].ewm(span=self.cfg.ema_fast_period, adjust=False).mean()
        df["ema_slow"] = df["close"].ewm(span=self.cfg.ema_slow_period, adjust=False).mean()
        df["rsi"] = _calc_rsi(df["close"], period=self.cfg.rsi_period)
        return df

    # ── Tus reglas de compra/venta ─────────────────────────────

    def check_signals(self, df: pd.DataFrame) -> tuple[str, str]:
        """
        ==========================================================
        >>> TUS REGLAS VAN AQUI <<<
        ==========================================================

        Recibe un DataFrame CON las columnas de indicadores ya
        calculadas.  Debe devolver una tupla:

            ("BUY",  "razón de la compra")
            ("SELL", "razón de la venta")
            ("WAIT", "")

        Estrategia por defecto: Cruce de Medias + RSI
        --------------------------------------------------
          BUY  → EMA rápida cruza POR ENCIMA de EMA lenta
                 Y el RSI está en zona de sobreventa (< 30)

          SELL → EMA rápida cruza POR DEBAJO de EMA lenta
                 Y el RSI está en zona de sobrecompra (> 70)
        --------------------------------------------------
        """
        if len(df) < 2:
            return "WAIT", "Datos insuficientes"

        curr = df.iloc[-1]
        prev = df.iloc[-2]

        # Guard: indicadores aún calentando (NaN)
        for col in ("ema_fast", "ema_slow", "rsi"):
            if pd.isna(curr.get(col)) or pd.isna(prev.get(col)):
                return "WAIT", "Indicadores calentando"

        # ────────────────────────────────────────────────────────
        # REGLAS DE COMPRA
        # ────────────────────────────────────────────────────────
        ema_cross_up = (
            prev["ema_fast"] <= prev["ema_slow"]
            and curr["ema_fast"] > curr["ema_slow"]
        )
        rsi_oversold = curr["rsi"] < self.cfg.rsi_oversold

        if ema_cross_up and rsi_oversold:
            return (
                "BUY",
                f"EMA{self.cfg.ema_fast_period} cruzo ARRIBA de "
                f"EMA{self.cfg.ema_slow_period} "
                f"| RSI {curr['rsi']:.1f} < {self.cfg.rsi_oversold}",
            )

        # ────────────────────────────────────────────────────────
        # REGLAS DE VENTA
        # ────────────────────────────────────────────────────────
        ema_cross_down = (
            prev["ema_fast"] >= prev["ema_slow"]
            and curr["ema_fast"] < curr["ema_slow"]
        )
        rsi_overbought = curr["rsi"] > self.cfg.rsi_overbought

        if ema_cross_down and rsi_overbought:
            return (
                "SELL",
                f"EMA{self.cfg.ema_fast_period} cruzo ABAJO de "
                f"EMA{self.cfg.ema_slow_period} "
                f"| RSI {curr['rsi']:.1f} > {self.cfg.rsi_overbought}",
            )

        # ────────────────────────────────────────────────────────
        # SIN SEÑAL
        # ────────────────────────────────────────────────────────
        return "WAIT", ""

    def process_bar(self, df: pd.DataFrame) -> tuple[str, str, dict]:
        """Ciclo completo: indicadores → señales → snapshot."""
        df = self.compute_indicators(df)
        signal, reason = self.check_signals(df)
        snap = _snapshot(df.iloc[-1])
        snap["signal"] = signal
        return signal, reason, snap


# ── Indicator helpers (pandas puro, sin dependencias externas) ────

def _calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """RSI de Wilder: media exponencial de ganancias vs pérdidas."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range — útil como indicador extra de volatilidad."""
    high_low = df["high"] - df["low"]
    high_prev = (df["high"] - df["close"].shift(1)).abs()
    low_prev = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_prev, low_prev], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


# ═══════════════════════════════════════════════════════════════════
#  SECCION 3: SISTEMA DE ALERTAS
# ───────────────────────────────────────────────────────────────────
#  Configura tus webhooks en el archivo .env:
#    DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
#    TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
#    TELEGRAM_CHAT_ID=987654321
#
#  Si ambos están vacíos, las alertas solo salen por consola.
# ═══════════════════════════════════════════════════════════════════

class AlertManager:
    """Envía señales a Discord, Telegram y/o consola."""

    def __init__(self, cfg: StrategyConfig):
        self.cfg = cfg
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def notify(self, signal: str, reason: str, snap: dict) -> None:
        """Formatea y envía la alerta a todos los canales configurados."""
        msg = self._format(signal, reason, snap)
        logger.info("\n%s", msg)

        coros = []
        if self.cfg.discord_webhook:
            coros.append(self._send_discord(msg))
        if self.cfg.telegram_token and self.cfg.telegram_chat_id:
            coros.append(self._send_telegram(msg))

        for result in await asyncio.gather(*coros, return_exceptions=True):
            if isinstance(result, Exception):
                logger.warning("Fallo en entrega de alerta: %s", result)

    def _format(self, signal: str, reason: str, snap: dict) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        price = snap.get("price", 0)
        ema_f = snap.get("ema_fast")
        ema_s = snap.get("ema_slow")
        rsi = snap.get("rsi")

        tag = f">>> SIGNAL: {signal}" if signal != "WAIT" else "--- WAIT"
        lines = [
            f"{tag} | {self.cfg.symbol} @ ${price:,.2f}",
            f"    Time:     {ts}",
            f"    EMA{self.cfg.ema_fast_period}:    {'${:,.2f}'.format(ema_f) if ema_f else 'n/a'}",
            f"    EMA{self.cfg.ema_slow_period}:   {'${:,.2f}'.format(ema_s) if ema_s else 'n/a'}",
            f"    RSI:      {f'{rsi:.1f}' if rsi else 'n/a'}",
        ]
        if reason:
            lines.append(f"    Reason:   {reason}")
        return "\n".join(lines)

    async def _send_discord(self, text: str) -> None:
        session = await self._get_session()
        payload = {"content": f"```\n{text}\n```", "username": "StrategyBot"}
        async with session.post(self.cfg.discord_webhook, json=payload) as r:
            if r.status not in (200, 204):
                logger.warning("Discord %d: %s", r.status, await r.text())

    async def _send_telegram(self, text: str) -> None:
        session = await self._get_session()
        url = f"https://api.telegram.org/bot{self.cfg.telegram_token}/sendMessage"
        payload = {
            "chat_id": self.cfg.telegram_chat_id,
            "text": f"```\n{text}\n```",
            "parse_mode": "Markdown",
        }
        async with session.post(url, json=payload) as r:
            if r.status != 200:
                logger.warning("Telegram %d: %s", r.status, await r.text())


# ═══════════════════════════════════════════════════════════════════
#  SECCION 4: MOTOR DE EJECUCION  (no necesitas editar esto)
# ═══════════════════════════════════════════════════════════════════

def _append_signal_log(signal: str, price: float, rsi: float | None, reason: str) -> None:
    """Append a BUY/SELL signal to data/signals_log.parquet."""
    log_path = config.DATA_DIR / "signals_log.parquet"
    row = pd.DataFrame([{
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "signal": signal,
        "price": price,
        "rsi": rsi,
        "reason": reason,
    }])
    try:
        if log_path.exists():
            existing = pd.read_parquet(log_path)
            row = pd.concat([existing, row], ignore_index=True)
        row.to_parquet(log_path, index=False)
    except Exception as exc:
        logger.warning("Failed to write signals log: %s", exc)


def _snapshot(row: pd.Series) -> dict:
    """Extrae un dict resumen de una fila del DataFrame."""
    def _safe(col: str):
        v = row.get(col)
        return float(v) if pd.notna(v) else None
    return {
        "timestamp": int(row.get("timestamp", 0)),
        "price": _safe("close"),
        "ema_fast": _safe("ema_fast"),
        "ema_slow": _safe("ema_slow"),
        "rsi": _safe("rsi"),
    }


class StrategyRunner:
    """Orquesta los modos LIVE (Redis) y BACKTEST (Parquet)."""

    def __init__(self, cfg: StrategyConfig):
        self.cfg = cfg
        self.strategy = CustomStrategy(cfg)
        self.alerts = AlertManager(cfg)

    # ── MODO LIVE ──────────────────────────────────────────────

    async def run_live(self) -> None:
        """Polling loop: lee velas de Redis → indicadores → señales."""
        redis_mgr = RedisManager()
        await redis_mgr.connect()

        self._banner("LIVE")
        logger.info(
            "Polling cada %ds | Barras minimas: %d",
            self.cfg.poll_interval,
            self.cfg.min_bars,
        )

        last_signal = "WAIT"
        scan = 0

        try:
            while True:
                scan += 1

                candles = await redis_mgr.get_candles(
                    timeframe=self.cfg.timeframe,
                    limit=100,
                )

                if len(candles) < self.cfg.min_bars:
                    logger.info(
                        "SCAN #%d | Buffering... (%d/%d barras)",
                        scan, len(candles), self.cfg.min_bars,
                    )
                    await asyncio.sleep(self.cfg.poll_interval)
                    continue

                df = pd.DataFrame(candles)
                signal, reason, snap = self.strategy.process_bar(df)

                # Alerta solo en transiciones de estado
                if signal != "WAIT" and signal != last_signal:
                    await self.alerts.notify(signal, reason, snap)
                    _append_signal_log(signal, snap["price"] or 0, snap["rsi"], reason)
                elif scan % 6 == 0:  # log silencioso cada ~60s
                    logger.info(
                        "SCAN #%d | $%,.2f | RSI %s | %s",
                        scan,
                        snap["price"] or 0,
                        f'{snap["rsi"]:.1f}' if snap["rsi"] else "n/a",
                        signal,
                    )

                last_signal = signal
                await asyncio.sleep(self.cfg.poll_interval)

        except KeyboardInterrupt:
            logger.info("Detenido por el usuario (Ctrl+C)")
        finally:
            await self.alerts.close()
            await redis_mgr.disconnect()

    # ── MODO BACKTEST ──────────────────────────────────────────

    async def run_backtest(self, file_path: str | None = None) -> None:
        """Procesa un archivo Parquet barra por barra con la misma lógica."""
        path = Path(
            file_path
            or config.PARQUET_DIR
            / f"{self.cfg.symbol.replace('/', '_')}_{self.cfg.timeframe}.parquet"
        )
        if not path.exists():
            logger.error("Parquet no encontrado: %s", path)
            return

        df_raw = pd.read_parquet(path)
        required = {"timestamp", "open", "high", "low", "close", "volume"}
        if not required.issubset(df_raw.columns):
            logger.error(
                "Columnas faltantes. Necesarias: %s | Presentes: %s",
                required, set(df_raw.columns),
            )
            return

        self._banner("BACKTEST")
        logger.info("Archivo: %s (%d barras)", path.name, len(df_raw))

        # Indicadores calculados UNA sola vez sobre todo el histórico
        df = self.strategy.compute_indicators(df_raw)

        signals: list[dict] = []
        for i in range(self.cfg.min_bars, len(df)):
            window = df.iloc[: i + 1]
            signal, reason = self.strategy.check_signals(window)

            if signal == "WAIT":
                continue

            snap = _snapshot(df.iloc[i])
            signals.append({
                "bar": i,
                "timestamp": snap["timestamp"],
                "price": snap["price"],
                "signal": signal,
                "reason": reason,
                "rsi": snap["rsi"],
            })
            logger.info(
                "Bar %d | %s @ $%,.2f | %s",
                i, signal, snap["price"] or 0, reason,
            )

        self._backtest_summary(df, signals)

    # ── Helpers internos ───────────────────────────────────────

    def _backtest_summary(self, df: pd.DataFrame, signals: list[dict]) -> None:
        buys = sum(1 for s in signals if s["signal"] == "BUY")
        sells = sum(1 for s in signals if s["signal"] == "SELL")
        t0 = datetime.fromtimestamp(df["timestamp"].iloc[0] / 1000, tz=timezone.utc)
        t1 = datetime.fromtimestamp(df["timestamp"].iloc[-1] / 1000, tz=timezone.utc)

        print(f"""
================================================================
  BACKTEST SUMMARY
================================================================
  Periodo:      {t0:%Y-%m-%d %H:%M} -> {t1:%Y-%m-%d %H:%M} UTC
  Total barras: {len(df):,}
  Senales:      {buys} BUY | {sells} SELL | {len(df) - len(signals):,} WAIT
  Parametros:   EMA({self.cfg.ema_fast_period}/{self.cfg.ema_slow_period}) + RSI({self.cfg.rsi_period})
================================================================""")

        if signals:
            print("  Signal log:")
            print("  " + "-" * 62)
            for s in signals:
                dt = datetime.fromtimestamp(s["timestamp"] / 1000, tz=timezone.utc)
                rsi_str = f"{s['rsi']:.1f}" if s["rsi"] is not None else "n/a"
                print(
                    f"  {dt:%Y-%m-%d %H:%M}  {s['signal']:4s}  "
                    f"${s['price']:>12,.2f}   RSI {rsi_str}"
                )
            print("  " + "-" * 62)

    def _banner(self, mode: str) -> None:
        print(f"""
================================================================
  STRATEGY ENGINE | {self.cfg.symbol}
  Mode: {mode} | TF: {self.cfg.timeframe} | EMA({self.cfg.ema_fast_period}/{self.cfg.ema_slow_period}) + RSI({self.cfg.rsi_period})
================================================================
""")


# ═══════════════════════════════════════════════════════════════════
#  ENTRYPOINT
# ═══════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Motor de estrategias manual (rule-based)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
ejemplos:
  python my_strategy.py live                       Tiempo real (Redis)
  python my_strategy.py live --poll 5              Polling cada 5s
  python my_strategy.py backtest                   Parquet por defecto
  python my_strategy.py backtest --file x.parquet  Archivo custom
        """,
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    # Subcomando: live
    live_p = sub.add_parser("live", help="Modo tiempo real via Redis")
    live_p.add_argument(
        "--poll", type=int, default=10,
        help="Segundos entre escaneos (default: 10)",
    )
    live_p.add_argument("--timeframe", default="1m")

    # Subcomando: backtest
    bt_p = sub.add_parser("backtest", help="Modo historico via Parquet")
    bt_p.add_argument("--file", default=None, help="Ruta al archivo .parquet")
    bt_p.add_argument("--timeframe", default="1m")

    args = parser.parse_args()

    cfg = StrategyConfig(timeframe=getattr(args, "timeframe", "1m"))
    if hasattr(args, "poll"):
        cfg.poll_interval = args.poll

    runner = StrategyRunner(cfg)

    if args.mode == "live":
        asyncio.run(runner.run_live())
    else:
        asyncio.run(runner.run_backtest(getattr(args, "file", None)))


if __name__ == "__main__":
    main()
