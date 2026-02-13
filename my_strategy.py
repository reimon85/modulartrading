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
import json
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
from src.notifier import AlertLevel, TelegramNotifier  # noqa: E402

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
    ema_fast_period: int = 34       # EMA rápida (señal)
    ema_slow_period: int = 144      # EMA lenta  (tendencia)
    ema_mid_period: int = 89       # EMA media (opcional)
    rsi_period: int = 14           # Periodo RSI
    rsi_oversold: float = 30.0     # Umbral de sobreventa
    rsi_overbought: float = 70.0   # Umbral de sobrecompra

    # ── Mercado ────────────────────────────────────────────────
    symbol: str = "BTC/USDT"
    timeframe: str = "2h"

    # ── Motor en vivo ──────────────────────────────────────────
    poll_interval: int = 10         # Segundos entre lecturas de Redis

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

# ── Indicator helpers (pandas puro, sin dependencias externas) ────

def _calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """RSI de Wilder: media exponencial de ganancias vs pérdidas."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    # Handle division by zero: if avg_loss is 0, rs should be np.inf
    rs = np.where(avg_loss == 0, np.inf, avg_gain / avg_loss)
    return pd.Series(100 - (100 / (1 + rs)), index=series.index)

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
        """Añade indicadores técnicos al DataFrame de velas."""
        df = df.copy()
        # --- Lógica de LAST UNHIT PIVOT y DAYLI BIAS ---
# 1. Asegurar formato de fecha para agrupar correctamente
        df['dt'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df['date'] = df['dt'].dt.date
        
# 2. Información de la vela de 2H ANTERIOR (ya cerrada)
        # Como el DF ya viene en velas de 2H, el desplazamiento es directo
        df['last_2h_close'] = df['close'].shift(1)
        df['last_2h_open'] = df['open'].shift(1)
# Diferencia en puntos y color
        df['last_2h_size'] = df['last_2h_close'] - df['last_2h_open']
        df['last_2h_color'] = np.where(df['last_2h_size'] > 0, 1, -1)
# 2. Calcular el Pivote Típico Diario (H+L+C)/3
        # Agrupamos por día para obtener los valores OHLC del día completo
        daily_stats = df.groupby('date').agg({
            'high': 'max',
            'low': 'min',
            'close': 'last'
        })
        daily_stats['pivot'] = (daily_stats['high'] + daily_stats['low'] + daily_stats['close']) / 3
        
        # LÓGICA: PIVOT BIAS FLAG ---
        # Comparamos el pivote de hoy con el de ayer
        daily_stats['prev_pivot'] = daily_stats['pivot'].shift(1)
        
        # Creamos el Flag: 1 para Alcista, -1 para Bajista, 0 para Neutral
        daily_stats['pivot_bias'] = 0
        daily_stats.loc[daily_stats['pivot'] > daily_stats['prev_pivot'], 'pivot_bias'] = 1
        daily_stats.loc[daily_stats['pivot'] < daily_stats['prev_pivot'], 'pivot_bias'] = -1

        # 3. Mapear estos valores de vuelta al DataFrame principal (minuto a minuto)
        df['pivot_bias'] = df['date'].map(daily_stats['pivot_bias']).fillna(0)

        # 3. Lógica "Last Unhit" (Buscando el imán más reciente)
        # Para cada día, buscamos hacia atrás el pivote más reciente que no fue tocado
        df['unhit_pivot_val'] = np.nan
        df['unhit_pivot_days'] = 0
        df['unhit_pivot_side'] = "NONE"

        unique_days = daily_stats.index.tolist()

        # Pre-computar high/low diarios para chequeo rápido de "touched"
        daily_highs = df.groupby('date')['high'].max()
        daily_lows = df.groupby('date')['low'].min()

        for d_idx in range(1, len(unique_days)):
            today = unique_days[d_idx]
            today_close = daily_stats.loc[today, 'close']

            # Buscar hacia atrás desde ayer
            for past_idx in range(d_idx - 1, -1, -1):
                past_date = unique_days[past_idx]
                p_val = daily_stats.loc[past_date, 'pivot']

                # Comprobar si algún día entre past_date+1 y today tocó el pivote
                check_days = unique_days[past_idx + 1 : d_idx + 1]
                touched = False
                for cd in check_days:
                    if daily_lows.get(cd, p_val - 1) <= p_val <= daily_highs.get(cd, p_val + 1):
                        touched = True
                        break

                if not touched:
                    mask = df['date'] == today
                    df.loc[mask, 'unhit_pivot_val'] = p_val
                    df.loc[mask, 'unhit_pivot_days'] = (today - past_date).days
                    # LOWER = precio por debajo del pivot (pivot perdido yendo abajo)
                    # UPPER = precio por encima del pivot (pivot perdido yendo arriba)
                    df.loc[mask, 'unhit_pivot_side'] = "LOWER" if today_close < p_val else "UPPER"
                    break

        # ── EMAs en timeframe 2H ─────────────────────────────────
        df["ema_fast"] = df["close"].ewm(span=self.cfg.ema_fast_period, adjust=False).mean()
        df["ema_slow"] = df["close"].ewm(span=self.cfg.ema_slow_period, adjust=False).mean()
        df["ema_mid"] = df["close"].ewm(span=self.cfg.ema_mid_period, adjust=False).mean()
        df["rsi"] = _calc_rsi(df["close"], period=self.cfg.rsi_period)

        # ── EMA GAP: posición relativa de las 3 EMAs ─────────────
        #   BULLISH  → EMA34 > EMA89 > EMA144
        #   BEARISH  → EMA34 < EMA89 < EMA144
        #   NEUTRAL  → cualquier otro orden
        df["ema_gap"] = np.where(
            (df["ema_fast"] > df["ema_mid"]) & (df["ema_mid"] > df["ema_slow"]),
            "BULLISH",
            np.where(
                (df["ema_fast"] < df["ema_mid"]) & (df["ema_mid"] < df["ema_slow"]),
                "BEARISH",
                "NEUTRAL",
            ),
        )

        # ── DAILY EMA34 SLOPE: pendiente de la EMA34 en gráfica diaria ──
        #   Calculamos EMA34 sobre closes diarios, luego mapeamos
        #   la pendiente (hoy vs ayer) a cada barra del DF.
        #   1 = subiendo (solo BUY permitido)
        #  -1 = bajando  (solo SELL permitido)
        #   0 = plana / sin datos
        daily_close = df.groupby('date')['close'].last()
        daily_ema34 = daily_close.ewm(span=self.cfg.ema_fast_period, adjust=False).mean()
        daily_ema34_prev = daily_ema34.shift(1)
        daily_slope = pd.Series(0, index=daily_ema34.index)
        daily_slope[daily_ema34 > daily_ema34_prev] = 1
        daily_slope[daily_ema34 < daily_ema34_prev] = -1
        df['daily_ema34_slope'] = df['date'].map(daily_slope).fillna(0).astype(int)

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
        # FILTRO DIRECCIONAL: pendiente EMA34 diaria
        # ────────────────────────────────────────────────────────
        #   slope  1 → solo BUY permitido
        #   slope -1 → solo SELL permitido
        slope = curr.get('daily_ema34_slope', 0)

        # ────────────────────────────────────────────────────────
        # REGLAS DE COMPRA
        # ────────────────────────────────────────────────────────
        #   Contexto: pivot_bias alcista + unhit_pivot UPPER + ema_gap BULLISH
        #   Trigger:  vela 2H cierra con caída > 200 pts (pullback)
        if (slope >= 0
                and curr['pivot_bias'] == 1
                and curr['unhit_pivot_side'] == "UPPER"
                and curr['ema_gap'] == "BULLISH"
                and curr['last_2h_size'] < -200):
            return (
                "BUY",
                f"Pullback 2H ({curr['last_2h_size']:.0f}) en estructura alcista"
                f" | pivot unhit ${curr['unhit_pivot_val']:,.0f}",
            )

        # ────────────────────────────────────────────────────────
        # REGLAS DE VENTA
        # ────────────────────────────────────────────────────────
        #   Contexto: pivot_bias bajista + unhit_pivot LOWER + ema_gap BEARISH
        #   Trigger:  vela 2H cierra con subida > 200 pts (rally)
        if (slope <= 0
                and curr['pivot_bias'] == -1
                and curr['unhit_pivot_side'] == "LOWER"
                and curr['ema_gap'] == "BEARISH"
                and curr['last_2h_size'] > 200):
            return (
                "SELL",
                f"Rally 2H (+{curr['last_2h_size']:.0f}) en estructura bajista"
                f" | pivot unhit ${curr['unhit_pivot_val']:,.0f}",
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
        self._notifier = TelegramNotifier()

    async def connect(self) -> None:
        await self._notifier.connect()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        await self._notifier.disconnect()

    async def notify(self, signal: str, reason: str, snap: dict) -> None:
        """Formatea y envía la alerta a todos los canales configurados."""
        msg = self._format(signal, reason, snap)
        logger.info("\n%s", msg)

        coros = []
        if self.cfg.discord_webhook:
            coros.append(self._send_discord(msg))
        # Telegram via TelegramNotifier
        price = snap.get("price", 0)
        details = (
            f"  {self.cfg.symbol} @ ${price:,.2f}\n"
            f"  Reason: {reason}"
        )
        coros.append(self._notifier.notify(
            AlertLevel.INFO, f"SIGNAL {signal}", details, source="Strategy",
        ))

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
        await self.alerts.connect()

        self._banner("LIVE")
        logger.info(
            "Polling cada %ds | Barras minimas: %d",
            self.cfg.poll_interval,
            self.cfg.min_bars,
        )

        last_signal = "WAIT"
        scan = 0

        # ── Warm-up: if Redis has insufficient bars, try Parquet fallback ──
        initial_candles = await redis_mgr.get_candles(
            timeframe=self.cfg.timeframe, limit=1000,
        )
        if len(initial_candles) < self.cfg.min_bars:
            pq_path = (
                config.PARQUET_DIR
                / f"{self.cfg.symbol.replace('/', '_')}_{self.cfg.timeframe}.parquet"
            )
            if pq_path.exists():
                try:
                    df_warm = pd.read_parquet(pq_path)
                    # Drop datetime col (Timestamp not JSON-serializable)
                    if "datetime" in df_warm.columns:
                        df_warm = df_warm.drop(columns=["datetime"])
                    warmup_candles = df_warm.to_dict("records")[-1000:]
                    await redis_mgr.store_candles(warmup_candles, timeframe=self.cfg.timeframe)
                    logger.info(
                        "Warm-up complete: %d bars loaded from Parquet (%s)",
                        len(warmup_candles), pq_path.name,
                    )
                except Exception as exc:
                    logger.warning("Warm-up from Parquet failed: %s", exc)
            else:
                logger.info(
                    "Warm-up: Redis has %d/%d bars, no Parquet fallback at %s",
                    len(initial_candles), self.cfg.min_bars, pq_path,
                )

        try:
            while True:
                try:
                    scan += 1

                    candles = await redis_mgr.get_candles(
                        timeframe=self.cfg.timeframe,
                        limit=1000,
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
                        await self._publish_signal(redis_mgr, signal, snap)
                    elif scan % 6 == 0:  # log silencioso cada ~60s
                        logger.info(
                            "SCAN #%d | $%.2f | RSI %s | %s",
                            scan,
                            snap["price"] or 0,
                            f'{snap["rsi"]:.1f}' if snap["rsi"] else "n/a",
                            signal,
                        )

                    last_signal = signal
                except Exception as exc:
                    logger.error("Error in live scan #%d: %s", scan, exc, exc_info=True)
                    # Wait a bit longer on error to avoid rapid-fire failure logs
                    await asyncio.sleep(max(self.cfg.poll_interval, 5))

                await asyncio.sleep(self.cfg.poll_interval)

        except KeyboardInterrupt:
            logger.info("Detenido por el usuario (Ctrl+C)")
        finally:
            await self.alerts.close()
            await redis_mgr.disconnect()

    # ── SIGNAL BRIDGE → EXECUTOR ────────────────────────────

    async def _publish_signal(
        self, redis_mgr: RedisManager, signal: str, snap: dict,
    ) -> None:
        """Publica señal al canal Redis para que el executor la reciba."""
        price = snap.get("price") or 0.0

        if not price or not np.isfinite(price):
            logger.error("Precio inválido (%.4s) — señal %s descartada", price, signal)
            return

        is_buy = signal == "BUY"

        if is_buy:
            sl_price = price - config.STRATEGY_SL_POINTS
            tp_price = price + config.STRATEGY_TP_POINTS
        else:
            sl_price = price + config.STRATEGY_SL_POINTS
            tp_price = price - config.STRATEGY_TP_POINTS

        # Ensure values are native Python types (numpy floats crash json.dumps)
        payload = {
            "action": signal,
            "pair": str(self.cfg.symbol),
            "exchange": str(config.SIGNAL_TARGET_EXCHANGE),
            "sl_price": float(round(sl_price, 2)),
            "tp_type": "PRICE",
            "tp_value": float(round(tp_price, 2)),
            "size": float(config.STRATEGY_ENTRY_SIZE_BTC),
            "max_entries": int(config.STRATEGY_MAX_ENTRIES),
        }

        # Guardia final: rechazar si algún valor numérico es NaN/Inf
        for key in ("sl_price", "tp_value", "size"):
            if not np.isfinite(payload[key]):
                logger.error("Payload con %s=%s — señal descartada", key, payload[key])
                return

        notifier = TelegramNotifier()
        try:
            await redis_mgr.client.publish(config.HL_SIGNAL_CHANNEL, json.dumps(payload))
            logger.info(
                "SIGNAL PUBLISHED → %s | SL=$%.2f | TP=$%.2f | size=%.4f | max_entries=%d",
                signal, sl_price, tp_price,
                config.STRATEGY_ENTRY_SIZE_BTC, config.STRATEGY_MAX_ENTRIES,
            )
            await notifier.notify(
                AlertLevel.INFO, f"Signal {signal} published",
                f"  {self.cfg.symbol} @ ${price:,.2f}\n  SL=${sl_price:,.2f} | TP=${tp_price:,.2f}",
                source="Strategy",
            )
        except Exception as exc:
            logger.error("Failed to publish signal: %s", exc)
            await notifier.notify(
                AlertLevel.CRITICAL, f"Signal {signal} FAILED to publish",
                f"  Error: {exc}", source="Strategy",
            )

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
                "Bar %d | %s @ $%.2f | %s",
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
