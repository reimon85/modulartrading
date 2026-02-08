"""
lighter_executor.py — Ejecutor Inteligente de Ordenes para Lighter.xyz DEX.

Arquitectura identica a hl_smart_executor.py pero usando LighterClient.
Redis keys con prefijo 'executor:lighter:' para no colisionar con HL.

Senales esperadas en canal Redis (JSON):
    {"action":"BUY", "pair":"BTC/USDT", "sl_price":95600, "tp_type":"PRICE", "tp_value":100400, "size":1.0, "max_entries":4}
    {"action":"SELL","pair":"BTC/USDT", "sl_price":44000, "tp_type":"TIME", "tp_value":180}
    {"action":"CLOSE","pair":"BTC/USDT"}

Uso:
    python -m executor.lighter_executor              # Produccion (necesita LIGHTER_API_KEY)
    python -m executor.lighter_executor --dry-run     # Solo logs, sin ordenes reales
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import redis.asyncio as aioredis

# ── Project imports ─────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import config  # noqa: E402
from src.database.trade_journal import TradeJournal  # noqa: E402
from src.notifier import AlertLevel, TelegramNotifier  # noqa: E402
from executor.lighter_client import LighterClient  # noqa: E402

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)-12s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("lighter_exec")


# ═══════════════════════════════════════════════════════════════════
#  1. MODELOS DE ESTADO
# ═══════════════════════════════════════════════════════════════════

@dataclass
class PositionState:
    """Estado de una posicion activa, persistido en Redis."""
    coin: str
    action: str              # BUY / SELL
    entry_price: float
    size: float
    sl_price: float
    sl_oid: int = 0
    tp_type: str = "PRICE"   # PRICE / TIME
    tp_value: float = 0.0    # precio o minutos
    tp_oid: int = 0
    tp_target_ts: int = 0    # epoch ms (solo para TIME)
    entry_ts: int = 0        # epoch ms
    status: str = "OPEN"
    entries: list = field(default_factory=list)
    max_entries: int = 4
    entry_size: float = 1.0
    trade_id: int = 0

    def to_redis(self) -> dict[str, str]:
        d = {k: str(v) for k, v in self.__dict__.items() if k != "entries"}
        d["entries"] = json.dumps(self.entries)
        return d

    @classmethod
    def from_redis(cls, data: dict[str, str]) -> PositionState:
        entries_raw = data.get("entries", "[]")
        try:
            entries = json.loads(entries_raw)
        except (json.JSONDecodeError, TypeError):
            entries = []
        return cls(
            coin=data["coin"], action=data["action"],
            entry_price=float(data["entry_price"]),
            size=float(data["size"]),
            sl_price=float(data["sl_price"]),
            sl_oid=int(data.get("sl_oid", 0) or 0),
            tp_type=data.get("tp_type", "PRICE"),
            tp_value=float(data.get("tp_value", 0) or 0),
            tp_oid=int(data.get("tp_oid", 0) or 0),
            tp_target_ts=int(data.get("tp_target_ts", 0) or 0),
            entry_ts=int(data.get("entry_ts", 0) or 0),
            status=data.get("status", "OPEN"),
            entries=entries,
            max_entries=int(data.get("max_entries", 4) or 4),
            entry_size=float(data.get("entry_size", 1.0) or 1.0),
            trade_id=int(data.get("trade_id", 0) or 0),
        )


# ═══════════════════════════════════════════════════════════════════
#  2. HELPERS
# ═══════════════════════════════════════════════════════════════════

def _extract_oid(result: dict) -> int | None:
    """Extrae order ID de la respuesta de LighterClient."""
    if result.get("status") != "ok":
        logger.error("Order rejected: %s", result.get("error", "unknown"))
        return None
    return result.get("oid")


def _ts_now() -> int:
    return int(time.time() * 1000)


def _fmt_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _fmt_duration(minutes: float) -> str:
    if minutes >= 1440:
        return f"{minutes / 1440:.1f} dias"
    if minutes >= 60:
        return f"{minutes / 60:.1f} horas"
    return f"{minutes:.0f} min"


# ═══════════════════════════════════════════════════════════════════
#  3. LIGHTER SMART EXECUTOR
# ═══════════════════════════════════════════════════════════════════

class LighterSmartExecutor:
    """
    Orquestador para Lighter.xyz — misma logica que HLSmartExecutor.

    Escucha senales de Redis, gestiona entradas con limit/market,
    SL/TP nativos, re-entries, y monitorea el estado de posiciones.

    Redis keys usan prefijo 'executor:lighter:' para coexistir con HL.
    """

    # Redis keys — prefijo diferente al de HL
    POS_KEY = "executor:lighter:position:{coin}"
    ACTIVE_KEY = "executor:lighter:active_pairs"

    def __init__(self, dry_run: bool = False):
        self.client = LighterClient(dry_run=dry_run)
        self.positions: dict[str, PositionState] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._redis: aioredis.Redis | None = None
        self._journal = TradeJournal()
        self._notifier = TelegramNotifier()
        self._running = True

    # ── Lifecycle ──────────────────────────────────────────────

    async def start(self) -> None:
        pool = aioredis.ConnectionPool.from_url(
            config.REDIS_URL, decode_responses=True, max_connections=5,
        )
        self._redis = aioredis.Redis(connection_pool=pool)
        await self._redis.ping()
        logger.info("Redis conectado")

        await self._journal.connect()
        await self._notifier.connect()

        await self._resume_positions()
        self._print_banner()

        try:
            await asyncio.gather(
                self._listen_signals(),
                self._state_monitor(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()

    async def _shutdown(self) -> None:
        self._running = False
        for name, task in self._tasks.items():
            task.cancel()
            logger.info("Task cancelada: %s", name)
        await self._journal.disconnect()
        await self._notifier.disconnect()
        await self.client.close()
        if self._redis:
            await self._redis.aclose()
        logger.info("Lighter Executor detenido")

    # ── Signal Listener ────────────────────────────────────────

    async def _listen_signals(self) -> None:
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(config.HL_SIGNAL_CHANNEL)
        logger.info("Escuchando canal: %s", config.HL_SIGNAL_CHANNEL)

        async for msg in pubsub.listen():
            if not self._running:
                break
            if msg["type"] != "message":
                continue
            try:
                signal = json.loads(msg["data"])
                # Filtro: ignorar senales destinadas a otro exchange
                exchange = signal.get("exchange", "").upper()
                if exchange and exchange not in ("", "LIGHTER", "ALL"):
                    logger.debug("Signal skip (exchange=%s, esperado LIGHTER)", exchange)
                    continue
                logger.info("SIGNAL recibida: %s", json.dumps(signal, indent=None))
                await self._dispatch(signal)
            except json.JSONDecodeError:
                logger.warning("Mensaje invalido (no JSON): %s", msg["data"])
            except Exception as exc:
                logger.error("Error procesando senal: %s", exc, exc_info=True)

    async def _dispatch(self, signal: dict) -> None:
        action = signal.get("action", "").upper()
        if action in ("BUY", "SELL"):
            await self._open_position(signal)
        elif action == "CLOSE":
            await self._close_position(signal)
        else:
            logger.warning("Accion desconocida: %s", action)

    # ── OPEN POSITION ──────────────────────────────────────────

    async def _open_position(self, signal: dict) -> None:
        pair = signal["pair"]
        coin = pair.split("/")[0]
        is_buy = signal["action"].upper() == "BUY"
        sl_price = float(signal["sl_price"])
        tp_type = signal.get("tp_type", "PRICE").upper()
        tp_value = float(signal.get("tp_value", 0))
        entry_size = float(signal.get("size", config.STRATEGY_ENTRY_SIZE_BTC))
        max_entries = int(signal.get("max_entries", config.STRATEGY_MAX_ENTRIES))
        side = "LONG" if is_buy else "SHORT"

        logger.info("=" * 60)
        logger.info("ABRIENDO %s %s | SL=$%.2f | TP(%s)=%s | size=%.4f",
                     side, coin, sl_price, tp_type,
                     f"${tp_value:,.2f}" if tp_type == "PRICE" else _fmt_duration(tp_value),
                     entry_size)

        # Re-entry check
        if coin in self.positions:
            await self._handle_reentry(signal)
            return

        # Validar saldo
        balance = await self.client.get_balance()
        mid_price = await self.client.get_mid_price(coin)
        required_usd = entry_size * mid_price
        if required_usd > balance:
            logger.error("Saldo insuficiente: necesario=$%.2f, disponible=$%.2f", required_usd, balance)
            await self._notifier.notify(
                AlertLevel.WARNING, f"Saldo insuficiente para {coin}",
                f"  Necesario: ${required_usd:,.2f}\n  Disponible: ${balance:,.2f}",
                source="Lighter",
            )
            return
        logger.info("  Balance: $%.2f | Entry size: %.4f %s ($%.2f)",
                     balance, entry_size, coin, required_usd)

        # Set leverage
        await self.client.set_leverage(coin, 1)

        # Tamano
        size = round(entry_size, self.client._sz_dec(coin))
        if size <= 0:
            logger.error("Tamano calculado = 0 — abortando")
            return

        # Entrada: Limit con fallback a Market
        entry = await self._place_entry(coin, is_buy, size, mid_price)
        if not entry:
            logger.error("ENTRADA FALLIDA — abortando operacion")
            await self._notifier.notify(
                AlertLevel.CRITICAL, f"Entrada FALLIDA {side} {coin}",
                f"  Size: {size} | Mid: ${mid_price:,.2f}", source="Lighter",
            )
            return

        entry_price = entry["price"]
        logger.info("  Entrada ejecutada: %s @ $%.2f (%s)", side, entry_price, entry["type"])

        # Stop Loss
        sl_result = await self.client.trigger_order(coin, not is_buy, size, sl_price, "sl")
        sl_oid = _extract_oid(sl_result) or 0

        # Take Profit
        tp_oid = 0
        tp_target_ts = 0

        if tp_type == "PRICE" and tp_value > 0:
            tp_result = await self.client.trigger_order(coin, not is_buy, size, tp_value, "tp")
            tp_oid = _extract_oid(tp_result) or 0
            logger.info("  TP PRECIO colocado @ $%.2f", tp_value)

        elif tp_type == "TIME" and tp_value > 0:
            tp_target_ts = _ts_now() + int(tp_value * 60 * 1000)
            task = asyncio.create_task(self._tp_time_monitor(coin, tp_target_ts))
            self._tasks[f"tp_{coin}"] = task
            logger.info("  TP TIEMPO iniciado: %s — cierre: %s",
                         _fmt_duration(tp_value), _fmt_ts(tp_target_ts))

        # Guardar estado
        first_entry = {"price": entry_price, "size": size, "ts": _ts_now()}
        state = PositionState(
            coin=coin, action=signal["action"].upper(),
            entry_price=entry_price, size=size,
            sl_price=sl_price, sl_oid=sl_oid,
            tp_type=tp_type, tp_value=tp_value,
            tp_oid=tp_oid, tp_target_ts=tp_target_ts,
            entry_ts=_ts_now(),
            entries=[first_entry],
            max_entries=max_entries,
            entry_size=entry_size,
        )
        self.positions[coin] = state

        # Journal: log open
        state.trade_id = await self._journal.log_open(
            exchange="lighter", coin=coin, action=signal["action"].upper(),
            entry_price=entry_price, size=size,
            sl_price=sl_price, tp_type=tp_type, tp_value=tp_value,
        )

        await self._save_to_redis(coin, state)

        await self._notifier.notify_trade(
            f"OPEN {side}", coin, entry_price, size,
            sl_price=sl_price, tp_type=tp_type, tp_value=tp_value,
            source="Lighter",
        )

        logger.info("  Posicion %s registrada y monitorizada (journal #%d)", coin, state.trade_id)
        logger.info("=" * 60)

    # ── RE-ENTRY ────────────────────────────────────────────────

    async def _handle_reentry(self, signal: dict) -> None:
        pair = signal["pair"]
        coin = pair.split("/")[0]
        state = self.positions[coin]
        is_buy = signal["action"].upper() == "BUY"

        if signal["action"].upper() != state.action:
            logger.warning("Re-entry rechazado: senal %s vs posicion %s", signal["action"], state.action)
            return

        if len(state.entries) >= state.max_entries:
            logger.warning("Re-entry rechazado: max entries alcanzado (%d/%d)", len(state.entries), state.max_entries)
            return

        entry_num = len(state.entries) + 1
        logger.info("RE-ENTRY #%d/%d para %s %s (size=%.4f)",
                     entry_num, state.max_entries, state.action, coin, state.entry_size)

        # Cancelar SL trigger existente
        if state.sl_oid:
            await self.client.cancel_order(coin, state.sl_oid)

        # Ejecutar entrada adicional
        mid_price = await self.client.get_mid_price(coin)
        size = round(state.entry_size, self.client._sz_dec(coin))
        entry = await self._place_entry(coin, is_buy, size, mid_price)
        if not entry:
            logger.error("Re-entry FALLIDA — restaurando SL original")
            await self._notifier.notify(
                AlertLevel.WARNING, f"Re-entry #{entry_num} FALLIDA {coin}",
                f"  Size: {size} | Mid: ${mid_price:,.2f}", source="Lighter",
            )
            sl_result = await self.client.trigger_order(coin, not is_buy, state.size, state.sl_price, "sl")
            state.sl_oid = _extract_oid(sl_result) or 0
            await self._save_to_redis(coin, state)
            return

        entry_price = entry["price"]
        logger.info("  Re-entry ejecutada: %s @ $%.2f (%s)", state.action, entry_price, entry["type"])

        # Actualizar estado
        new_entry = {"price": entry_price, "size": size, "ts": _ts_now()}
        state.entries.append(new_entry)
        state.size = round(state.size + size, self.client._sz_dec(coin))

        total_cost = sum(e["price"] * e["size"] for e in state.entries)
        total_size = sum(e["size"] for e in state.entries)
        state.entry_price = total_cost / total_size if total_size > 0 else entry_price

        # Nuevo SL para tamano TOTAL
        sl_result = await self.client.trigger_order(coin, not is_buy, state.size, state.sl_price, "sl")
        state.sl_oid = _extract_oid(sl_result) or 0

        # Recolocar TP si es PRICE
        if state.tp_type == "PRICE" and state.tp_oid:
            await self.client.cancel_order(coin, state.tp_oid)
            tp_result = await self.client.trigger_order(coin, not is_buy, state.size, state.tp_value, "tp")
            state.tp_oid = _extract_oid(tp_result) or 0

        # Journal: log reentry
        if state.trade_id:
            await self._journal.log_reentry(state.trade_id, entry_price, size)

        await self._save_to_redis(coin, state)

        await self._notifier.notify_trade(
            f"RE-ENTRY #{len(state.entries)}", coin, entry_price, size,
            sl_price=state.sl_price, tp_type=state.tp_type, tp_value=state.tp_value,
            source="Lighter",
        )

        logger.info("  Posicion actualizada: entries=%d/%d | total_size=%.4f | avg_entry=$%.2f",
                     len(state.entries), state.max_entries, state.size, state.entry_price)
        logger.info("=" * 60)

    # ── CLOSE POSITION ─────────────────────────────────────────

    async def _close_position(self, signal: dict) -> None:
        pair = signal["pair"]
        coin = pair.split("/")[0]

        logger.info("=" * 60)
        logger.info("CERRANDO %s por senal externa", coin)

        if coin not in self.positions:
            logger.warning("No hay posicion activa para %s", coin)
            pos = await self.client.get_position(coin)
            if pos:
                await self.client.market_close(coin)
                logger.info("  Posicion huerfana cerrada en exchange")
            return

        await self._cleanup_and_close(coin, reason="SIGNAL_CLOSE")
        logger.info("=" * 60)

    async def _cleanup_and_close(self, coin: str, reason: str) -> None:
        state = self.positions.get(coin)
        if not state:
            return

        if state.sl_oid:
            await self.client.cancel_order(coin, state.sl_oid)
        if state.tp_oid:
            await self.client.cancel_order(coin, state.tp_oid)

        task_key = f"tp_{coin}"
        if task_key in self._tasks:
            self._tasks[task_key].cancel()
            del self._tasks[task_key]
            logger.info("  Temporizador TP cancelado")

        try:
            await self.client.market_close(coin)
        except Exception as exc:
            logger.warning("  market_close fallo: %s", exc)

        mid = await self.client.get_mid_price(coin)
        if state.action == "BUY":
            pnl_pct = (mid / state.entry_price - 1) * 100
        else:
            pnl_pct = (state.entry_price / mid - 1) * 100

        logger.info("  Cierre: razon=%s | entry=$%.2f | exit~=$%.2f | PnL~=%.2f%%",
                     reason, state.entry_price, mid, pnl_pct)

        # Journal: log close
        if state.trade_id:
            await self._journal.log_close(state.trade_id, mid, reason)

        await self._notifier.notify_pnl(
            coin, state.entry_price, mid, pnl_pct, reason, source="Lighter",
        )

        del self.positions[coin]
        await self._clear_redis(coin)

    # ── ENTRY: LIMIT → MARKET FALLBACK ─────────────────────────

    async def _place_entry(self, coin: str, is_buy: bool, size: float, mid_price: float) -> dict | None:
        """Intenta limit order; detecta fill via position-size comparison (no get_open_orders)."""
        offset = mid_price * (config.HL_LIMIT_OFFSET_BPS / 10_000)
        limit_px = mid_price - offset if is_buy else mid_price + offset

        # Guardar tamano de posicion antes de la orden
        pos_before = await self.client.get_position(coin)
        size_before = abs(float(pos_before.get("szi", 0))) if pos_before else 0.0

        try:
            result = await self.client.limit_order(coin, is_buy, size, limit_px)
            oid = _extract_oid(result)
            if oid is None:
                raise RuntimeError("No se obtuvo OID del limit order")

            # Pollear position size para detectar fill
            for _ in range(config.HL_LIMIT_WAIT_SEC):
                await asyncio.sleep(1)
                pos = await self.client.get_position(coin)
                if pos:
                    size_now = abs(float(pos.get("szi", 0)))
                    if size_now - size_before >= size * 0.95:
                        return {"price": float(pos["entryPx"]), "type": "LIMIT"}

            logger.warning("  Limit no lleno en %ds — fallback a MARKET", config.HL_LIMIT_WAIT_SEC)
            await self.client.cancel_order(coin, oid)

        except Exception as exc:
            logger.warning("  Limit order fallo: %s — usando MARKET", exc)

        try:
            await self.client.market_order(coin, is_buy, size)
            await asyncio.sleep(0.5)
            pos = await self.client.get_position(coin)
            if pos:
                return {"price": float(pos.get("entryPx", mid_price)), "type": "MARKET"}
            return {"price": mid_price, "type": "MARKET"}
        except Exception as exc:
            logger.error("  MARKET order tambien fallo: %s", exc)
            await self._notifier.notify(
                AlertLevel.CRITICAL, f"MARKET order fallida {coin}",
                f"  Error: {exc}", source="Lighter",
            )
            return None

    # ── TP TIME MONITOR ────────────────────────────────────────

    async def _tp_time_monitor(self, coin: str, target_ts: int) -> None:
        logger.info("  [TP TIMER] %s — target: %s", coin, _fmt_ts(target_ts))
        try:
            while self._running:
                now = _ts_now()
                if now >= target_ts:
                    logger.info("  [TP TIMER] %s — TIEMPO CUMPLIDO — cerrando posicion", coin)
                    await self._cleanup_and_close(coin, reason="TP_TIME_EXPIRED")
                    return

                remaining_ms = target_ts - now
                remaining_min = remaining_ms / 60_000

                if remaining_min > 120:
                    if int(remaining_min) % 60 == 0:
                        logger.info("  [TP TIMER] %s — %s restantes", coin, _fmt_duration(remaining_min))
                elif remaining_min > 10:
                    logger.info("  [TP TIMER] %s — %.0f min restantes", coin, remaining_min)
                else:
                    logger.info("  [TP TIMER] %s — %.1f min restantes", coin, remaining_min)

                await asyncio.sleep(60)

        except asyncio.CancelledError:
            logger.info("  [TP TIMER] %s — cancelado", coin)

    # ── STATE MONITOR ──────────────────────────────────────────

    async def _state_monitor(self) -> None:
        """
        Monitorea posiciones activas en Lighter.

        - Cada ciclo: si posicion desaparece → cierre externo → limpiar + journal
        - Cada N ciclos: reconciliacion de tamano exchange vs local
        """
        logger.info("State monitor activo (poll cada %ds)", config.HL_STATE_POLL_SEC)
        reconcile_every = max(1, config.RECONCILE_INTERVAL_SEC // config.HL_STATE_POLL_SEC)
        cycle = 0

        while self._running:
            cycle += 1
            try:
                for coin in list(self.positions.keys()):
                    pos = await self.client.get_position(coin)
                    state = self.positions.get(coin)
                    if not state:
                        continue

                    if pos is None or float(pos.get("szi", 0)) == 0:
                        logger.info("[MONITOR] Posicion %s cerrada externamente (SL hit?)", coin)

                        mid = await self.client.get_mid_price(coin)

                        # Journal: log external close
                        if state.trade_id:
                            await self._journal.log_close(state.trade_id, mid, "EXTERNAL")

                        # PnL for notification
                        if state.action == "BUY":
                            pnl_pct = (mid / state.entry_price - 1) * 100
                        else:
                            pnl_pct = (state.entry_price / mid - 1) * 100
                        await self._notifier.notify_pnl(
                            coin, state.entry_price, mid, pnl_pct,
                            "EXTERNAL", source="Lighter",
                        )

                        task_key = f"tp_{coin}"
                        if task_key in self._tasks:
                            self._tasks[task_key].cancel()
                            del self._tasks[task_key]

                        del self.positions[coin]
                        await self._clear_redis(coin)

                    elif cycle % reconcile_every == 0:
                        # Reconciliacion: comparar tamano exchange vs local
                        exchange_size = abs(float(pos.get("szi", 0)))
                        local_size = abs(state.size)
                        if abs(exchange_size - local_size) > local_size * 0.05:
                            logger.warning(
                                "[RECONCILE] %s size mismatch: exchange=%.6f vs local=%.6f",
                                coin, exchange_size, local_size,
                            )
                            await self._notifier.notify(
                                AlertLevel.WARNING, f"Size mismatch: {coin}",
                                f"  Exchange={exchange_size:.6f} vs Local={local_size:.6f}",
                                source="Lighter",
                            )

            except Exception as exc:
                logger.warning("[MONITOR] Error: %s", exc)

            await asyncio.sleep(config.HL_STATE_POLL_SEC)

    # ── RESUME ─────────────────────────────────────────────────

    async def _resume_positions(self) -> None:
        active = await self._redis.smembers(self.ACTIVE_KEY)
        if not active:
            logger.info("Sin posiciones previas que resumir")
            return

        for coin in active:
            data = await self._redis.hgetall(self.POS_KEY.format(coin=coin))
            if not data or data.get("status") != "OPEN":
                await self._clear_redis(coin)
                continue

            pos = await self.client.get_position(coin)
            if pos is None or float(pos.get("szi", 0)) == 0:
                logger.info("[RESUME] %s: posicion cerrada mientras offline — limpiando", coin)
                await self._clear_redis(coin)
                continue

            state = PositionState.from_redis(data)
            self.positions[coin] = state
            logger.info("[RESUME] %s: posicion %s recuperada (entry=$%.2f, size=%.6f)",
                         coin, state.action, state.entry_price, state.size)

            if state.tp_type == "TIME" and state.tp_target_ts > 0:
                now = _ts_now()
                if now < state.tp_target_ts:
                    remaining = (state.tp_target_ts - now) / 60_000
                    task = asyncio.create_task(self._tp_time_monitor(coin, state.tp_target_ts))
                    self._tasks[f"tp_{coin}"] = task
                    logger.info("[RESUME] TP timer reanudado: %s restantes", _fmt_duration(remaining))
                else:
                    logger.info("[RESUME] TP timer expiro mientras offline — cerrando %s", coin)
                    await self._cleanup_and_close(coin, reason="TP_TIME_EXPIRED_OFFLINE")

    # ── Redis state persistence ────────────────────────────────

    async def _save_to_redis(self, coin: str, state: PositionState) -> None:
        key = self.POS_KEY.format(coin=coin)
        await self._redis.hset(key, mapping=state.to_redis())
        await self._redis.sadd(self.ACTIVE_KEY, coin)

    async def _clear_redis(self, coin: str) -> None:
        key = self.POS_KEY.format(coin=coin)
        await self._redis.delete(key)
        await self._redis.srem(self.ACTIVE_KEY, coin)
        logger.info("  Redis state limpiado para %s", coin)

    # ── Banner ─────────────────────────────────────────────────

    def _print_banner(self) -> None:
        mode = "DRY RUN" if self.client.dry_run else "LIVE"
        net = "MAINNET" if config.LIGHTER_MAINNET else "TESTNET"
        print(f"""
================================================================
  LIGHTER SMART EXECUTOR | {mode} | {net}
  Channel: {config.HL_SIGNAL_CHANNEL}
  Reconcile: every {config.RECONCILE_INTERVAL_SEC}s
  Journal: {config.TRADE_DB}
  Active positions: {len(self.positions)}
================================================================
""")


# ═══════════════════════════════════════════════════════════════════
#  4. ENTRYPOINT
# ═══════════════════════════════════════════════════════════════════

async def _run(dry_run: bool) -> None:
    executor = LighterSmartExecutor(dry_run=dry_run)
    try:
        await executor.start()
    except KeyboardInterrupt:
        logger.info("Detenido por el usuario")


def main() -> None:
    parser = argparse.ArgumentParser(description="Lighter.xyz Smart Executor")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Solo logs, sin enviar ordenes reales",
    )
    args = parser.parse_args()

    dry_run = args.dry_run or not config.LIGHTER_API_KEY
    if not config.LIGHTER_API_KEY and not args.dry_run:
        logger.info("LIGHTER_API_KEY vacio — activando DRY RUN automatico")

    asyncio.run(_run(dry_run))


if __name__ == "__main__":
    main()
