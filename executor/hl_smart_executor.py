"""
hl_smart_executor.py — Ejecutor Inteligente de Órdenes para Hyperliquid DEX.

Arquitectura:
    Redis (trading_signals)
        │
        ▼
    Signal Parser ──► Order Router
                        ├── Entry (Limit → Market fallback)
                        ├── SL  (Trigger nativo en Hyperliquid)
                        ├── TP  Price (Trigger nativo)
                        ├── TP  Time  (asyncio monitor cada 60s)
                        └── CLOSE (cancel all → market close)

    State Monitor ──► Hyperliquid API (detecta fills de SL)
    Redis Hashes  ──► Dashboard (tp_target_ts, estado posición)

Señales esperadas en canal Redis (JSON):
    {"action":"BUY",  "multiplier":1.5, "sl_price":42500, "tp_type":"PRICE", "tp_value":45000,  "pair":"BTC/USDT"}
    {"action":"SELL", "multiplier":1.0, "sl_price":44000, "tp_type":"TIME",  "tp_value":180,    "pair":"BTC/USDT"}
    {"action":"CLOSE","pair":"BTC/USDT"}

    tp_value para TIME → SIEMPRE en MINUTOS (180 = 3h, 1440 = 1d, 10080 = 7d)

Uso:
    python -m executor.hl_smart_executor              # Producción (necesita HL_PRIVATE_KEY)
    python -m executor.hl_smart_executor --dry-run     # Solo logs, sin órdenes reales
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import redis.asyncio as aioredis

# ── Project imports ─────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import config  # noqa: E402
from src.database.trade_journal import TradeJournal  # noqa: E402
from src.notifier import AlertLevel, TelegramNotifier  # noqa: E402
from executor.models import PositionState, validate_signal  # noqa: E402

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)-12s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("executor")


# ═══════════════════════════════════════════════════════════════════
#  1. HYPERLIQUID CLIENT (wrapper async-safe sobre el SDK)
# ═══════════════════════════════════════════════════════════════════

class HLClient:
    """
    Wrapper asíncrono sobre el SDK de Hyperliquid.

    Todos los métodos del SDK son síncronos; se ejecutan vía
    asyncio.to_thread() para no bloquear el event loop.

    Si dry_run=True, solo loguea sin enviar órdenes reales.
    """

    # Decimales de tamaño por activo (Hyperliquid szDecimals)
    SZ_DECIMALS = {"BTC": 5, "ETH": 4, "SOL": 2, "DOGE": 0, "WIF": 1}
    PX_DECIMALS = {"BTC": 1, "ETH": 2, "SOL": 3, "DOGE": 5, "WIF": 4}

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.address = config.HL_WALLET_ADDRESS
        self._exchange = None
        self._info = None

        if not dry_run and config.HL_PRIVATE_KEY:
            try:
                from eth_account import Account
                from hyperliquid.exchange import Exchange
                from hyperliquid.info import Info
                from hyperliquid.utils import constants

                account = Account.from_key(config.HL_PRIVATE_KEY)
                self.address = account.address
                url = constants.MAINNET_API_URL if config.HL_MAINNET else constants.TESTNET_API_URL
                self._exchange = Exchange(account, url)
                self._info = Info(url)
                logger.info(
                    "Hyperliquid conectado — %s | wallet: %s...%s",
                    "MAINNET" if config.HL_MAINNET else "TESTNET",
                    self.address[:8], self.address[-6:],
                )
            except ImportError:
                logger.warning("SDK no instalado — activando DRY RUN")
                self.dry_run = True
            except Exception as exc:
                logger.error("Error inicializando SDK: %s — activando DRY RUN", exc)
                self.dry_run = True
        else:
            self.dry_run = True
            logger.info("DRY RUN activo — no se enviarán órdenes reales")

    def _sz(self, coin: str) -> int:
        return self.SZ_DECIMALS.get(coin, 4)

    def _px(self, coin: str) -> int:
        return self.PX_DECIMALS.get(coin, 1)

    # ── Datos de mercado ───────────────────────────────────────

    async def get_mid_price(self, coin: str) -> float:
        if self.dry_run:
            return 43000.0 if coin == "BTC" else 3000.0
        mids = await asyncio.to_thread(self._info.all_mids)
        return float(mids.get(coin, 0))

    async def get_balance(self) -> float:
        if self.dry_run:
            return 100_000.0
        state = await asyncio.to_thread(self._info.user_state, self.address)
        return float(state["marginSummary"]["accountValue"])

    async def get_position(self, coin: str) -> dict | None:
        if self.dry_run:
            return None
        state = await asyncio.to_thread(self._info.user_state, self.address)
        for ap in state.get("assetPositions", []):
            pos = ap.get("position", ap)
            if pos.get("coin") == coin and float(pos.get("szi", 0)) != 0:
                return pos
        return None

    async def get_open_orders(self, coin: str | None = None) -> list:
        if self.dry_run:
            return []
        orders = await asyncio.to_thread(self._info.open_orders, self.address)
        if coin:
            orders = [o for o in orders if o.get("coin") == coin]
        return orders

    # ── Órdenes ────────────────────────────────────────────────

    async def limit_order(self, coin: str, is_buy: bool, sz: float, px: float) -> dict:
        sz = round(sz, self._sz(coin))
        px = round(px, self._px(coin))
        tag = "BUY" if is_buy else "SELL"
        logger.info("  LIMIT %s %s x%s @ $%s", tag, coin, sz, f"{px:,.{self._px(coin)}f}")
        if self.dry_run:
            return {"status": "ok", "response": {"data": {"statuses": [
                {"resting": {"oid": int(time.time() * 1000)}}
            ]}}}
        return await asyncio.to_thread(
            self._exchange.order, coin, is_buy, sz, px,
            {"limit": {"tif": "Gtc"}},
        )

    async def market_order(self, coin: str, is_buy: bool, sz: float) -> dict:
        sz = round(sz, self._sz(coin))
        tag = "BUY" if is_buy else "SELL"
        logger.info("  MARKET %s %s x%s", tag, coin, sz)
        if self.dry_run:
            return {"status": "ok", "response": {"data": {"statuses": [
                {"filled": {"oid": int(time.time() * 1000), "avgPx": "43000", "totalSz": str(sz)}}
            ]}}}
        return await asyncio.to_thread(self._exchange.market_open, coin, is_buy, sz)

    async def trigger_order(
        self, coin: str, is_buy: bool, sz: float, trigger_px: float, tpsl: str,
    ) -> dict:
        sz = round(sz, self._sz(coin))
        trigger_px = round(trigger_px, self._px(coin))
        label = "SL" if tpsl == "sl" else "TP"
        logger.info("  TRIGGER %s %s x%s @ $%s", label, coin, sz, f"{trigger_px:,.{self._px(coin)}f}")
        if self.dry_run:
            return {"status": "ok", "response": {"data": {"statuses": [
                {"resting": {"oid": int(time.time() * 1000) + 1}}
            ]}}}
        return await asyncio.to_thread(
            self._exchange.order, coin, is_buy, sz, trigger_px,
            {"trigger": {"triggerPx": str(trigger_px), "isMarket": True, "tpsl": tpsl}},
            True,  # reduce_only
        )

    async def cancel_order(self, coin: str, oid: int) -> None:
        logger.info("  CANCEL order %s oid=%d", coin, oid)
        if self.dry_run:
            return
        try:
            await asyncio.to_thread(self._exchange.cancel, coin, oid)
        except Exception as exc:
            logger.warning("  Cancel failed (puede que ya se haya ejecutado): %s", exc)

    async def market_close(self, coin: str) -> dict:
        logger.info("  MARKET CLOSE %s (full position)", coin)
        if self.dry_run:
            return {"status": "ok"}
        return await asyncio.to_thread(self._exchange.market_close, coin)

    async def set_leverage(self, coin: str, leverage: int) -> None:
        logger.info("  SET LEVERAGE %s x%d", coin, leverage)
        if self.dry_run:
            return
        try:
            await asyncio.to_thread(self._exchange.update_leverage, leverage, coin, True)
        except Exception as exc:
            logger.warning("  Leverage update failed: %s", exc)


# ═══════════════════════════════════════════════════════════════════
#  2. HELPERS
# ═══════════════════════════════════════════════════════════════════

def _extract_oid(result: dict) -> int | None:
    """Extrae el order ID de la respuesta del SDK de Hyperliquid."""
    try:
        if result.get("status") != "ok":
            logger.error("Order rejected: %s", result.get("response", "unknown"))
            return None
        statuses = result["response"]["data"]["statuses"]
        if not statuses:
            return None
        s = statuses[0]
        if "resting" in s:
            return int(s["resting"]["oid"])
        if "filled" in s:
            return int(s["filled"].get("oid", 0))
    except (KeyError, TypeError, IndexError):
        pass
    return None


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
#  4. SMART EXECUTOR
# ═══════════════════════════════════════════════════════════════════

class HLSmartExecutor:
    """
    Orquestador principal.

    Escucha señales de Redis, gestiona entradas con limit/market,
    SL/TP nativos o por tiempo, y monitorea el estado de posiciones.
    """

    # Redis keys
    POS_KEY = "executor:position:{coin}"
    ACTIVE_KEY = "executor:active_pairs"

    def __init__(self, dry_run: bool = False):
        self.client = HLClient(dry_run=dry_run)
        self.positions: dict[str, PositionState] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._redis: aioredis.Redis | None = None
        self._journal = TradeJournal()
        self._notifier = TelegramNotifier()
        self._running = True
        self._pos_lock = asyncio.Lock()

    # ── Lifecycle ──────────────────────────────────────────────

    async def start(self) -> None:
        """Punto de entrada: conecta Redis, resume estado, arranca listeners."""
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
        if self._redis:
            await self._redis.aclose()
        logger.info("Executor detenido")

    # ── Signal Listener ────────────────────────────────────────

    async def _listen_signals(self) -> None:
        """Suscripción Pub/Sub al canal de señales con reconexión automática."""
        backoff = 1
        while self._running:
            try:
                pubsub = self._redis.pubsub()
                await pubsub.subscribe(config.HL_SIGNAL_CHANNEL)
                logger.info("Escuchando canal: %s", config.HL_SIGNAL_CHANNEL)
                backoff = 1  # reset on successful connect

                async for msg in pubsub.listen():
                    if not self._running:
                        return
                    if msg["type"] != "message":
                        continue
                    try:
                        signal = json.loads(msg["data"])
                        exchange = signal.get("exchange", "").upper()
                        if exchange and exchange not in ("", "HL", "HYPERLIQUID", "ALL"):
                            logger.debug("Signal skip (exchange=%s, esperado HL)", exchange)
                            continue
                        logger.info("SIGNAL recibida: %s", json.dumps(signal, indent=None))
                        await self._dispatch(signal)
                    except json.JSONDecodeError:
                        logger.warning("Mensaje inválido (no JSON): %s", msg["data"])
                    except Exception as exc:
                        logger.error("Error procesando señal: %s", exc, exc_info=True)

            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("[PUBSUB] Conexión perdida: %s — reconectando en %ds", exc, backoff)
                await self._notifier.notify(
                    AlertLevel.WARNING, "Redis PubSub desconectado",
                    f"  Error: {exc}\n  Reconectando en {backoff}s", source="Hyperliquid",
                )
                try:
                    await pubsub.aclose()
                except Exception:
                    pass
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _dispatch(self, signal: dict) -> None:
        err = validate_signal(signal)
        if err:
            logger.warning("Señal rechazada: %s | payload=%s", err, signal)
            return

        async with self._pos_lock:
            action = signal.get("action", "").upper()
            if action in ("BUY", "SELL"):
                await self._open_position(signal)
            elif action == "CLOSE":
                await self._close_position(signal)

    # ── OPEN POSITION ──────────────────────────────────────────

    async def _open_position(self, signal: dict) -> None:
        pair = signal["pair"]
        coin = pair.split("/")[0]
        is_buy = signal["action"].upper() == "BUY"
        multiplier = float(signal.get("multiplier", 1.0))
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

        # 1. Re-entry check: if position already exists, delegate
        if coin in self.positions:
            await self._handle_reentry(signal)
            return

        # 2. Validar saldo y SL coherente
        balance = await self.client.get_balance()
        mid_price = await self.client.get_mid_price(coin)

        if is_buy and sl_price >= mid_price:
            logger.error("SL inválido para BUY: sl=$%.2f >= mid=$%.2f — abortando", sl_price, mid_price)
            return
        if not is_buy and sl_price <= mid_price:
            logger.error("SL inválido para SELL: sl=$%.2f <= mid=$%.2f — abortando", sl_price, mid_price)
            return

        required_usd = entry_size * mid_price
        if required_usd > balance:
            logger.error("Saldo insuficiente: necesario=$%.2f, disponible=$%.2f", required_usd, balance)
            await self._notifier.notify(
                AlertLevel.WARNING, f"Saldo insuficiente para {coin}",
                f"  Necesario: ${required_usd:,.2f}\n  Disponible: ${balance:,.2f}",
                source="Hyperliquid",
            )
            return
        logger.info("  Balance: $%.2f | Entry size: %.4f %s ($%.2f)",
                     balance, entry_size, coin, required_usd)

        # 3. Set leverage
        leverage = max(1, int(multiplier))
        await self.client.set_leverage(coin, leverage)

        # 4. Tamaño de la orden
        size = round(entry_size, self.client._sz(coin))

        if size <= 0:
            logger.error("Tamaño calculado = 0 — abortando")
            return

        # 5. Entrada: Limit con fallback a Market
        entry = await self._place_entry(coin, is_buy, size, mid_price)
        if not entry:
            logger.error("ENTRADA FALLIDA — abortando operación")
            await self._notifier.notify(
                AlertLevel.CRITICAL, f"Entrada FALLIDA {side} {coin}",
                f"  Size: {size} | Mid: ${mid_price:,.2f}", source="Hyperliquid",
            )
            return

        entry_price = entry["price"]
        filled_size = round(entry.get("size", size), self.client._sz(coin))
        if filled_size != size:
            logger.warning("  Partial fill: pedido=%.6f, llenado=%.6f", size, filled_size)
        size = filled_size
        logger.info("  Entrada ejecutada: %s x%.6f @ $%.2f (%s)", side, size, entry_price, entry["type"])

        # 6. Stop Loss (trigger nativo)
        sl_result = await self.client.trigger_order(coin, not is_buy, size, sl_price, "sl")
        sl_oid = _extract_oid(sl_result) or 0

        # 7. Take Profit
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
            logger.info("  TP TIEMPO iniciado: %s (%s) — cierre: %s",
                         _fmt_duration(tp_value), f"{tp_value:.0f} min", _fmt_ts(tp_target_ts))

        # 8. Guardar estado
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
            exchange="hyperliquid", coin=coin, action=signal["action"].upper(),
            entry_price=entry_price, size=size,
            sl_price=sl_price, tp_type=tp_type, tp_value=tp_value,
        )

        await self._save_to_redis(coin, state)

        await self._notifier.notify_trade(
            f"OPEN {side}", coin, entry_price, size,
            sl_price=sl_price, tp_type=tp_type, tp_value=tp_value,
            source="Hyperliquid",
        )

        logger.info("  Posición %s registrada y monitorizada (journal #%d)", coin, state.trade_id)
        logger.info("=" * 60)

    # ── RE-ENTRY ────────────────────────────────────────────────

    async def _handle_reentry(self, signal: dict) -> None:
        """Añade una entrada adicional a una posición ya abierta."""
        pair = signal["pair"]
        coin = pair.split("/")[0]
        state = self.positions[coin]
        is_buy = signal["action"].upper() == "BUY"

        # Validar misma dirección
        if signal["action"].upper() != state.action:
            logger.warning("Re-entry rechazado: señal %s vs posición %s", signal["action"], state.action)
            return

        # Validar max entries
        if len(state.entries) >= state.max_entries:
            logger.warning("Re-entry rechazado: max entries alcanzado (%d/%d)", len(state.entries), state.max_entries)
            return

        entry_num = len(state.entries) + 1
        logger.info("RE-ENTRY #%d/%d para %s %s (size=%.4f)",
                     entry_num, state.max_entries, state.action, coin, state.entry_size)

        # 1. Cancelar SL trigger existente (se recolocará con tamaño total)
        if state.sl_oid:
            await self.client.cancel_order(coin, state.sl_oid)

        # 2. Ejecutar entrada adicional (limit → market fallback)
        mid_price = await self.client.get_mid_price(coin)
        size = round(state.entry_size, self.client._sz(coin))
        entry = await self._place_entry(coin, is_buy, size, mid_price)
        if not entry:
            logger.error("Re-entry FALLIDA — restaurando SL original")
            await self._notifier.notify(
                AlertLevel.WARNING, f"Re-entry #{entry_num} FALLIDA {coin}",
                f"  Size: {size} | Mid: ${mid_price:,.2f}", source="Hyperliquid",
            )
            # Restaurar SL con tamaño actual
            sl_result = await self.client.trigger_order(coin, not is_buy, state.size, state.sl_price, "sl")
            state.sl_oid = _extract_oid(sl_result) or 0
            await self._save_to_redis(coin, state)
            return

        entry_price = entry["price"]
        logger.info("  Re-entry ejecutada: %s @ $%.2f (%s)", state.action, entry_price, entry["type"])

        # 3. Actualizar estado
        new_entry = {"price": entry_price, "size": size, "ts": _ts_now()}
        state.entries.append(new_entry)
        state.size = round(state.size + size, self.client._sz(coin))

        # Recalcular entry_price promedio ponderado
        total_cost = sum(e["price"] * e["size"] for e in state.entries)
        total_size = sum(e["size"] for e in state.entries)
        state.entry_price = total_cost / total_size if total_size > 0 else entry_price

        # 4. Colocar nuevo SL trigger para tamaño TOTAL
        sl_result = await self.client.trigger_order(coin, not is_buy, state.size, state.sl_price, "sl")
        state.sl_oid = _extract_oid(sl_result) or 0

        # 5. Si TP es PRICE, cancelar y recolocar con tamaño total
        if state.tp_type == "PRICE" and state.tp_oid:
            await self.client.cancel_order(coin, state.tp_oid)
            tp_result = await self.client.trigger_order(coin, not is_buy, state.size, state.tp_value, "tp")
            state.tp_oid = _extract_oid(tp_result) or 0

        # 6. Journal: log reentry
        if state.trade_id:
            await self._journal.log_reentry(state.trade_id, entry_price, size)

        # 7. Guardar estado actualizado
        await self._save_to_redis(coin, state)

        await self._notifier.notify_trade(
            f"RE-ENTRY #{len(state.entries)}", coin, entry_price, size,
            sl_price=state.sl_price, tp_type=state.tp_type, tp_value=state.tp_value,
            source="Hyperliquid",
        )

        logger.info("  Posición actualizada: entries=%d/%d | total_size=%.4f | avg_entry=$%.2f",
                     len(state.entries), state.max_entries, state.size, state.entry_price)
        logger.info("=" * 60)

    # ── CLOSE POSITION ─────────────────────────────────────────

    async def _close_position(self, signal: dict) -> None:
        pair = signal["pair"]
        coin = pair.split("/")[0]

        logger.info("=" * 60)
        logger.info("CERRANDO %s por señal externa", coin)

        if coin not in self.positions:
            logger.warning("No hay posición activa para %s", coin)
            # Intentar cerrar en exchange de todas formas
            pos = await self.client.get_position(coin)
            if pos:
                await self.client.market_close(coin)
                logger.info("  Posición huérfana cerrada en exchange")
            return

        await self._cleanup_and_close(coin, reason="SIGNAL_CLOSE")
        logger.info("=" * 60)

    async def _cleanup_and_close(self, coin: str, reason: str) -> None:
        """Cancel everything, market close, clean state."""
        state = self.positions.get(coin)
        if not state:
            return

        # Cancel SL
        if state.sl_oid:
            await self.client.cancel_order(coin, state.sl_oid)

        # Cancel TP (si es por precio)
        if state.tp_oid:
            await self.client.cancel_order(coin, state.tp_oid)

        # Cancel timer TP (si es por tiempo)
        task_key = f"tp_{coin}"
        if task_key in self._tasks:
            self._tasks[task_key].cancel()
            del self._tasks[task_key]
            logger.info("  Temporizador TP cancelado")

        # Market close
        try:
            await self.client.market_close(coin)
        except Exception as exc:
            logger.warning("  market_close falló (posición ya cerrada?): %s", exc)

        # P&L estimado
        mid = await self.client.get_mid_price(coin)
        if state.action == "BUY":
            pnl_pct = (mid / state.entry_price - 1) * 100
        else:
            pnl_pct = (state.entry_price / mid - 1) * 100

        logger.info("  Cierre: razón=%s | entry=$%.2f | exit~=$%.2f | PnL~=%.2f%%",
                     reason, state.entry_price, mid, pnl_pct)

        # Journal: log close
        if state.trade_id:
            await self._journal.log_close(state.trade_id, mid, reason)

        await self._notifier.notify_pnl(
            coin, state.entry_price, mid, pnl_pct, reason, source="Hyperliquid",
        )

        # Limpiar
        del self.positions[coin]
        await self._clear_redis(coin)

    # ── ENTRY: LIMIT → MARKET FALLBACK ─────────────────────────

    async def _place_entry(self, coin: str, is_buy: bool, size: float, mid_price: float) -> dict | None:
        """Intenta limit order; si no llena en N segundos, cancela y usa market."""
        offset = mid_price * (config.HL_LIMIT_OFFSET_BPS / 10_000)
        if is_buy:
            limit_px = mid_price - offset
        else:
            limit_px = mid_price + offset

        # Intentar limit
        try:
            result = await self.client.limit_order(coin, is_buy, size, limit_px)
            oid = _extract_oid(result)
            if oid is None:
                raise RuntimeError("No se obtuvo OID del limit order")

            # Esperar fill
            for sec in range(config.HL_LIMIT_WAIT_SEC):
                await asyncio.sleep(1)
                orders = await self.client.get_open_orders(coin)
                if not any(o.get("oid") == oid for o in orders):
                    # Order ya no está pendiente — verificar posición
                    pos = await self.client.get_position(coin)
                    if pos and abs(float(pos.get("szi", 0))) > 0:
                        return {"price": float(pos["entryPx"]), "size": abs(float(pos["szi"])), "type": "LIMIT"}
                    # Filled pero posición no encontrada? (raro)
                    return {"price": float(limit_px), "size": size, "type": "LIMIT"}

            # Timeout — cancelar limit y usar market
            logger.warning("  Limit no llenó en %ds — fallback a MARKET", config.HL_LIMIT_WAIT_SEC)
            await self.client.cancel_order(coin, oid)

        except Exception as exc:
            logger.warning("  Limit order falló: %s — usando MARKET", exc)

        # Market fallback
        try:
            await self.client.market_order(coin, is_buy, size)
            await asyncio.sleep(0.5)
            pos = await self.client.get_position(coin)
            if pos:
                return {"price": float(pos.get("entryPx", mid_price)), "size": abs(float(pos.get("szi", size))), "type": "MARKET"}
            return {"price": mid_price, "size": size, "type": "MARKET"}
        except Exception as exc:
            logger.error("  MARKET order también falló: %s", exc)
            await self._notifier.notify(
                AlertLevel.CRITICAL, f"MARKET order fallida {coin}",
                f"  Error: {exc}", source="Hyperliquid",
            )
            return None

    # ── TP TIME MONITOR (loop cada 60s) ────────────────────────

    async def _tp_time_monitor(self, coin: str, target_ts: int) -> None:
        """
        Monitorea el temporizador de TP por tiempo.

        Verifica cada 60 segundos si se cumplió el target.
        Soporta duraciones de días/semanas sin problema.
        """
        logger.info("  [TP TIMER] %s — target: %s", coin, _fmt_ts(target_ts))

        try:
            while self._running:
                now = _ts_now()
                if now >= target_ts:
                    logger.info("  [TP TIMER] %s — TIEMPO CUMPLIDO — cerrando posición", coin)
                    await self._cleanup_and_close(coin, reason="TP_TIME_EXPIRED")
                    return

                remaining_ms = target_ts - now
                remaining_min = remaining_ms / 60_000

                # Log progresivo: cada hora si quedan horas, cada 10 min si quedan minutos
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

    # ── STATE MONITOR (detecta SL fills) ───────────────────────

    async def _state_monitor(self) -> None:
        """
        Monitorea posiciones activas en Hyperliquid.

        - Cada ciclo: si posición desaparece → cierre externo → limpiar + journal
        - Cada N ciclos: reconciliación de tamaño exchange vs local
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
                        logger.info("[MONITOR] Posición %s cerrada externamente (SL hit?)", coin)

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
                            "EXTERNAL", source="Hyperliquid",
                        )

                        # Cancelar timer de TP si existe
                        task_key = f"tp_{coin}"
                        if task_key in self._tasks:
                            self._tasks[task_key].cancel()
                            del self._tasks[task_key]

                        # Limpiar estado
                        del self.positions[coin]
                        await self._clear_redis(coin)

                    elif cycle % reconcile_every == 0:
                        # Reconciliación: comparar tamaño exchange vs local
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
                                source="Hyperliquid",
                            )

            except Exception as exc:
                logger.warning("[MONITOR] Error: %s", exc)

            await asyncio.sleep(config.HL_STATE_POLL_SEC)

    # ── RESUME (reconstruir estado tras reinicio) ──────────────

    async def _resume_positions(self) -> None:
        """Al arrancar, recupera posiciones guardadas en Redis."""
        active = await self._redis.smembers(self.ACTIVE_KEY)
        if not active:
            logger.info("Sin posiciones previas que resumir")
            return

        for coin in active:
            data = await self._redis.hgetall(self.POS_KEY.format(coin=coin))
            if not data or data.get("status") != "OPEN":
                await self._clear_redis(coin)
                continue

            # Verificar que la posición sigue viva en el exchange
            pos = await self.client.get_position(coin)
            if pos is None or float(pos.get("szi", 0)) == 0:
                logger.info("[RESUME] %s: posición cerrada mientras offline — limpiando", coin)
                await self._clear_redis(coin)
                continue

            state = PositionState.from_redis(data)
            self.positions[coin] = state
            logger.info("[RESUME] %s: posición %s recuperada (entry=$%.2f, size=%.6f)",
                         coin, state.action, state.entry_price, state.size)

            # Reanudar timer de TP si aplica
            if state.tp_type == "TIME" and state.tp_target_ts > 0:
                now = _ts_now()
                if now < state.tp_target_ts:
                    remaining = (state.tp_target_ts - now) / 60_000
                    task = asyncio.create_task(self._tp_time_monitor(coin, state.tp_target_ts))
                    self._tasks[f"tp_{coin}"] = task
                    logger.info("[RESUME] TP timer reanudado: %s restantes", _fmt_duration(remaining))
                else:
                    logger.info("[RESUME] TP timer expiró mientras offline — cerrando %s", coin)
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
        net = "MAINNET" if config.HL_MAINNET else "TESTNET"
        print(f"""
================================================================
  HL SMART EXECUTOR | {mode} | {net}
  Channel: {config.HL_SIGNAL_CHANNEL}
  Position: {config.HL_POSITION_FRAC*100:.0f}% of balance
  Limit offset: {config.HL_LIMIT_OFFSET_BPS} bps | wait: {config.HL_LIMIT_WAIT_SEC}s
  Reconcile: every {config.RECONCILE_INTERVAL_SEC}s
  Journal: {config.TRADE_DB}
  Active positions: {len(self.positions)}
================================================================
""")


# ═══════════════════════════════════════════════════════════════════
#  5. ENTRYPOINT
# ═══════════════════════════════════════════════════════════════════

async def _run(dry_run: bool) -> None:
    executor = HLSmartExecutor(dry_run=dry_run)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(_handle_stop(executor, s)))
    await executor.start()


async def _handle_stop(executor: HLSmartExecutor, sig: signal.Signals) -> None:
    logger.info("Recibida señal %s — iniciando shutdown graceful", sig.name)
    await executor._shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description="Hyperliquid Smart Executor")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Solo logs, sin enviar órdenes reales",
    )
    args = parser.parse_args()

    dry_run = args.dry_run or not config.HL_PRIVATE_KEY
    if not config.HL_PRIVATE_KEY and not args.dry_run:
        logger.info("HL_PRIVATE_KEY vacío — activando DRY RUN automático")

    asyncio.run(_run(dry_run))


if __name__ == "__main__":
    main()
