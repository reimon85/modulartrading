"""
lighter_executor.py — Versión REPARADA y SIN FANTASMAS.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from pathlib import Path

# ── FORZAR PATH RAIZ ───────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import redis.asyncio as aioredis

from src import config  # noqa: E402
from src.database.trade_journal import TradeJournal  # noqa: E402
from executor.lighter_client import LighterClient  # noqa: E402
from executor.models import PositionState, validate_signal  # noqa: E402

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)-12s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("lighter_exec")


class LighterSmartExecutor:
    POS_KEY = "system3:executor:lighter:position:{coin}"
    ACTIVE_KEY = "system3:executor:lighter:active_pairs"

    def __init__(self, dry_run: bool = False):
        self.client = LighterClient(dry_run=dry_run)
        self.positions: dict[str, PositionState] = {}
        self._redis: aioredis.Redis | None = None
        self._journal = TradeJournal()
        self._running = True
        self._pos_lock = asyncio.Lock()

    async def start(self) -> None:
        pool = aioredis.ConnectionPool.from_url(config.REDIS_URL, decode_responses=True, max_connections=5)
        self._redis = aioredis.Redis(connection_pool=pool)
        await self._journal.connect()
        await self._resume_positions()
        try:
            await asyncio.gather(self._listen_signals(), self._health_monitor())
        except asyncio.CancelledError: pass
        finally: await self._shutdown()

    async def _shutdown(self) -> None:
        self._running = False
        await self._journal.disconnect()
        await self.client.close()
        if self._redis: await self._redis.aclose()

    async def _listen_signals(self) -> None:
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(config.HL_SIGNAL_CHANNEL)
        logger.info("Escuchando canal: %s", config.HL_SIGNAL_CHANNEL)
        async for msg in pubsub.listen():
            if msg["type"] != "message" or not self._running: continue
            try:
                signal_data = json.loads(msg["data"])
                if signal_data.get("exchange", "").upper() in ("", "LIGHTER", "ALL"):
                    await self._dispatch(signal_data)
            except Exception as e: logger.error("Error señal: %s", e)

    async def _dispatch(self, signal: dict) -> None:
        if validate_signal(signal): return
        async with self._pos_lock:
            action = signal.get("action", "").upper()
            if action in ("BUY", "SELL"): await self._handle_trade_atomic(signal)
            elif action == "CLOSE": await self._handle_close_atomic(signal)

    async def _handle_trade_atomic(self, signal: dict) -> None:
        coin = signal["pair"].split("/")[0]
        is_buy = signal["action"].upper() == "BUY"
        size_to_add = float(signal.get("size", 0.0002))
        
        logger.info("-" * 60)
        logger.info("INICIANDO FLUJO ATOMICO: %s %s BTC", signal["action"], size_to_add)
        
        signer = await self.client.get_signer()
        try:
            await self.client.cancel_all_orders(signer, 1000)
            await asyncio.sleep(10.0) 

            res = await self.client.market_order(signer, coin, is_buy, size_to_add, 2000)
            if res.get("status") != "ok":
                logger.error("  [!] Error mercado: %s", res.get("error"))
                return
            await asyncio.sleep(5.0) 

            await self._sync_and_protect(signer, coin, is_buy, signal)
            logger.info("FLUJO FINALIZADO")
        finally: await signer.close()
        logger.info("-" * 60)

    async def _sync_and_protect(self, signer, coin, is_buy=None, signal_data=None):
        pos_real = await self.client.get_position(coin)
        if not pos_real:
            if coin in self.positions:
                del self.positions[coin]
                await self._clear_redis(coin)
            return

        real_size = abs(float(pos_real["szi"]))
        real_avg = float(pos_real["entryPx"])
        
        # --- LÓGICA DE DETECCIÓN DE LADO ORGÁNICA (SIDE LOCK) ---
        old_state = self.positions.get(coin)
        if not old_state:
            # Intentamos recuperar desde Redis para ver si ya había un lado definido
            redis_data = await self._redis.hgetall(self.POS_KEY.format(coin=coin))
            if redis_data and "action" in redis_data and float(redis_data.get("size", 0)) > 0:
                action_real = redis_data["action"]
                is_buy_real = (action_real == "BUY")
                logger.info(f"  [Side Lock] Manteniendo lado {action_real} desde Redis.")
            elif is_buy is not None:
                # Si es una posición nueva y tenemos señal, la señal manda
                is_buy_real = is_buy
                action_real = "BUY" if is_buy else "SELL"
                logger.info(f"  [Side Lock] Nueva posición detectada por señal: {action_real}")
            else:
                # Último recurso: lo que diga la exchange (signo de szi)
                is_buy_real = float(pos_real["szi"]) > 0
                action_real = "BUY" if is_buy_real else "SELL"
                logger.info(f"  [Side Lock] Sin memoria previa. Asumiendo {action_real} por signo exchange.")
        else:
            # Si ya hay una posición abierta en memoria, el lado es inamovible
            action_real = old_state.action
            is_buy_real = (action_real == "BUY")
        # -------------------------------------------------------
        
        entries = []
        if old_state:
            entries = getattr(old_state, "entries", [])
            if real_size > old_state.size + 1e-6:
                added_size = real_size - old_state.size
                try:
                    new_price = (real_avg * real_size - old_state.entry_price * old_state.size) / added_size
                    entries.append({"price": round(new_price, 1), "size": round(added_size, 5), "ts": int(time.time() * 1000)})
                    logger.info("  [DCA] Registrada nueva entrada a %.1f", new_price)
                except ZeroDivisionError: pass
        else:
            entries = [{"price": real_avg, "size": real_size, "ts": int(time.time() * 1000)}]

        if signal_data and "dca_config" in signal_data:
            dca_cfg = signal_data["dca_config"]
            tp_map = dca_cfg.get("tp_levels", {1: 800, 2: 850, 3: 900, 4: 1100})
            
            # --- CONTEO DE LOTES POR TAMAÑO REAL ---
            lot_size = float(dca_cfg.get("lot_size", 0.01))
            num_lots = max(1, int(round(real_size / lot_size, 0)))
            # ---------------------------------------
            
            tp_dist = float(tp_map.get(str(num_lots), tp_map.get(num_lots, 800)))
            
            # Calculamos TP/SL desde el PROMEDIO REAL (real_avg)
            target_tp = round(real_avg + tp_dist if is_buy_real else real_avg - tp_dist, 1)
            
            # SL especial para Nivel 4 o Base
            sl_dist = float(dca_cfg.get("sl_level_4", 1240) if num_lots >= 4 else dca_cfg.get("base_sl", 2800))
            target_sl = round(real_avg - sl_dist if is_buy_real else real_avg + sl_dist, 1)
            
            logger.info(f"  [Proteccion] Nivel {num_lots} detectado. TP Dist: {tp_dist}, SL Dist: {sl_dist}")
        else:
            state = self.positions.get(coin)
            target_tp = state.tp_value if state else (real_avg + 800 if is_buy_real else real_avg - 800)
            target_sl = state.sl_price if state else (real_avg - 2800 if is_buy_real else real_avg + 2800)

        orders = await self.client.get_open_orders(signer)
        if len(orders) < 2:
            logger.info("  [Autocuracion] Detectada falta de proteccion en %s. Poniendo TP y SL...", coin)
            side_to_close = not is_buy_real
            res_tp = await self.client.trigger_order(signer, coin, side_to_close, real_size, target_tp, "tp", 3001)
            logger.info(f"  [Proteccion] Resultado TP: {res_tp.get('status')} {res_tp.get('error', '')}")
            await asyncio.sleep(2.0)
            res_sl = await self.client.trigger_order(signer, coin, side_to_close, real_size, target_sl, "sl", 3002)
            logger.info(f"  [Proteccion] Resultado SL: {res_sl.get('status')} {res_sl.get('error', '')}")

        self.positions[coin] = PositionState(
            coin=coin, action=action_real, 
            entry_price=real_avg, size=real_size, 
            sl_price=target_sl, tp_value=target_tp, 
            entries=entries
        )
        await self._save_to_redis(coin, self.positions[coin])

    async def _handle_close_atomic(self, signal: dict) -> None:
        coin = signal["pair"].split("/")[0]
        pos_real = await self.client.get_position(coin)
        if not pos_real: return

        size_to_close = abs(float(pos_real["szi"]))
        is_long = float(pos_real["szi"]) > 0
        signer = await self.client.get_signer()
        try:
            await self.client.cancel_all_orders(signer, 4000)
            await asyncio.sleep(2.0)
            await self.client.market_order(signer, coin, not is_long, size_to_close, 5000)
            if coin in self.positions: del self.positions[coin]
            await self._clear_redis(coin)
            logger.info("Cierre total ejecutado.")
        finally: await signer.close()

    async def _health_monitor(self) -> None:
        while self._running:
            await asyncio.sleep(300) 
            async with self._pos_lock:
                signer = await self.client.get_signer()
                try:
                    await self._sync_and_protect(signer, "BTC")
                except Exception as e: logger.error(f"Error monitor salud: {e}")
                finally: await signer.close()

    async def _resume_positions(self) -> None:
        active = await self._redis.smembers(self.ACTIVE_KEY)
        for coin in active:
            data = await self._redis.hgetall(self.POS_KEY.format(coin=coin))
            if data:
                try:
                    self.positions[coin] = PositionState.from_redis(data)
                except Exception as e:
                    logger.error(f"Error cargando posicion {coin} desde Redis: {e}")

    async def _save_to_redis(self, coin: str, state: PositionState) -> None:
        await self._redis.hset(self.POS_KEY.format(coin=coin), mapping=state.to_redis())
        await self._redis.sadd(self.ACTIVE_KEY, coin)

    async def _clear_redis(self, coin: str) -> None:
        await self._redis.delete(self.POS_KEY.format(coin=coin))
        await self._redis.srem(self.ACTIVE_KEY, coin)

async def _run() -> None:
    executor = LighterSmartExecutor()
    await executor.start()

if __name__ == "__main__":
    asyncio.run(_run())
