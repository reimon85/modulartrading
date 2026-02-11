"""
lighter_executor.py — Ejecutor de ALTA PRECISION para Lighter.xyz.

VERSION FINAL: Flujo Atomico para BUY, SELL y CLOSE.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from pathlib import Path

import redis.asyncio as aioredis

# ── Project imports ─────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
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
            await self._listen_signals()
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
            if msg["type"] != "message": continue
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
        current_nonce = int(time.time() * 1000) % 2147483647
        signer = await self.client.get_signer()
        try:
            current_nonce = await self.client.cancel_all_orders(signer, current_nonce)
            await asyncio.sleep(2.0)
            res = await self.client.market_order(signer, coin, is_buy, size_to_add, current_nonce)
            if res.get("status") != "ok":
                logger.error("  [!] Error mercado: %s", res.get("error"))
                return
            current_nonce += 1
            await asyncio.sleep(3.0) 
            pos_real = await self.client.get_position(coin)
            if pos_real:
                real_size = abs(float(pos_real["szi"]))
                real_avg = float(pos_real["entryPx"])
                logger.info("[3/4] Realidad: Total=%.5f BTC | Promedio=$%.2f", real_size, real_avg)
                target_tp = float(signal.get("tp_value", real_avg + 700 if is_buy else real_avg - 700))
                target_sl = float(signal.get("sl_price", real_avg - 2550 if is_buy else real_avg + 2550))
                await self.client.trigger_order(signer, coin, is_buy, real_size, target_tp, "tp", current_nonce)
                current_nonce += 1
                await asyncio.sleep(2.0) 
                await self.client.trigger_order(signer, coin, is_buy, real_size, target_sl, "sl", current_nonce)
                self.positions[coin] = PositionState(coin=coin, action=signal["action"].upper(), entry_price=real_avg, size=real_size, sl_price=target_sl, tp_value=target_tp, entries=[])
                await self._save_to_redis(coin, self.positions[coin])
            logger.info("FLUJO FINALIZADO")
        finally: await signer.close()
        logger.info("-" * 60)

    async def _handle_close_atomic(self, signal: dict) -> None:
        coin = signal["pair"].split("/")[0]
        logger.info("-" * 60)
        logger.info("INICIANDO CIERRE ATOMICO EN %s", coin)
        
        pos_real = await self.client.get_position(coin)
        if not pos_real:
            logger.info("No hay posicion abierta para cerrar.")
            return

        size_to_close = abs(float(pos_real["szi"]))
        is_long = float(pos_real["szi"]) > 0
        current_nonce = int(time.time() * 1000) % 2147483647
        
        signer = await self.client.get_signer()
        try:
            logger.info("[1/3] Limpiando proteccion...")
            current_nonce = await self.client.cancel_all_orders(signer, current_nonce)
            await asyncio.sleep(2.0)

            logger.info("[2/3] Ejecutando VENTA mercado para cerrar %.5f BTC...", size_to_close)
            # Para cerrar un LONG, is_buy debe ser False
            res = await self.client.market_order(signer, coin, not is_long, size_to_close, current_nonce)
            if res.get("status") == "ok":
                logger.info("[3/3] Cierre ejecutado con exito.")
                if coin in self.positions: del self.positions[coin]
                await self._clear_redis(coin)
            else:
                logger.error("  [!] Error al cerrar: %s", res.get("error"))
        finally: await signer.close()
        logger.info("-" * 60)

    async def _state_monitor(self) -> None:
        while self._running:
            for coin in list(self.positions.keys()):
                pos = await self.client.get_position(coin)
                if not pos:
                    logger.info("[MONITOR] Posicion %s cerrada", coin)
                    del self.positions[coin]
                    await self._clear_redis(coin)
            await asyncio.sleep(10)

    async def _resume_positions(self) -> None:
        active = await self._redis.smembers(self.ACTIVE_KEY)
        for coin in active:
            data = await self._redis.hgetall(self.POS_KEY.format(coin=coin))
            if data: self.positions[coin] = PositionState.from_redis(data)

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
