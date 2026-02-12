"""
extended_executor.py — Versión de Vigilancia Activa Implacable.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import redis.asyncio as aioredis

# ── Project imports ─────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import config  # noqa: E402
from src.database.trade_journal import TradeJournal  # noqa: E402
from src.notifier import AlertLevel, TelegramNotifier  # noqa: E402
from executor.models import PositionState  # noqa: E402

# X10 SDK Imports
try:
    from x10.perpetual.accounts import StarkPerpetualAccount
    from x10.perpetual.configuration import MAINNET_CONFIG, TESTNET_CONFIG
    from x10.perpetual.orders import OrderSide
    from x10.perpetual.trading_client import PerpetualTradingClient
    from fast_stark_crypto import get_public_key
    HAS_X10 = True
except ImportError:
    HAS_X10 = False

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("extended_exec")

def _ts_now() -> int: return int(time.time() * 1000)

class ExtendedSmartExecutor:
    POS_KEY = "executor:extended:position:{coin}"
    ACTIVE_KEY = "executor:extended:active_pairs"

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run or not HAS_X10
        self._client: PerpetualTradingClient | None = None
        self._redis: aioredis.Redis | None = None
        self._running = True
        self._pos_lock = asyncio.Lock()

    async def _init_client(self):
        if self.dry_run: return
        conf = MAINNET_CONFIG if config.EXTENDED_MAINNET else TESTNET_CONFIG
        pk_int = int(config.EXTENDED_PRIVATE_KEY, 16)
        account = StarkPerpetualAccount(
            vault=int(config.EXTENDED_VAULT_ID or 0),
            private_key=config.EXTENDED_PRIVATE_KEY,
            public_key=hex(get_public_key(pk_int)),
            api_key=config.EXTENDED_API_KEY,
        )
        self._client = PerpetualTradingClient(conf, account)
        logger.info(f"Conexión X10 Establecida.")

    async def start(self) -> None:
        pool = aioredis.ConnectionPool.from_url(config.REDIS_URL, decode_responses=True, max_connections=5)
        self._redis = aioredis.Redis(connection_pool=pool)
        await self._init_client()
        logger.info("🔥 MONITOR SUPREMO ACTIVO")
        await asyncio.gather(self._listen_signals(), self._high_reliability_monitor())

    async def _high_reliability_monitor(self) -> None:
        while self._running:
            try:
                # HEARTBEAT
                logger.info("--- CICLO DE VIGILANCIA ---")
                
                # 1. Obtener posiciones REALES
                res_pos = await self._client.account.get_positions()
                if res_pos.status != "OK":
                    logger.warning("Fallo al leer posiciones. Reintentando..."); await asyncio.sleep(5); continue
                
                if not res_pos.data:
                    logger.info("Sin posiciones abiertas.")
                
                # 2. Obtener órdenes REALES
                res_orders = await self._client.account.get_open_orders()
                open_orders = res_orders.data if res_orders.status == "OK" else []

                for p in res_pos.data:
                    coin = p.market.split("-")[0]
                    real_size = float(p.size)
                    real_avg = float(p.open_price)
                    mid = float(p.mark_price) # Usamos mark_price que ya viene en la posición (más rápido)
                    
                    # Cargar plan de Redis
                    redis_data = await self._redis.hgetall(self.POS_KEY.format(coin=coin))
                    if not redis_data:
                        logger.warning(f"Posición {coin} sin datos en Redis. Saltando...")
                        continue

                    action = redis_data["action"]
                    tp_price = float(redis_data.get("tp_value", 0))
                    sl_price = float(redis_data.get("sl_price", 0))
                    
                    logger.info(f"VIGILANDO {coin} {action} | Px: {mid:.1f} | TP: {tp_price} | SL: {sl_price}")

                    # COMPROBACIÓN DE TAKE PROFIT
                    has_tp = False
                    for o in open_orders:
                        if o.market == p.market and abs(float(o.price) - tp_price) < 2.0:
                            has_tp = True; break
                    
                    if not has_tp and tp_price > 0:
                        logger.error(f"🚨 ORDEN FALTANTE para {coin}. Restaurando...")
                        await self._ensure_tp_order(p.market, real_size, action, tp_price)
                    else:
                        logger.info(f" ✅ Protección TP confirmada.")

                    # COMPROBACIÓN DE STOP LOSS (Local)
                    is_buy = (action == "BUY")
                    if sl_price > 0:
                        if (is_buy and mid <= sl_price) or (not is_buy and mid >= sl_price):
                            logger.error(f"🔥 STOP LOSS TRIGGERED en {mid}!")
                            await self._cleanup_and_close(coin, real_size, action, "SL_MONITOR")

            except Exception as e:
                logger.error(f"Error en monitor: {e}")
            
            await asyncio.sleep(120)

    async def _ensure_tp_order(self, market: str, size: float, action: str, price: float):
        try:
            # Cancelamos cualquier orden vieja para evitar duplicados o errores de margen
            await self._client.orders.mass_cancel()
            await asyncio.sleep(1)
            res = await self._client.place_order(
                market_name=market, amount_of_synthetic=Decimal(str(round(size, 5))),
                price=Decimal(str(round(price))), 
                side=OrderSide.BUY if action == "SELL" else OrderSide.SELL,
                reduce_only=True
            )
            if res.status == "OK": logger.info(f" ✅ TP Restaurado con éxito.")
            else: logger.error(f" ❌ Error al poner TP: {res}")
        except Exception as e: logger.error(f"Excepción en TP: {e}")

    async def _listen_signals(self) -> None:
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(config.HL_SIGNAL_CHANNEL)
        async for msg in pubsub.listen():
            if msg["type"] == "message":
                try:
                    sig = json.loads(msg["data"])
                    if sig.get("exchange", "").upper() in ("EXTENDED", "X10", "ALL"):
                        await self._handle_trade_atomic(sig)
                except: pass

    async def _handle_trade_atomic(self, sig: dict):
        coin = sig["pair"].split("/")[0]
        action = sig["action"].upper()
        logger.info(f"⚡ SEÑAL RECIBIDA: {action}")
        try:
            market = f"{coin}-USD"
            res = await self._client.place_order(
                market_name=market, amount_of_synthetic=Decimal(str(sig["size"])),
                price=Decimal("67000"), # Dummy
                side=OrderSide.BUY if action == "BUY" else OrderSide.SELL
            )
            if res.status == "OK":
                logger.info(f" ✅ Orden mercado OK. El monitor actualizará el TP en breve.")
        except Exception as e: logger.error(f"Error signal: {e}")

    async def _cleanup_and_close(self, coin: str, size: float, action: str, reason: str):
        try:
            market = f"{coin}-USD"
            await self._client.orders.mass_cancel()
            await self._client.place_order(
                market_name=market, amount_of_synthetic=Decimal(str(size)),
                price=Decimal("67000"), 
                side=OrderSide.SELL if action == "BUY" else OrderSide.BUY,
                reduce_only=True
            )
            logger.info(f"🏁 Posición CERRADA por {reason}.")
            await self._redis.delete(self.POS_KEY.format(coin=coin))
            await self._redis.srem(self.ACTIVE_KEY, coin)
        except: pass

async def _run(dry_run: bool) -> None:
    executor = ExtendedSmartExecutor(dry_run=dry_run)
    await executor.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(_run(args.dry_run))
