"""
extended_executor.py — Ejecutor DCA Profesional para X10 (Extended).
OPTIMIZACIÓN: Cálculo de SL/TP dinámico basado en distancias para precisión quirúrgica.
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

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)-12s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("extended_exec")

def _ts_now() -> int: return int(time.time() * 1000)

def _pair_to_x10(pair: str) -> str:
    base = pair.split("/")[0]
    return f"{base}-USD"

class ExtendedSmartExecutor:
    POS_KEY = "executor:extended:position:{coin}"
    ACTIVE_KEY = "executor:extended:active_pairs"

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run or not HAS_X10
        self._client: PerpetualTradingClient | None = None
        self._redis: aioredis.Redis | None = None
        self.positions: dict[str, PositionState] = {}
        self._journal = TradeJournal()
        self._notifier = TelegramNotifier()
        self._running = True
        self._pos_lock = asyncio.Lock()

    async def _init_client(self):
        if self.dry_run: return
        conf = MAINNET_CONFIG if config.EXTENDED_MAINNET else TESTNET_CONFIG
        pk_int = int(config.EXTENDED_PRIVATE_KEY, 16)
        pub_key_hex = hex(get_public_key(pk_int))
        account = StarkPerpetualAccount(
            vault=int(config.EXTENDED_VAULT_ID or 0),
            private_key=config.EXTENDED_PRIVATE_KEY,
            public_key=pub_key_hex,
            api_key=config.EXTENDED_API_KEY,
        )
        self._client = PerpetualTradingClient(conf, account)
        logger.info(f"🚀 Cliente X10 Inicializado (Pub: {pub_key_hex[:10]}...)")

    async def start(self) -> None:
        pool = aioredis.ConnectionPool.from_url(config.REDIS_URL, decode_responses=True, max_connections=5)
        self._redis = aioredis.Redis(connection_pool=pool)
        await self._journal.connect()
        await self._notifier.connect()
        await self._init_client()
        
        await self._sync_state_with_exchange()
        
        logger.info("🔥 MONITOR SUPREMO X10 ACTIVO")
        
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

        await asyncio.gather(
            self._listen_signals(),
            self._high_reliability_monitor()
        )

    async def stop(self):
        logger.info("🛑 Apagando ejecutor...")
        self._running = False
        await self._journal.disconnect()
        await self._notifier.disconnect()
        if self._redis:
            await self._redis.aclose()

    async def _sync_state_with_exchange(self):
        if self.dry_run: return
        
        active = await self._redis.smembers(self.ACTIVE_KEY)
        for coin in active:
            data = await self._redis.hgetall(self.POS_KEY.format(coin=coin))
            if data:
                self.positions[coin] = PositionState.from_redis(data)
        
        try:
            res = await self._client.account.get_positions()
            if res.status == "OK":
                exchange_coins = set()
                for p in res.data:
                    coin = p.market.split("-")[0]
                    exchange_coins.add(coin)
                    real_size = float(p.size)
                    
                    if coin not in self.positions:
                        logger.info(f"Sincronizando posición externa detectada: {coin}")
                        self.positions[coin] = PositionState(
                            coin=coin, action="BUY" if p.side.upper() == "LONG" else "SELL",
                            entry_price=float(p.open_price), size=real_size,
                            sl_price=0, tp_value=0, entry_ts=_ts_now()
                        )
                    else:
                        if abs(self.positions[coin].size - real_size) > 1e-6:
                            logger.info(f"Ajustando tamaño {coin}: {self.positions[coin].size} -> {real_size}")
                            self.positions[coin].size = real_size
                    
                    await self._save_to_redis(coin, self.positions[coin])
                
                for coin in list(self.positions.keys()):
                    if coin not in exchange_coins:
                        logger.warning(f"Posición {coin} no existe en exchange. Limpiando local/Redis.")
                        await self._clear_local_and_redis(coin)
        except Exception as e:
            logger.error(f"Error en sincronización inicial: {e}")

    async def _high_reliability_monitor(self) -> None:
        while self._running:
            try:
                if not self.positions:
                    await asyncio.sleep(10)
                    continue

                res_pos = await self._client.account.get_positions()
                if res_pos.status != "OK":
                    await asyncio.sleep(5); continue
                
                res_orders = await self._client.account.get_open_orders()
                open_orders = res_orders.data if res_orders.status == "OK" else []
                
                exchange_positions = {p.market.split("-")[0]: p for p in res_pos.data}

                for coin, state in list(self.positions.items()):
                    market = _pair_to_x10(f"{coin}/USD")
                    
                    if coin not in exchange_positions:
                        logger.error(f"🚨 Posición {coin} DESAPARECIDA en exchange. Limpiando...")
                        await self._clear_local_and_redis(coin)
                        continue
                    
                    p = exchange_positions[coin]
                    mid = float(p.mark_price)
                    is_buy = state.action in ("BUY", "LONG")
                    
                    # Restauramos cálculo de PnL para el log
                    pnl = (mid/state.entry_price - 1)*100 if is_buy else (state.entry_price/mid - 1)*100
                    logger.info(f"VIGILANCIA | {coin} {state.action} | Px: {mid:.1f} | Entry: {state.entry_price:.1f} | PnL: {pnl:.2f}% | TP: {state.tp_value}")
                    
                    if state.sl_price > 0:
                        if (is_buy and mid <= state.sl_price) or (not is_buy and mid >= state.sl_price):
                            logger.error(f"🔥 STOP LOSS TRIGGERED para {coin} @ {mid}")
                            await self._cleanup_and_close(coin, "SL_MONITOR")
                            continue

                    if state.tp_value > 0:
                        has_tp_order = any(o.market == market and abs(float(o.price) - state.tp_value) < 1.0 for o in open_orders)
                        if not has_tp_order:
                            logger.warning(f"⚠️ TP Faltante para {coin}. Restaurando orden limit...")
                            await self._place_tp_order(coin, state, float(p.size))

            except Exception as e:
                logger.error(f"Error en monitor: {e}", exc_info=True)
            
            await asyncio.sleep(60)

    async def _place_tp_order(self, coin: str, state: PositionState, real_size: float):
        if self.dry_run: return
        try:
            market = _pair_to_x10(f"{coin}/USD")
            await self._client.orders.mass_cancel() 
            await asyncio.sleep(1)
            
            res = await self._client.place_order(
                market_name=market, 
                amount_of_synthetic=Decimal(str(round(real_size, 5))),
                price=Decimal(str(round(state.tp_value))), 
                side=OrderSide.SELL if state.action == "BUY" else OrderSide.BUY,
                reduce_only=True
            )
            if res.status == "OK":
                logger.info(f"✅ TP Restaurado para {coin} @ {state.tp_value}")
            else:
                logger.error(f"❌ Error al restaurar TP: {res}")
        except Exception as e:
            logger.error(f"Excepción en restauración de TP: {e}")

    async def _listen_signals(self) -> None:
        while self._running:
            try:
                pubsub = self._redis.pubsub()
                await pubsub.subscribe(config.HL_SIGNAL_CHANNEL)
                async for msg in pubsub.listen():
                    if not self._running: break
                    if msg["type"] == "message":
                        try:
                            sig = json.loads(msg["data"])
                            if sig.get("exchange", "").upper() in ("EXTENDED", "X10", "ALL"):
                                await self._handle_trade_atomic(sig)
                        except Exception as e:
                            logger.error(f"Error procesando señal JSON: {e}")
            except Exception as e:
                logger.error(f"Error en listener de señales: {e}")
                await asyncio.sleep(5)

    async def _handle_trade_atomic(self, sig: dict):
        sig_id = sig.get("signal_id") or str(hash(json.dumps(sig, sort_keys=True)))
        dedup_key = f"executor:extended:dedup:{sig_id}"
        
        if await self._redis.get(dedup_key):
            logger.info(f"♻️ Señal duplicada omitida: {sig_id}")
            return
            
        async with self._pos_lock:
            if await self._redis.get(dedup_key): return
            await self._redis.set(dedup_key, "1", ex=600)
            
            coin = sig["pair"].split("/")[0]
            action = sig["action"].upper()
            logger.info(f"⚡ SEÑAL RECIBIDA: {coin} {action} (ID: {sig_id})")
            
            if action == "CLOSE":
                await self._cleanup_and_close(coin, "SIGNAL")
            elif action in ("BUY", "SELL"):
                await self._open_at_market(coin, action, sig)

    async def _open_at_market(self, coin: str, action: str, sig: dict):
        if self.dry_run:
            logger.info(f"[DRY RUN] Abriendo {coin} {action}")
            return

        try:
            market = _pair_to_x10(sig["pair"])
            mid = await self._get_mid(market)
            if mid <= 0: return
            
            px = round(mid * (1.01 if action == "BUY" else 0.99))
            size = float(sig.get("size", 0.001))
            
            # --- PRECISIÓN QUIRÚRGICA: Cálculo de SL/TP basado en el MID real de X10 ---
            tp_dist = float(sig.get("tp_distance", 800))
            sl_dist = float(sig.get("sl_distance", 2800))
            
            target_tp = float(round(mid + tp_dist if action == "BUY" else mid - tp_dist, 2))
            target_sl = float(round(mid - sl_dist if action == "BUY" else mid + sl_dist, 2))
            # -------------------------------------------------------------------------

            res = await self._client.place_order(
                market_name=market, amount_of_synthetic=Decimal(str(round(size, 5))),
                price=Decimal(str(px)), 
                side=OrderSide.BUY if action == "BUY" else OrderSide.SELL
            )
            
            if res.status == "OK":
                logger.info(f"✅ Orden {action} ejecutada para {coin} @ {mid}")
                if coin not in self.positions:
                    self.positions[coin] = PositionState(
                        coin=coin, action=action, entry_price=mid, size=size,
                        sl_price=target_sl, tp_value=target_tp,
                        entry_ts=_ts_now()
                    )
                else:
                    st = self.positions[coin]
                    new_total_size = st.size + size
                    st.entry_price = ((st.entry_price * st.size) + (mid * size)) / new_total_size
                    st.size = new_total_size
                    # Actualizamos TP/SL con los nuevos objetivos de la señal (basados en el último MID)
                    st.tp_value = target_tp
                    st.sl_price = target_sl
                
                await self._save_to_redis(coin, self.positions[coin])
        except Exception as e:
            logger.error(f"Error al abrir posición: {e}")

    async def _cleanup_and_close(self, coin: str, reason: str):
        if coin not in self.positions: return
        state = self.positions[coin]
        
        if not self.dry_run:
            try:
                market = _pair_to_x10(f"{coin}/USD")
                await self._client.orders.mass_cancel()
                mid = await self._get_mid(market)
                px = round(mid * (0.98 if state.action == "BUY" else 1.02))
                
                res = await self._client.place_order(
                    market_name=market, amount_of_synthetic=Decimal(str(round(state.size, 5))),
                    price=Decimal(str(px)), 
                    side=OrderSide.SELL if state.action == "BUY" else OrderSide.BUY,
                    reduce_only=True
                )
                if res.status == "OK":
                    logger.info(f"🏁 Posición {coin} CERRADA con éxito ({reason}).")
                else:
                    if "1137" in str(res) or "missing" in str(res).lower():
                        logger.info(f"🏁 Posición {coin} ya no existía en exchange. Limpiando local.")
                    else:
                        logger.error(f"❌ Error al cerrar: {res}")
                        return
            except Exception as e:
                logger.error(f"Excepción al cerrar: {e}")
                return

        await self._clear_local_and_redis(coin)

    async def _get_mid(self, market: str) -> float:
        try:
            res = await self._client.markets_info.get_markets()
            if res.status == "OK":
                for m in res.data:
                    if m.name == market: return float(m.market_stats.last_price)
        except: pass
        return 0.0

    async def _save_to_redis(self, coin: str, state: PositionState) -> None:
        await self._redis.hset(self.POS_KEY.format(coin=coin), mapping=state.to_redis())
        await self._redis.sadd(self.ACTIVE_KEY, coin)

    async def _clear_local_and_redis(self, coin: str) -> None:
        if coin in self.positions: del self.positions[coin]
        await self._redis.delete(self.POS_KEY.format(coin=coin))
        await self._redis.srem(self.ACTIVE_KEY, coin)

async def _run(dry_run: bool) -> None:
    executor = ExtendedSmartExecutor(dry_run=dry_run)
    await executor.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(_run(args.dry_run))
