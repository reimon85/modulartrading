"""
extended_executor.py — Ejecutor DCA Inteligente para Extended (X10).
Gestiona múltiples entradas (DCA) y protección dinámica vía monitor local.
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
from executor.models import PositionState, validate_signal  # noqa: E402

# X10 SDK Imports
try:
    from x10.perpetual.accounts import StarkPerpetualAccount
    from x10.perpetual.configuration import MAINNET_CONFIG, TESTNET_CONFIG
    from x10.perpetual.orders import (
        OrderSide, 
        OrderTpslType, 
        OrderTriggerPriceType, 
        OrderPriceType
    )
    from x10.perpetual.order_object import OrderTpslTriggerParam
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
        self.positions: dict[str, PositionState] = {}
        self._redis: aioredis.Redis | None = None
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
        logger.info(f"Cliente Extended inicializado (Pub: {pub_key_hex[:10]}...)")

    async def start(self) -> None:
        pool = aioredis.ConnectionPool.from_url(config.REDIS_URL, decode_responses=True, max_connections=5)
        self._redis = aioredis.Redis(connection_pool=pool)
        await self._journal.connect()
        await self._notifier.connect()
        await self._init_client()
        await self._resume_positions()
        
        # Sincronización inicial
        if not self.dry_run and self._client:
            try:
                res = await self._client.account.get_positions()
                if res.status == "OK":
                    for p in res.data:
                        coin = p.market.split("-")[0]
                        if coin not in self.positions:
                            logger.info(f"Sincronizando posicion externa {coin} ({p.side} {p.size})")
                            state = PositionState(
                                coin=coin, action="BUY" if p.side.upper() == "LONG" else "SELL", 
                                entry_price=float(p.open_price),
                                size=float(p.size), sl_price=0.0, tp_value=0.0, entry_ts=_ts_now()
                            )
                            self.positions[coin] = state
                            await self._save_to_redis(coin, state)
                        else:
                            # Si ya existe, asegurar que el tamaño esté sincronizado
                            st = self.positions[coin]
                            if abs(st.size - float(p.size)) > 1e-6:
                                logger.info(f"Actualizando tamaño de {coin}: {st.size} -> {p.size}")
                                st.size = float(p.size)
                                await self._save_to_redis(coin, st)
                        
                        # Asegurar que el TP sea visible para todas las posiciones (incluso las ya existentes)
                        st = self.positions[coin]
                        if st.tp_value > 0:
                            await self._place_tp_order(coin, st)
            except Exception as e: logger.warning(f"Error sync positions: {e}")

            try:
                orders_res = await self._client.account.get_open_orders()
                if orders_res.status == "OK":
                    logger.info(f"Detectadas {len(orders_res.data)} ordenes abiertas en exchange.")
            except Exception as e: logger.warning(f"Error sync orders: {e}")

        logger.info("Extended DCA Executor operativo.")
        await asyncio.gather(self._listen_signals(), self._state_monitor())

    async def _listen_signals(self) -> None:
        while self._running:
            try:
                pubsub = self._redis.pubsub()
                await pubsub.subscribe(config.HL_SIGNAL_CHANNEL)
                while self._running:
                    msg = await pubsub.get_message(timeout=1.0)
                    if msg and msg["type"] == "message":
                        sig = json.loads(msg["data"])
                        if sig.get("exchange", "").upper() in ("EXTENDED", "X10", "ALL"):
                            await self._dispatch(sig)
            except Exception as e:
                logger.error(f"Error listener: {e}")
                await asyncio.sleep(5)

    async def _dispatch(self, sig: dict) -> None:
        async with self._pos_lock:
            action = sig["action"].upper()
            coin = sig["pair"].split("/")[0]
            
            # Log market context if available
            ctx = sig.get("market_context", {})
            if ctx:
                logger.info(f"Signal Context | SlopeD: {ctx.get('slope_d',0):.2f} | Gap: {ctx.get('emagap')} | Pivot: {ctx.get('pivot_bias')}")

            if action in ("BUY", "SELL"):
                if coin in self.positions:
                    if self.positions[coin].action == action:
                        await self._handle_dca(coin, sig)
                    else:
                        logger.warning(f"Señal contraria para {coin} ignorada.")
                else:
                    await self._handle_open(coin, sig)
            elif action == "CLOSE":
                await self._cleanup_and_close(coin, "SIGNAL")

    async def _handle_open(self, coin: str, sig: dict) -> None:
        market = _pair_to_x10(sig["pair"])
        is_buy = sig["action"].upper() == "BUY"
        size = float(sig.get("size", 0.001))
        
        mid = await self._get_mid(market)
        price = round(mid * (1.005 if is_buy else 0.995))
        
        logger.info(f"Abriendo {coin} en Extended: {size} @ {price}")
        res = await self._client.place_order(
            market_name=market, amount_of_synthetic=Decimal(str(round(size, 5))),
            price=Decimal(str(price)), side=OrderSide.BUY if is_buy else OrderSide.SELL
        )
        
        if res.status == "OK":
            state = PositionState(
                coin=coin, action=sig["action"].upper(), entry_price=price, size=size,
                sl_price=float(sig["sl_price"]), tp_value=float(sig.get("tp_value", 0)),
                tp_type="PRICE", entry_ts=_ts_now()
            )
            self.positions[coin] = state
            state.trade_id = await self._journal.log_open("extended", coin, state.action, price, size, state.sl_price, "PRICE", state.tp_value)
            await self._save_to_redis(coin, state)
            await self._notifier.notify_trade(f"OPEN {state.action}", coin, price, size, state.sl_price, "PRICE", state.tp_value, source="Extended")
            
            # Colocar TP visible
            if state.tp_value > 0:
                await self._place_tp_order(coin, state)

    async def _handle_dca(self, coin: str, sig: dict) -> None:
        state = self.positions[coin]
        market = _pair_to_x10(sig["pair"])
        size = float(sig.get("size", 0.001))
        is_buy = state.action == "BUY"
        
        mid = await self._get_mid(market)
        price = round(mid * (1.005 if is_buy else 0.995))
        
        logger.info(f"DCA ADD {coin}: {size} @ {price}")
        res = await self._client.place_order(
            market_name=market, amount_of_synthetic=Decimal(str(round(size, 5))),
            price=Decimal(str(price)), side=OrderSide.BUY if is_buy else OrderSide.SELL
        )
        
        if res.status == "OK":
            new_sz = state.size + size
            state.entry_price = (state.entry_price * state.size + price * size) / new_sz
            state.size = new_sz
            state.sl_price = float(sig.get("sl_price", state.sl_price))
            state.tp_value = float(sig.get("tp_value", state.tp_value))
            
            if state.trade_id: await self._journal.log_reentry(state.trade_id, price, size)
            await self._save_to_redis(coin, state)
            await self._notifier.notify_trade(f"DCA ADD", coin, price, size, state.sl_price, "PRICE", state.tp_value, source="Extended")
            
            # Actualizar TP visible (cancelar anterior y poner nuevo)
            await self._place_tp_order(coin, state)

    async def _place_tp_order(self, coin: str, state: PositionState) -> None:
        """Coloca una orden LIMIT REDUCE_ONLY para el Take Profit con reintentos y ajuste de tamaño."""
        if self.dry_run or not self._client: return
        market = _pair_to_x10(f"{coin}/USD")
        is_long = state.action in ("BUY", "LONG")
        
        for attempt in range(5):
            try:
                # 1. Consultar posicion real en exchange para asegurar el tamaño correcto
                pos_res = await self._client.account.get_positions()
                ex_size = 0.0
                if pos_res.status == "OK":
                    for p in pos_res.data:
                        if p.market == market:
                            ex_size = float(p.size)
                            break
                
                if ex_size <= 0:
                    logger.warning(f"Intento {attempt+1}: No hay posicion en exchange para {coin}. Reintentando en 3s...")
                    await asyncio.sleep(3)
                    continue

                # Usar el minimo entre el estado local y el real del exchange para evitar Error 1136
                tp_size = min(state.size, ex_size)
                if tp_size != state.size:
                    logger.warning(f"Sincronizando tamaño para {coin}: Local={state.size} -> Exchange={ex_size}")
                    state.size = ex_size  # Actualizar estado local para estar en sync
                    await self._save_to_redis(coin, state)

                # 2. Cancelar ordenes previas
                await self._client.orders.mass_cancel()
                
                # 3. Colocar nueva orden LIMIT TP
                res = await self._client.place_order(
                    market_name=market, 
                    amount_of_synthetic=Decimal(str(round(tp_size, 5))),
                    price=Decimal(str(round(state.tp_value))), 
                    side=OrderSide.SELL if is_long else OrderSide.BUY,
                    reduce_only=True
                )
                if res.status == "OK":
                    logger.info(f"Orden TP visible colocada para {coin} @ {state.tp_value} (Size: {tp_size})")
                    return
                
                err_msg = str(res)
                if "1136" in err_msg or "Reduce-only" in err_msg:
                    logger.warning(f"Intento {attempt+1}: Error 1136 persistente. Reintentando en 3s...")
                    await asyncio.sleep(3)
                else:
                    logger.warning(f"Error colocación TP: {err_msg}")
                    break
            except Exception as e:
                err_msg = str(e)
                if "1136" in err_msg or "Reduce-only" in err_msg:
                    logger.warning(f"Intento {attempt+1}: Error 1136 (excep). Reintentando en 3s...")
                    await asyncio.sleep(3)
                else:
                    logger.warning(f"Excepcion colocando TP visible: {e}")
                    break

    async def _cleanup_and_close(self, coin: str, reason: str) -> None:
        state = self.positions.get(coin)
        if not state: return
        market = _pair_to_x10(f"{coin}/USD")
        is_long = state.action in ("BUY", "LONG")
        
        mid = await self._get_mid(market)
        px = round(mid * (0.99 if is_long else 1.01))
        
        # Cancelar TP visible antes de cerrar
        if not self.dry_run and self._client:
            try: await self._client.orders.mass_cancel()
            except: pass

        logger.info(f"Cerrando {coin} por {reason}: {state.size} @ {px}")
        res = await self._client.place_order(
            market_name=market, amount_of_synthetic=Decimal(str(round(state.size, 5))),
            price=Decimal(str(px)), side=OrderSide.SELL if is_long else OrderSide.BUY,
            reduce_only=True
        )
        
        if res.status == "OK":
            pnl = (px/state.entry_price - 1)*100 if is_long else (state.entry_price/px - 1)*100
            if state.trade_id: await self._journal.log_close(state.trade_id, px, reason)
            await self._notifier.notify_pnl(coin, state.entry_price, px, pnl, reason, source="Extended")
            del self.positions[coin]
            await self._clear_redis(coin)

    async def _get_mid(self, market: str) -> float:
        try:
            res = await self._client.markets_info.get_markets()
            for m in res.data:
                if m.name == market: return float(m.market_stats.last_price)
        except: pass
        return 0.0

    async def _state_monitor(self) -> None:
        """Monitor local de SL/TP para gestionar la posicion total."""
        while self._running:
            try:
                # 0. Sincronizar ordenes abiertas una vez por ciclo para evitar llamadas excesivas
                open_markets = set()
                if not self.dry_run and self._client:
                    try:
                        orders_res = await self._client.account.get_open_orders()
                        if orders_res.status == "OK":
                            open_markets = {o.market for o in orders_res.data}
                    except: pass

                # 1. Heartbeat y Garbage Collector
                if not self.positions:
                    ctx = await self._redis.hgetall("strategy:dca:context:btc")
                    if ctx:
                        logger.info(f"Market | BTC | Price: {float(ctx.get('price',0)):.1f} | SlopeD: {float(ctx.get('slope_d',0)):.2f} | Gap: {ctx.get('emagap')} | Pivot: {ctx.get('pivot_bias')}")
                    
                    # Limpiar ordenes huerfanas si no hay posiciones locales
                    if not self.dry_run and self._client and open_markets:
                        logger.info(f"Limpiando {len(open_markets)} mercados con ordenes huerfanas")
                        await self._client.orders.mass_cancel()

                # 2. Monitor active positions
                for coin, state in list(self.positions.items()):
                    market = _pair_to_x10(f"{coin}/USD")
                    mid = await self._get_mid(market)
                    if mid <= 0: continue
                    
                    # Log status with context (SlopeD, Gap, Pivot) from strategy if available in Redis
                    is_buy = state.action in ("BUY", "LONG")
                    pnl = (mid/state.entry_price - 1)*100 if is_buy else (state.entry_price/mid - 1)*100
                    
                    # Intentar obtener contexto de mercado de Redis (si la estrategia lo guarda ahí)
                    ctx = await self._redis.hgetall(f"strategy:dca:context:{coin.lower()}")
                    ctx_str = ""
                    if ctx:
                        ctx_str = f" | SlopeD: {float(ctx.get('slope_d',0)):.2f} | Gap: {ctx.get('emagap')} | Pivot: {ctx.get('pivot_bias')}"
                    
                    logger.info(f"Monitor | {coin} {state.action} | Price: {mid:.1f} | Entry: {state.entry_price:.1f} | PnL: {pnl:.2f}%{ctx_str} | SL: {state.sl_price:.1f} | TP: {state.tp_value:.1f}")

                    # Asegurar TP visible en exchange
                    if not self.dry_run and state.tp_value > 0 and market not in open_markets:
                        logger.info(f"TP no detectado en exchange para {coin}. Colocando...")
                        await self._place_tp_order(coin, state)

                    # Stop Loss
                    if state.sl_price > 0:
                        if (is_buy and mid <= state.sl_price) or (not is_buy and mid >= state.sl_price):
                            await self._cleanup_and_close(coin, "SL_MONITOR")
                    # Take Profit
                    if state.tp_value > 0:
                        if (is_buy and mid >= state.tp_value) or (not is_buy and mid <= state.tp_value):
                            await self._cleanup_and_close(coin, "TP_MONITOR")
            except Exception as e: logger.error(f"Error monitor: {e}")
            await asyncio.sleep(20)

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

async def _run(dry_run: bool) -> None:
    executor = ExtendedSmartExecutor(dry_run=dry_run)
    await executor.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(_run(args.dry_run))