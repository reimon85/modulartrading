"""
lighter_client.py — Wrapper async sobre el SDK de Lighter.xyz DEX.

El SDK de Lighter es nativo async — no necesita asyncio.to_thread().
Si dry_run=True (o LIGHTER_API_KEY vacio), solo loguea sin enviar ordenes.

Interfaz espejo de HLClient para intercambiabilidad.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import config  # noqa: E402

logger = logging.getLogger("lighter_client")

# Market indices en Lighter.xyz
MARKET_INDEX = {"BTC": 1, "ETH": 0, "SOL": 2}
# Precio/tamaño: Lighter usa enteros escalados
# Price: dollars * 100 (e.g., $98000 = 9800000)
# Size: base units (e.g., BTC szDecimals=4 → 1 BTC = 10000)
PX_SCALE = {"BTC": 100, "ETH": 100, "SOL": 100}
SZ_SCALE = {"BTC": 10000, "ETH": 10000, "SOL": 100}
SZ_DECIMALS = {"BTC": 4, "ETH": 4, "SOL": 2}


class LighterClient:
    """
    Wrapper async sobre el SDK de Lighter.xyz.

    Interfaz compatible con HLClient para que LighterSmartExecutor
    pueda usarlo como drop-in replacement.
    """

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self._signer = None
        self._api = None
        self._account_api = None
        self._order_api = None
        self._account_index = None

        if not dry_run and config.LIGHTER_API_KEY:
            try:
                import lighter

                url = (
                    "https://mainnet.zklighter.elliot.ai"
                    if config.LIGHTER_MAINNET
                    else "https://testnet.zklighter.elliot.ai"
                )

                # SignerClient para ordenes
                self._signer = lighter.SignerClient(
                    url=url,
                    api_private_keys={0: config.LIGHTER_API_KEY},
                    account_index=0,
                )

                # ApiClient para lectura
                self._api = lighter.ApiClient(configuration=lighter.Configuration(host=url))
                self._account_api = lighter.AccountApi(self._api)
                self._order_api = lighter.OrderApi(self._api)

                logger.info(
                    "Lighter conectado — %s | wallet: %s",
                    "MAINNET" if config.LIGHTER_MAINNET else "TESTNET",
                    config.LIGHTER_WALLET_ADDRESS[:8] + "..." if config.LIGHTER_WALLET_ADDRESS else "n/a",
                )
            except ImportError:
                logger.warning("lighter-sdk no instalado — activando DRY RUN")
                self.dry_run = True
            except Exception as exc:
                logger.error("Error inicializando Lighter SDK: %s — activando DRY RUN", exc)
                self.dry_run = True
        else:
            self.dry_run = True
            logger.info("DRY RUN activo — no se enviaran ordenes a Lighter")

    def _sz_dec(self, coin: str) -> int:
        return SZ_DECIMALS.get(coin, 4)

    def _to_px(self, coin: str, price: float) -> int:
        """Convierte precio float a entero escalado de Lighter."""
        return int(price * PX_SCALE.get(coin, 100))

    def _to_sz(self, coin: str, size: float) -> int:
        """Convierte tamano float a entero escalado de Lighter."""
        return int(size * SZ_SCALE.get(coin, 10000))

    def _market_idx(self, coin: str) -> int:
        return MARKET_INDEX.get(coin, 0)

    # ── Datos de mercado ───────────────────────────────────────

    async def get_mid_price(self, coin: str) -> float:
        if self.dry_run:
            return 98000.0 if coin == "BTC" else 3000.0
        try:
            details = await self._order_api.order_book_details(
                market_id=self._market_idx(coin),
            )
            return float(details.last_trade_price)
        except Exception as exc:
            logger.error("get_mid_price failed: %s", exc)
            return 0.0

    async def get_balance(self) -> float:
        if self.dry_run:
            return 100_000.0
        try:
            account = await self._account_api.account(
                by="l1_address", value=config.LIGHTER_WALLET_ADDRESS,
            )
            return float(account.available_balance)
        except Exception as exc:
            logger.error("get_balance failed: %s", exc)
            return 0.0

    async def get_position(self, coin: str) -> dict | None:
        if self.dry_run:
            return None
        try:
            account = await self._account_api.account(
                by="l1_address", value=config.LIGHTER_WALLET_ADDRESS,
            )
            # Lighter devuelve posiciones dentro del account object
            # Necesitamos buscar la posicion del market_index correspondiente
            positions = None
            if hasattr(account, "positions"):
                positions = account.positions
            elif isinstance(account, dict):
                positions = account.get("positions")

            if positions:
                mkt_idx = self._market_idx(coin)
                for pos in positions:
                    # Soportar tanto dict (pos.get()) como object (getattr())
                    if isinstance(pos, dict):
                        mi = int(pos.get("market_index", -1))
                        sz = float(pos.get("size", 0))
                        entry_px = pos.get("entry_price", "0")
                    else:
                        mi = int(getattr(pos, "market_index", -1))
                        sz = float(getattr(pos, "size", 0))
                        entry_px = getattr(pos, "entry_price", "0")
                    if mi == mkt_idx and sz != 0:
                        return {
                            "coin": coin,
                            "szi": str(sz),
                            "entryPx": str(entry_px),
                        }
            return None
        except Exception as exc:
            logger.error("get_position failed: %s", exc)
            return None

    async def get_open_orders(self, coin: str | None = None) -> list:
        """Stub — Lighter SDK lacks a direct open-orders endpoint.
        Not used in the main flow; fill detection uses position-size comparison instead."""
        return []

    # ── Ordenes ────────────────────────────────────────────────

    async def limit_order(self, coin: str, is_buy: bool, sz: float, px: float) -> dict:
        sz_r = round(sz, self._sz_dec(coin))
        tag = "BUY" if is_buy else "SELL"
        logger.info("  LIMIT %s %s x%s @ $%.2f", tag, coin, sz_r, px)
        if self.dry_run:
            oid = int(time.time() * 1000)
            return {"status": "ok", "oid": oid}
        try:
            api_key_index, nonce = self._signer.nonce_manager.next_nonce()
            tx, tx_hash, err = await self._signer.create_order(
                market_index=self._market_idx(coin),
                client_order_index=int(time.time() * 1000) % 2**31,
                base_amount=self._to_sz(coin, sz_r),
                price=self._to_px(coin, px),
                is_ask=not is_buy,
                order_type=self._signer.ORDER_TYPE_LIMIT,
                time_in_force=self._signer.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=False,
                trigger_price=0,
                nonce=nonce,
                api_key_index=api_key_index,
            )
            if err:
                logger.error("Lighter limit_order error: %s", err)
                return {"status": "error", "error": str(err)}
            return {"status": "ok", "tx_hash": tx_hash, "oid": int(time.time() * 1000)}
        except Exception as exc:
            logger.error("Lighter limit_order exception: %s", exc)
            return {"status": "error", "error": str(exc)}

    async def market_order(self, coin: str, is_buy: bool, sz: float) -> dict:
        sz_r = round(sz, self._sz_dec(coin))
        tag = "BUY" if is_buy else "SELL"
        logger.info("  MARKET %s %s x%s", tag, coin, sz_r)
        if self.dry_run:
            oid = int(time.time() * 1000)
            return {"status": "ok", "oid": oid, "avgPx": "98000", "totalSz": str(sz_r)}
        try:
            mid = await self.get_mid_price(coin)
            # Slippage protection: 1% from mid
            max_px = mid * (0.99 if not is_buy else 1.01)
            tx, tx_hash, err = await self._signer.create_market_order(
                market_index=self._market_idx(coin),
                client_order_index=int(time.time() * 1000) % 2**31,
                base_amount=self._to_sz(coin, sz_r),
                avg_execution_price=self._to_px(coin, max_px),
                is_ask=not is_buy,
            )
            if err:
                logger.error("Lighter market_order error: %s", err)
                return {"status": "error", "error": str(err)}
            return {"status": "ok", "tx_hash": tx_hash, "oid": int(time.time() * 1000)}
        except Exception as exc:
            logger.error("Lighter market_order exception: %s", exc)
            return {"status": "error", "error": str(exc)}

    async def trigger_order(
        self, coin: str, is_buy: bool, sz: float, trigger_px: float, tpsl: str,
    ) -> dict:
        """Coloca SL o TP trigger order."""
        sz_r = round(sz, self._sz_dec(coin))
        label = "SL" if tpsl == "sl" else "TP"
        logger.info("  TRIGGER %s %s x%s @ $%.2f", label, coin, sz_r, trigger_px)
        if self.dry_run:
            oid = int(time.time() * 1000) + 1
            return {"status": "ok", "oid": oid}
        try:
            api_key_index, nonce = self._signer.nonce_manager.next_nonce()
            if tpsl == "sl":
                tx, tx_hash, err = await self._signer.create_sl_order(
                    market_index=self._market_idx(coin),
                    client_order_index=int(time.time() * 1000) % 2**31,
                    base_amount=self._to_sz(coin, sz_r),
                    trigger_price=self._to_px(coin, trigger_px),
                    price=self._to_px(coin, trigger_px),
                    is_ask=not is_buy,
                    reduce_only=True,
                    nonce=nonce,
                    api_key_index=api_key_index,
                )
            else:
                tx, tx_hash, err = await self._signer.create_tp_order(
                    market_index=self._market_idx(coin),
                    client_order_index=int(time.time() * 1000) % 2**31,
                    base_amount=self._to_sz(coin, sz_r),
                    trigger_price=self._to_px(coin, trigger_px),
                    price=self._to_px(coin, trigger_px),
                    is_ask=not is_buy,
                    reduce_only=True,
                    nonce=nonce,
                    api_key_index=api_key_index,
                )
            if err:
                logger.error("Lighter trigger_order error: %s", err)
                return {"status": "error", "error": str(err)}
            return {"status": "ok", "tx_hash": tx_hash, "oid": int(time.time() * 1000)}
        except Exception as exc:
            logger.error("Lighter trigger_order exception: %s", exc)
            return {"status": "error", "error": str(exc)}

    async def cancel_order(self, coin: str, oid: int) -> None:
        logger.info("  CANCEL order %s oid=%d", coin, oid)
        if self.dry_run:
            return
        try:
            api_key_index, nonce = self._signer.nonce_manager.next_nonce()
            await self._signer.cancel_order(
                market_index=self._market_idx(coin),
                order_index=oid,
                nonce=nonce,
                api_key_index=api_key_index,
            )
        except Exception as exc:
            logger.warning("  Cancel failed: %s", exc)

    async def market_close(self, coin: str) -> dict:
        """Cierra toda la posicion a mercado."""
        logger.info("  MARKET CLOSE %s (full position)", coin)
        if self.dry_run:
            return {"status": "ok"}
        pos = await self.get_position(coin)
        if pos is None:
            return {"status": "ok", "msg": "no position"}
        sz = abs(float(pos.get("szi", 0)))
        if sz == 0:
            return {"status": "ok", "msg": "zero size"}
        is_long = float(pos.get("szi", 0)) > 0
        return await self.market_order(coin, is_buy=not is_long, sz=sz)

    async def set_leverage(self, coin: str, leverage: int) -> None:
        logger.info("  SET LEVERAGE %s x%d", coin, leverage)
        if self.dry_run:
            return
        try:
            api_key_index, nonce = self._signer.nonce_manager.next_nonce()
            await self._signer.update_leverage(
                market_index=self._market_idx(coin),
                margin_mode=0,  # CROSS
                leverage=leverage,
                nonce=nonce,
                api_key_index=api_key_index,
            )
        except Exception as exc:
            logger.warning("  Leverage update failed: %s", exc)

    async def close(self) -> None:
        """Cierra las conexiones del SDK."""
        if self._signer:
            try:
                await self._signer.close()
            except Exception:
                pass
        if self._api:
            try:
                await self._api.close()
            except Exception:
                pass
