"""
lighter_client.py — Wrapper async sobre el SDK de Lighter.xyz DEX.

VERSION FINAL: Sincronizada para Lote 4 con SL optimizado.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
import sys
import asyncio

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import config  # noqa: E402

logger = logging.getLogger("lighter_client")

PX_SCALE = {"BTC": 10, "ETH": 10, "SOL": 10}
SZ_SCALE = {"BTC": 100000, "ETH": 100000, "SOL": 100}


class LighterClient:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self._api = None
        self._account_api = None
        self._order_api = None
        self._pk = None

        if not dry_run:
            try:
                import lighter
                from vault import load_secrets
                secrets = load_secrets()
                self._pk = secrets.get("LIGHTER_PRIVATE_KEY") or secrets.get("HYPERLIQUID_PRIVATE_KEY")
                
                url = "https://mainnet.zklighter.elliot.ai"
                self._api = lighter.ApiClient(lighter.Configuration(host=url))
                self._account_api = lighter.AccountApi(self._api)
                self._order_api = lighter.OrderApi(self._api)
                logger.info("Lighter conectado — Account: %s", config.LIGHTER_ACCOUNT_INDEX)
            except Exception as exc:
                logger.error("Error API: %s", exc)
                self.dry_run = True

    async def get_signer(self):
        import lighter
        return lighter.SignerClient(
            url="https://mainnet.zklighter.elliot.ai",
            api_private_keys={2: self._pk},
            account_index=config.LIGHTER_ACCOUNT_INDEX
        )

    def _to_sz(self, coin: str) -> int:
        # BTC market uses 10^5 decimals
        return 100000

    def _to_px(self, coin: str) -> int:
        # BTC market uses 10^1 decimals
        return 10

    async def get_position(self, coin: str) -> dict | None:
        try:
            resp = await self._account_api.account(by="index", value=str(config.LIGHTER_ACCOUNT_INDEX))
            for pos in resp.accounts[0].positions:
                if int(pos.market_id) == 1 and abs(float(pos.position)) > 1e-8:
                    return {"coin": coin, "szi": float(pos.position), "entryPx": float(pos.avg_entry_price)}
            return None
        except Exception: return None

    async def get_open_orders(self, signer) -> list:
        try:
            auth_data = signer.create_auth_token_with_expiry(deadline=600, api_key_index=2)
            resp = await self._order_api.account_active_orders(config.LIGHTER_ACCOUNT_INDEX, 1, auth=auth_data[0])
            return getattr(resp, "orders", [])
        except Exception: return []

    async def cancel_all_orders(self, signer, nonce_start: int) -> int:
        orders = await self.get_open_orders(signer)
        curr = nonce_start
        if not orders: return curr
        logger.info("  [Limpieza] Borrando %d ordenes...", len(orders))
        for o in orders:
            oid = getattr(o, 'order_index', None) or getattr(o, 'index', None)
            if oid:
                _, nonce = signer.nonce_manager.next_nonce()
                await signer.cancel_order(1, int(oid), nonce, 2)
                curr = nonce + 1
                await asyncio.sleep(0.5)
        await asyncio.sleep(2.0)
        return curr

    async def market_order(self, signer, coin: str, is_buy: bool, sz: float, nonce: int) -> dict:
        try:
            details = await self._order_api.order_book_details(market_id=1)
            mid = float(details.order_book_details[0].last_trade_price)
            max_px = mid * (1.10 if is_buy else 0.90)
            sz_int = int(round(sz * 100000, 0))
            px_int = int(round(max_px * 10, 0))
            _, nonce_real = signer.nonce_manager.next_nonce()
            tx, tx_hash, err = await signer.create_market_order(1, 0, sz_int, px_int, not is_buy, False, nonce_real, 2)
            if err: return {"status": "error", "error": str(err)}
            return {"status": "ok", "tx_hash": tx_hash}
        except Exception as e: return {"status": "error", "error": str(e)}

    async def trigger_order(self, signer, coin: str, is_buy: bool, sz: float, trigger_px: float, tpsl: str, idx: int) -> dict:
        try:
            px_int = int(round(trigger_px * 10, 0))
            sz_int = int(round(sz * 100000, 0))
            _, nonce_real = signer.nonce_manager.next_nonce()
            method = signer.create_sl_order if tpsl == "sl" else signer.create_tp_order
            tx, tx_hash, err = await method(1, idx, sz_int, px_int, px_int, is_buy, True, nonce_real, 2)
            if err: return {"status": "error", "error": str(err)}
            return {"status": "ok", "tx_hash": tx_hash}
        except Exception as e: return {"status": "error", "error": str(e)}

    async def set_leverage(self, signer, coin: str, leverage: int) -> None:
        try:
            _, nonce = signer.nonce_manager.next_nonce()
            await signer.update_leverage(1, 0, leverage, nonce, 2)
        except Exception: pass

    async def close(self) -> None:
        if self._api: await self._api.close()
