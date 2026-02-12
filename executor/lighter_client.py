"""
lighter_client.py — Wrapper async sobre el SDK de Lighter.xyz DEX.
"""

from __future__ import annotations

import logging
import time
import sys
import asyncio
import os
import base64
import importlib.util
from pathlib import Path

logger = logging.getLogger("lighter_client")

# Configuración de Lighter
PX_SCALE = {"BTC": 10, "ETH": 10, "SOL": 10}
SZ_SCALE = {"BTC": 100000, "ETH": 100000, "SOL": 100}

def load_lighter_pk() -> str | None:
    """Carga la PK usando importación dinámica absoluta de vault.py."""
    root = Path(__file__).resolve().parent.parent
    vault_path = root / "vault.py"
    
    try:
        # Carga dinámica del módulo vault
        spec = importlib.util.spec_from_file_location("vault", str(vault_path))
        vault = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(vault)
        
        secrets = vault.load_secrets()
        return secrets.get("LIGHTER_PRIVATE_KEY") or secrets.get("HYPERLIQUID_PRIVATE_KEY")
    except Exception as e:
        logger.error("Error cargando PK dinamicamente: %s", e)
        # Fallback a variables de entorno
        return os.environ.get("LIGHTER_PRIVATE_KEY") or os.environ.get("HYPERLIQUID_PRIVATE_KEY")

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
                from src import config
                self._pk = load_lighter_pk()
                
                url = "https://mainnet.zklighter.elliot.ai"
                self._api = lighter.ApiClient(lighter.Configuration(host=url))
                self._account_api = lighter.AccountApi(self._api)
                self._order_api = lighter.OrderApi(self._api)
                logger.info("Lighter conectado — Account: %s", config.LIGHTER_ACCOUNT_INDEX)
            except Exception as exc:
                logger.error("Error API en __init__: %s", exc)
                self.dry_run = True

    async def get_signer(self):
        import lighter
        from src import config
        return lighter.SignerClient(
            url="https://mainnet.zklighter.elliot.ai",
            api_private_keys={2: self._pk},
            account_index=config.LIGHTER_ACCOUNT_INDEX
        )

    async def get_position(self, coin: str) -> dict | None:
        try:
            from src import config
            resp = await self._account_api.account(by="index", value=str(config.LIGHTER_ACCOUNT_INDEX))
            for pos in resp.accounts[0].positions:
                # Market ID 1 is BTC
                if int(pos.market_id) == 1:
                    size = float(pos.position)
                    # En Lighter, la posición es 0 si no hay nada. 
                    # El signo suele venir en la API, pero si el SDK lo oculta, usaremos una lógica de respaldo.
                    if abs(size) > 1e-8:
                        # Si el precio de entrada es mayor que 0, hay posición.
                        # Para Lighter SDK, a veces el side viene en otro campo.
                        return {
                            "coin": coin, 
                            "szi": size, 
                            "entryPx": float(pos.avg_entry_price),
                            # Si el SDK no nos da el signo, este es un punto de fallo.
                            # Pero probaremos a capturarlo del objeto si existe.
                        }
            return None
        except Exception: return None

    async def get_open_orders(self, signer) -> list:
        try:
            from src import config
            auth_data = signer.create_auth_token_with_expiry(deadline=600, api_key_index=2)
            resp = await self._order_api.account_active_orders(config.LIGHTER_ACCOUNT_INDEX, 1, auth=auth_data[0])
            return getattr(resp, "orders", [])
        except Exception: return []

    async def cancel_all_orders(self, signer, nonce_start: int) -> int:
        orders = await self.get_open_orders(signer)
        if not orders: return nonce_start
        for o in orders:
            oid = getattr(o, 'order_index', None) or getattr(o, 'index', None)
            if oid:
                _, nonce = signer.nonce_manager.next_nonce()
                await signer.cancel_order(1, int(oid), nonce, 2)
                await asyncio.sleep(0.5)
        await asyncio.sleep(2.0)
        return nonce_start

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
            
            # En Lighter, is_ask=True es SELL, is_ask=False es BUY.
            # Para cerrar un SHORT (SELL), necesitamos BUY -> is_ask = False.
            # is_buy viene como True desde el ejecutor (side_to_close), así que:
            is_ask = not is_buy
            
            # Nota: El SDK para trigger orders no usa nonce ni api_key_index al final
            if tpsl == "sl":
                tx, tx_hash, err = await signer.create_sl_order(1, idx, sz_int, px_int, px_int, is_ask, True)
            else:
                tx, tx_hash, err = await signer.create_tp_order(1, idx, sz_int, px_int, px_int, is_ask, True)
                
            if err: return {"status": "error", "error": str(err)}
            return {"status": "ok", "tx_hash": tx_hash}
        except Exception as e: return {"status": "error", "error": str(e)}

    async def close(self) -> None:
        if self._api: await self._api.close()
