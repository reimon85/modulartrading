"""
notifier.py — Singleton TelegramNotifier for categorised alerts with rate-limiting.

If TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID are empty the notifier is a silent
no-op (zero overhead).

Usage (async):
    notifier = TelegramNotifier()
    await notifier.connect()
    await notifier.notify(AlertLevel.TRADE, "OPEN LONG BTC", "details...", source="Hyperliquid")
    await notifier.disconnect()

Usage (sync, e.g. from launcher signal handler):
    from src.notifier import notify_sync
    notify_sync(AlertLevel.CRITICAL, "System shutting down", source="Launcher")
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from enum import Enum

import aiohttp

from src import config

logger = logging.getLogger("notifier")


class AlertLevel(Enum):
    TRADE = "TRADE"
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"


# Emoji per level
_EMOJI = {
    AlertLevel.TRADE: "\U0001f4b0",      # money bag
    AlertLevel.CRITICAL: "\U0001f6a8",    # rotating light
    AlertLevel.WARNING: "\u26a0\ufe0f",   # warning sign
    AlertLevel.INFO: "\u2139\ufe0f",      # info
}

# Sliding window limits: max messages per 60 seconds
_RATE_LIMITS: dict[AlertLevel, int] = {
    AlertLevel.TRADE: 20,
    AlertLevel.CRITICAL: 10,
    AlertLevel.WARNING: 5,
    AlertLevel.INFO: 3,
}


class TelegramNotifier:
    """Singleton Telegram alerter with per-category rate-limiting."""

    _instance: TelegramNotifier | None = None

    def __new__(cls) -> TelegramNotifier:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialised = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialised:
            return
        self._initialised = True

        self._token = config.TELEGRAM_BOT_TOKEN
        self._chat_id = config.TELEGRAM_CHAT_ID
        self._enabled = bool(self._token and self._chat_id)
        self._session: aiohttp.ClientSession | None = None

        # Sliding windows for rate limiting: deque of timestamps per level
        self._windows: dict[AlertLevel, deque[float]] = {
            level: deque() for level in AlertLevel
        }

        if not self._enabled:
            logger.debug("TelegramNotifier disabled (no token/chat_id)")

    # ── Lifecycle ──────────────────────────────────────────────

    async def connect(self) -> None:
        if not self._enabled:
            return
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
            )

    async def disconnect(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ── Rate limiting ──────────────────────────────────────────

    def _check_rate(self, level: AlertLevel) -> bool:
        """Return True if the message is allowed under the sliding window."""
        now = time.monotonic()
        window = self._windows[level]
        # Purge entries older than 60s
        while window and window[0] < now - 60:
            window.popleft()
        if len(window) >= _RATE_LIMITS[level]:
            return False
        window.append(now)
        return True

    # ── Core send ──────────────────────────────────────────────

    async def _send(self, text: str) -> None:
        if self._session is None or self._session.closed:
            await self.connect()
        if self._session is None:
            return
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": "HTML",
        }
        try:
            async with self._session.post(url, json=payload) as r:
                if r.status != 200:
                    body = await r.text()
                    logger.warning("Telegram %d: %s", r.status, body[:200])
        except Exception as exc:
            logger.warning("Telegram send failed: %s", exc)

    # ── Public API ─────────────────────────────────────────────

    async def notify(
        self,
        level: AlertLevel,
        title: str,
        details: str = "",
        source: str = "",
    ) -> None:
        """Send a categorised alert. Respects rate-limit per level."""
        if not self._enabled:
            return
        if not self._check_rate(level):
            logger.debug("Rate-limited %s alert: %s", level.value, title)
            return

        emoji = _EMOJI[level]
        header = f"{emoji} <b>{level.value}</b>"
        if source:
            header += f" | {source}"
        text = f"{header}\n{title}"
        if details:
            text += f"\n<pre>{details}</pre>"

        await self._send(text)

    async def notify_trade(
        self,
        action_label: str,
        coin: str,
        price: float,
        size: float,
        sl_price: float = 0,
        tp_type: str = "",
        tp_value: float = 0,
        source: str = "",
    ) -> None:
        """Rich notification for trade open / re-entry."""
        title = f"{action_label} {coin} @ ${price:,.2f}"
        lines = [f"  Size: {size:.6f} {coin}"]
        if sl_price:
            tp_str = ""
            if tp_type == "PRICE" and tp_value:
                tp_str = f" | TP: PRICE=${tp_value:,.2f}"
            elif tp_type == "TIME" and tp_value:
                tp_str = f" | TP: TIME={tp_value:.0f}min"
            lines.append(f"  SL: ${sl_price:,.2f}{tp_str}")
        await self.notify(AlertLevel.TRADE, title, "\n".join(lines), source=source)

    async def notify_pnl(
        self,
        coin: str,
        entry_price: float,
        exit_price: float,
        pnl_pct: float,
        reason: str,
        source: str = "",
    ) -> None:
        """Notification for trade close with PnL."""
        emoji = "\u2705" if pnl_pct >= 0 else "\u274c"
        title = f"{emoji} CLOSED {coin} | PnL: {pnl_pct:+.2f}%"
        details = (
            f"  Entry: ${entry_price:,.2f} -> Exit: ${exit_price:,.2f}\n"
            f"  Reason: {reason}"
        )
        await self.notify(AlertLevel.TRADE, title, details, source=source)


# ── Sync helper (for non-async contexts) ─────────────────────

def notify_sync(
    level: AlertLevel,
    title: str,
    details: str = "",
    source: str = "",
) -> None:
    """Fire-and-forget Telegram alert from synchronous code."""
    notifier = TelegramNotifier()
    if not notifier._enabled:
        return
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(notifier.notify(level, title, details, source))
    except RuntimeError:
        # No running loop — create a one-shot one
        asyncio.run(notifier.notify(level, title, details, source))
