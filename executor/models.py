"""Modelos y utilidades compartidas entre executors."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field


@dataclass
class PositionState:
    """Estado de una posición activa, persistido en Redis."""
    coin: str
    action: str              # BUY / SELL
    entry_price: float
    size: float
    sl_price: float
    sl_oid: int = 0
    tp_type: str = "PRICE"   # PRICE / TIME
    tp_value: float = 0.0    # precio o minutos
    tp_oid: int = 0
    tp_target_ts: int = 0    # epoch ms (solo para TIME)
    entry_ts: int = 0        # epoch ms
    status: str = "OPEN"
    entries: list = field(default_factory=list)   # [{"price": float, "size": float, "ts": int}]
    max_entries: int = 4
    entry_size: float = 1.0
    trade_id: int = 0

    def to_redis(self) -> dict[str, str]:
        d = {k: str(v) for k, v in asdict(self).items() if k != "entries"}
        d["entries"] = json.dumps(self.entries)
        return d

    @classmethod
    def from_redis(cls, data: dict[str, str]) -> PositionState:
        entries_raw = data.get("entries", "[]")
        try:
            entries = json.loads(entries_raw)
        except (json.JSONDecodeError, TypeError):
            entries = []
        return cls(
            coin=data["coin"], action=data["action"],
            entry_price=float(data["entry_price"]),
            size=float(data["size"]),
            sl_price=float(data["sl_price"]),
            sl_oid=int(data.get("sl_oid", 0) or 0),
            tp_type=data.get("tp_type", "PRICE"),
            tp_value=float(data.get("tp_value", 0) or 0),
            tp_oid=int(data.get("tp_oid", 0) or 0),
            tp_target_ts=int(data.get("tp_target_ts", 0) or 0),
            entry_ts=int(data.get("entry_ts", 0) or 0),
            status=data.get("status", "OPEN"),
            entries=entries,
            max_entries=int(data.get("max_entries", 4) or 4),
            entry_size=float(data.get("entry_size", 1.0) or 1.0),
            trade_id=int(data.get("trade_id", 0) or 0),
        )


def validate_signal(signal: dict) -> str | None:
    """Valida campos obligatorios de una señal. Retorna mensaje de error o None si OK."""
    action = signal.get("action", "").upper()
    if action == "CLOSE":
        if "pair" not in signal or "/" not in signal.get("pair", ""):
            return "CLOSE signal sin 'pair' válido (esperado 'COIN/QUOTE')"
        return None

    if action not in ("BUY", "SELL"):
        return f"action desconocida: {signal.get('action')!r}"

    pair = signal.get("pair", "")
    if "/" not in pair:
        return f"'pair' ausente o malformado: {pair!r} (esperado 'COIN/QUOTE')"

    if "sl_price" not in signal:
        return "'sl_price' ausente en señal"
    try:
        sl = float(signal["sl_price"])
        if sl <= 0:
            return f"sl_price debe ser > 0, recibido: {sl}"
    except (ValueError, TypeError):
        return f"sl_price no es numérico: {signal['sl_price']!r}"

    size = signal.get("size")
    if size is not None:
        try:
            if float(size) <= 0:
                return f"size debe ser > 0, recibido: {size}"
        except (ValueError, TypeError):
            return f"size no es numérico: {size!r}"

    return None
