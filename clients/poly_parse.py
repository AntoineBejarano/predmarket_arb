"""Parsing robusto de campos JSON de APIs Polymarket (Gamma / CLOB)."""

from __future__ import annotations

import json
from typing import Any, Optional, Tuple


def parse_json_maybe(val: Any) -> Any:
    """Si ``val`` es str JSON (p. ej. Gamma ``clobTokenIds``), parsea; si no, devuelve tal cual."""
    if val is None:
        return None
    if isinstance(val, (list, dict)):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return None
    return val


def api_bool_true(v: Any) -> bool:
    """True solo para valores claramente afirmativos (evita truthiness de strings raros)."""
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes", "y")
    return False


def clob_market_tradeable(m: dict[str, Any]) -> Tuple[bool, str]:
    """
    Comprueba flags CLOB/Gamma-style (snake_case o camelCase).
    Devuelve (ok, razón si no ok).
    """
    acc = m.get("accepting_orders")
    if acc is None:
        acc = m.get("acceptingOrders")
    if not api_bool_true(acc):
        return False, "not_accepting"

    closed = m.get("closed")
    if closed is None:
        closed = m.get("isClosed")
    if api_bool_true(closed):
        return False, "closed"

    eob = m.get("enable_order_book")
    if eob is None:
        eob = m.get("enableOrderBook")
    if not api_bool_true(eob):
        return False, "no_orderbook"

    return True, ""


def gamma_market_token_ids(m: dict[str, Any]) -> list[str]:
    """Token IDs CLOB desde payload Gamma (``clobTokenIds`` JSON o lista)."""
    raw = m.get("clobTokenIds") or m.get("clob_token_ids")
    parsed = parse_json_maybe(raw)
    if not isinstance(parsed, (list, tuple)):
        return []
    out: list[str] = []
    for x in parsed:
        sx = str(x).strip()
        if sx:
            out.append(sx)
    return out


def gamma_condition_id(m: dict[str, Any]) -> Optional[str]:
    cid = m.get("conditionId") or m.get("condition_id")
    if cid is None:
        return None
    s = str(cid).strip()
    return s or None
