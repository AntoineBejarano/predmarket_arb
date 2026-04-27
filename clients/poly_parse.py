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


def parse_outcomes_list(m: dict[str, Any]) -> list[str]:
    """Lista de etiquetas de outcome (Gamma), p. ej. ``[\"Yes\", \"No\"]``."""
    raw = m.get("outcomes")
    parsed = parse_json_maybe(raw)
    if not isinstance(parsed, (list, tuple)):
        return []
    return [str(x).strip() for x in parsed]


def gamma_yes_token_id(m: dict[str, Any]) -> Optional[str]:
    """
    Token CLOB del lado **Yes** para un market binario Gamma.
    Alinea ``outcomes[i]`` con ``clobTokenIds[i]`` (misma longitud).
    """
    outcomes = parse_outcomes_list(m)
    raw_tok = m.get("clobTokenIds") or m.get("clob_token_ids")
    tokens = parse_json_maybe(raw_tok)
    if not isinstance(tokens, (list, tuple)) or len(outcomes) != len(tokens):
        return None
    for i, lab in enumerate(outcomes):
        if str(lab).strip().lower() == "yes":
            tid = str(tokens[i]).strip()
            return tid or None
    return None


def gamma_market_child_eligible(m: dict[str, Any]) -> Tuple[bool, str]:
    """Child market dentro de un event Gamma: activo, tradable CLOB, con token YES."""
    if not api_bool_true(m.get("active")):
        return False, "inactive"
    if api_bool_true(m.get("closed")) or api_bool_true(m.get("isClosed")):
        return False, "closed"
    if api_bool_true(m.get("archived")):
        return False, "archived"
    if api_bool_true(m.get("restricted")):
        return False, "restricted"
    ok, reason = clob_market_tradeable(m)
    if not ok:
        return False, reason
    if not gamma_market_token_ids(m):
        return False, "no_clob_tokens"
    if gamma_yes_token_id(m) is None:
        return False, "no_yes_token"
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
