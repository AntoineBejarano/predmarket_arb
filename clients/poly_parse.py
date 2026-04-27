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


def parse_json_list_maybe(val: Any) -> tuple[Optional[list[Any]], Optional[str]]:
    """
    Parsea una lista que puede venir como lista real o string JSON.
    Devuelve (lista|None, error|None) con motivo explícito.
    """
    if val is None:
        return None, "missing"
    if isinstance(val, tuple):
        return list(val), None
    if isinstance(val, list):
        return val, None
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None, "empty_string"
        try:
            parsed = json.loads(s)
        except json.JSONDecodeError:
            return None, "malformed_json_list"
        if not isinstance(parsed, list):
            return None, "json_not_list"
        return parsed, None
    return None, "not_list"


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


def _normalize_outcome_label(item: Any) -> str:
    if isinstance(item, dict):
        for k in ("name", "label", "value", "outcome"):
            v = item.get(k)
            if v is not None:
                return str(v).strip()
        return ""
    return str(item).strip()


def extract_yes_token_id(
    outcomes: Any,
    clob_token_ids: Any,
    *,
    assume_first: bool = False,
) -> tuple[Optional[str], str, Optional[str]]:
    """
    Extrae token YES alineando outcomes con clobTokenIds.
    Retorna (token_id|None, yes_token_source, reject_reason|None).
    """
    parsed_outcomes, out_err = parse_json_list_maybe(outcomes)
    if parsed_outcomes is None:
        return None, "unknown", "malformed_outcomes" if out_err != "missing" else "missing_outcomes"
    parsed_tokens, tok_err = parse_json_list_maybe(clob_token_ids)
    if parsed_tokens is None:
        return None, "unknown", "malformed_clob_token_ids" if tok_err != "missing" else "missing_clob_token_ids"
    if len(parsed_outcomes) != len(parsed_tokens):
        return None, "unknown", "length_mismatch"
    for idx, item in enumerate(parsed_outcomes):
        if _normalize_outcome_label(item).lower() == "yes":
            tid = str(parsed_tokens[idx]).strip()
            if tid:
                return tid, "explicit_yes_outcome", None
            return None, "unknown", "empty_yes_token_id"
    if assume_first and parsed_tokens:
        tid0 = str(parsed_tokens[0]).strip()
        if tid0:
            return tid0, "assumed_first_token", None
    return None, "unknown", "no_yes_outcome"


def gamma_yes_token_id(m: dict[str, Any]) -> Optional[str]:
    """
    Token CLOB del lado **Yes** para un market binario Gamma.
    Alinea ``outcomes[i]`` con ``clobTokenIds[i]`` (misma longitud).
    """
    raw_tok = m.get("clobTokenIds") or m.get("clob_token_ids")
    yid, _source, _reason = extract_yes_token_id(
        m.get("outcomes"),
        raw_tok,
        assume_first=False,
    )
    return yid


def gamma_market_child_discoverable(m: dict[str, Any]) -> Tuple[bool, str]:
    """
    Hijo en ``event.markets`` (Gamma keyset) para **armar candidatos**.

    Los objetos anidados a menudo **no** traen ``acceptingOrders`` / ``enableOrderBook``;
    si faltan, no se rechaza aquí (la revalidación CLOB en ``get_market`` sigue siendo obligatoria).
    Si vienen explícitamente en falso, sí se filtra.
    """
    if not api_bool_true(m.get("active")):
        return False, "inactive"
    if api_bool_true(m.get("closed")) or api_bool_true(m.get("isClosed")):
        return False, "closed"
    if api_bool_true(m.get("archived")):
        return False, "archived"
    if api_bool_true(m.get("restricted")):
        return False, "restricted"
    acc = m.get("acceptingOrders")
    if acc is None:
        acc = m.get("accepting_orders")
    if acc is not None and not api_bool_true(acc):
        return False, "not_accepting"
    eob = m.get("enableOrderBook")
    if eob is None:
        eob = m.get("enable_order_book")
    if eob is not None and not api_bool_true(eob):
        return False, "no_orderbook"
    if not gamma_market_token_ids(m):
        return False, "no_clob_tokens"
    if gamma_yes_token_id(m) is None:
        return False, "no_yes_token"
    return True, ""


def gamma_market_child_eligible(m: dict[str, Any]) -> Tuple[bool, str]:
    """Child estricto: Gamma + flags CLOB (p. ej. tras ``get_market``)."""
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
    parsed, _err = parse_json_list_maybe(raw)
    if not isinstance(parsed, list):
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
