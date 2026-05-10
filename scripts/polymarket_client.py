"""
Cliente singleton de cuenta Polymarket (CLOB) vía ``py-clob-client``.

Credenciales L2 **solo** desde variables de entorno:

- ``POLY_API_KEY``, ``POLY_API_SECRET``, ``POLY_PASSPHRASE``, ``POLY_PRIVATE_KEY``

(p. ej. ``.env`` / Railway). ``<DATA_DIR>/polymarket_account.json`` **no se lee**;
para derivar el trio L2 usa ``scripts/derive_polymarket_clob_api_key.py`` y
copia los valores al entorno.

Nunca loguear ``api_secret`` ni ``private_key``.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import uuid
from pathlib import Path
from typing import Any, Optional

from lab.paths import data_dir

log = logging.getLogger("polymarket_client")

# Ruta legada (API/UI); el cliente no lee este archivo.
_ACCOUNT_PATH = data_dir() / "polymarket_account.json"

_LOCK = threading.RLock()
_CLOB_CLIENT: Any = None  # ClobClient instance

# Trading proxies keyed por dry_run de la estrategia (solo dos ramas).
_TRADING: dict[bool, "PolymarketTradingClient"] = {}


def _env_dry_run_global() -> bool:
    return os.getenv("DRY_RUN", "true").strip().lower() != "false"


def _effective_dry_run(strategy_dry_run: bool) -> bool:
    """Si el proceso va en DRY_RUN global, forzar simulación aunque el JSON de estrategia diga live."""
    return bool(_env_dry_run_global() or strategy_dry_run)


def _normalize_hex_key(key: str) -> str:
    s = (key or "").strip()
    if not s:
        return ""
    if not s.startswith("0x"):
        s = "0x" + s
    return s


def _clob_credential_bundle() -> dict[str, str]:
    """api_key / api_secret / api_passphrase / private_key solo desde POLY_* (env)."""
    pk_raw = os.getenv("POLY_PRIVATE_KEY", "").strip()
    return {
        "private_key": _normalize_hex_key(pk_raw),
        "api_key": os.getenv("POLY_API_KEY", "").strip(),
        "api_secret": os.getenv("POLY_API_SECRET", "").strip(),
        "api_passphrase": os.getenv("POLY_PASSPHRASE", "").strip(),
    }


def live_account_fingerprint() -> dict[str, Any]:
    """
    Huella no sensible de la cuenta CLOB para depuración en UI.
    Nunca expone secretos completos.
    """
    creds = _clob_credential_bundle()
    api_key = creds.get("api_key") or ""
    private_key = creds.get("private_key") or ""
    api_suffix = api_key[-8:] if len(api_key) >= 8 else api_key
    pk_suffix = private_key[-8:] if len(private_key) >= 8 else private_key
    out: dict[str, Any] = {
        "api_key_suffix": api_suffix or None,
        "private_key_suffix": pk_suffix or None,
        "funder": (os.getenv("POLY_FUNDER", "").strip() or None),
    }
    if private_key:
        try:
            from eth_account import Account

            out["signer_address"] = str(Account.from_key(private_key).address)
        except Exception:
            out["signer_address"] = None
    else:
        out["signer_address"] = None
    return out


def _get_clob_client() -> Any:
    """ClobClient L2 (singleton). Requiere POLY_* en env + deps instaladas."""
    global _CLOB_CLIENT
    with _LOCK:
        if _CLOB_CLIENT is not None:
            return _CLOB_CLIENT
        creds = _clob_credential_bundle()
        pk = creds["private_key"]
        api_key = creds["api_key"]
        api_secret = creds["api_secret"]
        api_pass = creds["api_passphrase"]
        if not pk or not api_key or not api_secret or not api_pass:
            raise RuntimeError(
                "Faltan credenciales CLOB L2: define POLY_API_KEY, POLY_API_SECRET, "
                "POLY_PASSPHRASE y POLY_PRIVATE_KEY en el entorno (.env / Railway). "
                "El archivo polymarket_account.json no se usa."
            )
        log.info("Credenciales CLOB desde variables POLY_* (env).")
        try:
            from py_clob_client_v2.client import ClobClient
            from py_clob_client_v2.clob_types import ApiCreds
        except ImportError:
            try:
                from py_clob_client.client import ClobClient  # type: ignore[assignment]
                from py_clob_client.clob_types import ApiCreds  # type: ignore[assignment]
                log.warning(
                    "py-clob-client-v2 no instalado — usando v1 (puede causar order_version_mismatch con sig_type=1)"
                )
            except ImportError as e:
                raise RuntimeError("Instala py-clob-client-v2 (pyproject/requirements)") from e

        host = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").rstrip("/")
        chain_id = int(os.getenv("POLYGON_CHAIN_ID", "137"))
        sig_type = int(os.getenv("POLY_SIGNATURE_TYPE", "0"))
        funder_raw = os.getenv("POLY_FUNDER", "").strip()
        funder = funder_raw or None

        creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_pass)
        _CLOB_CLIENT = ClobClient(host, chain_id, pk, creds, sig_type, funder)
        log.info(
            "Polymarket CLOB client inicializado (host=%s chain_id=%s)",
            host,
            chain_id,
        )
        return _CLOB_CLIENT


def _clob_token_id_list(m: dict[str, Any]) -> list[str]:
    raw = m.get("tokens") or m.get("clobTokenIds") or m.get("clob_token_ids")
    parsed = raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = []
    if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
        out: list[str] = []
        for t in parsed:
            tid = t.get("token_id") or t.get("tokenId") or t.get("id")
            if tid:
                out.append(str(tid).strip())
        return out
    from clients.poly_parse import parse_json_list_maybe

    lst, _ = parse_json_list_maybe(parsed)
    if not lst:
        return []
    return [str(x).strip() for x in lst if str(x).strip()]


def _resolve_token_id_for_side(market_id: str, side: str) -> str:
    """``market_id`` = condition_id CLOB; ``side`` YES|NO."""
    from clients.poly_parse import extract_yes_token_id

    client = _get_clob_client()
    m = client.get_market(str(market_id).strip())
    if not isinstance(m, dict):
        raise RuntimeError("get_market: respuesta inválida")
    outcomes = m.get("outcomes")
    token_ids = _clob_token_id_list(m) or m.get("clobTokenIds") or m.get("clob_token_ids")
    yid, _, rej = extract_yes_token_id(outcomes, token_ids, assume_first=True)
    s = str(side or "YES").upper().strip()
    if s == "YES":
        if not yid:
            raise RuntimeError(f"sin token YES: {rej}")
        return str(yid)
    if s == "NO":
        toks, terr = _parse_token_list(token_ids)
        if toks and yid:
            for tid in toks:
                if str(tid) != str(yid):
                    return str(tid)
        # fallback: outcomes alineados
        outs, _ = _parse_outcomes_list(outcomes)
        if toks and outs and len(toks) == len(outs):
            for o, tid in zip(outs, toks):
                ol = str(o).lower()
                if ol in ("no", "down", "n"):
                    return str(tid)
        raise RuntimeError(f"sin token NO ({terr or rej})")
    raise RuntimeError(f"side inválido: {side!r}")


def _parse_token_list(token_ids: Any) -> tuple[list[str], str]:
    from clients.poly_parse import parse_json_list_maybe

    lst, err = parse_json_list_maybe(token_ids)
    if lst is None:
        return [], err or "missing_tokens"
    out = [str(x).strip() for x in lst if str(x).strip()]
    return out, ""


def _parse_outcomes_list(outcomes: Any) -> tuple[list[str], str]:
    from clients.poly_parse import parse_json_list_maybe

    lst, err = parse_json_list_maybe(outcomes)
    if lst is None:
        return [], err or "missing_outcomes"
    return [str(x).strip() for x in lst], ""


class PolymarketTradingClient:
    """
    Envío de órdenes con flag dry_run por estrategia + anulación global ``DRY_RUN`` env.
    """

    def __init__(self, strategy_dry_run: bool) -> None:
        self._strategy_dry_run = bool(strategy_dry_run)

    def effective_dry_run(self) -> bool:
        return _effective_dry_run(self._strategy_dry_run)

    def place_order(
        self,
        market_id: str,
        side: str,
        amount_usdc: float,
        price: float,
        token_id: str = "",
    ) -> dict[str, Any]:
        """
        Coloca BUY en outcome YES/NO. ``amount_usdc`` = notional aproximado (como Kelly previo).
        """
        px = float(price)
        if px <= 0 or px >= 1.0:
            return {"success": False, "price": px, "order_id": None, "error": "invalid_price"}

        if self.effective_dry_run():
            oid = f"sim-{uuid.uuid4().hex[:12]}"
            log.info(
                "[DRY_RUN] place_order simulado market_id=%s side=%s amount_usdc=%.4f price=%.4f order_id=%s",
                str(market_id)[:24],
                side,
                float(amount_usdc),
                px,
                oid,
            )
            return {"success": True, "price": px, "order_id": oid, "simulated": True}

        try:
            from py_clob_client_v2.clob_types import OrderArgs, OrderType, PartialCreateOrderOptions
            from py_clob_client_v2.order_builder.constants import BUY, SELL as _SELL
            _v2 = True
        except ImportError:
            try:
                from py_clob_client.clob_types import OrderArgs, OrderType  # type: ignore[assignment]
                from py_clob_client.order_builder.constants import BUY, SELL as _SELL  # type: ignore[assignment]
                PartialCreateOrderOptions = None  # type: ignore[assignment,misc]
                _v2 = False
            except ImportError as e:
                raise RuntimeError("py-clob-client-v2 no instalado") from e

        token_id_eff = str(token_id or "").strip()
        if not token_id_eff:
            token_id_eff = _resolve_token_id_for_side(market_id, side)
        share_size = float(amount_usdc) / max(px, 1e-9)
        if share_size <= 0 or not math.isfinite(share_size):
            return {"success": False, "price": px, "order_id": None, "error": "invalid_size"}

        _side_const = BUY if str(side).upper() in ("YES", "BUY") else _SELL
        client = _get_clob_client()
        order_args = OrderArgs(token_id=str(token_id_eff), price=float(px), size=float(share_size), side=_side_const)
        if _v2:
            opts = PartialCreateOrderOptions(tick_size="0.01", neg_risk=False)
            resp = client.create_and_post_order(order_args, options=opts, order_type=OrderType.GTC)
        else:
            resp = client.create_and_post_order(order_args, options=None)
        oid = None
        if isinstance(resp, dict):
            oid = resp.get("orderID") or resp.get("order_id") or resp.get("id")
        success = bool(oid) or (isinstance(resp, dict) and str(resp.get("success", "")).lower() in ("true", "1"))
        log.info(
            "place_order LIVE market_id=%s side=%s token=%s… price=%.4f size_shares=%.6f",
            str(market_id)[:20],
            side,
            str(token_id_eff)[:10],
            px,
            share_size,
        )
        return {
            "success": success,
            "price": px,
            "order_id": str(oid) if oid else "",
            "simulated": False,
            "raw": resp if isinstance(resp, dict) else {"response": resp},
        }

    def get_balance(self) -> dict[str, Any]:
        """USDC disponible (collateral) según CLOB."""
        if self.effective_dry_run():
            return {"usdc_available": None, "raw": None, "simulated": True}
        try:
            from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
        except ImportError:
            try:
                from py_clob_client.clob_types import AssetType, BalanceAllowanceParams  # type: ignore[assignment]
            except ImportError as e:
                raise RuntimeError("py-clob-client-v2 no instalado") from e

        client = _get_clob_client()
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, token_id=None, signature_type=-1)
        raw = client.get_balance_allowance(params)
        bal = _parse_balance(raw)
        return {"usdc_available": bal, "raw": raw, "simulated": False}

    def get_positions(self) -> list[dict[str, Any]]:
        """Órdenes abiertas CLOB como posiciones (mercado / lado / tamaño)."""
        if self.effective_dry_run():
            return []
        try:
            from py_clob_client_v2.clob_types import OpenOrderParams
        except ImportError:
            from py_clob_client.clob_types import OpenOrderParams  # type: ignore[assignment]

        client = _get_clob_client()
        orders = _get_open_orders(client, OpenOrderParams())
        out: list[dict[str, Any]] = []
        for o in orders or []:
            if not isinstance(o, dict):
                continue
            asset = str(o.get("asset_id") or o.get("token_id") or "")
            market = str(o.get("market") or o.get("condition_id") or o.get("market_id") or "")
            side_raw = str(o.get("side", "")).upper()
            outcome = str(o.get("outcome", "") or "")
            yes_no = "YES" if outcome.lower() in ("yes", "up", "y") else "NO" if outcome.lower() in ("no", "down", "n") else side_raw
            try:
                sz = float(o.get("original_size") or o.get("size") or 0)
            except (TypeError, ValueError):
                sz = 0.0
            try:
                matched = float(o.get("size_matched") or 0)
            except (TypeError, ValueError):
                matched = 0.0
            rem = max(0.0, sz - matched)
            try:
                p = float(o.get("price") or 0)
            except (TypeError, ValueError):
                p = 0.0
            cur = rem * p if rem and p else None
            out.append(
                {
                    "market_id": market,
                    "side": yes_no,
                    "amount": rem,
                    "current_value_usdc": round(cur, 6) if cur is not None else None,
                    "order_id": str(o.get("id") or o.get("orderID") or ""),
                    "raw_side": side_raw,
                }
            )
        return out

    def get_order_status(self, order_id: str) -> dict[str, Any]:
        if not str(order_id).strip():
            return {}
        if str(order_id).startswith("sim-") or str(order_id).startswith("blocked-"):
            return {"order_id": order_id, "status": "SIMULATED", "simulated": True}
        client = _get_clob_client()
        return client.get_order(str(order_id))


def get_polymarket_client(strategy_dry_run: bool) -> PolymarketTradingClient:
    """Singleton por flag ``dry_run`` persistido en la estrategia (el env se aplica dentro)."""
    key = bool(strategy_dry_run)
    with _LOCK:
        if key not in _TRADING:
            _TRADING[key] = PolymarketTradingClient(strategy_dry_run=key)
        return _TRADING[key]


def get_live_account_client() -> "PolymarketLiveAccount":
    """Cuenta real (balance/posiciones) ignorando dry_run de estrategia."""
    with _LOCK:
        return PolymarketLiveAccount()


class PolymarketLiveAccount:
    """Lecturas L2 siempre sobre la misma cuenta (JSON y/o variables POLY_*)."""

    def get_balance(self) -> dict[str, Any]:
        try:
            from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
        except ImportError:
            try:
                from py_clob_client.clob_types import AssetType, BalanceAllowanceParams  # type: ignore[assignment]
            except ImportError as e:
                raise RuntimeError("py-clob-client-v2 no instalado") from e

        client = _get_clob_client()
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, token_id=None, signature_type=-1)
        raw = client.get_balance_allowance(params)
        avail = _parse_balance(raw)
        in_pos = _estimate_open_orders_notional(client)
        total = (avail + in_pos) if avail is not None else None
        sig_type = int(os.getenv("POLY_SIGNATURE_TYPE", "0"))
        # Para sig_type=1 (POLY_PROXY) el endpoint CLOB devuelve 0 aunque haya fondos
        # disponibles en el proxy wallet — no es un error, es una limitación del endpoint.
        proxy_managed = sig_type in (1, 2) and total == 0.0
        return {
            "usdc_available": avail,
            "usdc_in_positions": in_pos,
            "total": total,
            "raw_collateral": raw,
            "signature_type": sig_type,
            "proxy_managed": proxy_managed,
        }

    def get_positions(self) -> list[dict[str, Any]]:
        try:
            from py_clob_client_v2.clob_types import OpenOrderParams
        except ImportError:
            from py_clob_client.clob_types import OpenOrderParams  # type: ignore[assignment]

        client = _get_clob_client()
        orders = _get_open_orders(client, OpenOrderParams())
        out: list[dict[str, Any]] = []
        for o in orders or []:
            if not isinstance(o, dict):
                continue
            asset = str(o.get("asset_id") or o.get("token_id") or "")
            market = str(o.get("market") or o.get("condition_id") or "")
            outcome = str(o.get("outcome", "") or "")
            yes_no = "YES" if outcome.lower() in ("yes", "up") else "NO" if outcome.lower() in ("no", "down") else str(o.get("side", ""))
            try:
                sz = float(o.get("original_size") or o.get("size") or 0)
                matched = float(o.get("size_matched") or 0)
                p = float(o.get("price") or 0)
            except (TypeError, ValueError):
                continue
            rem = max(0.0, sz - matched)
            cur = rem * p if rem and p else 0.0
            out.append(
                {
                    "market_id": market,
                    "token_id": asset,
                    "side": yes_no,
                    "amount": rem,
                    "current_value": round(cur, 6),
                    "order_id": str(o.get("id") or o.get("orderID") or ""),
                }
            )
        return out


def _parse_balance(raw: Any) -> Optional[float]:
    if not isinstance(raw, dict):
        return None
    for k in ("balance", "available", "collateral", "amount"):
        v = raw.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return None


def _get_open_orders(client: Any, params: Any) -> list:
    """Compatibilidad v1/v2: v2 usa get_open_orders, v1 usa get_orders."""
    if hasattr(client, "get_open_orders"):
        result = client.get_open_orders(params)
    else:
        result = client.get_orders(params)  # type: ignore[attr-defined]
    if isinstance(result, dict):
        # v2 puede devolver {"data": [...], "next_cursor": "..."}
        return result.get("data") or []
    return result or []


def _estimate_open_orders_notional(client: Any) -> float:
    """Suma aproximada USDC en libros (restante * precio) de órdenes abiertas."""
    try:
        from py_clob_client_v2.clob_types import OpenOrderParams
    except ImportError:
        from py_clob_client.clob_types import OpenOrderParams  # type: ignore[assignment]

    tot = 0.0
    try:
        orders = _get_open_orders(client, OpenOrderParams())
    except Exception:
        return 0.0
    for o in orders or []:
        if not isinstance(o, dict):
            continue
        try:
            sz = float(o.get("original_size") or o.get("size") or 0)
            matched = float(o.get("size_matched") or 0)
            p = float(o.get("price") or 0)
        except (TypeError, ValueError):
            continue
        rem = max(0.0, sz - matched)
        tot += rem * p
    return round(tot, 6)


def account_json_path() -> Path:
    """Ruta legada bajo DATA_DIR; el cliente CLOB no lee este archivo."""
    return _ACCOUNT_PATH
