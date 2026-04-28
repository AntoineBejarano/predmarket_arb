"""
Construcción y firma de órdenes limit GTC/GTD para el CLOB Polymarket.

Usa `py-order-utils` + `eth-account` (no incluye el SDK `py_clob_client`).
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional, Tuple

# Contratos CLOB Polymarket — mainnet Polygon (137), alineado con py_clob_client.config
_EXCHANGE = {
    (137, False): "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
    (137, True): "0xC5d563A36AE78145C45a50134d48A1215220f80a",
}

_TICK_ROUND = {
    "0.1": (1, 2, 3),
    "0.01": (2, 2, 4),
    "0.001": (3, 2, 5),
    "0.0001": (4, 2, 6),
}


def _round_down(x: float, sig_digits: int) -> float:
    return math.floor(x * (10**sig_digits)) / (10**sig_digits)


def _round_normal(x: float, sig_digits: int) -> float:
    return round(x * (10**sig_digits)) / (10**sig_digits)


def _round_up(x: float, sig_digits: int) -> float:
    return math.ceil(x * (10**sig_digits)) / (10**sig_digits)


def _decimal_places(x: float) -> int:
    return abs(Decimal(str(x)).as_tuple().exponent)


def _to_token_decimals(x: float) -> int:
    f = (10**6) * x
    if _decimal_places(f) > 0:
        f = _round_normal(f, 0)
    return int(f)


def _exchange_address(chain_id: int, neg_risk: bool) -> str:
    key = (chain_id, neg_risk)
    if key not in _EXCHANGE:
        raise ValueError(f"chain_id={chain_id} neg_risk={neg_risk} no soportado para CLOB")
    return _EXCHANGE[key]


def _amounts(side: str, size: float, price: float, tick_size: str) -> Tuple[int, int, int]:
    """Devuelve (side_int, maker_amount, taker_amount) en unidades 1e6 como py_clob OrderBuilder."""
    from py_order_utils.model.sides import BUY, SELL

    rc = _TICK_ROUND.get(tick_size)
    if not rc:
        raise ValueError(f"tick_size no soportado: {tick_size}")
    pr, sz, am = rc
    raw_price = _round_normal(price, pr)
    side_l = side.lower()
    if side_l == "buy":
        raw_taker = _round_down(size, sz)
        raw_maker = raw_taker * raw_price
        if _decimal_places(raw_maker) > am:
            raw_maker = _round_up(raw_maker, am + 4)
            if _decimal_places(raw_maker) > am:
                raw_maker = _round_down(raw_maker, am)
        return BUY, _to_token_decimals(raw_maker), _to_token_decimals(raw_taker)
    if side_l == "sell":
        raw_maker = _round_down(size, sz)
        raw_taker = raw_maker * raw_price
        if _decimal_places(raw_taker) > am:
            raw_taker = _round_up(raw_taker, am + 4)
            if _decimal_places(raw_taker) > am:
                raw_taker = _round_down(raw_taker, am)
        return SELL, _to_token_decimals(raw_maker), _to_token_decimals(raw_taker)
    raise ValueError("side debe ser buy o sell")


def build_post_order_body(
    private_key: str,
    api_key: str,
    chain_id: int,
    token_id: str,
    side: str,
    price: float,
    size_usdc: float,
    tick_size: str,
    neg_risk: bool,
    fee_rate_bps: int = 0,
    signature_type: Optional[int] = None,
    funder: Optional[str] = None,
    nonce: int = 0,
    expiration: int = 0,
    *,
    order_type: str = "GTC",
    post_only: bool = False,
) -> Tuple[str, dict[str, Any]]:
    """
    Construye el cuerpo POST /order y el JSON compacto para HMAC.
    Para BUY, `size_usdc` se interpreta como presupuesto en USDC (colateral maker);
    para SELL, como cantidad de shares a vender.
    """
    from eth_account import Account
    from py_order_utils.builders.order_builder import OrderBuilder
    from py_order_utils.model.order import OrderData
    from py_order_utils.model.signatures import EOA, POLY_GNOSIS_SAFE, POLY_PROXY
    from py_order_utils.signer import Signer as UtilsSigner

    acct = Account.from_key(private_key)
    signer_addr = acct.address
    sig_t = int(signature_type) if signature_type is not None else int(os.getenv("POLY_SIGNATURE_TYPE", "0"))
    if sig_t not in (EOA, POLY_PROXY, POLY_GNOSIS_SAFE):
        sig_t = EOA
    maker_funder = (funder or os.getenv("POLY_FUNDER") or signer_addr).strip()

    if side.lower() == "buy":
        share_size = size_usdc / max(price, 1e-9)
    else:
        share_size = size_usdc

    side_i, maker_amt, taker_amt = _amounts(side, share_size, price, tick_size)

    data = OrderData(
        maker=maker_funder,
        taker="0x0000000000000000000000000000000000000000",
        tokenId=str(token_id),
        makerAmount=str(maker_amt),
        takerAmount=str(taker_amt),
        side=side_i,
        feeRateBps=str(fee_rate_bps),
        nonce=str(nonce),
        signer=signer_addr,
        expiration=str(expiration),
        signatureType=sig_t,
    )

    ex = _exchange_address(chain_id, neg_risk)
    ub = OrderBuilder(ex, chain_id, UtilsSigner(key=private_key))
    signed = ub.build_signed_order(data)
    ot = (order_type or "GTC").strip().upper()
    if ot not in ("GTC", "GTD", "FOK"):
        ot = "GTC"
    body: dict[str, Any] = {
        "order": signed.dict(),
        "owner": api_key.strip(),
        "orderType": ot,
        "postOnly": bool(post_only),
    }
    serialized = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
    return serialized, body


@dataclass
class LiveDeps:
    ok: bool
    error: str = ""

    @staticmethod
    def check() -> LiveDeps:
        try:
            import eth_account  # noqa: F401
            from py_order_utils.builders import order_builder  # noqa: F401
        except ImportError as e:
            return LiveDeps(False, f"instala dependencias live: pip install eth-account py-order-utils ({e})")
        return LiveDeps(True, "")
