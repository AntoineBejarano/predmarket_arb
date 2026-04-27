"""Precios bundle: VWAP sobre asks hasta un notional objetivo (USDC)."""

from __future__ import annotations

from typing import Any, Optional, Tuple

from clients.poly_clob import _level_price, _level_size


def _sorted_asks(asks: list[Any]) -> list[tuple[float, float]]:
    """Niveles ask (precio asc) como lista de (precio, tamaño)."""
    levels: list[tuple[float, float]] = []
    for lvl in asks or []:
        p = _level_price(lvl)
        sz = _level_size(lvl)
        if p is None or sz is None or sz <= 0:
            continue
        levels.append((p, sz))
    levels.sort(key=lambda x: x[0])
    return levels


def vwap_buy_avg_price(
    asks: list[Any],
    target_notional_usdc: float,
    *,
    min_fill_ratio: float = 0.97,
) -> Tuple[Optional[float], float, Optional[float]]:
    """
    Simula compra agresando asks hasta gastar ``target_notional_usdc`` (USDC).

    Devuelve ``(precio_medio_por_share, usdc_gastado, notional_al_mejor_ask)``.
    ``precio_medio_por_share`` = usdc_gastado / shares_totales (como best_ask en un solo nivel).
    Si no se alcanza ``min_fill_ratio`` del objetivo, devuelve (None, gastado, best_nom).
    """
    if target_notional_usdc <= 0:
        return None, 0.0, None
    levels = _sorted_asks(asks)
    if not levels:
        return None, 0.0, None
    best_p, best_sz = levels[0]
    best_nom = best_p * best_sz

    remaining = target_notional_usdc
    total_usdc = 0.0
    total_shares = 0.0
    for p, sz in levels:
        if remaining <= 1e-12:
            break
        level_max_usdc = p * sz
        take_usdc = min(remaining, level_max_usdc)
        if take_usdc <= 0:
            continue
        shares_here = take_usdc / p
        total_usdc += take_usdc
        total_shares += shares_here
        remaining -= take_usdc

    if total_shares <= 0:
        return None, 0.0, best_nom
    if total_usdc < target_notional_usdc * min_fill_ratio:
        return None, total_usdc, best_nom
    return total_usdc / total_shares, total_usdc, best_nom
