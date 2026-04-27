"""Política de ejecución maker/taker para NegRisk Maker Bundle (Fase 2a + 2b)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

from clients.poly_markets import NegRiskBundleCandidate
from clients.poly_parse import api_bool_true

if TYPE_CHECKING:
    from clients.poly_clob import PolyCLOBClient


@dataclass(frozen=True)
class ExecutionPolicy:
    """Interfaz estable: quoter, FSM y parciales leen solo estos campos."""

    fee_class: str
    maker_allowed: bool
    taker_entry_allowed: bool
    taker_hedge_allowed: bool
    taker_emergency_sell_allowed: bool
    min_maker_edge: float
    min_taker_edge_fee_free: float
    min_emergency_edge: float
    max_partial_seconds: float
    max_event_exposure_usdc: float
    max_global_exposure_usdc: float
    max_partial_loss_usdc: float


def execution_policy_unknown_conservative() -> ExecutionPolicy:
    """Fase 2a: default conservador hasta que el classifier tenga datos fiables."""
    return ExecutionPolicy(
        fee_class="unknown",
        maker_allowed=True,
        taker_entry_allowed=False,
        taker_hedge_allowed=api_bool_true(os.getenv("BUNDLE_ALLOW_TAKER_HEDGE", "true")),
        taker_emergency_sell_allowed=api_bool_true(os.getenv("BUNDLE_ALLOW_TAKER_EMERGENCY_SELL", "true")),
        min_maker_edge=float(os.getenv("BUNDLE_MIN_MAKER_EDGE_UNKNOWN", "0.040")),
        min_taker_edge_fee_free=float(os.getenv("BUNDLE_MIN_TAKER_EDGE_FEE_FREE", "0.008")),
        min_emergency_edge=float(os.getenv("BUNDLE_MIN_EMERGENCY_EDGE", "0.004")),
        max_partial_seconds=float(os.getenv("BUNDLE_MAX_PARTIAL_SECONDS", "120")),
        max_event_exposure_usdc=float(os.getenv("BUNDLE_MAX_EVENT_EXPOSURE_USDC", "300")),
        max_global_exposure_usdc=float(os.getenv("BUNDLE_MAX_GLOBAL_EXPOSURE_USDC", "1500")),
        max_partial_loss_usdc=float(os.getenv("BUNDLE_MAX_PARTIAL_LOSS_USDC", "5")),
    )


def _edge_for_class(fee_class: str) -> float:
    if fee_class == "fee_free":
        return float(os.getenv("BUNDLE_MIN_MAKER_EDGE_FEE_FREE", "0.012"))
    if fee_class == "low":
        return float(os.getenv("BUNDLE_MIN_MAKER_EDGE_LOW_FEE", "0.018"))
    if fee_class == "medium":
        return float(os.getenv("BUNDLE_MIN_MAKER_EDGE_MEDIUM_FEE", "0.025"))
    if fee_class == "high":
        return float(os.getenv("BUNDLE_MIN_MAKER_EDGE_HIGH_FEE", "0.035"))
    return float(os.getenv("BUNDLE_MIN_MAKER_EDGE_UNKNOWN", "0.040"))


async def build_execution_policy_for_candidate(
    cand: NegRiskBundleCandidate,
    poly: "PolyCLOBClient",
    *,
    base: Optional[ExecutionPolicy] = None,
) -> ExecutionPolicy:
    """
    Fase 2b: clasifica fees con reglas estrictas (plan v3).

    fee_free solo si:
      no child has feesEnabled True (Gamma)
      AND at least one reliable fee signal (CLOB fee-rate sampled)
      AND all sampled fee_rate_bps == 0
    Si any_child.feesEnabled is True -> never fee_free.
    """
    base = base or execution_policy_unknown_conservative()
    any_gamma_fees_on = any(leg.fees_enabled is True for leg in cand.legs)

    bps_list: list[int] = []
    for leg in cand.legs:
        try:
            bps_list.append(await poly.get_fee_rate_bps(leg.yes_token_id))
        except Exception:
            bps_list.append(-1)

    reliable = all(b >= 0 for b in bps_list) and len(bps_list) == len(cand.legs) and len(cand.legs) > 0
    all_zero = reliable and all(b == 0 for b in bps_list)

    fee_class = "unknown"
    if any_gamma_fees_on:
        if not reliable:
            fee_class = "unknown"
        else:
            mx = max(bps_list)
            if mx == 0:
                fee_class = "medium"
            elif mx <= 50:
                fee_class = "low"
            elif mx <= 200:
                fee_class = "medium"
            else:
                fee_class = "high"
    elif reliable and all_zero:
        fee_class = "fee_free"
    elif reliable:
        mx = max(bps_list)
        if mx <= 50:
            fee_class = "low"
        elif mx <= 200:
            fee_class = "medium"
        else:
            fee_class = "high"
    else:
        fee_class = "unknown"

    min_maker = _edge_for_class(fee_class)
    taker_entry = False
    if fee_class == "fee_free":
        taker_entry = api_bool_true(os.getenv("BUNDLE_ALLOW_TAKER_FEE_FREE", "true")) and api_bool_true(
            os.getenv("BUNDLE_ALLOW_TAKER_ENTRY", "false")
        )

    return ExecutionPolicy(
        fee_class=fee_class,
        maker_allowed=True,
        taker_entry_allowed=taker_entry,
        taker_hedge_allowed=base.taker_hedge_allowed,
        taker_emergency_sell_allowed=base.taker_emergency_sell_allowed,
        min_maker_edge=min_maker,
        min_taker_edge_fee_free=base.min_taker_edge_fee_free,
        min_emergency_edge=base.min_emergency_edge,
        max_partial_seconds=base.max_partial_seconds,
        max_event_exposure_usdc=base.max_event_exposure_usdc,
        max_global_exposure_usdc=base.max_global_exposure_usdc,
        max_partial_loss_usdc=base.max_partial_loss_usdc,
    )
