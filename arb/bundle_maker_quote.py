"""Quoter maker NegRisk: misma q (shares), sum(bids) <= 1 - target_edge, caps bid/ask/fair."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Sequence, Tuple

from arb.negrisk_execution_policy import ExecutionPolicy


def _tick_to_float(tick_s: str) -> float:
    try:
        return float(tick_s)
    except (TypeError, ValueError):
        return 0.01


@dataclass
class LegBook:
    """Snapshot mínimo de libro por pierna YES."""

    token_id: str
    best_bid: Optional[float]
    best_ask: Optional[float]
    midpoint: Optional[float]
    tick_size: str


@dataclass
class MakerQuoteLeg:
    token_id: str
    bid_price: float
    size_shares: float
    size_usdc: float


@dataclass
class MakerQuoteBundle:
    """Bundle coherente: misma ``q`` en todas las piernas."""

    legs: list[MakerQuoteLeg]
    bundle_share_qty: float
    sum_bids: float
    target_edge: float


def _raw_bid_for_leg(
    lb: LegBook,
    *,
    spread_buffer: float,
) -> Optional[float]:
    tick = _tick_to_float(lb.tick_size)
    bb = lb.best_bid
    ba = lb.best_ask
    mid = lb.midpoint
    if bb is None:
        return None
    cap1 = bb + tick
    if ba is not None:
        cap2 = ba - tick
        if cap2 < cap1:
            cap1 = cap2
    if mid is not None:
        cap1 = min(cap1, mid - spread_buffer)
    if ba is not None and bb + tick >= ba:
        return None
    return cap1


def compute_maker_quotes(
    books: Sequence[LegBook],
    policy: ExecutionPolicy,
    *,
    target_bundle_usdc: float,
    spread_buffer: float = 0.002,
) -> Tuple[Optional[MakerQuoteBundle], str]:
    """
    Calcula bids escalados para ``sum(bids) <= 1 - min_maker_edge`` y ``q = target / sum(bid)``.
    Devuelve (None, reason) si alguna pierna no cotizable.
    """
    if not books:
        return None, "no_legs"
    target_edge = max(float(policy.min_maker_edge), 1e-9)
    cap_sum = 1.0 - target_edge

    raw: list[float] = []
    for lb in books:
        r = _raw_bid_for_leg(lb, spread_buffer=spread_buffer)
        if r is None or r <= 0:
            return None, f"no_raw_bid:{lb.token_id[:12]}"
        raw.append(r)

    s = sum(raw)
    if s <= 0:
        return None, "zero_sum_raw"
    scale = cap_sum / s
    bids = [max(1e-6, x * scale) for x in raw]

    sum_b = sum(bids)
    if sum_b > cap_sum + 1e-9:
        return None, "sum_bids_exceeds_cap_after_scale"

    q = target_bundle_usdc / sum_b
    if q <= 0:
        return None, "non_positive_q"

    out_legs: list[MakerQuoteLeg] = []
    for lb, px in zip(books, bids):
        usdc = q * px
        out_legs.append(MakerQuoteLeg(token_id=lb.token_id, bid_price=px, size_shares=q, size_usdc=usdc))

    return (
        MakerQuoteBundle(
            legs=out_legs,
            bundle_share_qty=q,
            sum_bids=sum_b,
            target_edge=target_edge,
        ),
        "ok",
    )


def order_outcomes_all_ok(batch_response: dict[str, Any]) -> Tuple[bool, list[dict[str, Any]]]:
    """
    Tras POST batch con HTTP 200: inspecciona cada resultado.
    Devuelve ``(all_ok, items_problemáticos)``.
    """
    failed: list[dict[str, Any]] = []
    data = batch_response.get("data")
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            err = item.get("error") or item.get("err") or item.get("message")
            ok = item.get("success", item.get("ok"))
            if err or ok is False:
                failed.append(item)
        return (len(failed) == 0), failed
    if batch_response.get("error"):
        return False, [batch_response]
    return True, []
