"""
Estado por pierna para emisión de SIGNAL en latency_arb_sports: solo ante
información nueva (cruce de umbral, mejora de edge, precio ejecutable, liquidez).

No sustituye la sanidad estructural en latency_sports_signal_sanity.py.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping, Optional, Tuple

SignalKey = Tuple[str, str, str, str, str]

# Salto mínimo de notional (USDC) para considerar LIQUIDITY_X2 (evita 1$→2$).
DEFAULT_MIN_ABS_LIQUIDITY_DELTA_USDC = 50.0


@dataclass
class LegSignalState:
    """Último SIGNAL emitido para la clave + último edge observado (cruce de umbral)."""

    last_edge_exec: float = float("nan")
    last_price_poly: float = float("nan")
    last_prob_pin: float = float("nan")
    last_available_size: Optional[float] = None
    last_signal_ts: float = 0.0
    prev_edge_exec: float = float("nan")


def get_signal_key(
    odds_event_id: str,
    normalized_team_pair: tuple[str, str],
    side: str,
    token_id: str,
) -> SignalKey:
    a, b = normalized_team_pair[0], normalized_team_pair[1]
    return (
        str(odds_event_id or "").strip(),
        str(a or "").strip(),
        str(b or "").strip(),
        str(side or "").strip(),
        str(token_id or "").strip(),
    )


def _emission_cleared(last: LegSignalState) -> bool:
    """True si no hay ancla de último SIGNAL (nunca emitido o reset por decay de edge)."""
    return not math.isfinite(last.last_edge_exec)


def should_emit_signal(
    last: Optional[LegSignalState],
    current_row: Mapping[str, Any],
    *,
    min_edge: float,
    edge_improvement_delta: float = 0.02,
    price_delta: float = 0.02,
    liquidity_x_mult: float = 2.0,
    first_observation_edge_margin: float = 0.02,
    min_abs_liquidity_delta_usdc: float = DEFAULT_MIN_ABS_LIQUIDITY_DELTA_USDC,
) -> tuple[bool, str]:
    """
    Disparadores (cualquiera basta):
      0) Primera observación / post-reset: edge >= min_edge + first_observation_edge_margin
      1) Cruce al alza del min_edge respecto al edge del tick previo
      2) Mejora de edge ejecutable vs último SIGNAL >= edge_improvement_delta
      3) Cambio de precio ejecutable vs último SIGNAL >= price_delta Y edge >= último edge emitido
      4) Liquidez: notional actual >= max(k×último, umbral absoluto); o cruce de umbral mínimo
    """
    cur_e = float(current_row["edge_exec"])
    cur_p = float(current_row["price_poly"])
    raw_sz = current_row.get("available_size")
    cur_sz: Optional[float]
    try:
        cur_sz = float(raw_sz) if raw_sz is not None else None
    except (TypeError, ValueError):
        cur_sz = None
    min_liq_thr = float(current_row.get("min_liquidity_threshold") or 0.0)
    min_e = float(min_edge)
    strong_e = min_e + float(first_observation_edge_margin)
    min_abs_liq = float(
        current_row.get("min_abs_liquidity_delta_usdc", min_abs_liquidity_delta_usdc) or 0.0
    )

    if last is None:
        if cur_e >= strong_e:
            return True, "FIRST_OBSERVATION_STRONG_EDGE"
        return False, "FIRST_OBSERVATION_WEAK"

    if _emission_cleared(last):
        if cur_e >= strong_e:
            return True, "FIRST_STRONG_AFTER_DECAY_RESET"
        prev_e = last.prev_edge_exec
        if math.isfinite(prev_e) and prev_e < min_e and cur_e >= min_e:
            return True, "EDGE_CROSSING_UP"
        return False, "NO_NEW_INFORMATION"

    prev_e = last.prev_edge_exec
    if math.isfinite(prev_e) and prev_e < min_e and cur_e >= min_e:
        return True, "EDGE_CROSSING_UP"

    if math.isfinite(last.last_edge_exec) and cur_e >= last.last_edge_exec + float(edge_improvement_delta):
        return True, "EDGE_IMPROVED"

    if (
        math.isfinite(last.last_price_poly)
        and abs(cur_p - last.last_price_poly) >= float(price_delta)
        and cur_e >= last.last_edge_exec - 1e-12
    ):
        return True, "PRICE_CHANGED"

    lsz = last.last_available_size
    if cur_sz is not None:
        if lsz is not None and lsz > 0:
            doubled = float(liquidity_x_mult) * lsz
            bar = max(doubled, min_abs_liq)
            if cur_sz >= bar:
                return True, "LIQUIDITY_X2"
        if (
            min_liq_thr > 0
            and lsz is not None
            and lsz < min_liq_thr
            and cur_sz >= min_liq_thr
        ):
            return True, "LIQUIDITY_CROSS_MIN"

    return False, "NO_NEW_INFORMATION"


def update_signal_state(
    store: MutableMapping[SignalKey, LegSignalState],
    key: SignalKey,
    current_row: Mapping[str, Any],
    *,
    emitted: bool,
    now_ts: float,
) -> None:
    """
    Actualiza estado tras evaluar: siempre avanza prev_edge_exec; si emitted, persiste último SIGNAL.

    Si el edge cae por debajo de min_edge * 0.8 (sin emitir), se borra el ancla del último SIGNAL
    para que una recuperación posterior cuente como nueva oportunidad (parecido a EDGE_DECAY_RESET).
    """
    cur_e = float(current_row["edge_exec"])
    cur_p = float(current_row["price_poly"])
    cur_pin = float(current_row["prob_pin"])
    raw_sz = current_row.get("available_size")
    try:
        cur_sz: Optional[float] = float(raw_sz) if raw_sz is not None else None
    except (TypeError, ValueError):
        cur_sz = None

    st = store.get(key)
    if st is None:
        st = LegSignalState()

    if emitted:
        st.last_edge_exec = cur_e
        st.last_price_poly = cur_p
        st.last_prob_pin = cur_pin
        st.last_available_size = cur_sz
        st.last_signal_ts = float(now_ts)
    else:
        min_e = float(current_row.get("min_edge", 0.0) or 0.0)
        reset_thr = min_e * 0.8 if min_e > 0.0 else 0.0
        rel_decay = float(current_row.get("edge_rel_decay_reset", 0.02) or 0.02)
        if math.isfinite(st.last_edge_exec) and st.last_edge_exec >= min_e:
            below_soft_floor = min_e > 0.0 and cur_e < reset_thr
            faded_from_peak = cur_e <= st.last_edge_exec - rel_decay + 1e-12
            if below_soft_floor or faded_from_peak:
                st.last_edge_exec = float("nan")
                st.last_price_poly = float("nan")
                st.last_prob_pin = float("nan")
                st.last_available_size = None
                st.last_signal_ts = 0.0

    st.prev_edge_exec = cur_e
    store[key] = st
