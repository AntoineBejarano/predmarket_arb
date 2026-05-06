"""
Sanidad previa a SIGNAL para latency_arb_sports: normalización de equipos,
probabilidades fair, edges y coherencia bidireccional home/away.
"""

from __future__ import annotations

import math
import re
import unicodedata
from typing import Any, Optional

from clients.odds_api import implied_prob, remove_vig

# Umbrales alineados con el plan de auditoría / producción
MAX_ABS_EDGE_FOR_SIGNAL = 0.15
MAX_SUM_EDGE_MAG = 0.05  # |edge_home + edge_away| debe ser pequeño si mids+probs son coherentes
PROB_TWO_WAY_SUM_TOL = 1e-3
PROB_THREE_WAY_SUM_TOL = 2e-3

# Sufijos finales de token a retirar (reservas, categorías); token = palabra al final del nombre.
_TEAM_SUFFIX_TOKENS: frozenset[str] = frozenset(
    {
        "fc",
        "cf",
        "afc",
        "sc",
        "ac",
        "b",
        "u21",
        "u23",
        "u22",
        "ii",
        "iii",
        "2",
        "bk",
        "if",
        "ff",
        "w",
        "women",
        "ladies",
    }
)


def normalize_team_name(s: str) -> str:
    """
    Lowercase, sin acentos, espacios colapsados, sufijos de club habituales al final.
    Ej.: \"Sparta Prague B\" -> \"sparta prague\".
    """
    raw = unicodedata.normalize("NFKD", (s or "").strip().lower())
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    raw = re.sub(r"[^\w\s]+", " ", raw, flags=re.UNICODE)
    parts = [p for p in raw.split() if p]
    # Sufijos al final (reservas, categorías)
    while len(parts) >= 2 and parts[-1] in _TEAM_SUFFIX_TOKENS:
        parts.pop()
    # Prefijos de club al inicio (p. ej. "fc barcelona")
    while len(parts) >= 2 and parts[0] in _TEAM_SUFFIX_TOKENS:
        parts.pop(0)
    return " ".join(parts)


def normalized_team_pair(home: str, away: str) -> tuple[str, str]:
    """Par canónico ordenado para agrupación y dedupe_key."""
    a, b = normalize_team_name(home), normalize_team_name(away)
    return tuple(sorted((a, b)))


def normalize_probabilities(
    home_odds: float,
    away_odds: float,
    draw_odds: Optional[float] = None,
) -> tuple[float, float, Optional[float]]:
    """
    Implied probs + remove_vig (misma semántica que clients.odds_api.remove_vig).
    Devuelve (ph_fair, pa_fair, pd_fair o None).
    """
    ph = implied_prob(home_odds)
    pa = implied_prob(away_odds)
    pd: Optional[float] = implied_prob(draw_odds) if draw_odds is not None and draw_odds > 0 else None
    return remove_vig(ph, pa, pd)


def validate_market_row(row: dict[str, Any]) -> tuple[bool, str]:
    """
    Heurística mínima moneyline desde fila CSV (sin objeto OpenPolymarketGame).
    Rechaza Over/Under explícitos en nombres de equipo.
    """
    h = normalize_team_name(str(row.get("home_team") or ""))
    a = normalize_team_name(str(row.get("away_team") or ""))
    if not h or not a:
        return False, "missing_teams"
    if h in ("over", "under") and a in ("over", "under"):
        return False, "ou_market"
    if ("over" in h and "under" in a) or ("under" in h and "over" in a):
        return False, "ou_market"
    for lab in (h, a):
        if "over 2.5" in lab or "under 2.5" in lab or "over 1.5" in lab or "under 1.5" in lab:
            return False, "ou_market"
        if lab.startswith("over ") or lab.startswith("under "):
            return False, "ou_market"
    return True, ""


def compute_edge_safe(p_fair: float, price: float, *, side: str = "") -> float:
    """Edge = fair - price con validación de rangos [0,1]."""
    pf = float(p_fair)
    px = float(price)
    if not (0.0 <= pf <= 1.0 and 0.0 <= px <= 1.0):
        return float("nan")
    return pf - px


def should_emit_signal(state: dict[str, Any]) -> tuple[bool, str]:
    """
    Veto estructural previo a SIGNAL (no sustituye filtros de liquidez/spread en la estrategia).

    state opcional:
      p_h_fair, p_a_fair, p_draw_fair (optional)
      edge_home, edge_away, mids_both_present (bool)
      edge_exec (optional), edge_mid
      skip_leg_edge_check: si True, solo probs (+ par si mids_both_present); no valida |edge| de la pierna.
    """
    skip_leg = bool(state.get("skip_leg_edge_check"))
    ph = state.get("p_h_fair")
    pa = state.get("p_a_fair")
    pd = state.get("p_draw_fair")
    if ph is not None and pa is not None:
        try:
            fph, fpa = float(ph), float(pa)
            if pd is None:
                s = fph + fpa
                if abs(s - 1.0) > PROB_TWO_WAY_SUM_TOL:
                    return False, "REF_PROB_NORMALIZE"
            else:
                fpd = float(pd)
                s = fph + fpa + fpd
                if abs(s - 1.0) > PROB_THREE_WAY_SUM_TOL:
                    return False, "REF_PROB_NORMALIZE"
        except (TypeError, ValueError):
            return False, "REF_PROB_NORMALIZE"

    mids_both = bool(state.get("mids_both_present"))
    ehr, ear = state.get("edge_home"), state.get("edge_away")
    if mids_both and ehr is not None and ear is not None:
        try:
            eh = float(ehr)
            ea = float(ear)
        except (TypeError, ValueError):
            return False, "INCONSISTENT_PRICING"
        if eh > 0.0 and ea > 0.0:
            return False, "BOTH_SIDES_POSITIVE"
        if abs(eh + ea) > MAX_SUM_EDGE_MAG:
            return False, "INCONSISTENT_PRICING"

    if skip_leg:
        return True, ""

    ee = state.get("edge_exec")
    em = state.get("edge_mid")
    try:
        edge_use = float(ee) if ee is not None else float(em)
    except (TypeError, ValueError):
        return False, "EDGE_IMPLAUSIBLE"
    if math.isnan(edge_use) or abs(edge_use) > MAX_ABS_EDGE_FOR_SIGNAL:
        return False, "EDGE_IMPLAUSIBLE"

    return True, ""


def map_reason_to_skip_action(reason: str) -> str:
    """Prefijo action CSV para logging."""
    m = {
        "REF_PROB_NORMALIZE": "SKIP:REF_PROB_NORMALIZE",
        "BOTH_SIDES_POSITIVE": "SKIP:BOTH_SIDES_POSITIVE",
        "INCONSISTENT_PRICING": "SKIP:INCONSISTENT_PRICING",
        "EDGE_IMPLAUSIBLE": "SKIP:EDGE_IMPLAUSIBLE",
        "DUPLICATE_SIGNAL_SPAM": "SKIP:DUPLICATE_SIGNAL_SPAM",
        "DUPLICATE_IO_PAIR": "SKIP:DUPLICATE_IO_PAIR",
    }
    return m.get(reason, "SKIP:SIGNAL_SANITY")
