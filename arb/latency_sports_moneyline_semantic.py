"""Validación pura: mercado Polymarket Gamma ≈ moneyline (team win), no props ni -more-markets sin metadata."""

from __future__ import annotations

import re
from typing import Any

from clients.odds_api import teams_match_odds_gamma
from clients.odds_api_io import normalize_name_order


def _norm_smt(raw: str) -> str:
    return re.sub(r"[\s_-]+", "", (raw or "").strip().lower())


# Tipos Gamma / feed que aceptamos como moneyline explícito.
_MONEYLINE_SMT: frozenset[str] = frozenset(
    {
        "moneyline",
        "matchwinner",
        "matchwinnermoneyline",
        "winner",
        "towin",
        "gamewinner",
        "gamewinnermoneyline",
        "h2h",
        "headtohead",
        "ml",
        "fullgamemoneyline",
    }
)


def _sports_market_type_allowed(smt: str) -> bool:
    n = _norm_smt(smt)
    if not n:
        return False
    if n in _MONEYLINE_SMT:
        return True
    if "moneyline" in n and "spread" not in n and "total" not in n:
        return True
    if n == "winner" or n.endswith("winner"):
        return True
    return False


def _effective_question(game: Any) -> tuple[str, bool]:
    """Texto para heurísticas; bool = hay pregunta de mercado explícita (no solo título evento)."""
    mq = (getattr(game, "market_question", "") or "").strip()
    if mq:
        return mq, True
    return (getattr(game, "raw_title", "") or "").strip(), False


def _more_markets_context(game: Any) -> bool:
    s = (getattr(game, "slug", "") or "").casefold()
    ms = (getattr(game, "market_slug", "") or "").casefold()
    return "more-markets" in s or "more-markets" in ms


def _question_has_non_moneyline_patterns(q: str) -> Optional[str]:
    """Devuelve código de razón si detecta mercado no moneyline; None si no aplica patrón."""
    t = (q or "").casefold()
    if not t:
        return None
    checks: list[tuple[str, str]] = [
        ("over", "prop_total_over"),
        ("under", "prop_total_under"),
        ("total points", "prop_total_points"),
        ("total goals", "prop_total_goals"),
        ("spread", "prop_spread"),
        ("handicap", "prop_handicap"),
        ("point spread", "prop_point_spread"),
        ("first half", "prop_first_half"),
        ("1st half", "prop_first_half"),
        ("second half", "prop_second_half"),
        ("2nd half", "prop_second_half"),
        ("correct score", "prop_correct_score"),
        ("both teams to score", "prop_btts"),
        ("both teams", "prop_btts_generic"),
        (" btts", "prop_btts"),
        ("corner", "prop_corners"),
        ("corners", "prop_corners"),
        ("booking", "prop_bookings"),
        (" to qualify", "prop_qualify"),
        ("to qualify", "prop_qualify"),
        (" advance", "prop_advance"),
        ("group stage", "prop_group_stage"),
        ("group winner", "prop_group_winner"),
        ("draw no bet", "prop_draw_no_bet"),
        ("dnb", "prop_draw_no_bet_short"),
    ]
    for needle, reason in checks:
        if needle in t:
            return reason
    if re.search(r"\bover\b.*/\s*\bunder\b", t) or re.search(r"\bunder\b.*/\s*\bover\b", t):
        return "prop_over_under_slash"
    return None


def _explicit_team_win_question(q: str) -> bool:
    """Victoria de equipo explícita (p. ej. Will X beat Y?), excluyendo ambigüedades ya filtradas."""
    t = (q or "").casefold()
    if not t:
        return False
    if " beat " in t or " beats " in t or "defeat" in t:
        return True
    if re.search(r"\bwill\b.+\b(beat|win|defeat)\b", t):
        return True
    if " win?" in t or t.rstrip().endswith(" win") or " win on " in t:
        return True
    if " win the match" in t or " win this game" in t:
        return True
    return False


def _outcome_labels_literal_yes_no(pairs: list[tuple[str, str]]) -> bool:
    if len(pairs) != 2:
        return False
    a = str(pairs[0][0] or "").strip().lower()
    b = str(pairs[1][0] or "").strip().lower()
    return {a, b} == {"yes", "no"}


def _outcomes_match_teams_moneyline(game: Any) -> bool:
    ot = list(getattr(game, "outcome_tokens", []) or [])
    if len(ot) != 2:
        return False
    if _outcome_labels_literal_yes_no(ot):
        return False
    sk = (getattr(game, "sport_slug", "") or "").strip() or None
    (l0, _), (l1, _) = ot
    gh, ga = normalize_name_order(game.home), normalize_name_order(game.away)
    n0, n1 = normalize_name_order(str(l0)), normalize_name_order(str(l1))
    return (
        teams_match_odds_gamma(n0, gh, sport_slug=sk) and teams_match_odds_gamma(n1, ga, sport_slug=sk)
    ) or (
        teams_match_odds_gamma(n0, ga, sport_slug=sk) and teams_match_odds_gamma(n1, gh, sport_slug=sk)
    )


def is_valid_polymarket_moneyline(game: Any) -> tuple[bool, str]:
    """
    Valida que el mercado concreto sea moneyline / team win antes de resolve/edge/SIGNAL.
    Conservador: -more-markets exige tipo moneyline explícito + pregunta de victoria de equipo.
    """
    ot = list(getattr(game, "outcome_tokens", []) or [])
    if len(ot) != 2:
        return False, "need_two_outcomes"

    q_eff, has_market_q = _effective_question(game)
    bad = _question_has_non_moneyline_patterns(q_eff)
    if bad is not None:
        return False, bad

    smt_raw = (getattr(game, "sports_market_type", "") or "").strip()
    if smt_raw:
        parts = [p.strip() for p in smt_raw.split("|") if p.strip()]
        if len(parts) > 1:
            if not all(_sports_market_type_allowed(p) for p in parts):
                return False, "sports_market_type_not_moneyline"
        elif not _sports_market_type_allowed(smt_raw):
            return False, "sports_market_type_not_moneyline"

    if _outcome_labels_literal_yes_no(ot):
        if not _explicit_team_win_question(q_eff):
            return False, "literal_yes_no_needs_team_win_question"
    else:
        if not _outcomes_match_teams_moneyline(game):
            return False, "outcomes_not_team_moneyline"

    if _more_markets_context(game):
        if not smt_raw:
            return False, "more_markets_unverified"
        parts_mm = [p.strip() for p in smt_raw.split("|") if p.strip()]
        if not parts_mm or not all(_sports_market_type_allowed(p) for p in parts_mm):
            return False, "more_markets_unverified"
        if not _explicit_team_win_question(q_eff):
            return False, "more_markets_unverified"
        if not has_market_q:
            return False, "more_markets_unverified"

    return True, "moneyline_semantic_ok"
