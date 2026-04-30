"""Tests heurística ML-like / briefing (Gamma-only)."""

from __future__ import annotations

from datetime import datetime, timezone

from arb.latency_arb_sports import OpenPolymarketGame, _client_poly_key_for_odds_io, _is_ml_like_open_game
from arb.latency_sports_schedule import pick_gamma_upcoming


def _g(
    *,
    home: str,
    away: str,
    kickoff: datetime | None = None,
    slug: str = "t",
    cid: str = "0x1",
) -> OpenPolymarketGame:
    return OpenPolymarketGame(
        sport_slug="atp",
        home=home,
        away=away,
        condition_id=cid,
        token_yes="t1",
        end_date=None,
        raw_title=f"{home} vs {away}",
        slug=slug,
        outcome_tokens=[(home, "t1"), (away, "t2")],
        end_date_s=None,
        kickoff_utc=kickoff,
    )


def test_is_ml_like_normal_moneyline() -> None:
    assert _is_ml_like_open_game(_g(home="Sinner", away="Alcaraz")) is True


def test_is_ml_like_rejects_over_under_sides() -> None:
    assert _is_ml_like_open_game(_g(home="Over", away="Under")) is False


def test_is_ml_like_rejects_over_under_in_names() -> None:
    assert _is_ml_like_open_game(_g(home="Over 2.5", away="Under 2.5")) is False


def test_is_ml_like_rejects_prefix_over_under() -> None:
    assert _is_ml_like_open_game(_g(home="over games", away="under sets")) is False


def test_is_ml_like_rejects_empty_side() -> None:
    assert _is_ml_like_open_game(_g(home="", away="B")) is False


def test_client_poly_key_football_and_basketball() -> None:
    assert _client_poly_key_for_odds_io("football", "ucl") == "soccer_uefa_champs_league"
    assert _client_poly_key_for_odds_io("football", "uel") == "soccer_uefa_europa_league"
    assert _client_poly_key_for_odds_io("football", "epl") == "soccer_epl"
    assert _client_poly_key_for_odds_io("basketball", "nba") == "basketball_nba"


def test_pick_gamma_upcoming_excludes_non_ml_like() -> None:
    now = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)
    kick = datetime(2026, 4, 29, 15, 0, 0, tzinfo=timezone.utc)
    normal = _g(home="A", away="B", kickoff=kick, slug="ok", cid="0xa")
    prop = _g(home="Over 2.5", away="Under 2.5", kickoff=kick, slug="prop", cid="0xb")
    out = pick_gamma_upcoming([normal, prop], now, limit=10)
    assert len(out) == 1
    assert out[0].slug == "ok"
