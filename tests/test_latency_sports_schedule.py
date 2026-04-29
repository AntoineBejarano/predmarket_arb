"""Tests planificador sports (solo Gamma / kickoff)."""

from __future__ import annotations

from datetime import datetime, timezone

from arb.latency_arb_sports import OpenPolymarketGame, gamma_event_kickoff_utc
from arb.latency_sports_schedule import compute_should_run_from_games, pick_gamma_upcoming


def _game(
    *,
    kickoff: datetime | None,
    slug: str = "atp-test-2026-04-29",
    cid: str = "0xabc",
) -> OpenPolymarketGame:
    return OpenPolymarketGame(
        sport_slug="atp",
        home="A",
        away="B",
        condition_id=cid,
        token_yes="t1",
        end_date=None,
        raw_title="A vs B",
        slug=slug,
        outcome_tokens=[("A", "t1"), ("B", "t2")],
        end_date_s=None,
        kickoff_utc=kickoff,
    )


def test_compute_should_run_from_games_inside_window() -> None:
    now = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)
    kick = datetime(2026, 4, 29, 12, 30, 0, tzinfo=timezone.utc)
    games = [_game(kickoff=kick)]
    ok, reason, n = compute_should_run_from_games(games, now, pre_start_sec=3600.0, post_end_sec=3600.0)
    assert ok is True
    assert n == 1
    assert "inside_window" in reason


def test_compute_should_run_from_games_outside() -> None:
    now = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)
    kick = datetime(2026, 4, 29, 20, 0, 0, tzinfo=timezone.utc)
    games = [_game(kickoff=kick)]
    ok, reason, n = compute_should_run_from_games(games, now, pre_start_sec=2700.0, post_end_sec=18000.0)
    assert ok is False
    assert reason == "no_active_window"
    assert n == 1


def test_pick_gamma_upcoming_excludes_old_kickoff() -> None:
    now = datetime(2026, 4, 29, 21, 30, 0, tzinfo=timezone.utc)
    old = _game(kickoff=datetime(2026, 4, 29, 19, 0, 0, tzinfo=timezone.utc), slug="old", cid="0x1")
    soon = _game(kickoff=datetime(2026, 4, 29, 22, 0, 0, tzinfo=timezone.utc), slug="soon", cid="0x2")
    out = pick_gamma_upcoming([old, soon], now, limit=10)
    assert len(out) == 1
    assert out[0].slug == "soon"


def test_gamma_event_kickoff_from_slug_date() -> None:
    ref = datetime(2026, 4, 29, 10, 0, 0, tzinfo=timezone.utc)
    ev = {"slug": "atp-foo-bar-2026-04-29", "markets": []}
    k = gamma_event_kickoff_utc(ev, ref=ref)
    assert k is not None
    assert k.year == 2026 and k.month == 4 and k.day == 29
