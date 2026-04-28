"""Tests unitarios: Odds API helpers y matching equipos / Gamma."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from arb.latency_arb_sports import (
    GammaSportMarket,
    _event_matches_odds_teams,
    _pick_best_market_in_event,
)
from clients.odds_api import implied_prob, remove_vig, teams_match_odds_gamma


class TestLatencySportsOdds(unittest.TestCase):
    def test_remove_vig_two_way(self) -> None:
        h, a, d = remove_vig(0.5, 0.5, None)
        self.assertIsNone(d)
        self.assertAlmostEqual(h + a, 1.0)
        self.assertAlmostEqual(h, 0.5)

    def test_implied_prob(self) -> None:
        self.assertAlmostEqual(implied_prob(2.0), 0.5)

    def test_teams_match_manchester(self) -> None:
        self.assertTrue(teams_match_odds_gamma("Manchester City", "Man City"))
        self.assertTrue(teams_match_odds_gamma("Boston Celtics", "Celtics"))

    def test_event_matches_odds_teams_title(self) -> None:
        ev_gamma = {
            "title": "EPL: Arsenal vs Chelsea FC",
            "slug": "epl-ars-che-2026-04-28",
        }
        self.assertTrue(_event_matches_odds_teams(ev_gamma, "Arsenal", "Chelsea"))
        self.assertFalse(_event_matches_odds_teams(ev_gamma, "Arsenal", "Tottenham"))

    def test_pick_best_market_in_event_time_window(self) -> None:
        odds_commence = datetime(2026, 4, 28, 18, 15, tzinfo=timezone.utc)
        m = {
            "acceptingOrders": True,
            "closed": False,
            "enableOrderBook": True,
            "conditionId": "0xabc",
            "slug": "test-nba",
            "outcomes": '["Celtics", "76ers"]',
            "clobTokenIds": '["1", "2"]',
            "question": "NBA test",
            "endDate": "2026-04-28T18:30:00Z",
            "groupItemTitle": "NBA",
        }
        ev_gamma = {"markets": [m]}
        row = _pick_best_market_in_event(ev_gamma, "basketball_nba", odds_commence)
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row.slug, "test-nba")
        self.assertIsInstance(row, GammaSportMarket)


class TestTeamMatching(unittest.TestCase):
    def test_positive_pairs(self) -> None:
        pairs = (
            ("Los Angeles Lakers", "Lakers"),
            ("Golden State Warriors", "Warriors"),
            ("Arsenal FC", "Arsenal"),
            ("Chelsea FC", "Chelsea"),
            ("Manchester City", "Man City"),
            ("Wolverhampton Wanderers", "Wolves"),
            ("Brighton & Hove Albion", "Brighton"),
            ("Nottingham Forest", "Nottm Forest"),
        )
        for odds_name, gamma_name in pairs:
            with self.subTest(odds=odds_name, gamma=gamma_name):
                self.assertTrue(
                    teams_match_odds_gamma(odds_name, gamma_name),
                    msg=f"{odds_name!r} vs {gamma_name!r}",
                )

    def test_negative_pairs(self) -> None:
        pairs = (
            ("Arsenal", "Chelsea"),
            ("Lakers", "Warriors"),
            ("Manchester City", "Manchester United"),
        )
        for odds_name, gamma_name in pairs:
            with self.subTest(odds=odds_name, gamma=gamma_name):
                self.assertFalse(teams_match_odds_gamma(odds_name, gamma_name))


if __name__ == "__main__":
    unittest.main()
