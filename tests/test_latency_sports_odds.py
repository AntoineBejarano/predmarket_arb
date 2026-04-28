"""Tests unitarios: Odds API helpers y matching equipos."""

from __future__ import annotations

import unittest

from arb.latency_arb_sports import GammaSportMarket, _match_gamma_for_event
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

    def test_match_gamma_time_window(self) -> None:
        g = GammaSportMarket(
            condition_id="0xabc",
            slug="test-nba",
            sport_key="basketball_nba",
            league="NBA",
            home_team="Celtics",
            away_team="76ers",
            outcome_tokens=[("Celtics", "1"), ("76ers", "2")],
            question="NBA test",
            end_date_s="2026-04-28T18:30:00Z",
            start_date_s=None,
        )
        ev = {
            "home_team": "Boston Celtics",
            "away_team": "Philadelphia 76ers",
            "commence_time": "2026-04-28T18:15:00Z",
            "sport_key": "basketball_nba",
        }
        by = {"basketball_nba": [g]}
        m = _match_gamma_for_event(ev, "basketball_nba", by)
        self.assertIsNotNone(m)
        assert m is not None
        self.assertEqual(m.slug, "test-nba")


if __name__ == "__main__":
    unittest.main()
