"""Tests unitarios: Odds API helpers y matching equipos / Gamma."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from arb.latency_arb_sports import (
    GammaSportMarket,
    _event_matches_odds_teams,
    _odds_io_reference_instant_utc,
    _odds_io_updated_age_sec,
    _pick_best_market_in_event,
)
from clients.odds_api import implied_prob, remove_vig, teams_match_odds_gamma
from clients.odds_api_io import OddsEvent, find_event_matching_teams


class TestStaleReferenceUtc(unittest.TestCase):
    def test_ws_received_trumps_stale_book_updated_at(self) -> None:
        recent = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
        ev = OddsEvent(
            home="A",
            away="B",
            home_odds=2.0,
            away_odds=2.0,
            draw_odds=None,
            bookie="Betfair Exchange",
            updated_at="2000-01-01T00:00:00",
            event_id="x",
            sport="tennis",
            ws_received_at_utc=recent,
        )
        ref = _odds_io_reference_instant_utc(ev)
        assert ref is not None
        age = _odds_io_updated_age_sec(ev)
        assert age is not None
        self.assertLess(age, 30.0, msg="age should use ws_received_at, not ancient updated_at")


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

    def test_nba_cle_tor_variants(self) -> None:
        nba = "basketball_nba"
        self.assertTrue(
            teams_match_odds_gamma("Cleveland Cavaliers", "Cavaliers vs Raptors", sport_slug=nba)
        )
        self.assertTrue(
            teams_match_odds_gamma("Toronto Raptors", "Cavaliers vs Raptors", sport_slug=nba)
        )
        self.assertTrue(
            teams_match_odds_gamma("CLE", "cle vs tor", sport_slug=nba)
        )
        self.assertTrue(
            teams_match_odds_gamma("Toronto Raptors", "Cleveland Cavaliers vs Toronto Raptors", sport_slug=nba)
        )
        self.assertTrue(
            teams_match_odds_gamma("Cleveland Cavaliers", "Toronto Raptors vs Cleveland Cavaliers", sport_slug=nba)
        )

    def test_find_event_matching_teams_nba_swap(self) -> None:
        ev = OddsEvent(
            home="Cleveland Cavaliers",
            away="Toronto Raptors",
            home_odds=2.0,
            away_odds=2.0,
            draw_odds=None,
            bookie="Betfair Exchange",
            updated_at="",
            event_id="e1",
            sport="basketball",
        )
        pool = [ev]
        hit = find_event_matching_teams(pool, "Raptors", "Cavaliers", "basketball_nba")
        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.event_id, "e1")

    def test_epl_man_utd_spurs(self) -> None:
        epl = "soccer_epl"
        self.assertTrue(
            teams_match_odds_gamma("Manchester United", "Man United vs Spurs", sport_slug=epl)
        )
        self.assertTrue(
            teams_match_odds_gamma("Tottenham Hotspur", "Man United vs Spurs", sport_slug=epl)
        )
        self.assertTrue(
            teams_match_odds_gamma("Tottenham Hotspur", "Tottenham vs Manchester United", sport_slug=epl)
        )
        ev = OddsEvent(
            home="Manchester United",
            away="Tottenham Hotspur",
            home_odds=2.0,
            away_odds=2.0,
            draw_odds=None,
            bookie="Betfair Exchange",
            updated_at="",
            event_id="epl1",
            sport="football",
        )
        hit = find_event_matching_teams(
            [ev], "Man United", "Spurs", epl
        )
        self.assertIsNotNone(hit)


if __name__ == "__main__":
    unittest.main()
