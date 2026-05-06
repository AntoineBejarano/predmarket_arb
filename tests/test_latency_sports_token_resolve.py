"""Tests: resolve_poly_odds_legs (token↔cuota IO alineada, sin YES=home genérico)."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from arb.latency_arb_sports import OpenPolymarketGame, resolve_poly_odds_legs


def _game_dual_yes(
    *,
    home: str = "Los Angeles Lakers",
    away: str = "Boston Celtics",
    labels: tuple[str, str] = ("Lakers", "Celtics"),
    tids: tuple[str, str] = ("tok_l", "tok_c"),
    title: str = "Lakers vs Celtics",
) -> OpenPolymarketGame:
    return OpenPolymarketGame(
        sport_slug="basketball_nba",
        home=home,
        away=away,
        condition_id="0x1",
        token_yes="dummy",
        end_date=datetime(2026, 5, 1, tzinfo=timezone.utc),
        raw_title=title,
        slug="nba-test",
        outcome_tokens=[(labels[0], tids[0]), (labels[1], tids[1])],
        end_date_s=None,
    )


class TestResolvePolyOddsLegs(unittest.TestCase):
    def test_dual_yes_team_labels_semantic_sides(self) -> None:
        g = _game_dual_yes()
        legs, err = resolve_poly_odds_legs(
            g, "Lakers", "Celtics", 0.55, 0.45, sport_slug="basketball_nba"
        )
        self.assertEqual(err, "")
        self.assertEqual(len(legs), 2)
        self.assertEqual({x.side for x in legs}, {"TEAM_HOME_YES", "TEAM_AWAY_YES"})
        by_tok = {x.token_id: x for x in legs}
        self.assertAlmostEqual(by_tok["tok_l"].odds_prob, 0.55)
        self.assertAlmostEqual(by_tok["tok_c"].odds_prob, 0.45)

    def test_swapped_outcome_order_maps_away_yes_to_p_away(self) -> None:
        """Primer outcome es away en Gamma: ese token debe llevar p_a_fair."""
        g = _game_dual_yes(
            labels=("Boston Celtics", "Los Angeles Lakers"),
            tids=("t_c", "t_l"),
        )
        legs, err = resolve_poly_odds_legs(
            g,
            "Los Angeles Lakers",
            "Boston Celtics",
            0.52,
            0.48,
            sport_slug="basketball_nba",
        )
        self.assertEqual(err, "")
        bt = {x.token_id: x for x in legs}
        self.assertAlmostEqual(bt["t_l"].odds_prob, 0.52)
        self.assertAlmostEqual(bt["t_c"].odds_prob, 0.48)

    def test_literal_yes_no_title_home(self) -> None:
        g = OpenPolymarketGame(
            sport_slug="basketball_nba",
            home="Lakers",
            away="Celtics",
            condition_id="x",
            token_yes="y",
            end_date=None,
            raw_title="Will the Lakers win?",
            slug="s",
            outcome_tokens=[("Yes", "tid_y"), ("No", "tid_n")],
            end_date_s=None,
        )
        legs, err = resolve_poly_odds_legs(
            g, "Lakers", "Celtics", 0.61, 0.39, sport_slug="basketball_nba"
        )
        self.assertEqual(err, "")
        self.assertEqual({x.side for x in legs}, {"LITERAL_YES", "LITERAL_NO"})
        by_tok = {x.token_id: x for x in legs}
        self.assertAlmostEqual(by_tok["tid_y"].odds_prob, 0.61)
        self.assertAlmostEqual(by_tok["tid_n"].odds_prob, 0.39)

    @patch("arb.latency_arb_sports.single_side_match_score", return_value=0.95)
    def test_ambiguous_perm_empty(self, _m: object) -> None:
        g = _game_dual_yes()
        legs, err = resolve_poly_odds_legs(
            g, "Lakers", "Celtics", 0.5, 0.5, sport_slug="basketball_nba"
        )
        self.assertEqual(legs, [])
        self.assertEqual(err, "ambiguous_perm")

    def test_need_two_outcomes(self) -> None:
        g = _game_dual_yes()
        g = OpenPolymarketGame(
            sport_slug=g.sport_slug,
            home=g.home,
            away=g.away,
            condition_id=g.condition_id,
            token_yes=g.token_yes,
            end_date=g.end_date,
            raw_title=g.raw_title,
            slug=g.slug,
            outcome_tokens=[("Only", "t1")],
            end_date_s=g.end_date_s,
        )
        legs, err = resolve_poly_odds_legs(g, "A", "B", 0.5, 0.5, sport_slug="basketball_nba")
        self.assertEqual(legs, [])
        self.assertEqual(err, "need_two_outcomes")


if __name__ == "__main__":
    unittest.main()
