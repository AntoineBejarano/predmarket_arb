"""Tests: is_valid_polymarket_moneyline (filtro semántico moneyline / -more-markets)."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from arb.latency_arb_sports import OpenPolymarketGame
from arb.latency_sports_moneyline_semantic import is_valid_polymarket_moneyline


def _base_game(**kw: object) -> OpenPolymarketGame:
    defaults: dict[str, object] = {
        "sport_slug": "basketball_nba",
        "home": "Los Angeles Lakers",
        "away": "Boston Celtics",
        "condition_id": "0xc1",
        "token_yes": "tok_y",
        "end_date": datetime(2026, 5, 2, tzinfo=timezone.utc),
        "raw_title": "Lakers vs. Celtics",
        "slug": "nba-lal-bos-2026-05-02",
        "outcome_tokens": [("Lakers", "t1"), ("Celtics", "t2")],
        "end_date_s": None,
        "market_question": "Will the Lakers beat the Celtics?",
        "market_slug": "mkt-lal-bos",
        "group_item_title": "NBA",
        "sports_market_type": "",
        "raw_market_json": None,
    }
    defaults.update(kw)
    return OpenPolymarketGame(**defaults)  # type: ignore[arg-type]


class TestMoneylineSemantic(unittest.TestCase):
    def test_nba_team_outcomes_beat_question_ok(self) -> None:
        ok, reason = is_valid_polymarket_moneyline(_base_game())
        self.assertTrue(ok)
        self.assertEqual(reason, "moneyline_semantic_ok")

    def test_more_markets_no_metadata_unverified(self) -> None:
        g = _base_game(
            slug="nba-lal-bos-2026-05-02-more-markets",
            market_question="",
            sports_market_type="",
        )
        ok, reason = is_valid_polymarket_moneyline(g)
        self.assertFalse(ok)
        self.assertEqual(reason, "more_markets_unverified")

    def test_more_markets_with_moneyline_and_explicit_win_ok(self) -> None:
        g = _base_game(
            slug="nba-lal-bos-2026-05-02-more-markets",
            sports_market_type="moneyline",
            market_question="Will the Lakers beat the Celtics?",
        )
        ok, reason = is_valid_polymarket_moneyline(g)
        self.assertTrue(ok)
        self.assertEqual(reason, "moneyline_semantic_ok")

    def test_over_under_question_rejected(self) -> None:
        g = _base_game(market_question="Will there be over 2.5 goals in the first half?")
        ok, reason = is_valid_polymarket_moneyline(g)
        self.assertFalse(ok)
        self.assertIn("prop", reason)

    def test_first_half_rejected(self) -> None:
        g = _base_game(market_question="Will the Lakers win the first half?")
        ok, reason = is_valid_polymarket_moneyline(g)
        self.assertFalse(ok)
        self.assertEqual(reason, "prop_first_half")

    def test_draw_no_bet_rejected(self) -> None:
        g = _base_game(market_question="Lakers draw no bet vs Celtics")
        ok, reason = is_valid_polymarket_moneyline(g)
        self.assertFalse(ok)
        self.assertEqual(reason, "prop_draw_no_bet")

    def test_ambiguous_outcomes_rejected(self) -> None:
        g = _base_game(
            home="North Huskies",
            away="South Lynx",
            raw_title="North Huskies vs. South Lynx",
            outcome_tokens=[("Mystery A", "t1"), ("Mystery B", "t2")],
            market_question="Will Mystery A beat Mystery B?",
        )
        ok, reason = is_valid_polymarket_moneyline(g)
        self.assertFalse(ok)
        self.assertEqual(reason, "outcomes_not_team_moneyline")


if __name__ == "__main__":
    unittest.main()
