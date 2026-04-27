"""Tests descubrimiento bundle_arb, parseo API y registry (sin red)."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from arb.bundle_pricing import vwap_buy_avg_price
from clients.poly_markets import (
    MarketsRegistry,
    NegRiskBundleCandidate,
    NegRiskLeg,
    _parse_end_date,
    _score_negrisk_event,
)
from clients.poly_parse import (
    api_bool_true,
    clob_market_tradeable,
    gamma_market_child_discoverable,
    gamma_market_child_eligible,
    gamma_market_token_ids,
    gamma_yes_token_id,
    parse_outcomes_list,
)


class TestPolyParse(unittest.TestCase):
    def test_api_bool_true(self) -> None:
        self.assertTrue(api_bool_true(True))
        self.assertFalse(api_bool_true(False))
        self.assertTrue(api_bool_true("true"))
        self.assertTrue(api_bool_true("1"))
        self.assertFalse(api_bool_true("false"))
        self.assertFalse(api_bool_true("0"))
        self.assertFalse(api_bool_true(None))

    def test_clob_market_tradeable_string_false_not_closed(self) -> None:
        m = {
            "accepting_orders": True,
            "closed": "false",
            "enable_order_book": True,
        }
        ok, _ = clob_market_tradeable(m)
        self.assertTrue(ok)

    def test_clob_market_tradeable_string_true_closed(self) -> None:
        m = {
            "accepting_orders": True,
            "closed": "true",
            "enable_order_book": True,
        }
        ok, reason = clob_market_tradeable(m)
        self.assertFalse(ok)
        self.assertEqual(reason, "closed")

    def test_gamma_market_token_ids_json_string(self) -> None:
        m = {"clobTokenIds": '["111", "222"]'}
        self.assertEqual(gamma_market_token_ids(m), ["111", "222"])

    def test_parse_outcomes_and_yes_token(self) -> None:
        m = {
            "outcomes": '["Yes", "No"]',
            "clobTokenIds": '["tok_yes", "tok_no"]',
            "acceptingOrders": True,
            "enableOrderBook": True,
            "closed": False,
            "active": True,
        }
        self.assertEqual(parse_outcomes_list(m), ["Yes", "No"])
        self.assertEqual(gamma_yes_token_id(m), "tok_yes")

    def test_gamma_market_child_eligible(self) -> None:
        good = {
            "outcomes": '["Yes", "No"]',
            "clobTokenIds": '["a", "b"]',
            "acceptingOrders": True,
            "enableOrderBook": True,
            "closed": False,
            "active": True,
        }
        self.assertTrue(gamma_market_child_eligible(good)[0])
        bad = {**good, "active": False}
        self.assertFalse(gamma_market_child_eligible(bad)[0])

    def test_gamma_market_child_discoverable_allows_missing_clob_flags(self) -> None:
        """Hijos en events keyset suelen omitir acceptingOrders / enableOrderBook."""
        m = {
            "active": True,
            "closed": False,
            "outcomes": '["Yes", "No"]',
            "clobTokenIds": '["a", "b"]',
        }
        self.assertTrue(gamma_market_child_discoverable(m)[0])
        self.assertFalse(gamma_market_child_eligible(m)[0])

    def test_gamma_market_child_discoverable_rejects_explicit_not_accepting(self) -> None:
        m = {
            "active": True,
            "closed": False,
            "outcomes": '["Yes", "No"]',
            "clobTokenIds": '["a", "b"]',
            "acceptingOrders": False,
        }
        self.assertFalse(gamma_market_child_discoverable(m)[0])


class TestVwapPricing(unittest.TestCase):
    def test_vwap_single_level(self) -> None:
        asks = [{"price": "0.4", "size": "100"}]
        px, spent, best = vwap_buy_avg_price(asks, 10.0)
        self.assertAlmostEqual(px, 0.4, places=6)
        self.assertAlmostEqual(spent, 10.0, places=5)
        self.assertAlmostEqual(best, 40.0, places=5)

    def test_vwap_multi_level(self) -> None:
        asks = [
            {"price": "0.5", "size": "10"},
            {"price": "0.6", "size": "100"},
        ]
        px, spent, _best = vwap_buy_avg_price(asks, 8.0)
        self.assertAlmostEqual(spent, 8.0, places=5)
        # 5 USDC a 0.5 (10 shares) + 3 USDC a 0.6 (5 shares) → 8/15
        self.assertAlmostEqual(px, 8.0 / 15.0, places=6)


class TestNegRiskScoring(unittest.TestCase):
    def test_score_positive(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        ev = {
            "endDate": "2026-03-01T00:00:00Z",
            "liquidityClob": 1000,
            "volume24hr": 500,
        }
        s = _score_negrisk_event(ev, n_legs=3, now=now)
        self.assertGreater(s, 0)


class TestMarketsRegistry(unittest.IsolatedAsyncioTestCase):
    async def test_gamma_filters_outcomes(self) -> None:
        reg = MarketsRegistry(min_outcomes=2, max_outcomes=3, gamma_max_pages=1, gamma_limit=10)
        m_ok = {
            "conditionId": "0xabc",
            "question": "q",
            "clobTokenIds": ["1", "2"],
            "liquidityNum": 99999,
            "volume24hr": 99999,
            "endDate": "2099-01-01T00:00:00Z",
        }
        m_one_leg = {**m_ok, "clobTokenIds": ["1"]}
        self.assertTrue(reg._passes_gamma_filters(m_ok, 2)[0])
        self.assertFalse(reg._passes_gamma_filters(m_one_leg, 1)[0])

    def test_parse_end_date_event(self) -> None:
        ev = {"endDate": "2099-06-01T00:00:00Z"}
        dt = _parse_end_date(ev)
        self.assertIsNotNone(dt)
        assert dt is not None
        self.assertEqual(dt.tzinfo, timezone.utc)

    def test_negrisk_candidate_dataclass(self) -> None:
        c = NegRiskBundleCandidate(
            event_id="evt1",
            slug="s",
            end_date_iso="2099-01-01",
            legs=[NegRiskLeg("0xc1", "t1", "q1")],
            score=1.0,
        )
        self.assertEqual(len(c.legs), 1)
        self.assertEqual(c.legs[0].yes_token_id, "t1")
