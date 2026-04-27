"""Tests descubrimiento bundle_arb, parseo API y registry (sin red)."""

from __future__ import annotations

import unittest

from clients.poly_parse import api_bool_true, clob_market_tradeable, gamma_market_token_ids
from clients.poly_markets import MarketsRegistry


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
