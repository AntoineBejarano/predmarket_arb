"""Tests descubrimiento bundle_arb, parseo API y registry (sin red)."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch

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
    extract_yes_token_id,
    gamma_market_child_discoverable,
    gamma_market_child_eligible,
    gamma_market_token_ids,
    gamma_yes_token_id,
    parse_json_list_maybe,
    parse_outcomes_list,
)


class TestPolyParse(unittest.TestCase):
    def test_parse_json_list_maybe_list_real(self) -> None:
        v, err = parse_json_list_maybe(["a", "b"])
        self.assertEqual(v, ["a", "b"])
        self.assertIsNone(err)

    def test_parse_json_list_maybe_string_json(self) -> None:
        v, err = parse_json_list_maybe('["a","b"]')
        self.assertEqual(v, ["a", "b"])
        self.assertIsNone(err)

    def test_parse_json_list_maybe_malformed(self) -> None:
        v, err = parse_json_list_maybe("[a,b")
        self.assertIsNone(v)
        self.assertEqual(err, "malformed_json_list")

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

    def test_extract_yes_token_yes_no(self) -> None:
        yid, src, reason = extract_yes_token_id(["Yes", "No"], ["tok_yes", "tok_no"])
        self.assertEqual(yid, "tok_yes")
        self.assertEqual(src, "explicit_yes_outcome")
        self.assertIsNone(reason)

    def test_extract_yes_token_no_yes(self) -> None:
        yid, src, reason = extract_yes_token_id(["No", "Yes"], ["tok_no", "tok_yes"])
        self.assertEqual(yid, "tok_yes")
        self.assertEqual(src, "explicit_yes_outcome")
        self.assertIsNone(reason)

    def test_extract_yes_token_missing_yes(self) -> None:
        yid, src, reason = extract_yes_token_id(["Up", "Down"], ["tok_up", "tok_down"])
        self.assertIsNone(yid)
        self.assertEqual(src, "unknown")
        self.assertEqual(reason, "no_yes_outcome")

    def test_outcomes_list_of_dicts(self) -> None:
        yid, src, reason = extract_yes_token_id(
            [{"label": "No"}, {"name": "Yes"}],
            ["tok_no", "tok_yes"],
        )
        self.assertEqual(yid, "tok_yes")
        self.assertEqual(src, "explicit_yes_outcome")
        self.assertIsNone(reason)

    def test_clob_token_ids_string_json_with_spaces(self) -> None:
        m = {"clobTokenIds": ' [ "111" , "222" ] '}
        self.assertEqual(gamma_market_token_ids(m), ["111", "222"])

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


class _FakeResponse:
    def __init__(self, payload: dict[str, Any], status: int = 200):
        self.status = status
        self._payload = payload

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def text(self) -> str:
        return json.dumps(self._payload)


class _FakeSession:
    def __init__(self, payload: dict[str, Any]):
        self._payload = payload

    def get(self, *args, **kwargs) -> _FakeResponse:
        return _FakeResponse(self._payload)


class _FakeSessionPages:
    """Sesión fake que devuelve una página distinta en cada GET (orden fijo)."""

    def __init__(self, pages: list[dict[str, Any]]):
        self._pages = pages
        self._i = 0

    def get(self, *args, **kwargs) -> _FakeResponse:
        payload = self._pages[min(self._i, len(self._pages) - 1)]
        self._i += 1
        return _FakeResponse(payload)


class TestGammaEventsDiscoveryAudit(unittest.IsolatedAsyncioTestCase):
    async def test_valid_event_builds_candidate(self) -> None:
        event = {
            "id": "evt-1",
            "slug": "evt-1",
            "negRisk": True,
            "negRiskAugmented": False,
            "endDate": "2099-01-01T00:00:00Z",
            "markets": [
                {
                    "conditionId": "c1",
                    "active": True,
                    "closed": False,
                    "outcomes": '["Yes","No"]',
                    "clobTokenIds": '["y1","n1"]',
                },
                {
                    "conditionId": "c2",
                    "active": True,
                    "closed": False,
                    "outcomes": '["No","Yes"]',
                    "clobTokenIds": '["n2","y2"]',
                },
            ],
        }
        reg = MarketsRegistry(min_outcomes=2, max_outcomes=5, gamma_events_max_pages=1, gamma_events_limit=10)
        payload = {"events": [event], "next_cursor": ""}
        with patch.dict(
            os.environ,
            {
                "BUNDLE_DISCOVERY_AUDIT": "false",
                "BUNDLE_MIN_DAYS_TO_EXPIRY": "0",
                "BUNDLE_MAX_DAYS_TO_EXPIRY": "50000",
            },
            clear=False,
        ):
            cands, diag = await reg.discover_gamma_events_keyset(_FakeSession(payload))
        self.assertEqual(len(cands), 1)
        self.assertEqual(diag.get("candidates_built"), 1)

    async def test_augmented_precedence_allow_over_skip(self) -> None:
        event = {
            "id": "evt-aug",
            "slug": "evt-aug",
            "negRisk": True,
            "negRiskAugmented": True,
            "endDate": "2099-01-01T00:00:00Z",
            "markets": [
                {
                    "conditionId": "c1",
                    "active": True,
                    "closed": False,
                    "outcomes": ["Yes", "No"],
                    "clobTokenIds": ["y1", "n1"],
                },
                {
                    "conditionId": "c2",
                    "active": True,
                    "closed": False,
                    "outcomes": ["Yes", "No"],
                    "clobTokenIds": ["y2", "n2"],
                },
            ],
        }
        reg = MarketsRegistry(min_outcomes=2, max_outcomes=5, gamma_events_max_pages=1, gamma_events_limit=10)
        payload = {"events": [event], "next_cursor": ""}
        env = {
            "BUNDLE_ALLOW_AUGMENTED_NEGRISK": "true",
            "BUNDLE_SKIP_AUGMENTED": "true",
            "BUNDLE_DISCOVERY_AUDIT": "false",
            "BUNDLE_MIN_DAYS_TO_EXPIRY": "0",
            "BUNDLE_MAX_DAYS_TO_EXPIRY": "50000",
        }
        with patch.dict(os.environ, env, clear=False):
            cands, diag = await reg.discover_gamma_events_keyset(_FakeSession(payload))
        self.assertEqual(len(cands), 1)
        self.assertEqual(diag.get("skip_augmented"), 0)

    async def test_length_mismatch_rejected_and_audited(self) -> None:
        bad = {
            "id": "evt-mismatch",
            "slug": "evt-mismatch",
            "negRisk": True,
            "negRiskAugmented": False,
            "endDate": "2099-01-01T00:00:00Z",
            "markets": [
                {
                    "conditionId": "c1",
                    "active": True,
                    "closed": False,
                    "outcomes": '["Yes","No"]',
                    "clobTokenIds": '["only_one"]',
                }
            ],
        }
        reg = MarketsRegistry(min_outcomes=2, max_outcomes=5, gamma_events_max_pages=1, gamma_events_limit=10)
        payload = {"events": [bad], "next_cursor": ""}
        with tempfile.TemporaryDirectory() as td:
            env = {
                "DATA_DIR": td,
                "BUNDLE_DISCOVERY_AUDIT": "true",
                "BUNDLE_DISCOVERY_AUDIT_RAW_SAMPLES": "20",
                "BUNDLE_MIN_DAYS_TO_EXPIRY": "0",
                "BUNDLE_MAX_DAYS_TO_EXPIRY": "50000",
            }
            with patch.dict(os.environ, env, clear=False):
                cands, diag = await reg.discover_gamma_events_keyset(_FakeSession(payload))
            self.assertEqual(len(cands), 0)
            self.assertTrue(diag.get("sample_rejects_available"))
            p = os.path.join(td, "logs", "negrisk_discovery_reject_samples.json")
            with open(p, "r", encoding="utf-8") as fh:
                js = json.load(fh)
            self.assertIn("samples_by_reason", js)
            self.assertIn("length_mismatch", js["samples_by_reason"])

    async def test_sample_cap_per_reason(self) -> None:
        events = []
        for i in range(10):
            events.append(
                {
                    "id": f"evt-{i}",
                    "slug": f"evt-{i}",
                    "negRisk": True,
                    "negRiskAugmented": False,
                    "endDate": "2099-01-01T00:00:00Z",
                    "markets": [
                        {
                            "conditionId": f"c{i}",
                            "active": True,
                            "closed": False,
                            "outcomes": '["Yes","No"]',
                            "clobTokenIds": '["only_one"]',
                        }
                    ],
                }
            )
        reg = MarketsRegistry(min_outcomes=2, max_outcomes=5, gamma_events_max_pages=1, gamma_events_limit=50)
        payload = {"events": events, "next_cursor": ""}
        with tempfile.TemporaryDirectory() as td:
            env = {
                "DATA_DIR": td,
                "BUNDLE_DISCOVERY_AUDIT": "true",
                "BUNDLE_DISCOVERY_AUDIT_RAW_SAMPLES": "7",
                "BUNDLE_MIN_DAYS_TO_EXPIRY": "0",
                "BUNDLE_MAX_DAYS_TO_EXPIRY": "50000",
            }
            with patch.dict(os.environ, env, clear=False):
                await reg.discover_gamma_events_keyset(_FakeSession(payload))
            p = os.path.join(td, "logs", "negrisk_discovery_reject_samples.json")
            with open(p, "r", encoding="utf-8") as fh:
                js = json.load(fh)
            lm = js["samples_by_reason"].get("length_mismatch", [])
            self.assertLessEqual(len(lm), 5)
            self.assertLessEqual(js.get("sample_count", 0), 7)

    async def test_audit_limit_does_not_stop_event_scan(self) -> None:
        def _ev(i: int) -> dict[str, Any]:
            return {
                "id": f"e{i}",
                "slug": f"e{i}",
                "negRisk": False,
                "negRiskAugmented": False,
                "endDate": "2099-01-01T00:00:00Z",
                "markets": [],
            }

        page1 = {"events": [_ev(i) for i in range(60)], "next_cursor": "c1"}
        page2 = {"events": [_ev(i + 60) for i in range(60)], "next_cursor": ""}
        reg = MarketsRegistry(min_outcomes=2, max_outcomes=5, gamma_events_max_pages=2, gamma_events_limit=60)
        env = {
            "BUNDLE_DISCOVERY_AUDIT": "false",
            "BUNDLE_DISCOVERY_AUDIT_LIMIT": "100",
            "BUNDLE_MIN_DAYS_TO_EXPIRY": "0",
            "BUNDLE_MAX_DAYS_TO_EXPIRY": "50000",
        }
        with patch.dict(os.environ, env, clear=False):
            _cands, diag = await reg.discover_gamma_events_keyset(_FakeSessionPages([page1, page2]))
        self.assertEqual(diag.get("events_seen_total"), 120)
        self.assertEqual(diag.get("skip_not_negrisk"), 120)

    async def test_audit_on_empty_writes_files_without_full_audit_flag(self) -> None:
        ev = {
            "id": "evt-empty",
            "slug": "evt-empty",
            "negRisk": True,
            "negRiskAugmented": False,
            "endDate": "2099-01-01T00:00:00Z",
            "markets": [
                {
                    "conditionId": "c1",
                    "active": True,
                    "closed": False,
                    "restricted": True,
                    "outcomes": ["Yes", "No"],
                    "clobTokenIds": ["y1", "n1"],
                }
            ],
        }
        reg = MarketsRegistry(min_outcomes=2, max_outcomes=5, gamma_events_max_pages=1, gamma_events_limit=10)
        payload = {"events": [ev], "next_cursor": ""}
        with tempfile.TemporaryDirectory() as td:
            env = {
                "DATA_DIR": td,
                "DRY_RUN": "true",
                "BUNDLE_DISCOVERY_AUDIT": "false",
                "BUNDLE_MIN_DAYS_TO_EXPIRY": "0",
                "BUNDLE_MAX_DAYS_TO_EXPIRY": "50000",
            }
            with patch.dict(os.environ, env, clear=False):
                cands, diag = await reg.discover_gamma_events_keyset(_FakeSession(payload))
            self.assertEqual(len(cands), 0)
            self.assertTrue(diag.get("discovery_audit_path"))
            audit_p = os.path.join(td, "logs", "negrisk_discovery_audit.json")
            self.assertTrue(os.path.isfile(audit_p))
