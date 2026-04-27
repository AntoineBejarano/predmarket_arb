"""Tests NegRisk maker: quoter, policy default, batch response parse."""

from __future__ import annotations

import unittest

from arb.bundle_maker_quote import LegBook, compute_maker_quotes, order_outcomes_all_ok
from arb.negrisk_execution_policy import ExecutionPolicy, execution_policy_unknown_conservative


class TestExecutionPolicyDefault(unittest.TestCase):
    def test_unknown_no_taker_entry(self) -> None:
        p = execution_policy_unknown_conservative()
        self.assertEqual(p.fee_class, "unknown")
        self.assertFalse(p.taker_entry_allowed)


class TestMakerQuoter(unittest.TestCase):
    def test_sum_bids_under_one_minus_edge(self) -> None:
        pol = ExecutionPolicy(
            fee_class="unknown",
            maker_allowed=True,
            taker_entry_allowed=False,
            taker_hedge_allowed=True,
            taker_emergency_sell_allowed=True,
            min_maker_edge=0.04,
            min_taker_edge_fee_free=0.008,
            min_emergency_edge=0.004,
            max_partial_seconds=120.0,
            max_event_exposure_usdc=300.0,
            max_global_exposure_usdc=1500.0,
            max_partial_loss_usdc=5.0,
        )
        books = [
            LegBook("t1", 0.2, 0.22, 0.21, "0.01"),
            LegBook("t2", 0.25, 0.27, 0.26, "0.01"),
        ]
        qb, reason = compute_maker_quotes(books, pol, target_bundle_usdc=10.0)
        self.assertEqual(reason, "ok")
        assert qb is not None
        self.assertLessEqual(qb.sum_bids, 1.0 - pol.min_maker_edge + 1e-8)
        self.assertAlmostEqual(qb.legs[0].size_shares, qb.legs[1].size_shares, places=5)

    def test_bid_below_ask(self) -> None:
        pol = execution_policy_unknown_conservative()
        # best_bid + tick alcanza o supera el ask → pierna no cotizable
        books = [LegBook("t1", 0.49, 0.50, 0.495, "0.01")]
        qb, reason = compute_maker_quotes(books, pol, target_bundle_usdc=5.0)
        self.assertTrue(reason.startswith("no_raw_bid"))
        self.assertIsNone(qb)


class TestBatchResponseParse(unittest.TestCase):
    def test_200_with_per_order_error(self) -> None:
        resp = {"data": [{"success": True, "id": "a"}, {"success": False, "error": "bad"}]}
        ok, failed = order_outcomes_all_ok(resp)
        self.assertFalse(ok)
        self.assertEqual(len(failed), 1)

    def test_all_ok(self) -> None:
        resp = {"data": [{"success": True}, {"ok": True}]}
        ok, failed = order_outcomes_all_ok(resp)
        self.assertTrue(ok)
        self.assertEqual(failed, [])
