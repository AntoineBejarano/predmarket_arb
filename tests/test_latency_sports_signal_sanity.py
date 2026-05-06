"""Tests para arb/latency_sports_signal_sanity.py."""

from __future__ import annotations

from arb.latency_sports_signal_sanity import (
    MAX_ABS_EDGE_FOR_SIGNAL,
    MAX_SUM_EDGE_MAG,
    compute_edge_safe,
    map_reason_to_skip_action,
    normalize_probabilities,
    normalize_team_name,
    normalized_team_pair,
    should_emit_signal,
    validate_market_row,
)


def test_normalize_team_name_sparta_prague_b() -> None:
    assert normalize_team_name("Sparta Prague B") == "sparta prague"


def test_normalize_team_name_strips_fc_suffix() -> None:
    assert normalize_team_name("FC Barcelona") == "barcelona"


def test_normalized_team_pair_order_invariant() -> None:
    a = normalized_team_pair("Team B", "Team A")
    b = normalized_team_pair("Team A", "Team B")
    assert a == b


def test_should_emit_signal_prob_sum_two_way() -> None:
    ok, _ = should_emit_signal(
        {
            "p_h_fair": 0.5,
            "p_a_fair": 0.5,
            "p_draw_fair": None,
            "skip_leg_edge_check": True,
        }
    )
    assert ok
    ok2, r2 = should_emit_signal(
        {
            "p_h_fair": 0.6,
            "p_a_fair": 0.6,
            "p_draw_fair": None,
            "skip_leg_edge_check": True,
        }
    )
    assert not ok2
    assert r2 == "REF_PROB_NORMALIZE"


def test_should_emit_signal_both_sides_positive() -> None:
    ok, r = should_emit_signal(
        {
            "p_h_fair": 0.5,
            "p_a_fair": 0.5,
            "p_draw_fair": None,
            "mids_both_present": True,
            "edge_home": 0.1,
            "edge_away": 0.05,
            "skip_leg_edge_check": True,
        }
    )
    assert not ok
    assert r == "BOTH_SIDES_POSITIVE"


def test_should_emit_signal_inconsistent_pricing_sum() -> None:
    ok, r = should_emit_signal(
        {
            "p_h_fair": 0.55,
            "p_a_fair": 0.45,
            "p_draw_fair": None,
            "mids_both_present": True,
            "edge_home": 0.2,
            "edge_away": -0.05,
            "skip_leg_edge_check": True,
        }
    )
    assert not ok
    assert r == "INCONSISTENT_PRICING"
    assert abs(0.2 + (-0.05)) > MAX_SUM_EDGE_MAG


def test_should_emit_signal_abs_edge_veto() -> None:
    ok, r = should_emit_signal(
        {
            "p_h_fair": 0.5,
            "p_a_fair": 0.5,
            "p_draw_fair": None,
            "mids_both_present": False,
            "edge_exec": -(MAX_ABS_EDGE_FOR_SIGNAL + 0.01),
            "edge_mid": 0.0,
        }
    )
    assert not ok
    assert r == "EDGE_IMPLAUSIBLE"


def test_compute_edge_safe() -> None:
    import math

    assert abs(compute_edge_safe(0.7, 0.5) - 0.2) < 1e-9
    assert math.isnan(compute_edge_safe(1.5, 0.5))


def test_normalize_probabilities_roundtrip() -> None:
    ph, pa, pd = normalize_probabilities(2.0, 2.0, None)
    assert abs(ph + pa - 1.0) < 1e-9
    assert pd is None


def test_validate_market_row_rejects_ou() -> None:
    ok, _ = validate_market_row({"home_team": "Over", "away_team": "Under"})
    assert not ok


def test_map_reason_to_skip_action() -> None:
    assert map_reason_to_skip_action("EDGE_IMPLAUSIBLE") == "SKIP:EDGE_IMPLAUSIBLE"
