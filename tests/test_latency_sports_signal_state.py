"""Tests para arb/latency_sports_signal_state.py (emisión event-driven)."""

from __future__ import annotations

import math
import time

from arb.latency_sports_signal_state import (
    LegSignalState,
    get_signal_key,
    should_emit_signal,
    update_signal_state,
)


def _row(**kwargs: float) -> dict:
    base = {
        "edge_exec": 0.1,
        "price_poly": 0.45,
        "prob_pin": 0.55,
        "available_size": 20.0,
        "min_liquidity_threshold": 10.0,
        "min_edge": 0.05,
    }
    base.update(kwargs)
    return base


def test_get_signal_key_stable() -> None:
    k = get_signal_key("e1", ("a", "b"), "TEAM_HOME_YES", "tok42")
    assert k == ("e1", "a", "b", "TEAM_HOME_YES", "tok42")


def test_first_observation_strong_only() -> None:
    ok, r = should_emit_signal(None, _row(edge_exec=0.08), min_edge=0.05)
    assert ok and r == "FIRST_OBSERVATION_STRONG_EDGE"


def test_first_observation_weak_blocked() -> None:
    ok, r = should_emit_signal(None, _row(edge_exec=0.06), min_edge=0.05)
    assert not ok and r == "FIRST_OBSERVATION_WEAK"


def test_edge_crossing_after_dip() -> None:
    store: dict = {}
    key = get_signal_key("ev", ("x", "y"), "HOME", "t1")
    min_e = 0.05
    row_hi = _row(edge_exec=0.10, min_edge=min_e)
    assert should_emit_signal(None, row_hi, min_edge=min_e)[0]
    update_signal_state(store, key, row_hi, emitted=True, now_ts=time.monotonic())

    row_lo = _row(edge_exec=0.02, min_edge=min_e)
    update_signal_state(store, key, row_lo, emitted=False, now_ts=time.monotonic())

    row_back = _row(edge_exec=0.055, min_edge=min_e)
    ok, r = should_emit_signal(store[key], row_back, min_edge=min_e)
    assert ok and r == "EDGE_CROSSING_UP"


def test_relative_decay_reset_then_strong_re_emit() -> None:
    """0.20 → 0.19 (no reset) → 0.18 (reset por decay relativo) → 0.21 (nueva oportunidad)."""
    store: dict = {}
    key = get_signal_key("ev", ("a", "b"), "SIDE", "tok")
    min_e = 0.05
    r0 = _row(edge_exec=0.20, min_edge=min_e)
    assert should_emit_signal(None, r0, min_edge=min_e)[0]
    update_signal_state(store, key, r0, emitted=True, now_ts=1.0)

    update_signal_state(store, key, _row(edge_exec=0.19, min_edge=min_e), emitted=False, now_ts=2.0)
    assert math.isfinite(store[key].last_edge_exec)

    update_signal_state(store, key, _row(edge_exec=0.18, min_edge=min_e), emitted=False, now_ts=3.0)
    assert not math.isfinite(store[key].last_edge_exec)

    ok, r = should_emit_signal(store[key], _row(edge_exec=0.21, min_edge=min_e), min_edge=min_e)
    assert ok and r == "FIRST_STRONG_AFTER_DECAY_RESET"


def test_prob_jitter_suppressed() -> None:
    st = LegSignalState(
        last_edge_exec=0.14,
        last_price_poly=0.50,
        last_prob_pin=0.62,
        last_available_size=30.0,
        last_signal_ts=1.0,
        prev_edge_exec=0.14,
    )
    row = _row(edge_exec=0.141, price_poly=0.50, prob_pin=0.621, available_size=30.0)
    ok, r = should_emit_signal(st, row, min_edge=0.05)
    assert not ok
    assert r == "NO_NEW_INFORMATION"


def test_edge_improvement_triggers() -> None:
    st = LegSignalState(
        last_edge_exec=0.10,
        last_price_poly=0.45,
        last_prob_pin=0.55,
        last_available_size=20.0,
        last_signal_ts=1.0,
        prev_edge_exec=0.10,
    )
    row = _row(edge_exec=0.13, prob_pin=0.58)
    ok, r = should_emit_signal(st, row, min_edge=0.05)
    assert ok and r == "EDGE_IMPROVED"


def test_price_delta_triggers_when_edge_not_worse() -> None:
    st = LegSignalState(
        last_edge_exec=0.12,
        last_price_poly=0.50,
        last_prob_pin=0.62,
        last_available_size=25.0,
        last_signal_ts=1.0,
        prev_edge_exec=0.12,
    )
    row = _row(edge_exec=0.12, price_poly=0.53)
    ok, r = should_emit_signal(st, row, min_edge=0.05)
    assert ok and r == "PRICE_CHANGED"


def test_price_delta_suppressed_when_edge_degrades() -> None:
    st = LegSignalState(
        last_edge_exec=0.12,
        last_price_poly=0.50,
        last_prob_pin=0.62,
        last_available_size=25.0,
        last_signal_ts=1.0,
        prev_edge_exec=0.12,
    )
    row = _row(edge_exec=0.08, price_poly=0.53)
    ok, r = should_emit_signal(st, row, min_edge=0.05)
    assert not ok


def test_liquidity_cross_min_threshold() -> None:
    st = LegSignalState(
        last_edge_exec=0.10,
        last_price_poly=0.45,
        last_prob_pin=0.55,
        last_available_size=6.0,
        last_signal_ts=1.0,
        prev_edge_exec=0.10,
    )
    row = _row(edge_exec=0.10, available_size=11.0)
    ok, r = should_emit_signal(st, row, min_edge=0.05)
    assert ok and r == "LIQUIDITY_CROSS_MIN"


def test_liquidity_doubles_requires_abs_floor() -> None:
    st = LegSignalState(
        last_edge_exec=0.10,
        last_price_poly=0.45,
        last_prob_pin=0.55,
        last_available_size=40.0,
        last_signal_ts=1.0,
        prev_edge_exec=0.10,
    )
    row = _row(edge_exec=0.10, available_size=85.0)
    ok, r = should_emit_signal(st, row, min_edge=0.05)
    assert ok and r == "LIQUIDITY_X2"


def test_liquidity_small_double_blocked() -> None:
    """2×1$ no basta si el salto absoluto es < 50$ (defecto)."""
    st = LegSignalState(
        last_edge_exec=0.10,
        last_price_poly=0.45,
        last_prob_pin=0.55,
        last_available_size=1.0,
        last_signal_ts=1.0,
        prev_edge_exec=0.10,
    )
    row = _row(edge_exec=0.10, available_size=2.0)
    ok, r = should_emit_signal(st, row, min_edge=0.05)
    assert not ok
