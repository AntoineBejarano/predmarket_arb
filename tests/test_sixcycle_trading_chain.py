"""
Cadena de cálculo Sixcycle + sizing Polymarket (determinista, sin red).

Cubre: P(UP) desde señal del scorer → CLOBSignalFilter → post-filtros empíricos
→ Kelly ``cycle_size`` → shares BUY ``_buy_share_size_meets_min_notional``.
Si falla un caso aquí, el motor LIVE/DRY_RUN producirá números incoherentes con la misma lógica.
"""

from __future__ import annotations

import builtins

import pytest

from scripts.clob_signal_filter import CLOBSignalFilter
from scripts.polymarket_client import _buy_share_size_meets_min_notional


# --- Polymarket BUY share size (CLOB: min 5 shares + notional >= $1) ---


@pytest.mark.parametrize(
    ("amount_usdc", "px", "expected_shares", "min_notion"),
    [
        # Caso Railway: 1 USDC @ 0.26 → ~3.85 shares sin floor; con CLOB mín. 5 shares ≈ 1.30 USDC
        (1.0, 0.26, 5.0, 5.0 * 0.26),
        (10.0, 0.5, 20.0, 10.0),
        # Precio muy bajo: notional $1 fuerza 100 shares aunque 5 shares ya cumplan mín. CLOB
        (1.0, 0.01, 100.0, 1.0),
    ],
)
def test_buy_share_size_meets_clob_and_notional(
    amount_usdc: float, px: float, expected_shares: float, min_notion: float
) -> None:
    s = _buy_share_size_meets_min_notional(amount_usdc, px)
    assert abs(s - expected_shares) < 1e-9
    assert s * px + 1e-9 >= 1.0  # mínimo $1 marketable
    assert s + 1e-9 >= 5.0  # mínimo 5 shares CLOB
    assert s * px + 1e-9 >= min_notion - 1e-6


def test_buy_share_ticks_are_point_zero_one() -> None:
    s = _buy_share_size_meets_min_notional(2.55, 0.34)
    assert abs(s - round(s / 0.01) * 0.01) < 1e-9


# --- CLOBSignalFilter (misma fórmula que documenta el módulo) ---


def test_clob_filter_yes_edge_and_signal() -> None:
    f = CLOBSignalFilter()
    r = f.evaluate(0.65, 0.50, min_edge=0.05, min_liquidity_usdc=50.0, liquidity=100.0)
    assert r["signal"] is True
    assert r["direction"] == "YES"
    assert abs(r["edge"] - 0.15) < 1e-12


def test_clob_filter_no_edge_and_signal() -> None:
    f = CLOBSignalFilter()
    r = f.evaluate(0.35, 0.50, min_edge=0.05, min_liquidity_usdc=50.0, liquidity=100.0)
    assert r["signal"] is True
    assert r["direction"] == "NO"
    assert abs(r["edge"] - 0.15) < 1e-12


def test_clob_filter_no_signal_low_edge() -> None:
    f = CLOBSignalFilter()
    r = f.evaluate(0.55, 0.52, min_edge=0.05, min_liquidity_usdc=50.0, liquidity=100.0)
    assert r["signal"] is False
    assert abs(r["edge"] - 0.03) < 1e-12


# --- sixcycle_engine: model_prob → validate → size (engine real, config fija) ---


@pytest.fixture
def sixcycle_engine_isolated(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """Motor con CSV bajo tmp y config por defecto sin leer disco de DATA_DIR."""
    from scripts import sixcycle_config_store as scs
    from scripts import sixcycle_engine as se

    monkeypatch.setattr(se, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(scs, "load_config", lambda: dict(scs.DEFAULT_SIXCYCLE_CONFIG))
    monkeypatch.setattr(builtins, "print", lambda *a, **k: None)
    eng = se.SixCycleEngine(on_live_failure=None)
    return se, eng


def test_model_prob_from_signal_up_down(sixcycle_engine_isolated) -> None:
    se, _eng = sixcycle_engine_isolated
    assert abs(se._model_prob_from_signal({"direction": "UP", "confidence": 0.2}) - 0.6) < 1e-12
    assert abs(se._model_prob_from_signal({"direction": "DOWN", "confidence": 0.2}) - 0.4) < 1e-12
    assert se._model_prob_from_signal({"direction": None, "confidence": 0.9}) == 0.5


def test_cycle_validate_then_cycle_size_golden_path(sixcycle_engine_isolated) -> None:
    """
    Señal UP con confianza → model_prob 0.6; CLOB YES 0.15 → edge 0.45; fill YES=0.15
    pasa filtros empíricos (default); Kelly con cap 5 → stake mínimo 1 USDC.
    Shares para 1 USDC @ 0.15 cumplen CLOB + notional.
    """
    _se, eng = sixcycle_engine_isolated
    sig = {"ready": True, "direction": "UP", "confidence": 0.2, "score": 5}
    out = eng.cycle_validate(sig, clob_yes_price=0.15, liquidity=100.0, market=None)
    assert out.get("signal") is True
    assert out.get("direction") == "YES"
    assert abs(float(out["edge"]) - 0.45) < 1e-9

    stake = eng.cycle_size(float(out["edge"]))
    # 0.25 * 0.45 * 5 = 0.5625 → clamp mínimo 1.0
    assert abs(stake - 1.0) < 1e-9

    shares = _buy_share_size_meets_min_notional(stake, 0.15)
    assert shares * 0.15 >= 1.0 - 1e-6
    assert shares >= 5.0 - 1e-6


def test_cycle_validate_no_direction_returns_no_signal(sixcycle_engine_isolated) -> None:
    _se, eng = sixcycle_engine_isolated
    sig = {"ready": False, "direction": None, "confidence": 0.0, "score": 0}
    out = eng.cycle_validate(sig, clob_yes_price=0.5, liquidity=200.0, market=None)
    assert out["signal"] is False
    assert "scorer" in (out.get("reason") or "").lower()


def test_cycle_validate_empirical_rejects_low_score(sixcycle_engine_isolated) -> None:
    _se, eng = sixcycle_engine_isolated
    sig = {"ready": True, "direction": "UP", "confidence": 0.2, "score": 2}
    out = eng.cycle_validate(sig, clob_yes_price=0.15, liquidity=100.0, market=None)
    assert out["signal"] is False
    assert "score" in (out.get("reason") or "").lower()


def test_cycle_validate_empirical_rejects_fill_dead_zone(sixcycle_engine_isolated) -> None:
    """fill 0.20 cae en [0.18, 0.24) con config por defecto."""
    _se, eng = sixcycle_engine_isolated
    sig = {"ready": True, "direction": "UP", "confidence": 0.2, "score": 5}
    out = eng.cycle_validate(sig, clob_yes_price=0.20, liquidity=100.0, market=None)
    assert out["signal"] is False
    assert "zona muerta" in (out.get("reason") or "").lower()


def test_cycle_size_clamps_to_cap(sixcycle_engine_isolated) -> None:
    from scripts import sixcycle_config_store as scs

    _se, eng = sixcycle_engine_isolated
    eng._config = dict(scs.DEFAULT_SIXCYCLE_CONFIG)
    eng._config["max_stake_usdc"] = 100.0
    eng._config["kelly_fraction"] = 0.5
    # stake = 0.5 * 0.8 * 100 = 40
    s = eng.cycle_size(0.8)
    assert abs(s - 40.0) < 1e-9
    # stake = 0.5 * 10 * 100 = 500 → cap 100
    s2 = eng.cycle_size(10.0)
    assert abs(s2 - 100.0) < 1e-9
