#!/usr/bin/env python3
"""
Worker de arbitraje puro matemático.
Corre estrategias en asyncio.gather(); arrancado por scripts/api.py o local:
  python scripts/arb_engine.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [arb_engine] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("arb_engine")

DRY_RUN = True
ENABLE_EXPERIMENTAL = True

from arb.bundle_arb import BundleArbStrategy
from arb.combinatorial_arb import CombinatorialArbStrategy
from arb.cross_exchange import CrossExchangeStrategy
from arb.latency_arb import LatencyArbStrategy
from arb.latency_arb_sports import LatencyArbSportsStrategy
from arb.market_maker import MarketMakerStrategy
from arb.term_structure import TermStructureStrategy
from risk.circuit_breaker import CircuitBreaker
from risk.strategy_state import StrategyStateManager


async def main() -> None:
    state_mgr = StrategyStateManager()
    _st = await state_mgr.get_all()
    _n_on = sum(1 for v in _st.values() if v.get("enabled"))
    log.info(
        "Control plane: %s/%s estrategias enabled (solo esas ejecutan run_once / trabajo CLOB u otras APIs)",
        _n_on,
        len(_st),
    )

    breaker = CircuitBreaker(max_daily_drawdown=0.08)

    base_cap = {
        "circuit_breaker": breaker,
        "start_capital": 10_000.0,
        "current_capital": 10_000.0,
    }

    config_bundle = {
        **base_cap,
        "poll_interval": 10.0,
        "min_edge": 0.025,
        "max_size_usdc": 300.0,
        "max_outcomes": 8,
        "discovery": "gamma_events",
        "bundle_mode": "taker_scan",
        "target_bundle_usdc": 50.0,
        "max_outcomes_live": 4,
        "maker_live_enabled": False,
        "maker_post_only": True,
        "maker_order_type": "GTD",
        "use_ws": False,
        "use_vwap": False,
        "target_vwap_usdc": 50.0,
        "exec_buffer_per_leg": 0.0025,
        "min_depth_per_leg_usdc": 0.0,
        "max_candidates_per_cycle": 120,
        "exclude_neg_risk": True,
    }
    config_cross = {
        **base_cap,
        "poll_interval": 30.0,
        "min_edge": 0.030,
    }
    config_mm = {
        **base_cap,
        "poll_interval": 30.0,
    }

    strategies = [
        BundleArbStrategy(config_bundle, dry_run=DRY_RUN),
        CrossExchangeStrategy(config_cross, dry_run=DRY_RUN),
        MarketMakerStrategy(config_mm, dry_run=DRY_RUN),
    ]

    if ENABLE_EXPERIMENTAL:
        config_combo = {**base_cap, "poll_interval": 60.0}
        config_term = {**base_cap, "poll_interval": 300.0}
        config_lat = {**base_cap, "poll_interval": 15.0}
        # latency_arb_sports: umbrales y poll fijados en arb/latency_arb_sports.py (_HARDCODE_*), no por env.
        config_lat_sports = {**base_cap}
        strategies += [
            CombinatorialArbStrategy(config_combo, dry_run=DRY_RUN),
            TermStructureStrategy(config_term, dry_run=DRY_RUN),
            LatencyArbStrategy(config_lat, dry_run=DRY_RUN),
            LatencyArbSportsStrategy(config_lat_sports, dry_run=DRY_RUN),
        ]

    log.info(
        "asyncio.gather: %s bucles de estrategia en paralelo (las disabled solo duermen; trabajo útil ≈ %s activas). "
        "DRY_RUN=%s ENABLE_EXPERIMENTAL=%s",
        len(strategies),
        _n_on,
        DRY_RUN,
        ENABLE_EXPERIMENTAL,
    )
    if not ENABLE_EXPERIMENTAL:
        log.info(
            "[arb_engine] ENABLE_EXPERIMENTAL=false: solo bundle_arb, cross_exchange, market_maker. "
            "Para combinatorial_arb, term_structure, latency_arb o latency_arb_sports, define ENABLE_EXPERIMENTAL=true (p. ej. en Railway)."
        )
    await asyncio.gather(*(s.run_loop(state_mgr) for s in strategies))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupción por teclado, saliendo.")
