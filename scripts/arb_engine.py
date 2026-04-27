#!/usr/bin/env python3
"""
Worker de arbitraje puro matemático.
Corre estrategias en asyncio.gather(); arrancado por scripts/api.py o local:
  python scripts/arb_engine.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

load_dotenv(REPO_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [arb_engine] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("arb_engine")

DRY_RUN = os.getenv("DRY_RUN", "true").lower() in ("true", "1", "yes")
ENABLE_EXPERIMENTAL = os.getenv("ENABLE_EXPERIMENTAL", "false").lower() in ("true", "1", "yes")

from arb.bundle_arb import BundleArbStrategy
from arb.combinatorial_arb import CombinatorialArbStrategy
from arb.cross_exchange import CrossExchangeStrategy
from arb.latency_arb import LatencyArbStrategy
from arb.market_maker import MarketMakerStrategy
from arb.term_structure import TermStructureStrategy
from risk.circuit_breaker import CircuitBreaker
from risk.strategy_state import StrategyStateManager


async def main() -> None:
    state_mgr = StrategyStateManager()
    _st = await state_mgr.get_all()
    _n_on = sum(1 for v in _st.values() if v.get("enabled"))
    log.info("Control plane: %s/%s estrategias enabled (solo esas harán trabajo CLOB)", _n_on, len(_st))

    breaker = CircuitBreaker(max_daily_drawdown=float(os.getenv("MAX_DAILY_DRAWDOWN", "0.08")))

    base_cap = {
        "circuit_breaker": breaker,
        "start_capital": float(os.getenv("ARB_START_CAPITAL", "10000")),
        "current_capital": float(os.getenv("ARB_CURRENT_CAPITAL", "10000")),
    }

    config_bundle = {
        **base_cap,
        "poll_interval": float(os.getenv("BUNDLE_POLL_INTERVAL", "10")),
        "min_edge": float(os.getenv("BUNDLE_MIN_EDGE", "0.025")),
        "max_size_usdc": float(os.getenv("BUNDLE_MAX_SIZE_USDC", "300")),
        "discovery": os.getenv("BUNDLE_DISCOVERY", "gamma").strip().lower(),
        "use_ws": os.getenv("BUNDLE_USE_WS", "false").lower() in ("1", "true", "yes"),
        "max_candidates_per_cycle": int(os.getenv("BUNDLE_MAX_CANDIDATES_PER_CYCLE", "120")),
        "exclude_neg_risk": os.getenv("BUNDLE_EXCLUDE_NEG_RISK", "true").lower() in ("1", "true", "yes"),
    }
    config_cross = {
        **base_cap,
        "poll_interval": float(os.getenv("CROSS_POLL_INTERVAL", "30")),
        "min_edge": float(os.getenv("CROSS_MIN_EDGE", "0.030")),
    }
    config_mm = {
        **base_cap,
        "poll_interval": float(os.getenv("MM_QUOTE_INTERVAL", "30")),
    }

    strategies = [
        BundleArbStrategy(config_bundle, dry_run=DRY_RUN),
        CrossExchangeStrategy(config_cross, dry_run=DRY_RUN),
        MarketMakerStrategy(config_mm, dry_run=DRY_RUN),
    ]

    if ENABLE_EXPERIMENTAL:
        config_combo = {**base_cap, "poll_interval": float(os.getenv("COMBO_SCAN_INTERVAL", "60"))}
        config_term = {**base_cap, "poll_interval": float(os.getenv("TERM_SCAN_INTERVAL", "300"))}
        config_lat = {**base_cap, "poll_interval": float(os.getenv("LAT_POLL_INTERVAL", "15"))}
        strategies += [
            CombinatorialArbStrategy(config_combo, dry_run=DRY_RUN),
            TermStructureStrategy(config_term, dry_run=DRY_RUN),
            LatencyArbStrategy(config_lat, dry_run=DRY_RUN),
        ]

    log.info("Arrancando %s estrategias. DRY_RUN=%s ENABLE_EXPERIMENTAL=%s", len(strategies), DRY_RUN, ENABLE_EXPERIMENTAL)
    await asyncio.gather(*(s.run_loop(state_mgr) for s in strategies))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupción por teclado, saliendo.")
