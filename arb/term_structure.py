"""Term structure / Poisson mispricing (stub sin pares calibrados)."""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any

import numpy as np

from arb.base import ArbStrategy

REPO_ROOT = Path(__file__).resolve().parent.parent


class TermStructureStrategy(ArbStrategy):
    slug = "term_structure"
    name = "Term Structure Arbitrage"

    @property
    def csv_columns(self) -> list[str]:
        return [
            "ts",
            "strategy",
            "action",
            "reason",
            "dry_run",
            "market_id_short",
            "market_id_long",
            "price_short",
            "price_long",
            "model_short",
            "model_long",
            "lambda_hat",
            "mispricing",
            "edge",
        ]

    def __init__(self, config: dict[str, Any], dry_run: bool = True) -> None:
        super().__init__(config, dry_run=dry_run)
        self.min_misp = float(config.get("min_mispricing", os.getenv("TERM_MIN_MISPRICING", "0.15")))
        self.history_path = Path(
            config.get("history_path", REPO_ROOT / "data" / "resolution_history.csv")
        )
        self._breaker = config.get("circuit_breaker")
        self._start_capital = float(config.get("start_capital", os.getenv("ARB_START_CAPITAL", "10000")))
        self._current_capital = float(config.get("current_capital", os.getenv("ARB_CURRENT_CAPITAL", "10000")))

    def _load_lambda(self) -> float:
        if not self.history_path.is_file():
            return 1e-6
        with open(self.history_path, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        if len(rows) < 2:
            return 1e-6
        days = []
        for r in rows:
            try:
                days.append(float(r.get("days_to_resolve", 0) or 0))
            except ValueError:
                continue
        if not days:
            return 1e-6
        return float(max(1e-8, 1.0 / (np.mean(np.array(days)) + 1e-6)))

    async def run_once(self) -> None:
        if self._breaker:
            ok = await self._breaker.check(self._current_capital, self._start_capital)
            if not ok:
                self.log_signal(
                    {
                        "action": "SKIP:CIRCUIT_BREAKER",
                        "reason": "max_daily_drawdown exceeded",
                        "market_id_short": "",
                        "market_id_long": "",
                        "price_short": "",
                        "price_long": "",
                        "model_short": "",
                        "model_long": "",
                        "lambda_hat": "",
                        "mispricing": "",
                        "edge": "",
                    }
                )
                return

        lam = self._load_lambda()
        if lam <= 1e-5:
            self.log_signal(
                {
                    "action": "SKIP:NO_MARKETS",
                    "reason": "insufficient resolution_history for lambda",
                    "market_id_short": "",
                    "market_id_long": "",
                    "price_short": "",
                    "price_long": "",
                    "model_short": "",
                    "model_long": "",
                    "lambda_hat": str(lam),
                    "mispricing": "0",
                    "edge": "0",
                }
            )
            return

        self.log_signal(
            {
                "action": "SKIP:NO_MARKETS",
                "reason": "term pairs scan not wired — lambda_hat available",
                "market_id_short": "",
                "market_id_long": "",
                "price_short": "",
                "price_long": "",
                "model_short": "",
                "model_long": "",
                "lambda_hat": str(lam),
                "mispricing": "0",
                "edge": "0",
            }
        )
