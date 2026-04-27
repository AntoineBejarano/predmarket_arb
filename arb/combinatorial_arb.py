"""Combinatorial / multi-leg arb (stub hasta market_graph.json)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from arb.base import ArbStrategy

REPO_ROOT = Path(__file__).resolve().parent.parent


class CombinatorialArbStrategy(ArbStrategy):
    slug = "combinatorial_arb"
    name = "Combinatorial Arbitrage"

    @property
    def csv_columns(self) -> list[str]:
        return [
            "ts",
            "strategy",
            "action",
            "reason",
            "dry_run",
            "arb_type",
            "markets",
            "legs",
            "cost_total",
            "fair_value",
            "edge",
        ]

    def __init__(self, config: dict[str, Any], dry_run: bool = True) -> None:
        super().__init__(config, dry_run=dry_run)
        self.graph_path = Path(config.get("graph_path", REPO_ROOT / "data" / "market_graph.json"))
        self._breaker = config.get("circuit_breaker")
        self._start_capital = float(config.get("start_capital", os.getenv("ARB_START_CAPITAL", "10000")))
        self._current_capital = float(config.get("current_capital", os.getenv("ARB_CURRENT_CAPITAL", "10000")))

    async def run_once(self) -> None:
        if self._breaker:
            ok = await self._breaker.check(self._current_capital, self._start_capital)
            if not ok:
                self.log_signal(
                    {
                        "action": "SKIP:CIRCUIT_BREAKER",
                        "reason": "max_daily_drawdown exceeded",
                        "arb_type": "",
                        "markets": "[]",
                        "legs": "{}",
                        "cost_total": "",
                        "fair_value": "",
                        "edge": "",
                    }
                )
                return

        if not self.graph_path.is_file():
            self.log_signal(
                {
                    "action": "SKIP:NO_MARKETS",
                    "reason": "data/market_graph.json missing — stub combinatorial",
                    "arb_type": "",
                    "markets": "[]",
                    "legs": "{}",
                    "cost_total": "0",
                    "fair_value": "0",
                    "edge": "0",
                }
            )
            return

        try:
            data = json.loads(self.graph_path.read_text(encoding="utf-8"))
            nodes = data.get("nodes") or data
            if not nodes:
                self.log_signal(
                    {
                        "action": "SKIP:NO_MARKETS",
                        "reason": "empty market graph",
                        "arb_type": "",
                        "markets": "[]",
                        "legs": "{}",
                        "cost_total": "0",
                        "fair_value": "0",
                        "edge": "0",
                    }
                )
                return
        except (json.JSONDecodeError, OSError) as e:
            self.log_signal(
                {
                    "action": "ERROR:API_ERROR",
                    "reason": str(e)[:200],
                    "arb_type": "",
                    "markets": "[]",
                    "legs": "{}",
                    "cost_total": "",
                    "fair_value": "",
                    "edge": "",
                }
            )
            return

        self.log_signal(
            {
                "action": "SKIP:NO_MARKETS",
                "reason": "graph present but scan not implemented",
                "arb_type": "COMBINATORIAL",
                "markets": json.dumps([]),
                "legs": json.dumps({}),
                "cost_total": "0",
                "fair_value": "0",
                "edge": "0",
            }
        )
