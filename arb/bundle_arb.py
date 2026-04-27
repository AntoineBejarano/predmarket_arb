"""Bundle arbitrage: ∑ best_ask < 1 (menos gas y MIN_EDGE)."""

from __future__ import annotations

import asyncio
import os
from typing import Any, Optional

from clients.poly_clob import PolyCLOBClient

from arb.base import ArbStrategy


class BundleArbStrategy(ArbStrategy):
    slug = "bundle_arb"
    name = "Bundle Arbitrage"

    @property
    def csv_columns(self) -> list[str]:
        return [
            "ts",
            "strategy",
            "action",
            "reason",
            "dry_run",
            "market_id",
            "n_outcomes",
            "sum_ask",
            "gas_est",
            "edge",
            "size_usdc",
        ]

    def __init__(self, config: dict[str, Any], dry_run: bool = True) -> None:
        super().__init__(config, dry_run=dry_run)
        self.min_edge = float(config.get("min_edge", os.getenv("BUNDLE_MIN_EDGE", "0.025")))
        self.max_size = float(config.get("max_size_usdc", os.getenv("BUNDLE_MAX_SIZE_USDC", "300")))
        self.max_outcomes = int(config.get("max_outcomes", os.getenv("BUNDLE_MAX_OUTCOMES", "5")))
        self.max_pages = int(config.get("max_pages", os.getenv("BUNDLE_MAX_PAGES", "5")))
        self.gas_per_leg = float(config.get("gas_per_leg", os.getenv("BUNDLE_GAS_PER_LEG", "0.012")))
        self.max_gas_per_tx = float(config.get("max_gas_per_tx", os.getenv("BUNDLE_MAX_GAS_PER_TX", "0.05")))
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
                        "market_id": "",
                        "n_outcomes": "",
                        "sum_ask": "",
                        "gas_est": "",
                        "edge": "",
                        "size_usdc": "0",
                    }
                )
                return

        try:
            async with PolyCLOBClient(
                api_key=os.getenv("POLY_API_KEY", ""),
                private_key=os.getenv("POLY_PRIVATE_KEY", ""),
                dry_run=self.dry_run,
            ) as poly:
                cursor = ""
                scanned = 0
                best_signal: Optional[dict[str, Any]] = None
                for _ in range(self.max_pages):
                    try:
                        page = await asyncio.wait_for(
                            poly.get_markets(limit=100, next_cursor=cursor),
                            timeout=12.0,
                        )
                    except asyncio.TimeoutError:
                        self.log_signal(
                            {
                                "action": "ERROR:API_TIMEOUT",
                                "reason": "CLOB /markets timeout",
                                "market_id": "",
                                "n_outcomes": "",
                                "sum_ask": "",
                                "gas_est": "",
                                "edge": "",
                                "size_usdc": "0",
                            }
                        )
                        return
                    markets = page.get("data") or []
                    cursor = page.get("next_cursor") or ""
                    for m in markets:
                        if not m.get("accepting_orders") or m.get("closed"):
                            continue
                        if not m.get("enable_order_book"):
                            continue
                        tokens = m.get("tokens") or []
                        n = len(tokens)
                        if n < 2 or n > self.max_outcomes:
                            continue
                        token_ids = [str(t.get("token_id", "")) for t in tokens if t.get("token_id")]
                        if len(token_ids) != n:
                            continue
                        asks: list[float] = []
                        bad = False
                        for tid in token_ids:
                            try:
                                ob = await asyncio.wait_for(poly.get_orderbook(tid), timeout=8.0)
                            except asyncio.TimeoutError:
                                bad = True
                                break
                            if isinstance(ob, dict) and ob.get("error"):
                                bad = True
                                break
                            ba = ob.get("best_ask")
                            if ba is None:
                                bad = True
                                break
                            asks.append(float(ba))
                        if bad:
                            continue
                        total = sum(asks)
                        gas_est = self.gas_per_leg * n
                        if (gas_est / max(n, 1)) > self.max_gas_per_tx:
                            continue
                        edge = 1.0 - total - gas_est
                        mid = str(m.get("condition_id", ""))
                        row_base = {
                            "market_id": mid,
                            "n_outcomes": str(n),
                            "sum_ask": f"{total:.6f}",
                            "gas_est": f"{gas_est:.6f}",
                            "edge": f"{edge:.6f}",
                        }
                        if edge > self.min_edge:
                            size = min(self.max_size, self.max_size)
                            row = {
                                **row_base,
                                "action": "SIGNAL",
                                "reason": f"bundle edge {edge:.4f} > min_edge {self.min_edge}",
                                "size_usdc": str(size),
                            }
                            if not self.dry_run:
                                try:
                                    for tid, px in zip(token_ids, asks):
                                        await poly.place_order(tid, "buy", px, size / max(n, 1))
                                    row["action"] = "EXECUTED"
                                    row["reason"] = "orders placed (live)"
                                except Exception as e:
                                    row["action"] = "ERROR:ORDER_FAIL"
                                    row["reason"] = str(e)[:200]
                            best_signal = row
                            break
                        scanned += 1
                    if best_signal:
                        break
                    if not cursor:
                        break

                if best_signal:
                    self.log_signal(best_signal)
                    return

                self.log_signal(
                    {
                        "action": "SKIP:NO_MARKETS",
                        "reason": f"no bundle opportunity in {self.max_pages} pages (scanned candidates)",
                        "market_id": "",
                        "n_outcomes": "",
                        "sum_ask": "",
                        "gas_est": "",
                        "edge": "",
                        "size_usdc": "0",
                    }
                )
        except Exception as e:
            self.log_signal(
                {
                    "action": "ERROR:API_ERROR",
                    "reason": str(e)[:200],
                    "market_id": "",
                    "n_outcomes": "",
                    "sum_ask": "",
                    "gas_est": "",
                    "edge": "",
                    "size_usdc": "0",
                }
            )
