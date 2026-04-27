"""Latency-style Binance move vs Poly (stub; mapping requerido)."""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

import aiohttp

from arb.base import ArbStrategy

REPO_ROOT = Path(__file__).resolve().parent.parent
BINANCE_TICK = "https://api.binance.com/api/v3/ticker/price"


class LatencyArbStrategy(ArbStrategy):
    slug = "latency_arb"
    name = "Latency Arbitrage"

    @property
    def csv_columns(self) -> list[str]:
        return [
            "ts",
            "strategy",
            "action",
            "reason",
            "dry_run",
            "symbol",
            "price_trigger",
            "delta_pct",
            "poly_market_id",
            "poly_price_before",
            "poly_price_after",
            "latency_ms",
            "edge_est",
        ]

    def __init__(self, config: dict[str, Any], dry_run: bool = True) -> None:
        super().__init__(config, dry_run=dry_run)
        raw = config.get("symbols", os.getenv("LAT_SYMBOLS", "BTCUSDT,ETHUSDT"))
        self.symbols = [s.strip().upper() for s in str(raw).split(",") if s.strip()]
        self.tick_threshold = float(config.get("tick_threshold", os.getenv("LAT_TICK_THRESHOLD", "0.003")))
        self.mapping_path = Path(config.get("mapping_path", REPO_ROOT / "data" / "binance_poly_mapping.json"))
        self._last_px: dict[str, float] = {}
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
                        "symbol": "",
                        "price_trigger": "",
                        "delta_pct": "",
                        "poly_market_id": "",
                        "poly_price_before": "",
                        "poly_price_after": "",
                        "latency_ms": "",
                        "edge_est": "",
                    }
                )
                return

        if not self.mapping_path.is_file():
            self.log_signal(
                {
                    "action": "SKIP:NO_MARKETS",
                    "reason": "data/binance_poly_mapping.json missing",
                    "symbol": "",
                    "price_trigger": "",
                    "delta_pct": "",
                    "poly_market_id": "",
                    "poly_price_before": "",
                    "poly_price_after": "",
                    "latency_ms": "",
                    "edge_est": "",
                }
            )
            return

        mapping = json.loads(self.mapping_path.read_text(encoding="utf-8"))
        t0 = time.perf_counter()
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as sess:
                for sym in self.symbols:
                    async with sess.get(BINANCE_TICK, params={"symbol": sym}) as resp:
                        if resp.status != 200:
                            self.log_signal(
                                {
                                    "action": "ERROR:API_ERROR",
                                    "reason": f"Binance HTTP {resp.status}",
                                    "symbol": sym,
                                    "price_trigger": "",
                                    "delta_pct": "",
                                    "poly_market_id": "",
                                    "poly_price_before": "",
                                    "poly_price_after": "",
                                    "latency_ms": "",
                                    "edge_est": "",
                                }
                            )
                            return
                        data = await resp.json()
                    price = float(data["price"])
                    prev = self._last_px.get(sym)
                    self._last_px[sym] = price
                    if prev is None or prev <= 0:
                        continue
                    delta_pct = abs(price - prev) / prev
                    if delta_pct < self.tick_threshold:
                        continue
                    block = mapping.get(sym) or {}
                    mids = (block.get("up_markets") or []) + (block.get("down_markets") or [])
                    if not mids:
                        self.log_signal(
                            {
                                "action": "SKIP:NO_MARKETS",
                                "reason": f"no poly markets mapped for {sym}",
                                "symbol": sym,
                                "price_trigger": str(price),
                                "delta_pct": f"{delta_pct:.6f}",
                                "poly_market_id": "",
                                "poly_price_before": "",
                                "poly_price_after": "",
                                "latency_ms": str(int((time.perf_counter() - t0) * 1000)),
                                "edge_est": "0",
                            }
                        )
                        return

                    self.log_signal(
                        {
                            "action": "SKIP:STALE_PRICE",
                            "reason": "mapping has ids but Poly WS/orderbook scan not implemented",
                            "symbol": sym,
                            "price_trigger": str(price),
                            "delta_pct": f"{delta_pct:.6f}",
                            "poly_market_id": str(mids[0]),
                            "poly_price_before": "",
                            "poly_price_after": "",
                            "latency_ms": str(int((time.perf_counter() - t0) * 1000)),
                            "edge_est": "0",
                        }
                    )
                    return

            self.log_signal(
                {
                    "action": "SKIP:NO_MARKETS",
                    "reason": "no Binance tick above threshold",
                    "symbol": self.symbols[0] if self.symbols else "",
                    "price_trigger": "",
                    "delta_pct": "",
                    "poly_market_id": "",
                    "poly_price_before": "",
                    "poly_price_after": "",
                    "latency_ms": "",
                    "edge_est": "",
                }
            )
        except asyncio.TimeoutError:
            self.log_signal(
                {
                    "action": "ERROR:API_TIMEOUT",
                    "reason": "Binance request timeout",
                    "symbol": "",
                    "price_trigger": "",
                    "delta_pct": "",
                    "poly_market_id": "",
                    "poly_price_before": "",
                    "poly_price_after": "",
                    "latency_ms": "",
                    "edge_est": "",
                }
            )
        except Exception as e:
            self.log_signal(
                {
                    "action": "ERROR:API_ERROR",
                    "reason": str(e)[:200],
                    "symbol": "",
                    "price_trigger": "",
                    "delta_pct": "",
                    "poly_market_id": "",
                    "poly_price_before": "",
                    "poly_price_after": "",
                    "latency_ms": "",
                    "edge_est": "",
                }
            )
