"""Avellaneda–Stoikov style quotes (paper)."""

from __future__ import annotations

import asyncio
import math
import os
from collections import deque
from typing import Any, Deque, Optional

from clients.poly_clob import PolyCLOBClient

from arb.base import ArbStrategy


class MarketMakerStrategy(ArbStrategy):
    slug = "market_maker"
    name = "Market Making (Avellaneda-Stoikov)"

    def __init__(self, config: dict[str, Any], dry_run: bool = True) -> None:
        super().__init__(config, dry_run=dry_run)
        self.gamma = float(config.get("gamma", os.getenv("MM_GAMMA", "0.1")))
        self.k = float(config.get("k", os.getenv("MM_K", "2.0")))
        self.min_spread = float(config.get("min_spread", os.getenv("MM_MIN_SPREAD", "0.04")))
        self.max_inventory = int(config.get("max_inventory", os.getenv("MM_MAX_INVENTORY", "50")))
        self.mid_history: Deque[float] = deque(maxlen=60)
        self.q_inventory = int(config.get("q_inventory", os.getenv("MM_Q_INVENTORY", "0")))
        self._breaker = config.get("circuit_breaker")
        self._start_capital = float(config.get("start_capital", os.getenv("ARB_START_CAPITAL", "10000")))
        self._current_capital = float(config.get("current_capital", os.getenv("ARB_CURRENT_CAPITAL", "10000")))
        self.max_pages = int(config.get("max_pages", os.getenv("MM_MAX_PAGES", "3")))

    @property
    def csv_columns(self) -> list[str]:
        return [
            "ts",
            "strategy",
            "action",
            "reason",
            "dry_run",
            "market_id",
            "mid",
            "q_inventory",
            "sigma",
            "reservation_price",
            "bid_quote",
            "ask_quote",
            "bid_placed",
            "ask_placed",
            "edge",
        ]

    def _sigma(self) -> float:
        if len(self.mid_history) < 5:
            return 0.02
        m = list(self.mid_history)
        rets = [abs((m[i] - m[i - 1]) / m[i - 1]) for i in range(1, len(m)) if m[i - 1]]
        if not rets:
            return 0.02
        mu = sum(rets) / len(rets)
        var = sum((x - mu) ** 2 for x in rets) / max(len(rets), 1)
        return max(0.005, math.sqrt(var))

    async def run_once(self) -> None:
        if self._breaker:
            ok = await self._breaker.check(self._current_capital, self._start_capital)
            if not ok:
                self.log_signal(
                    {
                        "action": "SKIP:CIRCUIT_BREAKER",
                        "reason": "max_daily_drawdown exceeded",
                        "market_id": "",
                        "mid": "",
                        "q_inventory": str(self.q_inventory),
                        "sigma": "",
                        "reservation_price": "",
                        "bid_quote": "",
                        "ask_quote": "",
                        "bid_placed": "",
                        "ask_placed": "",
                        "edge": "",
                    }
                )
                return

        if abs(self.q_inventory) >= self.max_inventory:
            self.log_signal(
                {
                    "action": "SKIP:LOW_EDGE",
                    "reason": "inventory at MM_MAX_INVENTORY",
                    "market_id": "",
                    "mid": "",
                    "q_inventory": str(self.q_inventory),
                    "sigma": "",
                    "reservation_price": "",
                    "bid_quote": "",
                    "ask_quote": "",
                    "bid_placed": "",
                    "ask_placed": "",
                    "edge": "0",
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
                picked: Optional[tuple[str, float, float, float]] = None
                for _ in range(self.max_pages):
                    page = await asyncio.wait_for(poly.get_markets(limit=80, next_cursor=cursor), timeout=12.0)
                    for m in page.get("data") or []:
                        if not m.get("accepting_orders") or m.get("closed") or not m.get("enable_order_book"):
                            continue
                        tokens = m.get("tokens") or []
                        if len(tokens) != 2:
                            continue
                        tid = str(tokens[0].get("token_id", ""))
                        if not tid:
                            continue
                        ob = await asyncio.wait_for(poly.get_orderbook(tid), timeout=8.0)
                        if ob.get("error") or ob.get("best_bid") is None or ob.get("best_ask") is None:
                            continue
                        bid = float(ob["best_bid"])
                        ask = float(ob["best_ask"])
                        spread = ask - bid
                        if spread < self.min_spread:
                            continue
                        mid = (bid + ask) / 2.0
                        picked = (str(m.get("condition_id", "")), mid, bid, ask)
                        break
                    if picked:
                        break
                    cursor = page.get("next_cursor") or ""
                    if not cursor:
                        break

                if not picked:
                    self.log_signal(
                        {
                            "action": "SKIP:NO_MARKETS",
                            "reason": "no market with book and spread >= MM_MIN_SPREAD",
                            "market_id": "",
                            "mid": "",
                            "q_inventory": str(self.q_inventory),
                            "sigma": "",
                            "reservation_price": "",
                            "bid_quote": "",
                            "ask_quote": "",
                            "bid_placed": "",
                            "ask_placed": "",
                            "edge": "",
                        }
                    )
                    return

                market_id, mid, _bid_mkt, _ask_mkt = picked
                self.mid_history.append(mid)
                sigma = self._sigma()
                q = float(self.q_inventory)
                T = 24.0
                t = 0.0
                r = mid - q * self.gamma * (sigma**2) * (T - t)
                delta = self.gamma * (sigma**2) * (T - t) / 2.0 + math.log(1 + self.gamma / max(self.k, 1e-6)) / max(
                    self.gamma, 1e-6
                )
                bid_q = max(0.01, min(0.99, r - delta))
                ask_q = max(0.01, min(0.99, r + delta))
                edge = max(0.0, (ask_q - bid_q) / 2)

                row: dict[str, Any] = {
                    "market_id": market_id,
                    "mid": f"{mid:.6f}",
                    "q_inventory": str(self.q_inventory),
                    "sigma": f"{sigma:.6f}",
                    "reservation_price": f"{r:.6f}",
                    "bid_quote": f"{bid_q:.6f}",
                    "ask_quote": f"{ask_q:.6f}",
                    "bid_placed": "",
                    "ask_placed": "",
                    "edge": f"{edge:.6f}",
                    "action": "QUOTE",
                    "reason": "AS quotes computed (paper)",
                }
                if self.dry_run:
                    row["bid_placed"] = f"{bid_q:.4f}"
                    row["ask_placed"] = f"{ask_q:.4f}"
                else:
                    try:
                        tok = None
                        page = await poly.get_market(market_id)
                        toks = page.get("tokens") or []
                        if toks:
                            tok = str(toks[0].get("token_id", ""))
                        if tok:
                            await poly.place_order(tok, "buy", bid_q, 10.0)
                            await poly.place_order(tok, "sell", ask_q, 10.0)
                        row["action"] = "EXECUTED"
                        row["reason"] = "placed bid/ask (live)"
                        row["bid_placed"] = f"{bid_q:.4f}"
                        row["ask_placed"] = f"{ask_q:.4f}"
                    except Exception as e:
                        row["action"] = "ERROR:ORDER_FAIL"
                        row["reason"] = str(e)[:200]
                self.log_signal(row)
        except asyncio.TimeoutError:
            self.log_signal(
                {
                    "action": "ERROR:API_TIMEOUT",
                    "reason": "CLOB timeout",
                    "market_id": "",
                    "mid": "",
                    "q_inventory": str(self.q_inventory),
                    "sigma": "",
                    "reservation_price": "",
                    "bid_quote": "",
                    "ask_quote": "",
                    "bid_placed": "",
                    "ask_placed": "",
                    "edge": "",
                }
            )
        except Exception as e:
            self.log_signal(
                {
                    "action": "ERROR:API_ERROR",
                    "reason": str(e)[:200],
                    "market_id": "",
                    "mid": "",
                    "q_inventory": str(self.q_inventory),
                    "sigma": "",
                    "reservation_price": "",
                    "bid_quote": "",
                    "ask_quote": "",
                    "bid_placed": "",
                    "ask_placed": "",
                    "edge": "",
                }
            )
