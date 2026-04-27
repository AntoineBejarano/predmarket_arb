"""Cross-exchange arb: Poly YES + Kalshi NO (mapping manual)."""

from __future__ import annotations

import asyncio
import csv
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from clients.kalshi_rest import KalshiRESTClient
from clients.poly_clob import PolyCLOBClient

from arb.base import ArbStrategy

REPO_ROOT = Path(__file__).resolve().parent.parent


class CrossExchangeStrategy(ArbStrategy):
    slug = "cross_exchange"
    name = "Cross-Exchange Arbitrage"

    @property
    def csv_columns(self) -> list[str]:
        return [
            "ts",
            "strategy",
            "action",
            "reason",
            "dry_run",
            "poly_market_id",
            "kalshi_ticker",
            "poly_yes_ask",
            "kalshi_no_ask",
            "spread_gross",
            "fees_total",
            "edge",
            "lockup_days",
        ]

    def __init__(self, config: dict[str, Any], dry_run: bool = True) -> None:
        super().__init__(config, dry_run=dry_run)
        self.min_edge = float(config.get("min_edge", os.getenv("CROSS_MIN_EDGE", "0.030")))
        self.max_lockup = int(config.get("max_lockup_days", os.getenv("CROSS_MAX_LOCKUP_DAYS", "21")))
        self.poly_fee = float(config.get("poly_fee", os.getenv("CROSS_POLY_FEE", "0.002")))
        self.kalshi_fee = float(config.get("kalshi_fee", os.getenv("CROSS_KALSHI_FEE", "0.002")))
        self.mapping_path = Path(
            config.get("mapping_path", REPO_ROOT / "data" / "poly_kalshi_mapping.csv")
        )
        self._breaker = config.get("circuit_breaker")
        self._start_capital = float(config.get("start_capital", os.getenv("ARB_START_CAPITAL", "10000")))
        self._current_capital = float(config.get("current_capital", os.getenv("ARB_CURRENT_CAPITAL", "10000")))

    def _load_mapping(self) -> list[dict[str, str]]:
        if not self.mapping_path.is_file():
            return []
        rows: list[dict[str, str]] = []
        with open(self.mapping_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                pid = (row.get("poly_market_id") or "").strip()
                kt = (row.get("kalshi_ticker") or "").strip()
                if pid and kt:
                    rows.append(row)
        return rows

    def _lockup_days(self, expires_at: str) -> Optional[int]:
        s = (expires_at or "").strip()
        if not s:
            return None
        try:
            exp = datetime.fromisoformat(s.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            return max(0, int((exp - now).total_seconds() // 86400))
        except ValueError:
            return None

    async def run_once(self) -> None:
        if self._breaker:
            ok = await self._breaker.check(self._current_capital, self._start_capital)
            if not ok:
                await self.log_signal_async(
                    {
                        "action": "SKIP:CIRCUIT_BREAKER",
                        "reason": "max_daily_drawdown exceeded",
                        "poly_market_id": "",
                        "kalshi_ticker": "",
                        "poly_yes_ask": "",
                        "kalshi_no_ask": "",
                        "spread_gross": "",
                        "fees_total": "",
                        "edge": "",
                        "lockup_days": "",
                    }
                )
                return

        mapping = self._load_mapping()
        if not mapping:
            await self.log_signal_async(
                {
                    "action": "SKIP:NO_MARKETS",
                    "reason": "data/poly_kalshi_mapping.csv empty or missing",
                    "poly_market_id": "",
                    "kalshi_ticker": "",
                    "poly_yes_ask": "",
                    "kalshi_no_ask": "",
                    "spread_gross": "",
                    "fees_total": "",
                    "edge": "",
                    "lockup_days": "",
                }
            )
            return

        best_low: Optional[tuple[float, dict[str, Any]]] = None

        try:
            async with PolyCLOBClient(
                api_key=os.getenv("POLY_API_KEY", ""),
                private_key=os.getenv("POLY_PRIVATE_KEY", ""),
                dry_run=self.dry_run,
            ) as poly:
                async with KalshiRESTClient(
                    api_key=os.getenv("KALSHI_API_KEY", ""),
                    api_secret=os.getenv("KALSHI_API_SECRET", ""),
                    dry_run=self.dry_run,
                ) as kalshi:
                    for row in mapping:
                        poly_id = row["poly_market_id"]
                        kal_t = row["kalshi_ticker"]
                        exp = row.get("expires_at") or ""
                        lock_days = self._lockup_days(exp)
                        if lock_days is not None and lock_days > self.max_lockup:
                            await self.log_signal_async(
                                {
                                    "action": "SKIP:LOCKUP",
                                    "reason": f"lockup_days {lock_days} > max {self.max_lockup}",
                                    "poly_market_id": poly_id,
                                    "kalshi_ticker": kal_t,
                                    "poly_yes_ask": "",
                                    "kalshi_no_ask": "",
                                    "spread_gross": "",
                                    "fees_total": "",
                                    "edge": "",
                                    "lockup_days": str(lock_days),
                                }
                            )
                            continue

                        try:
                            mmeta = await asyncio.wait_for(poly.get_market(poly_id), timeout=10.0)
                            if not isinstance(mmeta, dict) or mmeta.get("error"):
                                continue
                            tokens = mmeta.get("tokens") or []
                            yes_tid = None
                            for t in tokens:
                                if str(t.get("outcome", "")).lower() in ("yes", "y"):
                                    yes_tid = str(t.get("token_id", ""))
                                    break
                            if not tokens:
                                continue
                            if yes_tid is None:
                                yes_tid = str(tokens[0].get("token_id", ""))
                            if not yes_tid:
                                continue
                            obp = await asyncio.wait_for(poly.get_orderbook(yes_tid), timeout=10.0)
                            obk = await asyncio.wait_for(kalshi.get_orderbook(kal_t), timeout=10.0)
                        except asyncio.TimeoutError:
                            await self.log_signal_async(
                                {
                                    "action": "ERROR:API_TIMEOUT",
                                    "reason": "orderbook fetch timeout",
                                    "poly_market_id": poly_id,
                                    "kalshi_ticker": kal_t,
                                    "poly_yes_ask": "",
                                    "kalshi_no_ask": "",
                                    "spread_gross": "",
                                    "fees_total": "",
                                    "edge": "",
                                    "lockup_days": str(lock_days or ""),
                                }
                            )
                            return

                        if isinstance(obp, dict) and obp.get("error"):
                            continue
                        if isinstance(obk, dict) and (obk.get("error") or obk.get("http_status", 200) != 200):
                            await self.log_signal_async(
                                {
                                    "action": "ERROR:API_ERROR",
                                    "reason": str(obk.get("error", obk))[:200],
                                    "poly_market_id": poly_id,
                                    "kalshi_ticker": kal_t,
                                    "poly_yes_ask": "",
                                    "kalshi_no_ask": "",
                                    "spread_gross": "",
                                    "fees_total": "",
                                    "edge": "",
                                    "lockup_days": str(lock_days or ""),
                                }
                            )
                            return

                        py = obp.get("best_ask")
                        kn = obk.get("no_ask")
                        if py is None or kn is None:
                            continue
                        poly_yes = float(py)
                        kal_no = float(kn)
                        spread_gross = 1.0 - poly_yes - kal_no
                        fees_total = self.poly_fee + self.kalshi_fee
                        edge = spread_gross - fees_total
                        rec: dict[str, Any] = {
                            "poly_market_id": poly_id,
                            "kalshi_ticker": kal_t,
                            "poly_yes_ask": f"{poly_yes:.6f}",
                            "kalshi_no_ask": f"{kal_no:.6f}",
                            "spread_gross": f"{spread_gross:.6f}",
                            "fees_total": f"{fees_total:.6f}",
                            "edge": f"{edge:.6f}",
                            "lockup_days": str(lock_days if lock_days is not None else ""),
                        }
                        if edge > self.min_edge:
                            rec["action"] = "SIGNAL"
                            rec["reason"] = f"cross edge {edge:.4f} > min {self.min_edge}"
                            if not self.dry_run:
                                try:
                                    await poly.place_order(yes_tid, "buy", poly_yes, 50.0)
                                    await kalshi.place_order(kal_t, "no", kal_no, 1)
                                    rec["action"] = "EXECUTED"
                                    rec["reason"] = "live orders (stub sizes)"
                                except Exception as e:
                                    rec["action"] = "ERROR:ORDER_FAIL"
                                    rec["reason"] = str(e)[:200]
                            await self.log_signal_async(rec)
                            return

                        low_row = {
                            **rec,
                            "action": "SKIP:LOW_EDGE",
                            "reason": f"edge {edge:.4f} <= min {self.min_edge}",
                        }
                        if best_low is None or edge > best_low[0]:
                            best_low = (edge, low_row)

                    if best_low:
                        await self.log_signal_async(best_low[1])
                        return

                    await self.log_signal_async(
                        {
                            "action": "SKIP:NO_MARKETS",
                            "reason": "no valid prices for mapped rows",
                            "poly_market_id": "",
                            "kalshi_ticker": "",
                            "poly_yes_ask": "",
                            "kalshi_no_ask": "",
                            "spread_gross": "",
                            "fees_total": "",
                            "edge": "",
                            "lockup_days": "",
                        }
                    )
        except Exception as e:
            await self.log_signal_async(
                {
                    "action": "ERROR:API_ERROR",
                    "reason": str(e)[:200],
                    "poly_market_id": "",
                    "kalshi_ticker": "",
                    "poly_yes_ask": "",
                    "kalshi_no_ask": "",
                    "spread_gross": "",
                    "fees_total": "",
                    "edge": "",
                    "lockup_days": "",
                }
            )
