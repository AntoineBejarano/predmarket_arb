"""Bundle arbitrage: ∑ best_ask < 1 (menos gas y MIN_EDGE).

Estrategia 1 del RUNBOOK strategies/bundle_arb/RUNBOOK.md:
- CLOB GET /markets paginado + GET /book por token_id (best_ask).
- edge = 1.0 - sum(best_ask) - gas_est, gas_est = BUNDLE_GAS_PER_LEG * n.
- Señal si edge > BUNDLE_MIN_EDGE y gas_est <= BUNDLE_MAX_GAS_PER_TX.
- Prioriza mercados con 2–3 outcomes (orden de escaneo).
- Por ciclo se persiste la mejor oportunidad encontrada (mayor edge), no la primera.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Optional

from clients.poly_clob import PolyCLOBClient

from arb.base import ArbStrategy


def _market_token_ids(m: dict[str, Any]) -> list[str]:
    """Extrae token_ids CLOB desde el objeto mercado de /markets."""
    tokens = m.get("tokens") or []
    out: list[str] = []
    for t in tokens:
        if isinstance(t, dict):
            tid = t.get("token_id") or t.get("tokenId")
            if tid:
                out.append(str(tid))
    return out


def _outcome_priority(n: int) -> int:
    """Menor primero: 2 y 3 outcomes antes que 4–5 (RUNBOOK)."""
    if n in (2, 3):
        return 0
    if n == 4:
        return 1
    return 2


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
        self.min_outcomes = int(config.get("min_outcomes", os.getenv("BUNDLE_MIN_OUTCOMES", "2")))
        self.max_pages = int(config.get("max_pages", os.getenv("BUNDLE_MAX_PAGES", "5")))
        self.gas_per_leg = float(config.get("gas_per_leg", os.getenv("BUNDLE_GAS_PER_LEG", "0.012")))
        self.max_gas_per_tx = float(config.get("max_gas_per_tx", os.getenv("BUNDLE_MAX_GAS_PER_TX", "0.05")))
        self._breaker = config.get("circuit_breaker")
        self._start_capital = float(config.get("start_capital", os.getenv("ARB_START_CAPITAL", "10000")))
        self._current_capital = float(config.get("current_capital", os.getenv("ARB_CURRENT_CAPITAL", "10000")))

    async def _best_asks_for_tokens(
        self, poly: PolyCLOBClient, token_ids: list[str]
    ) -> tuple[Optional[list[float]], Optional[str]]:
        """Paraleliza GET /book; devuelve (asks, error_reason)."""

        async def one_ask(tid: str) -> tuple[str, Optional[float], Optional[str]]:
            try:
                ob = await asyncio.wait_for(poly.get_orderbook(tid), timeout=8.0)
            except asyncio.TimeoutError:
                return tid, None, "timeout"
            if isinstance(ob, dict) and ob.get("error"):
                return tid, None, str(ob.get("error", "book_error"))[:80]
            ba = ob.get("best_ask")
            if ba is None:
                return tid, None, "no_best_ask"
            try:
                return tid, float(ba), None
            except (TypeError, ValueError):
                return tid, None, "bad_price"

        results = await asyncio.gather(*[one_ask(tid) for tid in token_ids])
        asks: list[float] = []
        for tid, px, err in results:
            if err is not None or px is None:
                return None, f"{tid[:12]}…:{err or '?'}"
            asks.append(px)
        return asks, None

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

        best: Optional[dict[str, Any]] = None
        best_edge = -1e9
        best_meta: Optional[dict[str, Any]] = None
        scanned_books = 0
        skipped_gas = 0

        try:
            async with PolyCLOBClient(
                api_key=os.getenv("POLY_API_KEY", ""),
                private_key=os.getenv("POLY_PRIVATE_KEY", ""),
                dry_run=self.dry_run,
            ) as poly:
                cursor = ""
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

                    markets = list(page.get("data") or [])
                    markets.sort(
                        key=lambda m: (
                            _outcome_priority(len(m.get("tokens") or [])),
                            str(m.get("condition_id", "")),
                        )
                    )

                    for m in markets:
                        if not m.get("accepting_orders"):
                            continue
                        if m.get("closed"):
                            continue

                        token_ids = _market_token_ids(m)
                        n = len(token_ids)
                        if n < self.min_outcomes or n > self.max_outcomes:
                            continue

                        asks, _book_err = await self._best_asks_for_tokens(poly, token_ids)
                        scanned_books += 1
                        if asks is None:
                            continue

                        total = sum(asks)
                        gas_est = self.gas_per_leg * n
                        if gas_est > self.max_gas_per_tx:
                            skipped_gas += 1
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

                        if edge > best_edge:
                            best_edge = edge
                            best_meta = {"token_ids": token_ids, "asks": asks, **row_base}
                            if edge > self.min_edge:
                                size = min(self.max_size, self.max_size)
                                best = {
                                    **row_base,
                                    "action": "SIGNAL",
                                    "reason": f"bundle edge {edge:.4f} > min_edge {self.min_edge}",
                                    "size_usdc": str(size),
                                }

                    cursor = page.get("next_cursor") or ""
                    if not cursor:
                        break

                if best is not None and best_meta is not None:
                    row = dict(best)
                    if not self.dry_run:
                        try:
                            n = len(best_meta["token_ids"])
                            size = float(row.get("size_usdc", self.max_size))
                            per_leg = size / max(n, 1)
                            for tid, px in zip(best_meta["token_ids"], best_meta["asks"]):
                                await poly.place_order(tid, "buy", float(px), per_leg)
                            row["action"] = "EXECUTED"
                            row["reason"] = "orders placed (live)"
                        except Exception as e:
                            row["action"] = "ERROR:ORDER_FAIL"
                            row["reason"] = str(e)[:200]
                    self.log_signal(row)
                    return

                if best_edge > -1e8 and best_edge <= self.min_edge and best_meta is not None:
                    self.log_signal(
                        {
                            "action": "SKIP:LOW_EDGE",
                            "reason": f"best edge {best_edge:.4f} <= min_edge {self.min_edge} (sum_ask={best_meta['sum_ask']} gas={best_meta['gas_est']})",
                            "market_id": best_meta.get("market_id", ""),
                            "n_outcomes": best_meta.get("n_outcomes", ""),
                            "sum_ask": best_meta.get("sum_ask", ""),
                            "gas_est": best_meta.get("gas_est", ""),
                            "edge": f"{best_edge:.6f}",
                            "size_usdc": "0",
                        }
                    )
                    return

                self.log_signal(
                    {
                        "action": "SKIP:NO_MARKETS",
                        "reason": f"no bundle candidate in {self.max_pages} pages (book_ok={scanned_books}, skip_high_gas={skipped_gas})",
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
