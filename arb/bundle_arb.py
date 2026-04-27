"""Bundle arbitrage: ∑ best_ask < 1 (menos buffer y fees CLOB).

Estrategia 1 del RUNBOOK strategies/bundle_arb/RUNBOOK.md:
- CLOB GET /markets (solo mercados con libro habilitado) + GET /book por token_id.
- ``sum_ask`` = suma de best_ask; ``execution_buffer_est`` = heurística por pierna (no es API Polymarket).
- ``fees_est`` = comisión CLOB estimada sobre el nocional por pierna (GET /fee-rate).
- ``edge_gross`` = 1 - sum_ask - execution_buffer_est; ``edge`` (neto) = edge_gross - fees_est.
- Señal si ``edge`` > BUNDLE_MIN_EDGE, hay liquidez al mejor ask >= per_leg y checks opcionales de midpoint.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
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


def _market_mini(m: dict[str, Any]) -> dict[str, Any]:
    q = m.get("question") or m.get("title") or m.get("description") or ""
    return {
        "condition_id": str(m.get("condition_id", "")),
        "question": str(q)[:160],
        "n_tokens": len(_market_token_ids(m)),
        "accepting_orders": bool(m.get("accepting_orders")),
        "closed": bool(m.get("closed")),
        "enable_order_book": bool(m.get("enable_order_book")),
    }


def _bundle_scan_path() -> Path:
    base = Path(os.getenv("DATA_DIR", ".")).resolve()
    return base / "logs" / "bundle_arb_scan.json"


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
            "execution_buffer_est",
            "fees_est",
            "edge_gross",
            "edge",
            "size_usdc",
            "order_ids",
            "mid_dev_max",
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
        self.mid_max_dev = float(config.get("mid_max_dev", os.getenv("BUNDLE_MID_MAX_DEV", "0")))
        self._breaker = config.get("circuit_breaker")
        self._start_capital = float(config.get("start_capital", os.getenv("ARB_START_CAPITAL", "10000")))
        self._current_capital = float(config.get("current_capital", os.getenv("ARB_CURRENT_CAPITAL", "10000")))

    async def _legs_snapshot(
        self, poly: PolyCLOBClient, token_ids: list[str]
    ) -> tuple[Optional[list[float]], Optional[list[float]], Optional[str]]:
        """Paraleliza GET /book; devuelve (best_asks, best_ask_notional_usdc por pierna, error)."""

        async def one_leg(tid: str) -> tuple[str, Optional[float], Optional[float], Optional[str]]:
            try:
                ob = await asyncio.wait_for(poly.get_orderbook(tid), timeout=8.0)
            except asyncio.TimeoutError:
                return tid, None, None, "timeout"
            if isinstance(ob, dict) and ob.get("error"):
                return tid, None, None, str(ob.get("error", "book_error"))[:80]
            ba = ob.get("best_ask")
            if ba is None:
                return tid, None, None, "no_best_ask"
            try:
                px = float(ba)
            except (TypeError, ValueError):
                return tid, None, None, "bad_price"
            nom = ob.get("best_ask_notional_usdc")
            try:
                nom_f = float(nom) if nom is not None else None
            except (TypeError, ValueError):
                nom_f = None
            return tid, px, nom_f, None

        results = await asyncio.gather(*[one_leg(tid) for tid in token_ids])
        asks: list[float] = []
        noms: list[float] = []
        for tid, px, nom, err in results:
            if err is not None or px is None:
                return None, None, f"{tid[:12]}…:{err or '?'}"
            asks.append(px)
            noms.append(nom if nom is not None else float("nan"))
        return asks, noms, None

    def _passes_mid_dev(self, asks: list[float], mids: list[Optional[float]]) -> tuple[bool, str]:
        if self.mid_max_dev <= 0:
            return True, ""
        max_rel = 0.0
        for ba, mid in zip(asks, mids):
            if mid is None or mid <= 0:
                return False, "mid_unavailable"
            rel = abs(ba - mid) / mid
            max_rel = max(max_rel, rel)
            if rel > self.mid_max_dev:
                return False, f"mid_dev {rel:.4f}>{self.mid_max_dev:.4f}"
        return True, f"{max_rel:.6f}"

    def _empty_row(self) -> dict[str, str]:
        return {
            "market_id": "",
            "n_outcomes": "",
            "sum_ask": "",
            "execution_buffer_est": "",
            "fees_est": "",
            "edge_gross": "",
            "edge": "",
            "size_usdc": "0",
            "order_ids": "",
            "mid_dev_max": "",
        }

    @staticmethod
    def _extract_order_id(resp: dict[str, Any]) -> Optional[str]:
        for key in ("orderID", "order_id", "id", "ID"):
            v = resp.get(key)
            if v:
                return str(v)
        order = resp.get("order")
        if isinstance(order, dict):
            for key in ("orderID", "order_id", "id"):
                v = order.get(key)
                if v:
                    return str(v)
        return None

    def _persist_scan_diag(self, diag: dict[str, Any]) -> None:
        """Escribe diagnóstico del último ciclo para la UI (`/api/arb/strategy/bundle_arb/scan`)."""
        diag = {**diag, "updated_at": datetime.now(timezone.utc).isoformat(), "slug": self.slug}
        p = _bundle_scan_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(diag, indent=2, ensure_ascii=False), encoding="utf-8")

    async def run_once(self) -> None:
        empty = self._empty_row()
        if self._breaker:
            ok = await self._breaker.check(self._current_capital, self._start_capital)
            if not ok:
                await self.log_signal_async(
                    {
                        "action": "SKIP:CIRCUIT_BREAKER",
                        "reason": "max_daily_drawdown exceeded",
                        **empty,
                    }
                )
                self._persist_scan_diag(
                    {
                        "outcome": "SKIP:CIRCUIT_BREAKER",
                        "help": "Circuit breaker de drawdown; no se llamó al CLOB.",
                    }
                )
                return

        best: Optional[dict[str, Any]] = None
        best_edge_net = -1e9
        best_meta: Optional[dict[str, Any]] = None
        scanned_books = 0
        skipped_gas = 0
        skipped_depth = 0
        skipped_mid = 0
        pages_fetched = 0
        markets_raw_total = 0
        skip_not_accepting = 0
        skip_closed = 0
        skip_no_orderbook = 0
        skip_outcomes = 0
        skip_bad_book = 0
        eligible_samples: list[dict[str, Any]] = []
        _MAX_ELIGIBLE_SAMPLE = 12

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
                        await self.log_signal_async(
                            {
                                "action": "ERROR:API_TIMEOUT",
                                "reason": "CLOB /markets timeout",
                                **empty,
                            }
                        )
                        self._persist_scan_diag(
                            {
                                "outcome": "ERROR:API_TIMEOUT",
                                "pages_fetched": pages_fetched,
                                "markets_raw_total": markets_raw_total,
                                "help": "Timeout en GET /markets; revisa red o sube timeout.",
                            }
                        )
                        return

                    pages_fetched += 1
                    markets = list(page.get("data") or [])
                    markets.sort(
                        key=lambda m: (
                            _outcome_priority(len(m.get("tokens") or [])),
                            str(m.get("condition_id", "")),
                        )
                    )

                    for m in markets:
                        markets_raw_total += 1
                        if not m.get("accepting_orders"):
                            skip_not_accepting += 1
                            continue
                        if m.get("closed"):
                            skip_closed += 1
                            continue
                        if not m.get("enable_order_book"):
                            skip_no_orderbook += 1
                            continue

                        token_ids = _market_token_ids(m)
                        n = len(token_ids)
                        if n < self.min_outcomes or n > self.max_outcomes:
                            skip_outcomes += 1
                            continue

                        if len(eligible_samples) < _MAX_ELIGIBLE_SAMPLE:
                            eligible_samples.append(_market_mini(m))

                        asks, noms, _book_err = await self._legs_snapshot(poly, token_ids)
                        scanned_books += 1
                        if asks is None:
                            skip_bad_book += 1
                            continue

                        execution_buffer_est = self.gas_per_leg * n
                        if execution_buffer_est > self.max_gas_per_tx:
                            skipped_gas += 1
                            continue

                        size_target = self.max_size
                        per_leg = size_target / max(n, 1)
                        depth_ok = True
                        for nom in noms:
                            if nom is None or math.isnan(nom) or nom < per_leg - 1e-6:
                                depth_ok = False
                                break
                        if not depth_ok:
                            skipped_depth += 1
                            continue

                        fee_bpss = await asyncio.gather(*[poly.get_fee_rate_bps(t) for t in token_ids])
                        fees_est_val = sum(per_leg * (float(bps) / 10000.0) for bps in fee_bpss)

                        total = sum(asks)
                        edge_gross = 1.0 - total - execution_buffer_est
                        edge_net = edge_gross - fees_est_val

                        mid_label = ""
                        mids: list[Optional[float]] = []
                        if self.mid_max_dev > 0:
                            mids = await asyncio.gather(*[poly.get_midpoint(t) for t in token_ids])
                            ok_mid, mid_label = self._passes_mid_dev(asks, mids)
                            if not ok_mid:
                                skipped_mid += 1
                                continue
                        else:
                            mid_label = ""

                        cid = str(m.get("condition_id", ""))
                        row_base = {
                            "market_id": cid,
                            "n_outcomes": str(n),
                            "sum_ask": f"{total:.6f}",
                            "execution_buffer_est": f"{execution_buffer_est:.6f}",
                            "fees_est": f"{fees_est_val:.6f}",
                            "edge_gross": f"{edge_gross:.6f}",
                            "edge": f"{edge_net:.6f}",
                            "mid_dev_max": mid_label,
                        }

                        if edge_net > best_edge_net:
                            best_edge_net = edge_net
                            best_meta = {
                                "token_ids": token_ids,
                                "asks": asks,
                                "noms": noms,
                                "fee_bpss": fee_bpss,
                                **row_base,
                            }
                            if edge_net > self.min_edge:
                                best = {
                                    **row_base,
                                    "action": "SIGNAL",
                                    "reason": (
                                        f"edge_net {edge_net:.4f} > min_edge {self.min_edge} "
                                        f"(edge_gross={edge_gross:.4f}, sum_ask={total:.4f}, "
                                        f"execution_buffer_est={execution_buffer_est:.4f} heuristic not on-chain gas; "
                                        f"fees_est from CLOB /fee-rate)"
                                    ),
                                    "size_usdc": str(size_target),
                                    "order_ids": "",
                                }

                    cursor = page.get("next_cursor") or ""
                    if not cursor:
                        break

                if best is not None and best_meta is not None:
                    row = dict(best)
                    if not self.dry_run:
                        try:
                            n_legs = len(best_meta["token_ids"])
                            size = float(row.get("size_usdc", self.max_size))
                            per_leg_live = size / max(n_legs, 1)
                            order_ids: list[str] = []
                            for tid, px in zip(best_meta["token_ids"], best_meta["asks"]):
                                resp = await poly.place_order(tid, "buy", float(px), per_leg_live)
                                oid = self._extract_order_id(resp)
                                if oid:
                                    order_ids.append(oid)
                            row["action"] = "EXECUTED"
                            row["order_ids"] = ",".join(order_ids)
                            row["reason"] = (
                                "live: POST /order OK; order_ids from CLOB JSON; "
                                "execution_buffer_est is heuristic (not on-chain gas)"
                            )
                            if len(order_ids) < n_legs:
                                try:
                                    oo = await poly.get_open_orders()
                                    if isinstance(oo, dict) and not oo.get("error"):
                                        nd = len(oo.get("data") or [])
                                        row["reason"] += f"; GET /data/orders first_page n={nd}"
                                    else:
                                        row["reason"] += f"; get_open_orders: {str(oo.get('error', oo))[:80]}"
                                except Exception as ex:
                                    row["reason"] += f"; get_open_orders err={str(ex)[:60]}"
                        except Exception as e:
                            row["action"] = "ERROR:ORDER_FAIL"
                            row["reason"] = str(e)[:200]
                            row["order_ids"] = ""
                    await self.log_signal_async(row)
                    self._persist_scan_diag(
                        {
                            "outcome": row.get("action", ""),
                            "pages_fetched": pages_fetched,
                            "markets_raw_total": markets_raw_total,
                            "scanned_books": scanned_books,
                            "skip_not_accepting": skip_not_accepting,
                            "skip_closed": skip_closed,
                            "skip_no_orderbook": skip_no_orderbook,
                            "skip_outcomes": skip_outcomes,
                            "skip_bad_book": skip_bad_book,
                            "skip_high_buffer": skipped_gas,
                            "skip_insufficient_depth": skipped_depth,
                            "skip_mid": skipped_mid,
                            "best_edge_net": best_edge_net,
                            "min_edge": self.min_edge,
                            "max_size_usdc": self.max_size,
                            "eligible_sample": eligible_samples,
                            "best_market": best_meta.get("market_id") if best_meta else None,
                            "help": (
                                "Último ciclo encontró oportunidad que pasó filtros (libro, profundidad, fees, mid opcional). "
                                "Ver contadores para el universo escaneado en GET /markets."
                            ),
                        }
                    )
                    return

                if best_edge_net > -1e8 and best_edge_net <= self.min_edge and best_meta is not None:
                    await self.log_signal_async(
                        {
                            "action": "SKIP:LOW_EDGE",
                            "reason": (
                                f"best edge_net {best_edge_net:.4f} <= min_edge {self.min_edge} "
                                f"(sum_ask={best_meta['sum_ask']}, execution_buffer_est={best_meta['execution_buffer_est']}, "
                                f"fees_est={best_meta['fees_est']})"
                            ),
                            "market_id": best_meta.get("market_id", ""),
                            "n_outcomes": best_meta.get("n_outcomes", ""),
                            "sum_ask": best_meta.get("sum_ask", ""),
                            "execution_buffer_est": best_meta.get("execution_buffer_est", ""),
                            "fees_est": best_meta.get("fees_est", ""),
                            "edge_gross": best_meta.get("edge_gross", ""),
                            "edge": f"{best_edge_net:.6f}",
                            "size_usdc": "0",
                            "order_ids": "",
                            "mid_dev_max": best_meta.get("mid_dev_max", ""),
                        }
                    )
                    self._persist_scan_diag(
                        {
                            "outcome": "SKIP:LOW_EDGE",
                            "pages_fetched": pages_fetched,
                            "markets_raw_total": markets_raw_total,
                            "scanned_books": scanned_books,
                            "skip_not_accepting": skip_not_accepting,
                            "skip_closed": skip_closed,
                            "skip_no_orderbook": skip_no_orderbook,
                            "skip_outcomes": skip_outcomes,
                            "skip_bad_book": skip_bad_book,
                            "skip_high_buffer": skipped_gas,
                            "skip_insufficient_depth": skipped_depth,
                            "skip_mid": skipped_mid,
                            "best_edge_net": best_edge_net,
                            "min_edge": self.min_edge,
                            "best_market": best_meta.get("market_id"),
                            "eligible_sample": eligible_samples,
                            "help": (
                                "Había al menos un mercado con libro válido pero el mejor edge_net "
                                f"({best_edge_net:.4f}) no superó BUNDLE_MIN_EDGE ({self.min_edge})."
                            ),
                        }
                    )
                    return

                await self.log_signal_async(
                    {
                        "action": "SKIP:NO_MARKETS",
                        "reason": (
                            f"no bundle candidate in {self.max_pages} pages "
                            f"(book_ok={scanned_books}, skip_high_buffer={skipped_gas}, "
                            f"skip_insufficient_depth={skipped_depth}, skip_mid={skipped_mid}; "
                            f"raw={markets_raw_total}, !accepting={skip_not_accepting}, closed={skip_closed}, "
                            f"!orderbook={skip_no_orderbook}, outcomes={skip_outcomes}, bad_book={skip_bad_book})"
                        ),
                        **empty,
                    }
                )
                self._persist_scan_diag(
                    {
                        "outcome": "SKIP:NO_MARKETS",
                        "pages_fetched": pages_fetched,
                        "markets_raw_total": markets_raw_total,
                        "scanned_books": scanned_books,
                        "skip_not_accepting": skip_not_accepting,
                        "skip_closed": skip_closed,
                        "skip_no_orderbook": skip_no_orderbook,
                        "skip_outcomes": skip_outcomes,
                        "skip_bad_book": skip_bad_book,
                        "skip_high_buffer": skipped_gas,
                        "skip_insufficient_depth": skipped_depth,
                        "skip_mid": skipped_mid,
                        "best_edge_net": None if best_edge_net < -1e8 else best_edge_net,
                        "min_edge": self.min_edge,
                        "max_size_usdc": self.max_size,
                        "mid_max_dev": self.mid_max_dev,
                        "eligible_sample": eligible_samples,
                        "params": {
                            "max_pages": self.max_pages,
                            "min_outcomes": self.min_outcomes,
                            "max_outcomes": self.max_outcomes,
                        },
                        "help": (
                            "Origen de datos: CLOB https://clob.polymarket.com/markets (paginación). "
                            "Cada fila `markets.data[]` es un mercado; bundle solo considera los que pasan filtros "
                            "(ver contadores). Luego GET /book por token para sumar best_ask y comprobar profundidad "
                            f"≥ size_usdc/n ({self.max_size} USDC / n piernas). "
                            "Si casi todo cae en skip_insufficient_depth, baja BUNDLE_MAX_SIZE_USDC o asume poco liquidez al mejor ask."
                        ),
                    }
                )
        except Exception as e:
            await self.log_signal_async(
                {
                    "action": "ERROR:API_ERROR",
                    "reason": str(e)[:200],
                    **empty,
                }
            )
            self._persist_scan_diag(
                {
                    "outcome": "ERROR:API_ERROR",
                    "error": str(e)[:300],
                    "pages_fetched": pages_fetched,
                    "markets_raw_total": markets_raw_total,
                    "eligible_sample": eligible_samples,
                }
            )
