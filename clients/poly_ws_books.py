"""Caché en vivo de best_ask vía WebSocket canal ``market`` (Polymarket CLOB)."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional

from clients.poly_clob import PolyCLOBClient, _best_price_side, _size_at_price

log = logging.getLogger("poly_ws_books")


def _extract_token_id(data: dict[str, Any]) -> str:
    for k in ("asset_id", "assetId", "token_id", "tokenId"):
        v = data.get(k)
        if v:
            return str(v).strip()
    return ""


def _ingest_book_shape(data: dict[str, Any], tid: str, store: Dict[str, dict[str, Any]]) -> None:
    asks = data.get("asks") or []
    if not asks and isinstance(data.get("book"), dict):
        asks = data["book"].get("asks") or []
    ba = _best_price_side(asks, is_bid=False)
    if ba is None:
        return
    sz = _size_at_price(asks, float(ba))
    nom: Optional[float] = None
    if sz is not None:
        try:
            nom = float(ba) * float(sz)
        except (TypeError, ValueError):
            nom = None
    store[tid] = {
        "best_ask": float(ba),
        "best_ask_notional_usdc": nom,
        "ts": time.monotonic(),
    }


class WSBookStore:
    """
    Mantiene un dict token_id → {best_ask, best_ask_notional_usdc, ts} alimentado por WS.
    Si no hay dato fresco, bundle_arb usa REST.
    """

    def __init__(self, poly: PolyCLOBClient, max_age_sec: float = 8.0) -> None:
        self._poly = poly
        self._max_age = max_age_sec
        self._books: Dict[str, dict[str, Any]] = {}
        self._task: Optional[asyncio.Task] = None
        self._subscribed: frozenset[str] = frozenset()

    def snapshot(self, token_id: str) -> Optional[dict[str, Any]]:
        row = self._books.get(token_id)
        if not row:
            return None
        if time.monotonic() - float(row.get("ts", 0)) > self._max_age:
            return None
        return row

    def _on_message(self, data: dict[str, Any]) -> None:
        tid = _extract_token_id(data)
        et = str(data.get("event_type") or data.get("type") or "").lower()
        if tid and et in ("book", "price_change", "tick_size_change", ""):
            if data.get("asks") is not None or (isinstance(data.get("book"), dict) and data["book"].get("asks")):
                _ingest_book_shape(data, tid, self._books)
                return
        if not tid:
            return
        for key in ("best_ask", "bestAsk"):
            if key in data and data[key] is not None:
                try:
                    px = float(data[key])
                except (TypeError, ValueError):
                    return
                nom = None
                for nk in ("best_ask_size", "bestAskSize", "size"):
                    if nk in data and data[nk] is not None:
                        try:
                            nom = px * float(data[nk])
                        except (TypeError, ValueError):
                            pass
                        break
                self._books[tid] = {"best_ask": px, "best_ask_notional_usdc": nom, "ts": time.monotonic()}
                return

    async def ensure_subscription(self, token_ids: List[str], *, cap: int = 400) -> None:
        """Arranca o reinicia la suscripción si el conjunto de tokens cambió de forma relevante."""
        ids = [str(x).strip() for x in token_ids if str(x).strip()]
        ids = ids[:cap]
        new_set = frozenset(ids)
        if not new_set:
            return
        if new_set == self._subscribed and self._task and not self._task.done():
            return
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        self._books.clear()
        self._subscribed = new_set
        on_msg: Callable[[dict[str, Any]], None] = lambda d: self._on_message(d)
        self._task = asyncio.create_task(
            self._poly.subscribe_market(list(new_set), on_msg),
            name="bundle_ws_books",
        )
        log.info("WSBookStore: suscripción a %s tokens", len(new_set))

    async def aclose(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        self._task = None
        self._subscribed = frozenset()
        self._books.clear()
