"""Kalshi Trade API v2 client (aiohttp only)."""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

import aiohttp

log = logging.getLogger("kalshi_rest")

KALSHI_BASE = "https://trading-api.kalshi.com/trade-api/v2"

_DEFAULT_HEADERS = {
    "User-Agent": "predmarket-arb/arb-engine (aiohttp)",
    "Accept": "application/json",
}


def _parse_orderbook_fp(data: dict[str, Any]) -> dict[str, Any]:
    """
    Kalshi devuelve solo bids yes/no; un bid YES a p implica ask NO a (1-p) en escala 0-1.
    Ver docs: Get Market Orderbook — orderbook_fp.yes_dollars / no_dollars.
    """
    ob = data.get("orderbook") or data.get("orderbook_fp") or {}
    yes_levels = ob.get("yes_dollars") or []
    no_levels = ob.get("no_dollars") or []
    yes_bid: Optional[float] = None
    no_bid: Optional[float] = None
    if yes_levels and isinstance(yes_levels[0], (list, tuple)) and len(yes_levels[0]) >= 1:
        yes_bid = float(yes_levels[0][0])
    if no_levels and isinstance(no_levels[0], (list, tuple)) and len(no_levels[0]) >= 1:
        no_bid = float(no_levels[0][0])
    yes_ask = (1.0 - no_bid) if no_bid is not None else None
    no_ask = (1.0 - yes_bid) if yes_bid is not None else None
    return {
        "yes_bid": yes_bid,
        "no_bid": no_bid,
        "yes_ask": yes_ask,
        "no_ask": no_ask,
        "orderbook_fp": ob,
    }


class KalshiRESTClient:
    def __init__(self, api_key: str = "", api_secret: str = "", dry_run: bool = True) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.dry_run = dry_run
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> KalshiRESTClient:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15),
            headers=_DEFAULT_HEADERS,
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    def _require_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("KalshiRESTClient must be used with async context manager")
        return self._session

    def _auth_headers(self) -> dict[str, str]:
        h = dict(_DEFAULT_HEADERS)
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        return h

    async def get_market(self, ticker: str) -> dict[str, Any]:
        """GET /markets/{ticker}"""
        sess = self._require_session()
        url = f"{KALSHI_BASE}/markets/{ticker.strip()}"
        async with sess.get(url, headers=self._auth_headers()) as resp:
            text = await resp.text()
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                return {"error": "invalid_json", "raw": text[:500], "status": resp.status}
            if resp.status != 200:
                data["http_status"] = resp.status
            return data

    async def get_orderbook(self, ticker: str) -> dict[str, Any]:
        """GET /markets/{ticker}/orderbook — deriva yes_ask / no_ask desde bids."""
        sess = self._require_session()
        url = f"{KALSHI_BASE}/markets/{ticker.strip()}/orderbook"
        async with sess.get(url, headers=self._auth_headers()) as resp:
            text = await resp.text()
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                return {"error": "invalid_json", "raw": text[:500], "status": resp.status}
            if resp.status != 200:
                data["http_status"] = resp.status
                return data
            derived = _parse_orderbook_fp(data)
            out = dict(data)
            out.update(derived)
            return out

    async def place_order(self, ticker: str, side: str, price: float, count: int) -> dict[str, Any]:
        if self.dry_run:
            raise RuntimeError("DRY_RUN=true")
        raise NotImplementedError("Kalshi place_order: implementar POST /portfolio/orders")
