"""Native Polymarket CLOB client (aiohttp REST + websockets)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Callable, List, Optional, Union

import aiohttp
import websockets

from clients.poly_clob_auth import build_l2_headers

log = logging.getLogger("poly_clob")

CLOB_REST = "https://clob.polymarket.com"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

_DEFAULT_HEADERS = {
    "User-Agent": "predmarket-arb/arb-engine (aiohttp; +https://github.com)",
    "Accept": "application/json",
}


class PolyCLOBClient:
    def __init__(self, api_key: str = "", private_key: str = "", dry_run: bool = True) -> None:
        self.api_key = api_key.strip()
        self.private_key = private_key.strip()
        self.dry_run = dry_run
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> PolyCLOBClient:
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
            raise RuntimeError("PolyCLOBClient must be used with async context manager")
        return self._session

    def _chain_id(self) -> int:
        return int(os.getenv("POLYGON_CHAIN_ID", "137"))

    def _live_secrets(self) -> tuple[str, str]:
        secret = os.getenv("POLY_API_SECRET", "").strip()
        passphrase = os.getenv("POLY_PASSPHRASE", "").strip()
        return secret, passphrase

    async def get_markets(self, limit: int = 100, next_cursor: str = "") -> dict[str, Any]:
        """GET /markets — lista paginada; retorna {data, next_cursor, ...}."""
        sess = self._require_session()
        params: dict[str, str] = {"limit": str(limit)}
        if next_cursor:
            params["next_cursor"] = next_cursor
        url = f"{CLOB_REST}/markets"
        async with sess.get(url, params=params) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"CLOB /markets HTTP {resp.status}: {text[:500]}")
            return json.loads(text)

    async def get_orderbook(self, token_id: str) -> dict[str, Any]:
        """GET /book?token_id=… — bids[], asks[], best_bid, best_ask si hay libro."""
        sess = self._require_session()
        url = f"{CLOB_REST}/book"
        async with sess.get(url, params={"token_id": token_id}) as resp:
            text = await resp.text()
            data = json.loads(text)
            if isinstance(data, dict) and data.get("error"):
                return data
            if resp.status != 200:
                return {"error": f"HTTP {resp.status}", "raw": text[:500]}
            bids = data.get("bids") or []
            asks = data.get("asks") or []
            best_bid = _best_price_side(bids, is_bid=True)
            best_ask = _best_price_side(asks, is_bid=False)
            out = dict(data)
            out["best_bid"] = best_bid
            out["best_ask"] = best_ask
            return out

    async def get_market(self, market_id: str) -> dict[str, Any]:
        """GET /markets/{condition_id} — metadata de un mercado."""
        sess = self._require_session()
        mid = market_id.strip()
        url = f"{CLOB_REST}/markets/{mid}"
        async with sess.get(url) as resp:
            text = await resp.text()
            if resp.status != 200:
                return {"error": f"HTTP {resp.status}", "raw": text[:500]}
            return json.loads(text)

    async def get_tick_size(self, token_id: str) -> str:
        sess = self._require_session()
        async with sess.get(f"{CLOB_REST}/tick-size", params={"token_id": token_id}) as resp:
            text = await resp.text()
            data = json.loads(text)
            if resp.status != 200:
                raise RuntimeError(f"tick-size HTTP {resp.status}: {text[:300]}")
            return str(data.get("minimum_tick_size") or data.get("tick_size") or "0.01")

    async def get_neg_risk(self, token_id: str) -> bool:
        sess = self._require_session()
        async with sess.get(f"{CLOB_REST}/neg-risk", params={"token_id": token_id}) as resp:
            text = await resp.text()
            data = json.loads(text)
            if resp.status != 200:
                raise RuntimeError(f"neg-risk HTTP {resp.status}: {text[:300]}")
            return bool(data.get("neg_risk", False))

    async def get_fee_rate_bps(self, token_id: str) -> int:
        sess = self._require_session()
        async with sess.get(f"{CLOB_REST}/fee-rate", params={"token_id": token_id}) as resp:
            text = await resp.text()
            data = json.loads(text)
            if resp.status != 200:
                return 0
            v = data.get("base_fee")
            try:
                return int(v) if v is not None else 0
            except (TypeError, ValueError):
                return 0

    async def place_order(self, token_id: str, side: str, price: float, size_usdc: float) -> dict[str, Any]:
        """POST /order — firma EIP-712 vía py-order-utils + cabeceras L2. Requiere DRY_RUN=false y credenciales."""
        if self.dry_run:
            raise RuntimeError("DRY_RUN=true — orden simulada, no enviada")
        from clients.poly_order_live import LiveDeps, build_post_order_body

        deps = LiveDeps.check()
        if not deps.ok:
            raise RuntimeError(deps.error)
        if not self.private_key:
            raise RuntimeError("POLY_PRIVATE_KEY requerido para órdenes live")
        secret, passphrase = self._live_secrets()
        if not self.api_key or not secret or not passphrase:
            raise RuntimeError(
                "Faltan POLY_API_KEY, POLY_API_SECRET o POLY_PASSPHRASE para autenticación L2 del CLOB"
            )

        from eth_account import Account

        signer_addr = Account.from_key(self.private_key).address

        tick = await self.get_tick_size(token_id)
        neg = await self.get_neg_risk(token_id)
        fee_bps = await self.get_fee_rate_bps(token_id)

        def _build() -> tuple[str, dict[str, Any]]:
            return build_post_order_body(
                private_key=self.private_key,
                api_key=self.api_key,
                chain_id=self._chain_id(),
                token_id=token_id,
                side=side,
                price=price,
                size_usdc=size_usdc,
                tick_size=tick,
                neg_risk=neg,
                fee_rate_bps=fee_bps,
            )

        serialized, body = await asyncio.to_thread(_build)
        headers = build_l2_headers(
            signer_addr,
            self.api_key,
            secret,
            passphrase,
            "POST",
            "/order",
            serialized,
        )
        req_headers = {**_DEFAULT_HEADERS, **headers, "Content-Type": "application/json"}
        sess = self._require_session()
        url = f"{CLOB_REST}/order"
        async with sess.post(url, data=serialized.encode("utf-8"), headers=req_headers) as resp:
            text = await resp.text()
            try:
                out = json.loads(text)
            except json.JSONDecodeError:
                out = {"raw": text[:1000], "http_status": resp.status}
            if resp.status != 200:
                raise RuntimeError(f"CLOB POST /order HTTP {resp.status}: {text[:800]}")
            return out if isinstance(out, dict) else {"response": out}

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        if self.dry_run:
            raise RuntimeError("DRY_RUN=true")
        secret, passphrase = self._live_secrets()
        if not self.private_key or not self.api_key or not secret or not passphrase:
            raise RuntimeError("Credenciales incompletas para cancel_order")
        from eth_account import Account

        signer_addr = Account.from_key(self.private_key).address
        body = {"orderID": order_id}
        serialized = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        headers = build_l2_headers(
            signer_addr,
            self.api_key,
            secret,
            passphrase,
            "DELETE",
            "/order",
            serialized,
        )
        req_headers = {**_DEFAULT_HEADERS, **headers, "Content-Type": "application/json"}
        sess = self._require_session()
        url = f"{CLOB_REST}/order"
        async with sess.delete(url, data=serialized.encode("utf-8"), headers=req_headers) as resp:
            text = await resp.text()
            try:
                out = json.loads(text)
            except json.JSONDecodeError:
                out = {"raw": text[:500], "http_status": resp.status}
            if resp.status not in (200, 204):
                raise RuntimeError(f"CLOB DELETE /order HTTP {resp.status}: {text[:500]}")
            return out if isinstance(out, dict) else {}

    async def cancel_all(self) -> dict[str, Any]:
        """DELETE /cancel-all — cancela todas las órdenes del usuario (L2)."""
        if self.dry_run:
            raise RuntimeError("DRY_RUN=true")
        secret, passphrase = self._live_secrets()
        if not self.private_key or not self.api_key or not secret or not passphrase:
            raise RuntimeError("Credenciales incompletas para cancel_all")
        from eth_account import Account

        signer_addr = Account.from_key(self.private_key).address
        headers = build_l2_headers(
            signer_addr,
            self.api_key,
            secret,
            passphrase,
            "DELETE",
            "/cancel-all",
            None,
        )
        req_headers = {**_DEFAULT_HEADERS, **headers}
        sess = self._require_session()
        url = f"{CLOB_REST}/cancel-all"
        async with sess.delete(url, headers=req_headers) as resp:
            text = await resp.text()
            try:
                out = json.loads(text) if text.strip() else {}
            except json.JSONDecodeError:
                out = {"raw": text[:500], "http_status": resp.status}
            if resp.status not in (200, 204):
                raise RuntimeError(f"CLOB DELETE /cancel-all HTTP {resp.status}: {text[:500]}")
            return out if isinstance(out, dict) else {}

    async def cancel_market_orders(self, market: str = "", asset_id: str = "") -> dict[str, Any]:
        """DELETE /cancel-market-orders — body JSON {market, asset_id} (al menos uno típicamente)."""
        if self.dry_run:
            raise RuntimeError("DRY_RUN=true")
        secret, passphrase = self._live_secrets()
        if not self.private_key or not self.api_key or not secret or not passphrase:
            raise RuntimeError("Credenciales incompletas para cancel_market_orders")
        from eth_account import Account

        signer_addr = Account.from_key(self.private_key).address
        body = {"market": market, "asset_id": asset_id}
        serialized = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        headers = build_l2_headers(
            signer_addr,
            self.api_key,
            secret,
            passphrase,
            "DELETE",
            "/cancel-market-orders",
            serialized,
        )
        req_headers = {**_DEFAULT_HEADERS, **headers, "Content-Type": "application/json"}
        sess = self._require_session()
        url = f"{CLOB_REST}/cancel-market-orders"
        async with sess.delete(url, data=serialized.encode("utf-8"), headers=req_headers) as resp:
            text = await resp.text()
            try:
                out = json.loads(text)
            except json.JSONDecodeError:
                out = {"raw": text[:500], "http_status": resp.status}
            if resp.status not in (200, 204):
                raise RuntimeError(f"CLOB DELETE /cancel-market-orders HTTP {resp.status}: {text[:500]}")
            return out if isinstance(out, dict) else {}

    async def subscribe_market(
        self,
        token_ids: Union[str, List[str]],
        on_update: Callable[[dict[str, Any]], None],
    ) -> None:
        """
        WebSocket canal `market` (docs Polymarket).
        `token_ids`: uno o más **asset_id** (token_id CLOB), no el condition_id.
        Payload: type=market, assets_ids, custom_feature_enabled=true (best_bid_ask, etc.).
        """
        if isinstance(token_ids, str):
            ids: List[str] = [token_ids.strip()]
        else:
            ids = [str(x).strip() for x in token_ids if str(x).strip()]
        if not ids:
            raise ValueError("subscribe_market: token_ids vacío")

        sub = {
            "assets_ids": ids,
            "type": "market",
            "custom_feature_enabled": True,
        }
        sub_msg = json.dumps(sub)
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(
                    CLOB_WS,
                    ping_interval=20,
                    ping_timeout=20,
                    extra_headers={"User-Agent": _DEFAULT_HEADERS["User-Agent"]},
                ) as ws:
                    await ws.send(sub_msg)
                    backoff = 1.0
                    async for raw in ws:
                        try:
                            if isinstance(raw, bytes):
                                raw = raw.decode("utf-8")
                            data = json.loads(raw)
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            continue
                        if isinstance(data, dict):
                            on_update(data)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(
                    "WS market tokens=%s… reconnect in %.1fs: %s",
                    ids[0][:16],
                    backoff,
                    e,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)


def _best_price_side(levels: list, is_bid: bool) -> Optional[float]:
    """levels: [{price, size}, …] o [price, size] strings."""
    best: Optional[float] = None
    for lvl in levels:
        p = _level_price(lvl)
        if p is None:
            continue
        if best is None:
            best = p
        elif is_bid:
            best = max(best, p)
        else:
            best = min(best, p)
    return best


def _level_price(lvl: Any) -> Optional[float]:
    if isinstance(lvl, dict):
        raw = lvl.get("price")
    elif isinstance(lvl, (list, tuple)) and lvl:
        raw = lvl[0]
    else:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None
