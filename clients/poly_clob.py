"""Native Polymarket CLOB client (aiohttp REST + websockets)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
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
        self._last_req_mono: float = 0.0
        self._tick_cache: dict[str, tuple[float, str]] = {}
        self._neg_cache: dict[str, tuple[float, bool]] = {}
        self._fee_cache: dict[str, tuple[float, int]] = {}
        self._meta_ttl_sec: float = float(os.getenv("MARKET_META_CACHE_TTL_SEC", "600"))

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

    @property
    def http_session(self) -> aiohttp.ClientSession:
        """Sesión aiohttp compartida (p. ej. descubrimiento Gamma con el mismo cliente)."""
        return self._require_session()

    async def _clob_throttle(self) -> None:
        rps = float(os.getenv("CLOB_RATE_LIMIT_RPS", "0") or "0")
        if rps <= 0:
            return
        interval = 1.0 / max(rps, 0.01)
        now = time.monotonic()
        wait = interval - (now - self._last_req_mono)
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_req_mono = time.monotonic()

    async def _public_get_json(self, url: str, params: Optional[dict[str, str]] = None) -> Any:
        """GET público CLOB con throttle + reintentos ante 429/5xx."""
        max_retries = max(0, int(os.getenv("CLOB_RETRY_MAX", "3")))
        base_ms = float(os.getenv("CLOB_RETRY_BASE_MS", "200"))
        sess = self._require_session()
        attempt = 0
        while True:
            await self._clob_throttle()
            async with sess.get(url, params=params or None) as resp:
                text = await resp.text()
                if resp.status in (429, 500, 502, 503, 504) and attempt < max_retries:
                    attempt += 1
                    await asyncio.sleep((base_ms / 1000.0) * (2 ** (attempt - 1)))
                    continue
                if resp.status != 200:
                    raise RuntimeError(f"CLOB GET {url} HTTP {resp.status}: {text[:500]}")
                return json.loads(text)

    def _chain_id(self) -> int:
        return int(os.getenv("POLYGON_CHAIN_ID", "137"))

    def _live_secrets(self) -> tuple[str, str]:
        secret = os.getenv("POLY_API_SECRET", "").strip()
        passphrase = os.getenv("POLY_PASSPHRASE", "").strip()
        return secret, passphrase

    async def get_markets(self, limit: int = 100, next_cursor: str = "") -> dict[str, Any]:
        """GET /markets — lista paginada; retorna {data, next_cursor, ...}."""
        params: dict[str, str] = {"limit": str(limit)}
        if next_cursor:
            params["next_cursor"] = next_cursor
        data = await self._public_get_json(f"{CLOB_REST}/markets", params)
        return data if isinstance(data, dict) else {}

    async def get_simplified_markets(self, next_cursor: str = "") -> dict[str, Any]:
        """GET /simplified-markets — payload compacto con tokens y flags."""
        params: dict[str, str] = {}
        if next_cursor:
            params["next_cursor"] = next_cursor
        lim = os.getenv("BUNDLE_SIMPLIFIED_LIMIT", "").strip()
        if lim.isdigit():
            params["limit"] = lim
        data = await self._public_get_json(f"{CLOB_REST}/simplified-markets", params or None)
        return data if isinstance(data, dict) else {}

    async def get_sampling_markets(self) -> dict[str, Any]:
        """GET /sampling-markets — subset con rewards / mercados muestreados."""
        data = await self._public_get_json(f"{CLOB_REST}/sampling-markets", None)
        return data if isinstance(data, dict) else {}

    async def get_orderbook(self, token_id: str) -> dict[str, Any]:
        """GET /book?token_id=… — bids[], asks[], best_bid, best_ask si hay libro."""
        sess = self._require_session()
        url = f"{CLOB_REST}/book"
        params = {"token_id": token_id}
        max_retries = max(0, int(os.getenv("CLOB_RETRY_MAX", "3")))
        base_ms = float(os.getenv("CLOB_RETRY_BASE_MS", "200"))
        attempt = 0
        while True:
            await self._clob_throttle()
            async with sess.get(url, params=params) as resp:
                text = await resp.text()
                if resp.status in (429, 500, 502, 503, 504) and attempt < max_retries:
                    attempt += 1
                    await asyncio.sleep((base_ms / 1000.0) * (2 ** (attempt - 1)))
                    continue
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    return {"error": "json_decode", "raw": text[:500]}
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
                if best_ask is not None:
                    sz = _size_at_price(asks, best_ask)
                    out["best_ask_size"] = sz
                    if sz is not None:
                        out["best_ask_notional_usdc"] = float(best_ask) * float(sz)
                    else:
                        out["best_ask_notional_usdc"] = None
                else:
                    out["best_ask_size"] = None
                    out["best_ask_notional_usdc"] = None
                return out

    async def get_midpoint(self, token_id: str) -> Optional[float]:
        """GET /midpoint?token_id=… — precio medio CLOB (público)."""
        try:
            data = await self._public_get_json(f"{CLOB_REST}/midpoint", {"token_id": token_id})
        except Exception:
            return None
        if not isinstance(data, dict):
            return None
        mid = data.get("mid")
        if mid is None:
            return None
        try:
            return float(mid)
        except (TypeError, ValueError):
            return None

    async def get_price(self, token_id: str, side: str = "buy") -> Optional[float]:
        """GET /price?token_id=…&side=buy|sell (público)."""
        s = side.strip().lower()
        if s not in ("buy", "sell"):
            s = "buy"
        try:
            data = await self._public_get_json(
                f"{CLOB_REST}/price", {"token_id": token_id, "side": s}
            )
        except Exception:
            return None
        if not isinstance(data, dict):
            return None
        px = data.get("price") or data.get("p")
        if px is None:
            return None
        try:
            return float(px)
        except (TypeError, ValueError):
            return None

    async def get_open_orders(
        self,
        next_cursor: str = "MA==",
        market: str = "",
        asset_id: str = "",
        order_id: str = "",
    ) -> dict[str, Any]:
        """
        GET /data/orders — órdenes abiertas (L2). Requiere credenciales en env + private_key.
        Devuelve JSON con ``data`` y ``next_cursor`` como el CLOB.
        """
        secret, passphrase = self._live_secrets()
        if not self.private_key or not self.api_key or not secret or not passphrase:
            raise RuntimeError("Credenciales incompletas para get_open_orders")
        from eth_account import Account

        signer_addr = Account.from_key(self.private_key).address
        request_path = "/data/orders"
        headers = build_l2_headers(
            signer_addr,
            self.api_key,
            secret,
            passphrase,
            "GET",
            request_path,
            None,
        )
        req_headers = {**_DEFAULT_HEADERS, **headers}
        sess = self._require_session()
        params: dict[str, str] = {}
        if next_cursor:
            params["next_cursor"] = next_cursor
        if market:
            params["market"] = market
        if asset_id:
            params["asset_id"] = asset_id
        if order_id:
            params["id"] = order_id
        url = f"{CLOB_REST}{request_path}"
        async with sess.get(url, headers=req_headers, params=params or None) as resp:
            text = await resp.text()
            try:
                out = json.loads(text)
            except json.JSONDecodeError:
                return {"error": f"HTTP {resp.status}", "raw": text[:500], "data": []}
            if resp.status != 200:
                return {"error": f"HTTP {resp.status}", "raw": text[:500], "data": []}
            return out if isinstance(out, dict) else {"data": []}

    async def _l2_get_json(self, request_path: str, params: Optional[dict[str, str]] = None) -> Any:
        """GET firmado L2 bajo ``/data/...`` (mismo patrón que ``get_open_orders``)."""
        secret, passphrase = self._live_secrets()
        if not self.private_key or not self.api_key or not secret or not passphrase:
            raise RuntimeError("Credenciales incompletas para _l2_get_json")
        from eth_account import Account

        signer_addr = Account.from_key(self.private_key).address
        headers = build_l2_headers(
            signer_addr,
            self.api_key,
            secret,
            passphrase,
            "GET",
            request_path,
            None,
        )
        req_headers = {**_DEFAULT_HEADERS, **headers}
        sess = self._require_session()
        url = f"{CLOB_REST}{request_path}"
        await self._clob_throttle()
        async with sess.get(url, headers=req_headers, params=params or None) as resp:
            text = await resp.text()
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {"error": "json_decode", "raw": text[:500], "http_status": resp.status}

    async def get_order(self, order_id: str) -> dict[str, Any]:
        """GET /data/order — detalle de orden (p. ej. ``size_matched``, ``original_size``). Requiere L2."""
        oid = order_id.strip()
        if not oid:
            return {"error": "empty_order_id"}
        data = await self._l2_get_json("/data/order", {"id": oid})
        return data if isinstance(data, dict) else {"error": "invalid_response"}

    async def get_trades(
        self,
        *,
        asset_id: str = "",
        market: str = "",
        after: str = "",
        next_cursor: str = "",
    ) -> dict[str, Any]:
        """
        GET /data/trades — historial de trades (fills). Requiere L2.
        Filtros opcionales según API CLOB (``asset_id``, ``market``, ``after``, ``next_cursor``).
        """
        params: dict[str, str] = {}
        if asset_id:
            params["asset_id"] = asset_id
        if market:
            params["market"] = market
        if after:
            params["after"] = after
        if next_cursor:
            params["next_cursor"] = next_cursor
        data = await self._l2_get_json("/data/trades", params or None)
        return data if isinstance(data, dict) else {"error": "invalid_response", "data": []}

    async def post_orders_batch(self, orders_payload: list[dict[str, Any]]) -> dict[str, Any]:
        """
        POST /orders — hasta 15 órdenes por request (si el endpoint existe en el CLOB).
        ``orders_payload`` debe ser la lista de cuerpos ya firmados o el formato que exija la API.
        Si HTTP != 200, devuelve dict con error. Respuesta 200 puede contener fallos **por orden**.
        """
        if self.dry_run:
            raise RuntimeError("DRY_RUN=true")
        secret, passphrase = self._live_secrets()
        if not self.private_key or not self.api_key or not secret or not passphrase:
            raise RuntimeError("Credenciales incompletas para post_orders_batch")
        from eth_account import Account

        signer_addr = Account.from_key(self.private_key).address
        body_obj = orders_payload
        serialized = json.dumps(body_obj, separators=(",", ":"), ensure_ascii=False)
        headers = build_l2_headers(
            signer_addr,
            self.api_key,
            secret,
            passphrase,
            "POST",
            "/orders",
            serialized,
        )
        req_headers = {**_DEFAULT_HEADERS, **headers, "Content-Type": "application/json"}
        sess = self._require_session()
        url = f"{CLOB_REST}/orders"
        await self._clob_throttle()
        async with sess.post(url, data=serialized.encode("utf-8"), headers=req_headers) as resp:
            text = await resp.text()
            try:
                out = json.loads(text)
            except json.JSONDecodeError:
                out = {"raw": text[:1000], "http_status": resp.status}
            if not isinstance(out, dict):
                out = {"response": out, "http_status": resp.status}
            out["http_status"] = resp.status
            return out

    async def get_market(self, market_id: str) -> dict[str, Any]:
        """GET /markets/{condition_id} — metadata de un mercado."""
        mid = market_id.strip()
        try:
            return await self._public_get_json(f"{CLOB_REST}/markets/{mid}", None)
        except Exception as e:
            return {"error": str(e)[:200]}

    async def get_tick_size(self, token_id: str) -> str:
        now = time.monotonic()
        hit = self._tick_cache.get(token_id)
        if hit and hit[0] > now:
            return hit[1]
        data = await self._public_get_json(f"{CLOB_REST}/tick-size", {"token_id": token_id})
        if not isinstance(data, dict):
            raise RuntimeError("tick-size: invalid JSON")
        tick = str(data.get("minimum_tick_size") or data.get("tick_size") or "0.01")
        self._tick_cache[token_id] = (now + self._meta_ttl_sec, tick)
        return tick

    async def get_neg_risk(self, token_id: str) -> bool:
        now = time.monotonic()
        hit = self._neg_cache.get(token_id)
        if hit and hit[0] > now:
            return hit[1]
        data = await self._public_get_json(f"{CLOB_REST}/neg-risk", {"token_id": token_id})
        if not isinstance(data, dict):
            raise RuntimeError("neg-risk: invalid JSON")
        neg = bool(data.get("neg_risk", False))
        self._neg_cache[token_id] = (now + self._meta_ttl_sec, neg)
        return neg

    async def get_fee_rate_bps(self, token_id: str) -> int:
        now = time.monotonic()
        hit = self._fee_cache.get(token_id)
        if hit and hit[0] > now:
            return hit[1]
        data = await self._public_get_json(f"{CLOB_REST}/fee-rate", {"token_id": token_id})
        if not isinstance(data, dict):
            return 0
        v = data.get("base_fee")
        try:
            bps = int(v) if v is not None else 0
        except (TypeError, ValueError):
            bps = 0
        self._fee_cache[token_id] = (now + self._meta_ttl_sec, bps)
        return bps

    async def place_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size_usdc: float,
        *,
        order_type: str = "GTC",
        post_only: bool = False,
        order_expiration_unix: int = 0,
    ) -> dict[str, Any]:
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

        exp = int(order_expiration_unix)
        if (order_type or "GTC").strip().upper() == "GTD" and exp <= 0:
            ttl = max(120, int(os.getenv("BUNDLE_ORDER_TTL_SECONDS", "180")))
            exp = int(time.time()) + ttl

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
                expiration=exp,
                order_type=order_type,
                post_only=post_only,
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


def _level_size(lvl: Any) -> Optional[float]:
    if isinstance(lvl, dict):
        raw = lvl.get("size")
    elif isinstance(lvl, (list, tuple)) and len(lvl) > 1:
        raw = lvl[1]
    else:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _size_at_price(levels: list, target_price: float) -> Optional[float]:
    """Suma tamaños en niveles cuyo precio coincide con target (tolerancia float)."""
    eps = 1e-9
    total = 0.0
    found = False
    for lvl in levels:
        p = _level_price(lvl)
        if p is None or abs(p - target_price) > eps:
            continue
        sz = _level_size(lvl)
        if sz is not None:
            total += sz
            found = True
    return total if found else None


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
