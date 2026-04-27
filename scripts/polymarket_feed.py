"""
Gamma: mercados 5m crypto por slug (GET /markets/slug/{slug}).
RTDS: precios crypto (topic crypto_prices) — ver docs Polymarket.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

import requests

log = logging.getLogger("validate_edge")

# Slug en URL Polymarket: {prefix}-updown-5m-{unix_inicio_ventana_5m_utc}
SLUG_PREFIX: dict[str, str] = {
    "BTC": "btc",
    "ETH": "eth",
    "SOL": "sol",
    "XRP": "xrp",
    "BNB": "bnb",
}

# RTDS Binance source (minúsculas); BNB no figura en la doc oficial — no suscribimos por WS.
POLY_BINANCE_SYMBOL: dict[str, str] = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
    "XRP": "xrpusdt",
}

POLY_SYMBOL_TO_ASSET: dict[str, str] = {v: k for k, v in POLY_BINANCE_SYMBOL.items()}


def floor_5m_window_ts_utc(now: datetime) -> int:
    """Inicio de la ventana de 5 minutos actual en UTC (epoch segundos)."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    minute = (now.minute // 5) * 5
    floored = now.replace(minute=minute, second=0, microsecond=0)
    return int(floored.timestamp())


def slug_for_5m(asset: str, window_ts: int) -> str:
    pfx = SLUG_PREFIX.get(asset)
    if not pfx:
        raise KeyError(asset)
    return f"{pfx}-updown-5m-{window_ts}"


def fetch_market_by_slug(
    session: requests.Session,
    gamma_base: str,
    slug: str,
) -> dict[str, Any] | None:
    url = f"{gamma_base.rstrip('/')}/markets/slug/{slug}"
    try:
        r = session.get(url, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict):
            return data
        return None
    except Exception as e:
        log.debug("Gamma slug %s: %s", slug, e)
        return None


def fetch_5m_markets_by_slug(
    session: requests.Session,
    gamma_base: str,
    assets: list[str],
    now_utc: datetime,
) -> tuple[list[dict[str, Any]], int]:
    """
    Descarga un mercado por activo para la ventana 5m UTC actual.
    Prueba ts y ts±300 por desfases en el borde de ventana.
    """
    win_ts = floor_5m_window_ts_utc(now_utc)
    out: list[dict[str, Any]] = []
    for asset in assets:
        m: dict[str, Any] | None = None
        used_delta: int | None = None
        used_slug = ""
        for delta in (0, -300, 300, -600, 600):
            ts = win_ts + delta
            slug = slug_for_5m(asset, ts)
            m = fetch_market_by_slug(session, gamma_base, slug)
            if m is not None:
                used_delta = delta
                used_slug = slug
                break
        if m is None:
            log.warning(
                "Gamma slug: sin mercado para %s (ventana ts=%s; probado ±300/600)",
                asset,
                win_ts,
            )
            continue
        log.info(
            "Gamma slug OK %s → %s (offset_s=%s id=%s)",
            asset,
            used_slug,
            used_delta,
            m.get("id", ""),
        )
        row = dict(m)
        row["_validator_asset"] = asset
        out.append(row)
    return out, win_ts


def polymarket_crypto_ws_bundle(
    ws_url: str,
    stop_event: threading.Event,
    tick_lock: threading.Lock,
    raw_ticks: dict[str, Any],
    tick_counters: dict[str, int],
    closes_5m: dict[str, Any],
    last_price: dict[str, float],
    assets_ws: list[str],
    binance_poll_sec: float,
    aggregate_every_n_ticks: int,
    on_ws_status: Callable[[str], None] | None = None,
) -> list[threading.Thread]:
    """
    Hilos: WebSocket RTDS (actualiza último precio en memoria) + muestreo cada binance_poll_sec
    (misma semántica que price_worker: tick + agregación 5m).
    """
    try:
        import websocket  # type: ignore[import-untyped]
    except ImportError as e:
        raise RuntimeError("Instala websocket-client (pyproject / uv pip)") from e

    latest: dict[str, float] = {}
    ws_holder: dict[str, Any] = {"ws": None}
    first_rtds_price_logged: set[str] = set()
    rtds_lock = threading.Lock()

    def notify(msg: str) -> None:
        if on_ws_status:
            try:
                on_ws_status(msg)
            except Exception:
                pass

    # La API RTDS puede rechazar filters tipo "btcusdt" (validación JSON); sin filters recibes
    # todos los pares Binance y filtramos en cliente (docs: subscribe sin filters = all symbols).
    subscribe_msg = {
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "crypto_prices",
                "type": "update",
            }
        ],
    }

    def on_message(_ws: Any, message: str) -> None:
        if isinstance(message, bytes):
            message = message.decode("utf-8", errors="replace")
        if message == "PONG":
            return
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        if not isinstance(data, dict):
            return
        if data.get("topic") != "crypto_prices":
            return
        payload = data.get("payload")
        if not isinstance(payload, dict):
            return
        sym = str(payload.get("symbol", "")).lower()
        asset = POLY_SYMBOL_TO_ASSET.get(sym)
        if asset is None or asset not in assets_ws:
            return
        try:
            val = float(payload["value"])
        except (KeyError, TypeError, ValueError):
            return
        latest[asset] = val
        with rtds_lock:
            if asset not in first_rtds_price_logged:
                first_rtds_price_logged.add(asset)
                log.info(
                    "Polymarket RTDS: primer precio en vivo %s=%s (pair=%s)",
                    asset,
                    val,
                    sym,
                )

    def on_open(ws: Any) -> None:
        ws_holder["ws"] = ws
        try:
            ws.send(json.dumps(subscribe_msg))
            notify("RTDS conectado y suscrito crypto_prices")
            log.info(
                "Polymarket RTDS: WebSocket abierto %s — suscripción crypto_prices (activos %s)",
                ws_url,
                ",".join(assets_ws),
            )
        except Exception as e:
            log.warning("Polymarket RTDS: fallo al suscribir: %s", e)

    def on_error(_ws: Any, error: Any) -> None:
        log.warning("Polymarket RTDS error: %s", error)

    def on_close(_ws: Any, close_status_code: Any, close_msg: Any) -> None:
        ws_holder["ws"] = None
        log.info("Polymarket RTDS cerrado code=%s msg=%s", close_status_code, close_msg)

    def ping_loop() -> None:
        while not stop_event.wait(5.0):
            w = ws_holder.get("ws")
            if w is None:
                continue
            try:
                w.send("PING")
            except Exception:
                pass

    def sample_loop() -> None:
        # INFO en el 1.er ciclo y cada N ciclos para comprobar muestreo + últimos vistos en WS
        log_every_n = int(os.getenv("RTDS_SAMPLE_LOG_EVERY_N", "10"))
        sample_i = 0
        while not stop_event.wait(binance_poll_sec):
            sample_i += 1
            missing_before: list[str] = []
            with tick_lock:
                for asset in assets_ws:
                    px = latest.get(asset)
                    if px is None:
                        missing_before.append(asset)
                        continue
                    last_price[asset] = px
                    dq = raw_ticks[asset]
                    dq.append(px)
                    tick_counters[asset] = tick_counters.get(asset, 0) + 1
                    if tick_counters[asset] % aggregate_every_n_ticks == 0:
                        closes_5m[asset].append(px)
                        log.debug("5m close %s (RTDS): %s (n=%s)", asset, px, len(closes_5m[asset]))
            snap = {a: latest.get(a) for a in assets_ws}
            if sample_i == 1 or (log_every_n > 0 and sample_i % log_every_n == 0):
                log.info(
                    "Polymarket RTDS: muestreo #%s (cada %.0fs) últimos_WS=%s",
                    sample_i,
                    binance_poll_sec,
                    snap,
                )
            if missing_before and (sample_i <= 2 or sample_i % 20 == 0):
                log.warning(
                    "Polymarket RTDS: muestreo #%s sin precio WS aún para %s (normal los primeros segundos)",
                    sample_i,
                    ", ".join(missing_before),
                )

    def ws_run_loop() -> None:
        backoff = 2.0
        while not stop_event.is_set():
            try:
                app = websocket.WebSocketApp(
                    ws_url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                notify("RTDS conectando…")
                app.run_forever(ping_interval=0, ping_timeout=None)
            except Exception as e:
                log.warning("Polymarket RTDS run_forever: %s", e)
            ws_holder["ws"] = None
            if stop_event.is_set():
                break
            wait_s = min(backoff, 30.0)
            log.info("Polymarket RTDS: reconexión en %.1fs (backoff hasta 60s)", wait_s)
            if stop_event.wait(wait_s):
                break
            backoff = min(backoff * 1.5, 60.0)

    threads: list[threading.Thread] = []
    t_ws = threading.Thread(target=ws_run_loop, name="poly-rtds-ws", daemon=True)
    t_ping = threading.Thread(target=ping_loop, name="poly-rtds-ping", daemon=True)
    t_sample = threading.Thread(target=sample_loop, name="poly-rtds-sample", daemon=True)
    for t in (t_ws, t_ping, t_sample):
        t.start()
        threads.append(t)
    return threads


def clob_market_ws_bundle(
    stop_event: threading.Event,
    tick_lock: threading.Lock,
    market_prices: dict[str, float],
    token_ids: list[str],
) -> threading.Thread:
    """
    CLOB market WS (sin auth): mantiene midpoint por token_id (asset_id).
    Re-suscribe automáticamente si cambia token_ids.
    """
    try:
        import websocket  # type: ignore[import-untyped]
    except ImportError as e:
        raise RuntimeError("Instala websocket-client (pyproject / uv pip)") from e

    ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def _run() -> None:
        ws_holder: dict[str, Any] = {"ws": None}
        subscribed_ids: set[str] = set()
        first_price_logged: set[str] = set()

        def _wanted_ids() -> list[str]:
            # token_ids se comparte desde validate_edge; hacemos copia para evitar carreras.
            dedup = list(dict.fromkeys(str(t) for t in token_ids if str(t).strip()))
            return dedup

        def _send_subscription(ws: Any) -> None:
            wanted = _wanted_ids()
            if not wanted:
                return
            sub = {
                "assets_ids": wanted,
                "type": "market",
                "custom_feature_enabled": True,
            }
            ws.send(json.dumps(sub))
            subscribed_ids.clear()
            subscribed_ids.update(wanted)
            log.info("CLOB WS: subscribed to %s tokens", len(wanted))

        def on_open(ws: Any) -> None:
            ws_holder["ws"] = ws
            try:
                _send_subscription(ws)
            except Exception as e:
                log.warning("CLOB WS: fallo al suscribir: %s", e)

        def on_message(_ws: Any, message: Any) -> None:
            # Skip binary frames and empty/control messages.
            if not message:
                return
            if isinstance(message, bytes):
                return
            if len(message) < 2:
                return

            try:
                data = json.loads(message)
            except (json.JSONDecodeError, ValueError):
                return

            if not isinstance(data, dict):
                return

            event_type = data.get("event_type")
            if not event_type:
                log.debug("CLOB WS non-event message: %s", str(data)[:100])
                return

            asset_id = str(data.get("asset_id", ""))

            try:
                if event_type == "book":
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    if bids and asks and asset_id:
                        mid = (float(bids[0]["price"]) + float(asks[0]["price"])) / 2.0
                        with tick_lock:
                            is_first = asset_id not in market_prices
                            market_prices[asset_id] = mid
                        if is_first and asset_id not in first_price_logged:
                            first_price_logged.add(asset_id)
                            log.info("CLOB WS: first price %s=%.4f", asset_id[:16], mid)
                        log.debug("CLOB book %s: mid=%.4f", asset_id[:8], mid)
                elif event_type == "price_change":
                    for change in data.get("price_changes", []):
                        bid = change.get("best_bid")
                        ask = change.get("best_ask")
                        if bid and ask:
                            mid = (float(bid) + float(ask)) / 2.0
                            aid = str(change.get("asset_id", asset_id))
                            if not aid:
                                continue
                            with tick_lock:
                                is_first = aid not in market_prices
                                market_prices[aid] = mid
                            if is_first and aid not in first_price_logged:
                                first_price_logged.add(aid)
                                log.info("CLOB WS: first price %s=%.4f", aid[:16], mid)
                            log.debug("CLOB price_change %s: mid=%.4f", aid[:8], mid)
                elif event_type == "last_trade_price":
                    price = data.get("price")
                    if price and asset_id:
                        with tick_lock:
                            is_first = asset_id not in market_prices
                            market_prices.setdefault(asset_id, float(price))
                        if is_first and asset_id not in first_price_logged:
                            first_price_logged.add(asset_id)
                            log.info("CLOB WS: first price %s=%.4f", asset_id[:16], float(price))
            except Exception as e:
                log.warning("CLOB WS processing error: %s | data: %s", e, str(data)[:200])
                log.debug("CLOB WS skip: %s", e)

        def on_error(_ws: Any, error: Any) -> None:
            log.error("CLOB WS error: %s", error)

        def on_close(_ws: Any, code: Any, msg: Any) -> None:
            ws_holder["ws"] = None
            log.info("CLOB WS closed: %s %s", code, msg)

        def heartbeat() -> None:
            while not stop_event.is_set():
                try:
                    ws = ws_holder.get("ws")
                    if ws is not None:
                        ws.send(json.dumps({"type": "heartbeat"}))
                        wanted = set(_wanted_ids())
                        if wanted and wanted != subscribed_ids:
                            _send_subscription(ws)
                except Exception:
                    pass
                stop_event.wait(10)

        while not stop_event.is_set():
            ws_app = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            hb = threading.Thread(target=heartbeat, name="clob-market-heartbeat", daemon=True)
            hb.start()
            ws_app.run_forever(ping_interval=30)
            ws_holder["ws"] = None
            if not stop_event.is_set():
                log.info("CLOB WS: reconnecting in 5s")
                stop_event.wait(5)

    t = threading.Thread(target=_run, name="clob-market-ws", daemon=True)
    t.start()
    return t
