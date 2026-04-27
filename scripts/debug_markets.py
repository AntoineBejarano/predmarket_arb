#!/usr/bin/env python3
"""
Diagnóstico one-shot: un mercado Polymarket 5m activo (por slug) y muestra todos los campos.

Prueba local (desde la raíz del repo, con el venv que tenga ``requests``)::

    python scripts/debug_markets.py

En la salida, revisa la sección **WINDOW TIMING (validate_edge)**:

- ``events[0]["startTime"]`` → apertura de la ventana 5m (p. ej. ``2026-04-27T06:00:00Z``).
- ``endDate`` → cierre (p. ej. ``2026-04-27T06:05:00Z``).
- La diferencia debe ser **300 s (5 min)** para alinear el cálculo de ``minutes_elapsed`` con ``validate_edge.py``.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import polymarket_feed  # noqa: E402

GAMMA_API_URL = "https://gamma-api.polymarket.com"
ASSETS = ["BTC", "ETH", "SOL", "XRP", "BNB"]

# Tolerancia en segundos: cierre − apertura ≈ 300 s (ventana 5m).
_WINDOW_LEN_SEC = 300.0
_WINDOW_TOLERANCE_SEC = 1.0


def _parse_iso_utc(value: Any) -> datetime | None:
    if value is None or not isinstance(value, str):
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def window_timing_check(m: dict[str, Any]) -> dict[str, Any]:
    """
    Misma lógica de ventana que ``validate_edge``: inicio en ``events[0]['startTime']``
    (o ``eventStartTime``), cierre en ``endDate``. Comprueba Δ ≈ 5 min.
    """
    events = m.get("events") or []
    start_s: Any = None
    if events and isinstance(events, list) and len(events) > 0:
        ev0 = events[0]
        if isinstance(ev0, dict):
            start_s = ev0.get("startTime")
    if not start_s:
        start_s = m.get("eventStartTime")
    end_s = m.get("endDate")

    out: dict[str, Any] = {
        "events_0_startTime": start_s,
        "endDate": end_s,
        "delta_seconds": None,
        "delta_minutes": None,
        "ok_exact_5min": False,
        "fallback_start_derived": False,
    }

    start_dt = _parse_iso_utc(start_s)
    end_dt = _parse_iso_utc(end_s)

    if start_dt is None and end_dt is not None:
        try:
            start_dt = end_dt - timedelta(minutes=5)
            start_s = start_dt.isoformat().replace("+00:00", "Z")
            out["events_0_startTime"] = start_s
            out["fallback_start_derived"] = True
        except Exception:
            start_dt = None

    if start_dt is None or end_dt is None:
        out["note"] = "Falta start parseable o endDate"
        return out

    delta_sec = (end_dt - start_dt).total_seconds()
    out["delta_seconds"] = round(delta_sec, 3)
    out["delta_minutes"] = round(delta_sec / 60.0, 4)
    out["ok_exact_5min"] = abs(delta_sec - _WINDOW_LEN_SEC) <= _WINDOW_TOLERANCE_SEC
    return out


def _json_safe(obj: Any) -> Any:
    """Objeto serializable a JSON (strings para fechas, etc.)."""
    return json.loads(json.dumps(obj, default=str))


def run_polymarket_market_debug(
    gamma_api_url: str = GAMMA_API_URL,
    assets: list[str] | None = None,
) -> dict[str, Any]:
    """
    Descarga mercados 5m por slug + muestra de REST /markets.
    Devuelve dict con secciones estructuradas y clave ``text`` (informe plano para copiar).
    """
    assets = assets or list(ASSETS)
    session = requests.Session()
    session.headers.update({"User-Agent": "predmarket-arb-debug/0.1"})

    now = datetime.now(timezone.utc)
    lines: list[str] = []

    def emit(s: str = "") -> None:
        lines.append(s)

    emit()
    emit("=" * 60)
    emit(f"DEBUG: Fetching 5min markets at {now.isoformat()}")
    emit("=" * 60)
    emit()

    slug_section: dict[str, Any] = {
        "ok": False,
        "error": None,
        "markets_count": 0,
        "window_ts": None,
        "first_market": None,
        "date_fields": {},
        "window_timing": None,
    }

    try:
        markets, wts = polymarket_feed.fetch_5m_markets_by_slug(
            session, gamma_api_url, assets, now
        )
        slug_section["ok"] = True
        slug_section["markets_count"] = len(markets)
        slug_section["window_ts"] = wts
        emit(f"Slug fetch: {len(markets)} markets found")
        emit(f"Window timestamp: {wts}")
        emit()

        if markets:
            m = markets[0]
            wt = window_timing_check(m)
            slug_section["window_timing"] = _json_safe(wt)
            emit("WINDOW TIMING (validate_edge)")
            emit(f"  events[0][\"startTime\"] = {wt.get('events_0_startTime')!r}")
            emit(f"  endDate                 = {wt.get('endDate')!r}")
            if wt.get("fallback_start_derived"):
                emit("  (inicio derivado de endDate − 5 min; faltaba startTime en events)")
            ds = wt.get("delta_seconds")
            if ds is not None:
                mark = "OK (5 min)" if wt.get("ok_exact_5min") else "NO coincide con 5 min"
                emit(f"  delta                   = {ds} s ({wt.get('delta_minutes')} min)  → {mark}")
            else:
                emit(f"  delta                   = —  ({wt.get('note', 'sin fechas')})")
            emit()

            slug_section["first_market"] = _json_safe(m)
            emit("FIRST MARKET — ALL KEYS:")
            emit(json.dumps(m, indent=2, default=str))
            emit()
            emit("DATE FIELDS CHECK:")
            date_fields: dict[str, Any] = {}
            for key in m.keys():
                if any(x in key.lower() for x in ("date", "start", "end", "time", "ts")):
                    date_fields[str(key)] = m[key]
                    emit(f"  {key}: {m[key]}")
            slug_section["date_fields"] = _json_safe(date_fields)
        else:
            emit("NO MARKETS RETURNED")
    except Exception as e:
        slug_section["error"] = str(e)
        emit(f"Slug fetch failed: {e}")

    emit()
    emit("=" * 60)
    emit("DIRECT REST CHECK:")
    emit("=" * 60)

    direct_section: dict[str, Any] = {
        "ok": False,
        "error": None,
        "status_code": None,
        "first_market_keys": [],
        "interesting_fields": {},
    }

    try:
        r = session.get(
            f"{gamma_api_url.rstrip('/')}/markets",
            params={"active": "true", "limit": 5},
            timeout=10,
        )
        direct_section["status_code"] = r.status_code
        r.raise_for_status()
        data = r.json()
        sample = data[0] if isinstance(data, list) and data else {}
        direct_section["ok"] = True
        direct_section["first_market_keys"] = list(sample.keys()) if isinstance(sample, dict) else []
        emit(f"Direct API — first market keys: {direct_section['first_market_keys']}")

        interesting: dict[str, Any] = {}
        if isinstance(sample, dict):
            for key in sample.keys():
                if any(
                    x in key.lower()
                    for x in ("date", "start", "end", "time", "ts", "slug")
                ):
                    interesting[str(key)] = sample[key]
                    emit(f"  {key}: {sample[key]}")
        direct_section["interesting_fields"] = _json_safe(interesting)
    except Exception as e:
        direct_section["error"] = str(e)
        emit(f"Direct REST failed: {e}")

    return {
        "generated_at": now.isoformat(),
        "gamma_api_url": gamma_api_url,
        "slug_fetch": slug_section,
        "direct_rest": direct_section,
        "text": "\n".join(lines),
    }


def main() -> None:
    result = run_polymarket_market_debug()
    print(result["text"])


if __name__ == "__main__":
    main()
