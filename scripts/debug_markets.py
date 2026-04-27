#!/usr/bin/env python3
"""
Diagnóstico one-shot: un mercado Polymarket 5m activo (por slug) y muestra todos los campos.
Ejecutar en local: python scripts/debug_markets.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import polymarket_feed  # noqa: E402

GAMMA_API_URL = "https://gamma-api.polymarket.com"
ASSETS = ["BTC", "ETH", "SOL", "XRP", "BNB"]


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
