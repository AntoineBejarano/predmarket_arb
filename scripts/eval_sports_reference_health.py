#!/usr/bin/env python3
"""
Diagnóstico offline: mercados deportivos Gamma (gratis) + opcional odds-api.io (REST acotado).

Objetivo: ver **ventana prematch** (endDate vs motor), **updatedAt** de la referencia IO
(edad / stale / lag con la misma lógica que latency_arb_sports) y un cruce simple de nombres,
**sin arrancar arb_engine**.

  # Solo Polymarket (sin cuota odds-api)
  python scripts/eval_sports_reference_health.py

  # Incluye una sola ráfaga REST odds-api (≈ 1× GET /events + hasta 10× GET /odds/multi si hay 100 ids)
  python scripts/eval_sports_reference_health.py --odds-once

  # Slugs Poly (misma convención que _HARDCODE_POLY_SLUGS)
  python scripts/eval_sports_reference_health.py --poly-slugs atp,wta

Requiere red. odds-api.io usa la misma clave que el motor (env ODDS_API_IO_KEY o embebida en clients/odds_api_io).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiohttp

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from arb.latency_arb_sports import (  # noqa: E402
    LatencyArbSportsStrategy,
    POLY_SLUG_TO_ODDS_KEY,
    _client_poly_key_for_odds_io,
    _is_pre_game_listing_game,
    _is_reference_lag_over_limit,
    _is_reference_stale_for_io,
    _odds_io_updated_age_sec,
    find_event_matching_teams,
)
from clients.odds_api_io import OddsApiIo  # noqa: E402

_DEFAULT_HEADERS = {
    "User-Agent": "predmarket-arb/eval-sports-ref (aiohttp; +https://github.com)",
    "Accept": "application/json",
}


def _hours_to_end(game: Any) -> Optional[float]:
    if game.end_date is None:
        return None
    now = datetime.now(timezone.utc)
    return (game.end_date - now).total_seconds() / 3600.0


def _fmt_hours(h: Optional[float]) -> str:
    if h is None:
        return "?"
    if h < 0:
        return f"{h:.1f}h (pasado)"
    return f"{h:.1f}h"


def _print_gamma_table(games: list[Any], limit: int) -> None:
    pre = sum(1 for g in games if _is_pre_game_listing_game(g))
    ok = len(games) - pre
    print("\n=== Gamma: partidos descubiertos (misma lógica que latency_arb_sports) ===")
    print(f"total={len(games)}  pre_game_skip(>48h endDate)={pre}  dentro_ventana_48h≈{ok}")
    hrs = [_hours_to_end(g) for g in games if _hours_to_end(g) is not None]
    if hrs:
        b = Counter()
        for x in hrs:
            if x < 0:
                b["<0"] += 1
            elif x <= 6:
                b["0-6h"] += 1
            elif x <= 24:
                b["6-24h"] += 1
            elif x <= 48:
                b["24-48h"] += 1
            else:
                b[">48h"] += 1
        print("histograma horas hasta endDate:", dict(b))

    print(f"\nMuestra (primeros {limit}):")
    print(
        f"{'poly':<6} {'pre48':<6} {'h_end':<10} {'home':<22} {'away':<22} {'slug':<28} condition[:12]"
    )
    for g in games[:limit]:
        slug = (g.slug or "")[:28]
        pg = "Y" if _is_pre_game_listing_game(g) else "N"
        he = _fmt_hours(_hours_to_end(g))
        cid = (g.condition_id or "")[:12]
        print(
            f"{str(g.sport_slug):<6} {pg:<6} {he:<10} {str(g.home)[:22]:<22} {str(g.away)[:22]:<22} {slug:<28} {cid}"
        )


def _odds_events_union(client: OddsApiIo, poly_slugs: list[str]) -> list[Any]:
    """Une listas por clave cliente (atp/wta/wtt) evitando duplicar event_id+bookie."""
    seen: set[tuple[str, str]] = set()
    out: list[Any] = []
    for ps in poly_slugs:
        ok = POLY_SLUG_TO_ODDS_KEY.get(ps)
        if not ok:
            continue
        pk = _client_poly_key_for_odds_io(ok, ps)
        for ev in client.get_cached_odds(pk):
            k = (str(ev.event_id or ""), str(ev.bookie or "").casefold())
            if k in seen:
                continue
            seen.add(k)
            out.append(ev)
    return out


def _events_for_poly_game(client: OddsApiIo, sport_slug: str) -> list[Any]:
    ok = POLY_SLUG_TO_ODDS_KEY.get(sport_slug)
    if not ok:
        return []
    pk = _client_poly_key_for_odds_io(ok, sport_slug)
    return list(client.get_cached_odds(pk))


def _print_odds_table(events: list[Any], limit: int) -> None:
    print("\n=== odds-api.io: eventos en caché (tras --odds-once) ===")
    print(f"total_filas={len(events)}  (WS+REST según OddsApiIo.get_cached_odds)")
    print(
        f"\n{'event_id':<12} {'bookie':<18} {'age_s':>8} {'lag>30':>6} {'stale>120':>9} "
        f"{'updated_at':<28} home[:18] / away[:18]"
    )
    for ev in events[:limit]:
        age = _odds_io_updated_age_sec(ev)
        age_s = f"{age:.1f}" if age is not None else "None"
        lag = "Y" if _is_reference_lag_over_limit(ev) else "N"
        st = "Y" if _is_reference_stale_for_io(ev) else "N"
        ua = (ev.updated_at or "")[:28]
        h = (ev.home or "")[:18]
        a = (ev.away or "")[:18]
        print(f"{str(ev.event_id)[:12]:<12} {str(ev.bookie)[:18]:<18} {age_s:>8} {lag:>6} {st:>9} {ua:<28} {h} / {a}")


def _print_match_hints(games: list[Any], client: OddsApiIo, limit: int) -> None:
    print("\n=== Cruce find_event_matching_teams (solo Gamma con ventana ≤48h) ===")
    print("(misma lista get_cached_odds(poly_key) que el motor por sport_slug)")
    n_in = 0
    n_hit = 0
    for g in games:
        if _is_pre_game_listing_game(g):
            continue
        if not POLY_SLUG_TO_ODDS_KEY.get(g.sport_slug):
            continue
        n_in += 1
        pool = _events_for_poly_game(client, g.sport_slug)
        hit = find_event_matching_teams(pool, g.home, g.away, g.sport_slug)
        if hit is not None:
            n_hit += 1
    print(f"partidos_Gamma_no_pregame={n_in}  con_match_odds_io={n_hit}")

    shown = 0
    for g in games:
        if _is_pre_game_listing_game(g):
            continue
        if not POLY_SLUG_TO_ODDS_KEY.get(g.sport_slug):
            continue
        pool = _events_for_poly_game(client, g.sport_slug)
        hit = find_event_matching_teams(pool, g.home, g.away, g.sport_slug)
        if shown >= limit:
            break
        shown += 1
        st = "OK" if hit else "—"
        ua = (hit.updated_at if hit else "")[:24]
        print(f"  [{st}] {g.home} vs {g.away}  |  io_updated_at={ua}")


async def _run(args: argparse.Namespace) -> int:
    poly_slugs = [s.strip().lower() for s in args.poly_slugs.split(",") if s.strip()]
    cfg: dict[str, Any] = {"start_capital": 10_000.0, "current_capital": 10_000.0}
    strat = LatencyArbSportsStrategy(cfg, dry_run=True)
    strat.poly_slugs = poly_slugs

    async with aiohttp.ClientSession(headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=60)) as sess:
        games = await strat._fetch_open_polymarket_sports(sess)
        _print_gamma_table(games, args.limit)

        if args.dump_gamma_json:
            p = Path(args.dump_gamma_json)
            payload = [
                {
                    "sport_slug": g.sport_slug,
                    "home": g.home,
                    "away": g.away,
                    "slug": g.slug,
                    "condition_id": g.condition_id,
                    "end_date_s": g.end_date_s,
                    "pre_game_skip": _is_pre_game_listing_game(g),
                    "hours_to_end": _hours_to_end(g),
                }
                for g in games
            ]
            p.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            print(f"\n[gamma] volcado JSON: {p}")

        if not args.odds_once:
            print(
                "\n[odds-api.io] omitido (sin --odds-once). "
                "Añade --odds-once para una ráfaga REST controlada y tabla updatedAt."
            )
            return 0

        print(
            "\n[odds-api.io] AVISO: --odds-once dispara refresh_rest_cache → "
            "≈ 1 GET /v3/events + hasta 10 GET /v3/odds/multi (si hay muchos ids). "
            "Cuenta contra tu cuota REST."
        )
        client = OddsApiIo()
        # Una sola clave representativa (misma lógica RR del motor: primer slug con odds_key)
        pick_slug = next((s for s in poly_slugs if POLY_SLUG_TO_ODDS_KEY.get(s)), poly_slugs[0])
        poly_k = _client_poly_key_for_odds_io(POLY_SLUG_TO_ODDS_KEY[pick_slug], pick_slug)
        await client.refresh_rest_cache(sess, poly_k)
        events = _odds_events_union(client, poly_slugs)
        _print_odds_table(events, args.limit)
        _print_match_hints(games, client, min(args.limit, 25))

    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Evalúa salud Gamma + referencia odds-api (opcional).")
    ap.add_argument(
        "--poly-slugs",
        default="atp,wta,wttmen",
        help="Slugs Poly separados por coma (default: misma lista dura que latency_arb_sports).",
    )
    ap.add_argument("--limit", type=int, default=25, help="Filas de muestra por tabla.")
    ap.add_argument(
        "--odds-once",
        action="store_true",
        help="Una sola refresh_rest_cache odds-api.io (consume REST).",
    )
    ap.add_argument(
        "--dump-gamma-json",
        metavar="PATH",
        default="",
        help="Si se pasa, escribe JSON con todos los OpenPolymarketGame descubiertos.",
    )
    args = ap.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
