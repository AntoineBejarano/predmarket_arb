"""Latency arb sports: Pinnacle (The Odds API) vs precios CLOB Polymarket en mercados deportivos."""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import re
import threading
import time
import types
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import aiohttp
from aiohttp import WSMsgType

from arb.base import ArbStrategy
from arb.latency_sports_manual_match import load_manual_matches
from clients.odds_api import normalize_team_for_match, odds_team_matches_gamma_blob, teams_match_odds_gamma
from clients.odds_api_io import (
    ODDS_API_IO_SPORTS_EMBEDDED,
    OddsApiIo,
    OddsApiIoRestQuotaError,
    OddsEvent,
    find_event_matching_teams,
    normalize_name_order,
    poly_odds_key_to_io_sport,
    remove_vig as remove_vig_decimal,
)
from clients.poly_clob import PolyCLOBClient, _level_price, _level_size, _size_at_price
from clients.poly_markets import GAMMA_API_URL
from clients.poly_parse import api_bool_true, clob_market_tradeable, parse_json_list_maybe, parse_outcomes_list

log = logging.getLogger("latency_arb_sports")

SPORTS_WS_URL = "wss://sports-api.polymarket.com/ws"

# CSV de análisis (una fila por partido y ciclo con mids); columnas betfair_* = cuotas/probs del book IO (p. ej. Betfair) alineadas a Poly home/away.
_SNAPSHOT_CSV_COLUMNS: list[str] = [
    "snapshot_at",
    "game",
    "delta_home",
    "delta_away",
    "poly_mid_home",
    "poly_mid_away",
    "betfair_prob_home",
    "betfair_prob_away",
    "betfair_home_odds",
    "betfair_away_odds",
    "betfair_updated_at",
    "ts",
    "home",
    "away",
    "sport",
    "match_set",
    "match_score",
]


def _latency_snapshots_csv_path() -> Path:
    from lab.paths import data_dir

    return data_dir() / "logs" / "latency_arb_sports_snapshots.csv"


def _odds_event_meta_cache_clear_flag_path() -> Path:
    """Sentinel escrito por POST /api/admin/clear-meta-cache; el motor vacía RAM del cliente IO."""
    from lab.paths import data_dir

    return data_dir() / "logs" / ".odds_event_meta_cache_clear_requested"


def _fmt_opt_float(x: Optional[float], nd: int = 6) -> str:
    if x is None:
        return ""
    try:
        return f"{float(x):.{nd}f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return ""


def _orderbook_best_bid_size(ob: dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    bids = ob.get("bids") or []
    if not isinstance(bids, list):
        return None, None
    best: Optional[float] = None
    for lvl in bids:
        p = _level_price(lvl)
        if p is None:
            continue
        if best is None or p > best:
            best = p
    if best is None:
        return None, None
    sz = _size_at_price(bids, best)
    return best, sz


def _asks_notional_usdc_in_band(ob: dict[str, Any], center: float, half: float = 0.01) -> Optional[float]:
    asks = ob.get("asks") or []
    if not isinstance(asks, list):
        return None
    lo, hi = float(center) - float(half), float(center) + float(half)
    tot = 0.0
    any_hit = False
    for lvl in asks:
        p = _level_price(lvl)
        if p is None:
            continue
        if p < lo - 1e-12 or p > hi + 1e-12:
            continue
        sz = _level_size(lvl)
        if sz is None:
            continue
        tot += float(p) * float(sz)
        any_hit = True
    return tot if any_hit else 0.0


def _depth_pair_from_ws_payload(data: dict[str, Any]) -> tuple[str, str]:
    """depthHome/depthAway en mensaje WS odds-api.io (o dentro del primer odds ML)."""
    dh = data.get("depthHome") if data.get("depthHome") is not None else data.get("depth_home")
    da = data.get("depthAway") if data.get("depthAway") is not None else data.get("depth_away")
    if dh not in (None, "") or da not in (None, ""):
        return str(dh if dh is not None else ""), str(da if da is not None else "")
    mk = data.get("markets")
    if not isinstance(mk, list):
        return "", ""
    for item in mk:
        if not isinstance(item, dict):
            continue
        if str(item.get("name") or "").upper() != "ML":
            continue
        odds_list = item.get("odds") or []
        if not isinstance(odds_list, list) or not odds_list or not isinstance(odds_list[0], dict):
            break
        row0 = odds_list[0]
        dh2 = row0.get("depthHome") if row0.get("depthHome") is not None else row0.get("depth_home")
        da2 = row0.get("depthAway") if row0.get("depthAway") is not None else row0.get("depth_away")
        return str(dh2 if dh2 is not None else ""), str(da2 if da2 is not None else "")
    return "", ""


_DEFAULT_HEADERS = {
    "User-Agent": "predmarket-arb/latency-arb-sports (aiohttp; +https://github.com)",
    "Accept": "application/json",
}

MIN_DISCOVERY_TTL_SEC = 300
# Cooldown por partido+lado: solo reemitir si el mid cambia >0.5% o han pasado 30 s.
_SIGNAL_COOLDOWN_PRICE_EPS = 0.005
_SIGNAL_COOLDOWN_SEC = 30.0

# Parámetros fijos de la estrategia (no se leen LATENCY_SPORTS_* ni ODDS_API_IO_SPORTS del entorno).
# Rutas de datos: ``lab.paths.data_dir()`` (fijo ``<repo>/data``). Siguen por env: POLY_* (CLOB), claves Odds API en clients/odds_api_io.
# Slugs ``sport`` nativos Gamma GET https://gamma-api.polymarket.com/sports (nba, epl, ucl, …).
_HARDCODE_POLY_SLUGS = "atp,wta,wttmen,nba,epl,ucl"
_HARDCODE_MIN_EDGE = 0.005
_HARDCODE_MIN_EDGE_EXEC = 0.005
_HARDCODE_MAX_STAKE_USDC = 10.0
_HARDCODE_REGIONS = "eu"
_HARDCODE_POLL_INTERVAL = 5.0
_HARDCODE_POLL_INTERVAL_ACTIVE = 2.0
_HARDCODE_DISCOVERY_TTL = 300.0
_HARDCODE_DISCOVERY_TTL_ACTIVE = 30.0
_HARDCODE_WINDOW_HOURS_BEFORE = 3.0
_HARDCODE_BETFAIR_GAMMA_SEARCH_TTL = 60.0
_HARDCODE_BETFAIR_GAMMA_SEARCH_TTL_ACTIVE = 30.0
_HARDCODE_CLOB_WS_WARMUP_SEC = 0.35
_HARDCODE_GAMMA_PUBLIC_SEARCH_LIMIT = 40
_HARDCODE_MATCH_PARALLELISM = 8
_SNAPSHOT_CSV_ENABLED = True
# Calidad referencia odds-io: descartar emparejamientos demasiado viejos o pre-game listado.
_HARDCODE_MAX_REFERENCE_LAG_SEC = 30.0
_HARDCODE_STALE_ODDS_IO_SEC = 120.0
_HARDCODE_PRE_GAME_END_MARGIN_SEC = 172800.0
_HARDCODE_OPPOSITE_SIGNAL_DEDUPE_SEC = 120.0
_HARDCODE_MAX_SPREAD = 0.04
_HARDCODE_MIN_TOP_ASK_NOTIONAL_USDC = 10.0
_HARDCODE_MIN_ASK_BAND_NOTIONAL_USDC = 25.0
_HARDCODE_KILL_SWITCH_REF_BACKOFF_SEC = 600.0

# Gamma /events con ?sport= no filtra de forma fiable; usamos series_id del GET /sports nativo.
GAMMA_SPORTS_META_TTL_SEC = 3600.0

POLY_SLUG_TO_ODDS_KEY: dict[str, str] = {
    "wta": "tennis",
    "atp": "tennis",
    "wttmen": "table-tennis",
    "nba": "basketball",
    "epl": "football",
    "ucl": "football",
}

# odds-api.io slug deporte (OddsEvent.sport) → prefijos esperados en slug Gamma (public-search mezcla deportes).
_IO_SPORT_TO_GAMMA_SLUG_PREFIXES: dict[str, tuple[str, ...]] = {
    "tennis": ("atp-", "wta-", "wtt-"),
    "football": (
        "epl-",
        "mls-",
        "ucl-",
        "uel-",
        "soccer-",
        "premier-",
        "lal-",
        "bun-",
        "fl1-",
        "sea-",
        "ere-",
        "tur-",
        "mex-",
        "lib-",
        "sud-",
        "con-",
        "cof-",
        "afc-",
        "ofc-",
        "fif-",
        "arg-",
        "itc-",
        "lcs-",
        "uef-",
        "caf-",
        "rus-",
        "efa-",
        "efl-",
        "acn-",
    ),
    "soccer": (
        "epl-",
        "mls-",
        "ucl-",
        "uel-",
        "soccer-",
        "premier-",
        "lal-",
        "bun-",
        "fl1-",
        "sea-",
    ),
    "basketball": ("nba-",),
    "american-football": ("nfl-",),
    "ice-hockey": ("nhl-",),
    "baseball": ("mlb-",),
    "mixed-martial-arts": ("ufc-", "mma-",),
    "table-tennis": ("wttmen-", "wtt-",),
}

_IO_SPORT_ALIASES: dict[str, str] = {"american_football": "american-football", "ice_hockey": "ice-hockey"}

# Valor POLY_SLUG_TO_ODDS_KEY → slug IO odds-api.io (alineado con clients.odds_api_io ODDS_KEY_TO_IO_SPORT).
_ODDS_KEY_TO_IO_SPORT: dict[str, str] = {
    "tennis": "tennis",
    "table-tennis": "table-tennis",
    "basketball_nba": "basketball",
    "icehockey_nhl": "ice-hockey",
    "baseball_mlb": "baseball",
    "mma_mixed_martial_arts": "mixed-martial-arts",
    "soccer_uefa_champs_league": "football",
    "soccer_uefa_europa_league": "football",
    "soccer_epl": "football",
    "americanfootball_nfl": "american-football",
}

# Claves que espera clients.odds_api_io.poly_odds_key_to_io_sport (valores POLY arriba = slugs IO).
_ODDS_IO_CLIENT_POLY_KEY: dict[str, str] = {
    "tennis": "tennis_wta",
    "table-tennis": "tabletennis_wtt",
    "basketball": "basketball_nba",
    "football": "soccer_epl",
}


def _client_poly_key_for_odds_io(odds_key: str, poly_sport_slug: str = "") -> str:
    """Clave Odds API IO / caché: tennis por slug Gamma (atp vs wta); fútbol ucl/uel vs resto."""
    k = odds_key.strip()
    slug = str(poly_sport_slug).strip().lower()
    if k == "tennis":
        if slug == "atp":
            return "tennis_atp"
        if slug == "wta":
            return "tennis_wta"
    if k == "table-tennis" and slug == "wttmen":
        return "tabletennis_wtt"
    if k == "football":
        if slug == "ucl":
            return "soccer_uefa_champs_league"
        if slug == "uel":
            return "soccer_uefa_europa_league"
        return "soccer_epl"
    if k == "basketball":
        # odds-api.io deporte ``basketball``; clave interna NBA (misma caché REST/WS).
        return "basketball_nba"
    return _ODDS_IO_CLIENT_POLY_KEY.get(k, k)


def _latency_sports_skip_doubles() -> bool:
    """Dobles: nunca procesar. True fijo; no lee ninguna env (p. ej. LATENCY_SPORTS_SKIP_DOUBLES)."""
    return True


def _normalize_slashes(s: str) -> str:
    t = (s or "").replace("\u2044", "/").replace("\uff0f", "/").replace("／", "/")
    return t.replace("\u202f", " ").replace("\xa0", " ")


def _doubles_hint_in_text(s: str) -> bool:
    """
    Indica pareja de jugadores (dobles) o varios jugadores en un mismo string.
    Cubre slash con/sin espacios y títulos tipo «A / B / C / D» (Poly a veces compacta).
    """
    t = _normalize_slashes(s).strip()
    if not t or "/" not in t:
        return False
    if " / " in t:
        return True
    if t.count("/") >= 2:
        return True
    for rx in (r"[\w.'\-]\s*/\s*[\w.'\-]", r"[\w.'\-]/[\w.'\-]"):
        for m in re.finditer(rx, t, re.I):
            seg = re.sub(r"\s+", "", m.group(0)).lower()
            # Props (O/U, H/E) no son dobles; el regex anterior los confunde con «jugador / jugador».
            if seg in ("o/u", "h/e", "n/a", "y/n"):
                continue
            return True
    return False


def _gamma_event_doubles_signal(ev: dict[str, Any]) -> bool:
    """True si en cualquier parte visible del evento Gamma hay formato de dobles."""
    # No incluir description: URLs tipo ``…com/en/…`` disparan el regex ``[\w.'\-]/[\w.'\-]``
    # y bloquean moneylines ATP válidos (p. ej. Sinner vs Jodar).
    chunks: list[str] = [
        str(ev.get("title") or ""),
        str(ev.get("slug") or ""),
    ]
    for m in ev.get("markets") or []:
        if not isinstance(m, dict):
            continue
        chunks.append(str(m.get("question") or ""))
        chunks.append(str(m.get("groupItemTitle") or ""))
        for lab, _tid in _outcome_token_pairs(m):
            chunks.append(str(lab))
    for ch in chunks:
        if _doubles_hint_in_text(ch):
            return True
    return False


@dataclass
class OpenPolymarketGame:
    sport_slug: str
    home: str
    away: str
    condition_id: str
    token_yes: str
    end_date: Optional[datetime]
    raw_title: str
    slug: str
    outcome_tokens: list[tuple[str, str]]
    end_date_s: Optional[str]
    kickoff_utc: Optional[datetime] = None


def _odds_io_updated_age_sec(odds_event: OddsEvent) -> Optional[float]:
    raw = (odds_event.updated_at or "").strip()
    if not raw:
        return None
    u = _parse_iso_utc(raw)
    if u is None:
        return None
    return (datetime.now(timezone.utc) - u).total_seconds()


def _is_reference_stale_for_io(odds_event: OddsEvent) -> bool:
    age = _odds_io_updated_age_sec(odds_event)
    if age is None:
        return True
    return age > float(_HARDCODE_STALE_ODDS_IO_SEC)


def _is_reference_lag_over_limit(odds_event: OddsEvent) -> bool:
    age = _odds_io_updated_age_sec(odds_event)
    if age is None:
        return True
    return age > float(_HARDCODE_MAX_REFERENCE_LAG_SEC)


def _is_pre_game_listing_game(game: OpenPolymarketGame) -> bool:
    now = datetime.now(timezone.utc)
    if game.end_date is not None and (game.end_date - now).total_seconds() > float(_HARDCODE_PRE_GAME_END_MARGIN_SEC):
        return True
    return False


def _is_ml_like_open_game(game: OpenPolymarketGame) -> bool:
    """Excluye props típicos Over/Under (no moneyline jugador vs jugador). Solo heurística Gamma."""
    h = (game.home or "").strip().lower()
    a = (game.away or "").strip().lower()
    if not h or not a:
        return False
    if h in ("over", "under") and a in ("over", "under"):
        return False
    oh, oa = "over" in h, "over" in a
    uh, ua = "under" in h, "under" in a
    if (oh and ua) or (uh and oa):
        return False
    for lab in (h, a):
        if "over 2.5" in lab or "under 2.5" in lab or "over 1.5" in lab or "under 1.5" in lab:
            return False
        if lab.startswith("over ") or lab.startswith("under "):
            return False
    return True


def _compute_briefing_cycle_counts(
    open_games: list[OpenPolymarketGame],
    odds_events_by_key: dict[str, list[OddsEvent]],
) -> dict[str, int]:
    """
    Contadores solo lectura para UI: Gamma + match de nombres contra caché IO ya cargada en el ciclo.
    No dispara REST adicional.
    """
    pre_distant = sum(1 for g in open_games if _is_pre_game_listing_game(g))
    within = max(0, len(open_games) - pre_distant)
    ml_like = 0
    io_name_hit = 0
    for g in open_games:
        if _is_pre_game_listing_game(g):
            continue
        if not _is_ml_like_open_game(g):
            continue
        ml_like += 1
        odds_key = POLY_SLUG_TO_ODDS_KEY.get(g.sport_slug)
        if not odds_key:
            continue
        evs = odds_events_by_key.get(odds_key) or []
        if find_event_matching_teams(evs, g.home, g.away) is not None:
            io_name_hit += 1
    return {
        "briefing_pre_game_distant": int(pre_distant),
        "briefing_within_end_window": int(within),
        "briefing_ml_like": int(ml_like),
        "briefing_io_name_match_cache": int(io_name_hit),
    }


def _manual_tw_from_odds_event(odds_ev: OddsEvent, swap_sides: bool) -> tuple[str, float, str, float]:
    """Alineación fija Poly home/away ← IO (swap invierte lados IO respecto a Poly)."""
    if swap_sides:
        return (
            str(odds_ev.away or "").strip(),
            float(odds_ev.away_odds),
            str(odds_ev.home or "").strip(),
            float(odds_ev.home_odds),
        )
    return (
        str(odds_ev.home or "").strip(),
        float(odds_ev.home_odds),
        str(odds_ev.away or "").strip(),
        float(odds_ev.away_odds),
    )


def _build_pending_manual_payload(
    open_games: list[OpenPolymarketGame],
    odds_events_by_key: dict[str, list[OddsEvent]],
    manual_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Filas UI: ML-like, no lejano, sin match automático y sin manual resuelto en caché este ciclo."""
    pending_rows: list[dict[str, Any]] = []
    for g in open_games:
        if _is_pre_game_listing_game(g):
            continue
        if not _is_ml_like_open_game(g):
            continue
        ok = POLY_SLUG_TO_ODDS_KEY.get(g.sport_slug)
        if not ok:
            continue
        events_list = odds_events_by_key.get(ok) or []
        if find_event_matching_teams(events_list, g.home, g.away) is not None:
            continue
        cid = (g.condition_id or "").strip()
        if not cid:
            continue
        mm = manual_map.get(cid) if manual_map else None
        eid_manual = str((mm or {}).get("odds_event_id") or "").strip()
        if eid_manual and any(str(e.event_id) == eid_manual for e in events_list):
            continue
        manual_waiting_cache = bool(eid_manual) and not any(str(e.event_id) == eid_manual for e in events_list)
        candidates = [
            {
                "event_id": str(e.event_id),
                "home": e.home,
                "away": e.away,
                "bookie": e.bookie,
            }
            for e in events_list[:80]
        ]
        pending_rows.append(
            {
                "condition_id": cid,
                "poly_home": g.home,
                "poly_away": g.away,
                "sport_slug": g.sport_slug,
                "odds_key": ok,
                "slug": (g.slug or "")[:220],
                "raw_title": (g.raw_title or "")[:400],
                "io_candidates": candidates,
                "manual_waiting_cache": manual_waiting_cache,
            }
        )
    return {"pending": pending_rows, "pending_count": len(pending_rows)}


@dataclass
class GammaSportMarket:
    condition_id: str
    slug: str
    sport_key: str
    league: str
    home_team: str
    away_team: str
    outcome_tokens: list[tuple[str, str]]  # (label, token_id)
    question: str
    end_date_s: Optional[str]
    start_date_s: Optional[str]

    def commence_for_odds(self, odds_commence: datetime) -> Optional[datetime]:
        """Instante Gamma comparable a commence Odds (elige el candidato más cercano)."""
        candidates: list[datetime] = []
        pq = _parse_commence_from_question(self.question, odds_commence)
        if pq is not None:
            candidates.append(pq)
        for raw in (self.end_date_s, self.start_date_s):
            dt = _parse_iso_utc(raw)
            if dt is not None:
                candidates.append(dt)
        if not candidates:
            return None
        return min(candidates, key=lambda t: abs((t - odds_commence).total_seconds()))


def _parse_iso_utc(s: Any) -> Optional[datetime]:
    if not s:
        return None
    try:
        t = str(s).strip()
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _parse_commence_from_question(question: str, ref: datetime) -> Optional[datetime]:
    """Parse 'scheduled for ... ET?' a UTC usando America/New_York."""
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        return None
    m = re.search(r"scheduled for\s+([^?]+)\?", question, flags=re.I)
    if not m:
        return None
    tail = m.group(1).strip().replace(" ET", "").strip()
    et = ZoneInfo("America/New_York")
    ref_y = ref.astimezone(timezone.utc).year
    if not re.search(r"\b(19|20)\d{2}\b", tail):
        mo2 = re.match(r"^([A-Za-z]+)\s+(\d{1,2}),\s*(\d{1,2}:\d{2}\s*[AP]M)", tail, flags=re.I)
        if mo2:
            mon, day, tim = mo2.groups()
            tail = f"{mon} {int(day)}, {ref_y}, {tim}"
    for fmt in ("%B %d, %Y, %I:%M %p", "%B %d, %Y, %I:%M%p", "%B %d, %Y, %I %p"):
        try:
            dt_naive = datetime.strptime(tail.strip(), fmt)
            return dt_naive.replace(tzinfo=et).astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def _outcome_token_pairs(m: dict[str, Any]) -> list[tuple[str, str]]:
    outs = parse_outcomes_list(m)
    raw_tok = m.get("clobTokenIds") or m.get("clob_token_ids")
    toks, err = parse_json_list_maybe(raw_tok)
    if err or not isinstance(toks, list) or len(toks) != len(outs):
        return []
    out: list[tuple[str, str]] = []
    for o, t in zip(outs, toks):
        tid = str(t).strip()
        if tid:
            out.append((str(o).strip(), tid))
    return out


def _teams_from_market(m: dict[str, Any]) -> tuple[str, str]:
    """Infer home/away desde outcomes (2) o texto pregunta."""
    pairs = _outcome_token_pairs(m)
    if len(pairs) == 2:
        o0, o1 = pairs[0][0], pairs[1][0]
        if o0.lower() != "yes" and o1.lower() != "no":
            return o0, o1
    q = m.get("question") or ""
    mo = re.search(r"[-–]\s*([^?]+?)\s*$", q)
    if mo:
        tail = mo.group(1).strip()
        if " vs " in tail.lower():
            parts = re.split(r"\s+vs\.?\s+", tail, flags=re.I)
            if len(parts) == 2:
                return parts[0].strip(), parts[1].strip()
    return "", ""


def _gamma_row_from_market(m: dict[str, Any], sport_key: str, ref: datetime) -> Optional[GammaSportMarket]:
    cid = m.get("conditionId") or m.get("condition_id")
    if not cid:
        return None
    slug = str(m.get("slug") or "").strip() or str(cid)[:16]
    home, away = _teams_from_market(m)
    if not home or not away:
        return None
    pairs = _outcome_token_pairs(m)
    if len(pairs) < 2:
        return None
    league = str(m.get("groupItemTitle") or sport_key)[:120]
    q = str(m.get("question") or "")
    if not _parse_iso_utc(m.get("endDate")) and not _parse_iso_utc(m.get("startDate")) and not _parse_commence_from_question(q, ref):
        return None
    return GammaSportMarket(
        condition_id=str(cid),
        slug=slug,
        sport_key=sport_key,
        league=league,
        home_team=home,
        away_team=away,
        outcome_tokens=pairs,
        question=q,
        end_date_s=str(m.get("endDate") or "") or None,
        start_date_s=str(m.get("startDate") or "") or None,
    )


def _parse_poly_title_teams(raw: str) -> tuple[str, str]:
    """Tras último ':', split por ' vs ' o ' vs. '."""
    t = (raw or "").strip()
    if not t:
        return "", ""
    if ":" in t:
        t = t.rsplit(":", 1)[-1].strip()
    parts = re.split(r"\s+vs\.?\s+", t, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return "", ""
    return parts[0].strip(), parts[1].strip()


_TENNISLIKE_GAMMA_SLUG_RE = re.compile(
    r"^(?:atp|wta|wttmen|wtt)-(?P<mid>.+)-(?P<d>\d{4}-\d{2}-\d{2})$",
    re.IGNORECASE,
)


def _teams_from_tennislike_gamma_slug(slug: str) -> tuple[str, str]:
    """
    Gamma public-search a veces devuelve título vacío; el slug trae equipos
    (p. ej. atp-sinner-jodar-2026-04-29).
    """
    m = _TENNISLIKE_GAMMA_SLUG_RE.match(str(slug or "").strip().lower())
    if not m:
        return "", ""
    segs = [p for p in m.group("mid").split("-") if p]
    if len(segs) < 2:
        return "", ""
    if len(segs) == 2:
        return segs[0].title(), segs[1].title()
    if len(segs) == 3:
        return segs[0].title(), f"{segs[1]} {segs[2]}".title()
    if len(segs) == 4:
        return f"{segs[0]} {segs[1]}".title(), f"{segs[2]} {segs[3]}".title()
    mid = len(segs) // 2
    return " ".join(segs[:mid]).title(), " ".join(segs[mid:]).title()


def _yes_token_from_pairs(pairs: list[tuple[str, str]]) -> Optional[str]:
    for lab, tid in pairs:
        if str(lab).strip().lower() == "yes":
            return tid
    return pairs[0][1] if pairs else None


def _first_two_team_tradeable_market(ev: dict[str, Any]) -> Optional[dict[str, Any]]:
    for m in ev.get("markets") or []:
        if not isinstance(m, dict):
            continue
        if api_bool_true(m.get("closed")):
            continue
        ok, _why = clob_market_tradeable(m)
        if not ok:
            continue
        pairs = _outcome_token_pairs(m)
        if len(pairs) < 2:
            continue
        labs = [p[0].strip().lower() for p in pairs]
        if labs in (["yes", "no"], ["no", "yes"]):
            continue
        return m
    return None


def _kickoff_naive_utc_from_gamma_slug(slug: str) -> Optional[datetime]:
    m = _TENNISLIKE_GAMMA_SLUG_RE.match(str(slug or "").strip().lower())
    if not m:
        return None
    ds = m.group("d")
    try:
        y, mo, d = int(ds[0:4]), int(ds[5:7]), int(ds[8:10])
        return datetime(y, mo, d, 12, 0, 0, tzinfo=timezone.utc)
    except ValueError:
        return None


def gamma_event_kickoff_utc(ev: dict[str, Any], *, ref: datetime) -> Optional[datetime]:
    """
    Mejor esfuerzo de inicio del partido solo con datos Gamma (sin odds-api).
    Orden de señales: startDate evento, mercado 2-way (pregunta / startDate), fecha en slug tenis,
    y solo si no hay nada más, endDate del evento.
    """
    candidates: list[datetime] = []
    t0 = _parse_iso_utc(ev.get("startDate"))
    if t0 is not None:
        candidates.append(t0)
    tm = _first_two_team_tradeable_market(ev)
    if tm is not None:
        pq = _parse_commence_from_question(str(tm.get("question") or ""), ref)
        if pq is not None:
            candidates.append(pq)
        for raw in (tm.get("startDate"), tm.get("endDate")):
            t = _parse_iso_utc(raw)
            if t is not None:
                candidates.append(t)
    slug_dt = _kickoff_naive_utc_from_gamma_slug(str(ev.get("slug") or ""))
    if slug_dt is not None:
        candidates.append(slug_dt)
    if not candidates:
        end_ev = _parse_iso_utc(ev.get("endDate"))
        if end_ev is not None:
            candidates.append(end_ev)
    if not candidates:
        return None
    pruned = [c for c in candidates if abs((c - ref).total_seconds()) < 86400 * 400]
    if not pruned:
        return None
    return min(pruned, key=lambda c: abs((c - ref).total_seconds()))


def _poly_event_to_open_game(ev: dict[str, Any], sport_slug: str) -> Optional[OpenPolymarketGame]:
    title = str(ev.get("title") or "")
    home_p, away_p = _parse_poly_title_teams(title)
    slug_e0 = str(ev.get("slug") or "").strip()
    if not home_p or not away_p:
        home_p, away_p = _teams_from_tennislike_gamma_slug(slug_e0)
    if not home_p or not away_p:
        return None
    if _latency_sports_skip_doubles() and _gamma_event_doubles_signal(ev):
        return None
    end_dt = _parse_iso_utc(ev.get("endDate"))
    end_s = str(ev.get("endDate") or "").strip() or None
    slug_e = slug_e0[:240]
    hl, al = home_p.lower(), away_p.lower()

    two_team_m: Optional[dict[str, Any]] = None
    for m in ev.get("markets") or []:
        if not isinstance(m, dict):
            continue
        if api_bool_true(m.get("closed")):
            continue
        ok, _why = clob_market_tradeable(m)
        if not ok:
            continue
        pairs = _outcome_token_pairs(m)
        if len(pairs) < 2:
            continue
        labs = [p[0].strip().lower() for p in pairs]
        if labs in (["yes", "no"], ["no", "yes"]):
            continue
        two_team_m = m
        break

    ref_k = datetime.now(timezone.utc)
    kfu = gamma_event_kickoff_utc(ev, ref=ref_k)

    if two_team_m is not None:
        h2, a2 = _teams_from_market(two_team_m)
        if not h2 or not a2:
            return None
        pairs = _outcome_token_pairs(two_team_m)
        if len(pairs) < 2:
            return None
        cid = str(two_team_m.get("conditionId") or two_team_m.get("condition_id") or "")
        if not cid:
            return None
        token_yes = _yes_token_from_pairs(pairs) or pairs[0][1]
        # Nombres desde el mercado CLOB (p. ej. "A / B"); el título a veces compacta y pierde el slash.
        return OpenPolymarketGame(
            sport_slug=sport_slug,
            home=h2,
            away=a2,
            condition_id=cid,
            token_yes=token_yes,
            end_date=end_dt,
            raw_title=title,
            slug=slug_e,
            outcome_tokens=list(pairs),
            end_date_s=end_s,
            kickoff_utc=kfu,
        )

    home_yes: Optional[str] = None
    away_yes: Optional[str] = None
    mk_home: Optional[dict[str, Any]] = None
    mk_away: Optional[dict[str, Any]] = None
    for m in ev.get("markets") or []:
        if not isinstance(m, dict):
            continue
        if api_bool_true(m.get("closed")):
            continue
        ok, _why = clob_market_tradeable(m)
        if not ok:
            continue
        pairs = _outcome_token_pairs(m)
        if len(pairs) < 2:
            continue
        labs = [p[0].strip().lower() for p in pairs]
        if labs not in (["yes", "no"], ["no", "yes"]):
            continue
        q = str(m.get("question") or "").lower()
        if "end in a draw" in q or ("end in" in q and "tie" in q):
            continue
        if " win" not in q and " win on" not in q:
            continue
        if hl in q and al not in q:
            yt = _yes_token_from_pairs(pairs)
            if yt:
                home_yes = yt
                mk_home = m
        elif al in q and hl not in q:
            yt = _yes_token_from_pairs(pairs)
            if yt:
                away_yes = yt
                mk_away = m

    if home_yes and away_yes and mk_home and mk_away:
        outcome_tokens = [(home_p, home_yes), (away_p, away_yes)]
        cid = str(mk_home.get("conditionId") or mk_home.get("condition_id") or "") or str(
            mk_away.get("conditionId") or mk_away.get("condition_id") or ""
        )
        if not cid:
            return None
        if _latency_sports_skip_doubles() and (
            _doubles_hint_in_text(home_p) or _doubles_hint_in_text(away_p)
        ):
            return None
        return OpenPolymarketGame(
            sport_slug=sport_slug,
            home=home_p,
            away=away_p,
            condition_id=cid,
            token_yes=home_yes,
            end_date=end_dt,
            raw_title=title,
            slug=slug_e,
            outcome_tokens=outcome_tokens,
            end_date_s=end_s,
            kickoff_utc=kfu,
        )
    return None


def _gamma_event_usable(ev: dict[str, Any]) -> bool:
    if api_bool_true(ev.get("archived")):
        return False
    if api_bool_true(ev.get("closed")):
        return False
    return api_bool_true(ev.get("active", True))


def _event_matches_odds_teams(ev: dict[str, Any], home_odds: str, away_odds: str) -> bool:
    """Evento Gamma cuyo title/slug menciona ambos equipos (orden irrelevante)."""
    title = str(ev.get("title") or "").strip()
    if ":" in title:
        title = title.rsplit(":", 1)[-1].strip()
    slug = str(ev.get("slug") or "").strip()
    blob = (title + " " + slug).strip()
    if len(blob) < 3:
        return False
    ok_h = odds_team_matches_gamma_blob(home_odds, blob)
    ok_a = odds_team_matches_gamma_blob(away_odds, blob)
    if not (ok_h and ok_a):
        nh = normalize_team_for_match(home_odds)
        na = normalize_team_for_match(away_odds)
        log.debug(
            "[PS_FILTER] team_match_detail slug=%r blob_tail=%r io_norm_home=%r io_norm_away=%r "
            "home_hit=%s away_hit=%s",
            slug,
            blob[-200:] if len(blob) > 200 else blob,
            nh,
            na,
            ok_h,
            ok_a,
        )
    return ok_h and ok_a


def _io_sport_slug_coerce(raw: str) -> str:
    return str(raw or "").strip().lower()


def _io_surname_token(name_normalized: str) -> str:
    parts = [p for p in str(name_normalized or "").strip().split() if p]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    return parts[-1]


def _ws_cache_bookmaker_slots(client: OddsApiIo) -> int:
    """Filas OddsEvent en caché WS (un evento puede tener varias casas)."""
    return sum(len(by_bk) for by_bk in client._ws_odds_cache.values())


def _flatten_ws_odds_best_per_event(client: OddsApiIo) -> list[OddsEvent]:
    """Un OddsEvent por event_id IO (mejor bookie según prioridad del cliente)."""
    out: list[OddsEvent] = []
    for _eid, by_bk in client._ws_odds_cache.items():
        if not by_bk:
            continue
        best = min(by_bk.values(), key=lambda ev: (client._bookie_priority(ev.bookie), str(ev.event_id)))
        out.append(best)
    out.sort(key=lambda e: str(e.event_id))
    return out


def _gamma_slug_prefixes_for_io_sport(io_sport: str) -> tuple[str, ...]:
    sl = _IO_SPORT_ALIASES.get(_io_sport_slug_coerce(io_sport), _io_sport_slug_coerce(io_sport))
    return _IO_SPORT_TO_GAMMA_SLUG_PREFIXES.get(sl, ())


def _gamma_slug_matches_io_prefix(gamma_slug: str, io_sport: str) -> bool:
    prefs = _gamma_slug_prefixes_for_io_sport(io_sport)
    if not prefs:
        return True
    s = str(gamma_slug or "").strip().lower()
    return any(s.startswith(p) for p in prefs)


def _title_has_both_surname_tokens(title: str, ah: str, aa: str) -> bool:
    if not ah or not aa:
        return False
    t = str(title or "").casefold()
    return ah.casefold() in t and aa.casefold() in t


def _parse_public_search_response(text: str) -> tuple[list[dict[str, Any]], list[Any]]:
    """Gamma /public-search devuelve wrapper {events, markets}; nunca lista plana en la raíz."""
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return [], []
    if not isinstance(data, dict):
        return [], []
    evs = data.get("events", [])
    mks = data.get("markets", [])
    if not isinstance(evs, list):
        evs = []
    if not isinstance(mks, list):
        mks = []
    out_e: list[dict[str, Any]] = []
    for x in evs:
        if isinstance(x, dict):
            out_e.append(x)
    return out_e, mks


def _event_id_for_gamma_merge(ev: dict[str, Any]) -> str:
    return str(ev.get("id") or ev.get("eventId") or ev.get("event_id") or "").strip()


def _merge_root_markets_into_event(ev: dict[str, Any], root_markets: list[Any]) -> dict[str, Any]:
    """Si el evento no trae markets[], adjuntar los de la raíz con mismo event id."""
    mk = ev.get("markets")
    if isinstance(mk, list) and len(mk) > 0:
        return ev
    eid = _event_id_for_gamma_merge(ev)
    if not eid or not root_markets:
        return ev
    attached: list[dict[str, Any]] = []
    for m in root_markets:
        if not isinstance(m, dict):
            continue
        mid = str(m.get("eventId") or m.get("event_id") or m.get("event") or "").strip()
        if mid == eid:
            attached.append(m)
    if not attached:
        return ev
    merged = dict(ev)
    merged["markets"] = attached
    return merged


def _infer_poly_slug_from_gamma_slug(gamma_slug: str, poly_slugs_enabled: set[str]) -> Optional[str]:
    s = str(gamma_slug or "").strip().lower()
    if not s:
        return None
    ordered: list[tuple[str, str]] = [
        ("atp-", "atp"),
        ("wta-", "wta"),
        ("wttmen-", "wttmen"),
        ("nba-", "nba"),
        ("nfl-", "nfl"),
        ("nhl-", "nhl"),
        ("mlb-", "mlb"),
        ("ufc-", "ufc"),
        ("epl-", "epl"),
        ("ucl-", "ucl"),
        ("uel-", "uel"),
        ("lal-", "epl"),
        ("mls-", "epl"),
    ]
    for pref, poly in ordered:
        if poly not in poly_slugs_enabled:
            continue
        if s.startswith(pref):
            return poly
    if s.startswith("wtt-"):
        if "wttmen" in poly_slugs_enabled:
            return "wttmen"
        if "atp" in poly_slugs_enabled:
            return "atp"
    return None


def _default_poly_slug_for_io_sport(io_sport: str, poly_slugs_order: list[str]) -> Optional[str]:
    raw = _io_sport_slug_coerce(io_sport)
    want = _IO_SPORT_ALIASES.get(raw, raw)
    for p in poly_slugs_order:
        ok = POLY_SLUG_TO_ODDS_KEY.get(p)
        if not ok:
            continue
        mapped = _ODDS_KEY_TO_IO_SPORT.get(ok, "")
        if mapped == want:
            return p
    return None


def _pick_open_game_from_public_search_page(
    events_raw: list[dict[str, Any]],
    root_markets: list[Any],
    odds_ev: OddsEvent,
    ah: str,
    aa: str,
    poly_slugs_order: list[str],
) -> Optional[OpenPolymarketGame]:
    """Filtra eventos Gamma; prefijo slug IO; fallback título con ambos apellidos; construye OpenPolymarketGame."""
    poly_set = frozenset(poly_slugs_order)
    base: list[dict[str, Any]] = []
    _ps_filter_logged = 0
    _ps_filter_cap = 30
    for raw in events_raw:
        ev0 = _merge_root_markets_into_event(raw, root_markets)
        sslug = str(ev0.get("slug") or "?")
        usable = _gamma_event_usable(ev0)
        teams = _event_matches_odds_teams(ev0, odds_ev.home, odds_ev.away)
        if _ps_filter_logged < _ps_filter_cap:
            _ps_filter_logged += 1
            log.debug(
                "[PS_FILTER] slug=%s usable=%s teams=%s io_home=%r io_away=%r",
                sslug,
                usable,
                teams,
                odds_ev.home,
                odds_ev.away,
            )
        if not usable:
            continue
        if not teams:
            continue
        tit = str(ev0.get("title") or "")
        if _latency_sports_skip_doubles() and (
            _doubles_hint_in_text(tit)
            or _doubles_hint_in_text(str(ev0.get("slug") or ""))
            or _gamma_event_doubles_signal(ev0)
        ):
            continue
        base.append(ev0)

    io_raw = _io_sport_slug_coerce(odds_ev.sport)
    io_sport = _IO_SPORT_ALIASES.get(io_raw, io_raw)

    def try_build(ev1: dict[str, Any], require_slug_prefix: bool) -> Optional[OpenPolymarketGame]:
        gslug = str(ev1.get("slug") or "")
        if require_slug_prefix and not _gamma_slug_matches_io_prefix(gslug, io_sport):
            return None
        poly_slug = _infer_poly_slug_from_gamma_slug(gslug, poly_set) or _default_poly_slug_for_io_sport(
            io_sport, poly_slugs_order
        )
        if not poly_slug:
            return None
        return _poly_event_to_open_game(ev1, poly_slug)

    for ev in base:
        og = try_build(ev, True)
        if og is not None:
            return og
    for ev in base:
        if not _title_has_both_surname_tokens(str(ev.get("title") or ""), ah, aa):
            continue
        og = try_build(ev, False)
        if og is not None:
            return og
    return None


def _pick_best_market_in_event(
    ev_gamma: dict[str, Any],
    sport_key: str,
    odds_commence: datetime,
) -> Optional[GammaSportMarket]:
    """Mejor mercado hijo tradeable dentro de un evento public-search."""
    candidates: list[GammaSportMarket] = []
    for m in ev_gamma.get("markets") or []:
        if not isinstance(m, dict):
            continue
        ok, _why = clob_market_tradeable(m)
        if not ok:
            continue
        row = _gamma_row_from_market(m, sport_key, odds_commence)
        if row is None:
            continue
        cg = row.commence_for_odds(odds_commence)
        if cg is None or abs((cg - odds_commence).total_seconds()) >= 3600:
            continue
        candidates.append(row)
    if not candidates:
        return None

    def sort_key(r: GammaSportMarket) -> tuple[float, str]:
        cg2 = r.commence_for_odds(odds_commence) or odds_commence
        d = abs((cg2 - odds_commence).total_seconds())
        if "more-markets" in r.slug.lower():
            d += 1e6
        return (d, r.condition_id)

    return min(candidates, key=sort_key)


def _align_odds_io_to_game(ev: OddsEvent, game: OpenPolymarketGame) -> Optional[tuple[str, float, str, float]]:
    """(nombre lado Poly home, decimal, nombre lado Poly away, decimal) alineado con game.home/game.away."""
    gh, ga = (game.home or "").strip(), (game.away or "").strip()
    if teams_match_odds_gamma(gh, ev.home) and teams_match_odds_gamma(ga, ev.away):
        return ev.home, ev.home_odds, ev.away, ev.away_odds
    if teams_match_odds_gamma(gh, ev.away) and teams_match_odds_gamma(ga, ev.home):
        return ev.away, ev.away_odds, ev.home, ev.home_odds
    return None


def _map_outcomes_to_tokens(
    g: GammaSportMarket,
    odds_home: str,
    odds_away: str,
) -> tuple[Optional[str], Optional[str]]:
    """Devuelve (token_odds_home, token_odds_away) alineando labels Gamma con equipos Odds."""
    tok_home: Optional[str] = None
    tok_away: Optional[str] = None
    for label, tid in g.outcome_tokens:
        if tok_home is None and teams_match_odds_gamma(odds_home, label):
            tok_home = tid
    for label, tid in g.outcome_tokens:
        if tid == tok_home:
            continue
        if tok_away is None and teams_match_odds_gamma(odds_away, label):
            tok_away = tid
    if tok_home and tok_away and tok_home != tok_away:
        return tok_home, tok_away
    if len(g.outcome_tokens) == 2:
        t0, t1 = g.outcome_tokens[0][1], g.outcome_tokens[1][1]
        l0, l1 = g.outcome_tokens[0][0], g.outcome_tokens[1][0]
        if teams_match_odds_gamma(odds_home, l0) and teams_match_odds_gamma(odds_away, l1):
            return t0, t1
        if teams_match_odds_gamma(odds_home, l1) and teams_match_odds_gamma(odds_away, l0):
            return t1, t0
    return None, None


def _open_game_to_gamma_row(game: OpenPolymarketGame, odds_sport_key: str) -> GammaSportMarket:
    return GammaSportMarket(
        condition_id=game.condition_id,
        slug=game.slug or game.condition_id[:16],
        sport_key=odds_sport_key,
        league=game.sport_slug,
        home_team=game.home,
        away_team=game.away,
        outcome_tokens=list(game.outcome_tokens),
        question=game.raw_title,
        end_date_s=game.end_date_s,
        start_date_s=None,
    )


def _poly_token_ids_from_open_games(games: list[OpenPolymarketGame]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for g in games:
        for _lbl, tid in g.outcome_tokens:
            t = (tid or "").strip()
            if t and t not in seen:
                seen.add(t)
                out.append(t)
    return out


class LatencyArbSportsStrategy(ArbStrategy):
    slug = "latency_arb_sports"
    name = "Latency Arb — Sports"

    @property
    def csv_columns(self) -> list[str]:
        return [
            "ts",
            "strategy",
            "action",
            "reason",
            "dry_run",
            "signal_id",
            "game_slug",
            "league",
            "home_team",
            "away_team",
            "side",
            "token_id",
            "price_poly",
            "prob_pinnacle",
            "edge_mid",
            "edge_exec",
            "edge",
            "spread",
            "size_usdc",
            "status",
            "latency_ms_ref_to_clob",
            "order_attempted",
            "fill_price",
            "fill_size",
            "fee_est",
            "odds_io_updated_at",
            "clob_best_bid",
            "clob_best_bid_size",
            "clob_best_ask",
            "clob_best_ask_size",
            "clob_liquidity_at_price",
            "poly_price_30s",
            "poly_price_60s",
            "poly_price_120s",
            "betfair_depth_home",
            "betfair_depth_away",
            "match_score",
            "signal_anchor_ts",
        ]

    def __init__(self, config: dict[str, Any], dry_run: bool = True) -> None:
        super().__init__(config, dry_run=dry_run)
        self.poly_slugs = [s.strip().lower() for s in _HARDCODE_POLY_SLUGS.split(",") if s.strip()]
        self.min_edge = float(_HARDCODE_MIN_EDGE)
        self.min_edge_exec = float(_HARDCODE_MIN_EDGE_EXEC)
        self.max_stake = float(_HARDCODE_MAX_STAKE_USDC)
        self.max_spread = float(_HARDCODE_MAX_SPREAD)
        self.min_top_ask_notional = float(_HARDCODE_MIN_TOP_ASK_NOTIONAL_USDC)
        self.min_ask_band_notional = float(_HARDCODE_MIN_ASK_BAND_NOTIONAL_USDC)
        self.ref_backoff_kill_switch_sec = float(_HARDCODE_KILL_SWITCH_REF_BACKOFF_SEC)
        self.regions = _HARDCODE_REGIONS.strip()
        self.poll_interval = float(_HARDCODE_POLL_INTERVAL)
        self.poll_interval_active = float(_HARDCODE_POLL_INTERVAL_ACTIVE)
        ttl_raw = float(_HARDCODE_DISCOVERY_TTL)
        if ttl_raw < MIN_DISCOVERY_TTL_SEC:
            log.warning(
                "[latency_arb_sports] discovery TTL clamped to %ss (was %s)",
                MIN_DISCOVERY_TTL_SEC,
                ttl_raw,
            )
        self.discovery_ttl = max(MIN_DISCOVERY_TTL_SEC, ttl_raw)
        self.discovery_ttl_active = float(_HARDCODE_DISCOVERY_TTL_ACTIVE)
        self.window_hours_before = float(_HARDCODE_WINDOW_HOURS_BEFORE)
        self._window_past_hours = 2.0
        self._breaker = config.get("circuit_breaker")
        self._start_capital = float(config.get("start_capital", 10_000.0))
        self._current_capital = float(config.get("current_capital", 10_000.0))
        self._gamma_sports_meta_mono = 0.0
        self._gamma_sports_meta: list[dict[str, Any]] = []
        self._poly_series_by_slug: dict[str, str] = {}
        self._open_games_cache_mono = 0.0
        self._open_games: list[OpenPolymarketGame] = []
        self._ws_cache: dict[str, dict[str, Any]] = {}
        self._ws_task: Optional[asyncio.Task[None]] = None
        self._shutdown = asyncio.Event()
        self._cycle_seq = 0
        self._odds_client = OddsApiIo()
        self._ref_debug_done = False
        self._cache_debug_done = False
        self._snapshot_csv_enabled = _SNAPSHOT_CSV_ENABLED
        self._snapshot_csv_path = _latency_snapshots_csv_path()
        self._snapshot_csv_lock = threading.Lock()
        # Cooldown SIGNAL por mercado+lado (clave estable: slug Gamma + YES/NO; no home/away por orden variable).
        self._last_signal_price: dict[str, float] = {}
        self._last_signal_mono: dict[str, float] = {}
        # Betfair-first: resultado de public-search por event_id IO (TTL corto, independiente del discovery por series).
        self._betfair_gamma_search_cache: dict[str, tuple[float, Optional[OpenPolymarketGame]]] = {}
        self._betfair_gamma_search_ttl = float(_HARDCODE_BETFAIR_GAMMA_SEARCH_TTL)
        self._cycle_public_search_http = 0
        self._cycle_public_search_cache_hits = 0
        # Última profundidad Betfair por (event_id, bookie) desde WS IO (hook en _apply_ws_created_updated).
        self._io_betfair_depth_last: dict[tuple[str, str], tuple[str, str]] = {}
        self._io_ws_depth_hook_installed = False
        # Follow-up precios Poly a 30/60/120s tras SIGNAL/SKIP (clave uuid).
        self._pending_poly_followups: dict[str, dict[str, Any]] = {}
        self._rest_rr_index = 0
        # condition_id -> (último side YES|NO con SIGNAL, monotonic) para bloquear opuesto en ventana corta.
        self._condition_signal_recent: dict[str, tuple[str, float]] = {}

    def _ensure_csv(self) -> None:
        """Migra cabecera si hay columnas nuevas (base solo fusiona ficticio)."""
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        want = self.all_csv_columns
        if not self.csv_path.exists():
            with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=want, extrasaction="ignore")
                w.writeheader()
            return
        with open(self.csv_path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            old_fields = list(r.fieldnames or [])
            rows = list(r)
        if tuple(old_fields) == tuple(want):
            return
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=want, extrasaction="ignore")
            w.writeheader()
            for row in rows:
                out = {k: row.get(k, "") for k in want}
                w.writerow(out)

    def _install_io_ws_depth_capture_hook(self) -> None:
        if self._io_ws_depth_hook_installed:
            return
        self._io_ws_depth_hook_installed = True
        c = self._odds_client
        if not getattr(c, "ws_enabled", False):
            return
        orig = c._apply_ws_created_updated

        async def wrapped(
            self_c: OddsApiIo,
            session: aiohttp.ClientSession,
            data: dict[str, Any],
            sport_param: str,
            message_type: str,
        ) -> None:
            if isinstance(data, dict):
                eid = str(data.get("id") or "").strip()
                bk = str(data.get("bookie") or "").strip()
                if eid and bk:
                    dh, da = _depth_pair_from_ws_payload(data)
                    self._io_betfair_depth_last[(eid, bk.casefold())] = (dh, da)
            return await orig(session, data, sport_param, message_type)

        c._apply_ws_created_updated = types.MethodType(wrapped, c)

    def _betfair_depth_for_event(self, odds_event: OddsEvent) -> tuple[str, str]:
        eid = (odds_event.event_id or "").strip()
        bk = (odds_event.bookie or "").strip().casefold()
        if not eid:
            return "", ""
        return self._io_betfair_depth_last.get((eid, bk), ("", ""))

    async def _clob_trace_for_token(
        self, clob: PolyCLOBClient, token_id: str, mid_signal: float
    ) -> dict[str, str]:
        ob = clob.get_cached_orderbook_snapshot(token_id)
        if ob is None:
            ob = await clob.get_orderbook(token_id)
        if not isinstance(ob, dict) or ob.get("error"):
            return {
                "clob_best_bid": "",
                "clob_best_bid_size": "",
                "clob_best_ask": "",
                "clob_best_ask_size": "",
                "clob_liquidity_at_price": "",
            }
        bb = ob.get("best_bid")
        ba = ob.get("best_ask")
        bb_f = float(bb) if bb is not None else None
        ba_f = float(ba) if ba is not None else None
        _, bb_sz = _orderbook_best_bid_size(ob)
        ba_sz = ob.get("best_ask_size")
        try:
            ba_sz_f = float(ba_sz) if ba_sz is not None else None
        except (TypeError, ValueError):
            ba_sz_f = None
        liq = _asks_notional_usdc_in_band(ob, float(mid_signal), 0.01)
        return {
            "clob_best_bid": _fmt_opt_float(bb_f),
            "clob_best_bid_size": _fmt_opt_float(bb_sz),
            "clob_best_ask": _fmt_opt_float(ba_f),
            "clob_best_ask_size": _fmt_opt_float(ba_sz_f),
            "clob_liquidity_at_price": _fmt_opt_float(liq, nd=4) if liq is not None else "",
        }

    async def _poly_mid_for_followup(self, clob: PolyCLOBClient, token_id: str) -> Optional[float]:
        cm = clob.get_cached_mid(token_id)
        if cm is not None:
            return float(cm)
        m = await clob.get_midpoint(token_id)
        if m is not None:
            return float(m)
        ob = await clob.get_orderbook(token_id)
        if not isinstance(ob, dict) or ob.get("error"):
            return None
        bb, ba = ob.get("best_bid"), ob.get("best_ask")
        if bb is not None and ba is not None:
            return (float(bb) + float(ba)) / 2.0
        return None

    def _register_poly_followup(self, snap: dict[str, Any], anchor_iso: str) -> None:
        uid = str(uuid.uuid4())
        self._pending_poly_followups[uid] = {
            "t0": time.time(),
            "anchor_iso": anchor_iso,
            "snap": snap,
            "p30": None,
            "p60": None,
            "p120": None,
        }

    async def _drain_poly_followups(self, clob: PolyCLOBClient) -> None:
        now = time.time()
        to_emit: list[tuple[str, dict[str, Any], str]] = []
        for uid, p in list(self._pending_poly_followups.items()):
            age = now - float(p["t0"])
            tok = str(p["snap"].get("token_id") or "")
            if not tok:
                del self._pending_poly_followups[uid]
                continue
            if p["p30"] is None and age >= 30.0:
                p["p30"] = await self._poly_mid_for_followup(clob, tok)
            if p["p60"] is None and age >= 60.0:
                p["p60"] = await self._poly_mid_for_followup(clob, tok)
            if p["p120"] is None and age >= 120.0:
                p["p120"] = await self._poly_mid_for_followup(clob, tok)
            if p["p120"] is not None:
                to_emit.append((uid, p, "ok"))
            elif age >= 150.0:
                to_emit.append((uid, p, "partial"))

        for uid, p, mode in to_emit:
            self._pending_poly_followups.pop(uid, None)
            if mode == "partial" and p["p30"] is None and p["p60"] is None and p["p120"] is None:
                continue
            snap = p["snap"]
            empty_clob = {
                "odds_io_updated_at": "",
                "clob_best_bid": "",
                "clob_best_bid_size": "",
                "clob_best_ask": "",
                "clob_best_ask_size": "",
                "clob_liquidity_at_price": "",
            }
            oe = snap.get("_odds_event")
            dh, da = ("", "")
            if isinstance(oe, OddsEvent):
                dh, da = self._betfair_depth_for_event(oe)
            row: dict[str, Any] = {
                "action": "FOLLOWUP_POLY",
                "reason": f"poly_mid_{mode}_after_signal",
                "signal_id": snap.get("signal_id", ""),
                "game_slug": snap.get("game_slug", ""),
                "league": snap.get("league", ""),
                "home_team": snap.get("home_team", ""),
                "away_team": snap.get("away_team", ""),
                "side": snap.get("side", ""),
                "token_id": snap.get("token_id", ""),
                "price_poly": snap.get("price_poly", ""),
                "prob_pinnacle": snap.get("prob_pinnacle", ""),
                "edge_mid": snap.get("edge_mid", ""),
                "edge_exec": snap.get("edge_exec", ""),
                "edge": snap.get("edge", ""),
                "spread": snap.get("spread", ""),
                "size_usdc": "0",
                "status": "INFO",
                "latency_ms_ref_to_clob": snap.get("latency_ms_ref_to_clob", ""),
                "order_attempted": "false",
                "fill_price": "",
                "fill_size": "",
                "fee_est": "",
                **empty_clob,
                "poly_price_30s": _fmt_opt_float(p["p30"]),
                "poly_price_60s": _fmt_opt_float(p["p60"]),
                "poly_price_120s": _fmt_opt_float(p["p120"]),
                "betfair_depth_home": dh,
                "betfair_depth_away": da,
                "match_score": snap.get("match_score", ""),
                "signal_anchor_ts": p["anchor_iso"],
            }
            await self.log_signal_async(row)

    def _ws_snapshot_match_fields(self, game: OpenPolymarketGame) -> tuple[str, str]:
        """Best-effort desde sports WS cache (esquema variable); vacío si no hay datos."""
        blob = self._ws_cache.get(game.sport_slug)
        if not isinstance(blob, dict):
            return "", ""
        for ks, kc in (("match_set", "match_score"), ("matchSet", "matchScore")):
            if ks in blob or kc in blob:
                return str(blob.get(ks) or ""), str(blob.get(kc) or "")
        inner = blob.get("game")
        if isinstance(inner, dict):
            for ks, kc in (("match_set", "match_score"), ("matchSet", "matchScore")):
                if ks in inner or kc in inner:
                    return str(inner.get(ks) or ""), str(inner.get(kc) or "")
        return "", ""

    def _append_latency_snapshot_row(self, row: dict[str, Any]) -> None:
        if not self._snapshot_csv_enabled:
            return
        out = {k: "" if row.get(k) is None else str(row.get(k)) for k in _SNAPSHOT_CSV_COLUMNS}
        path = self._snapshot_csv_path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with self._snapshot_csv_lock:
                new_file = not path.is_file()
                with open(path, "a", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=_SNAPSHOT_CSV_COLUMNS, extrasaction="ignore")
                    if new_file:
                        w.writeheader()
                    w.writerow(out)
        except Exception as e:
            log.warning("[latency_arb_sports] snapshot csv append: %s", e)

    def _write_cycle_metrics_json(
        self,
        open_poly_games: int,
        reference_aligned: int,
        csv_rows_last_cycle: int,
        ws_cache_size: int,
        pipeline_entered: int,
        *,
        briefing_pre_game_distant: int = 0,
        briefing_within_end_window: int = 0,
        briefing_ml_like: int = 0,
        briefing_io_name_match_cache: int = 0,
        ws_odds_bookmaker_slots: int = 0,
        betfair_first_ws_rows: int = 0,
    ) -> None:
        """Expone último ciclo a /api/arb/status (lee scripts/api.py) para dashboards sin parsear logs."""
        try:
            from lab.paths import data_dir

            path = data_dir() / "logs" / "latency_arb_sports_cycle_metrics.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "open_poly_games": int(open_poly_games),
                # ``reference_matched`` = Poly↔odds alineados con tokens CLOB (no confundir con “intentos de pipeline”).
                "reference_matched": int(reference_aligned),
                "reference_aligned_last_cycle": int(reference_aligned),
                "pipeline_entered_last_cycle": int(pipeline_entered),
                "csv_rows_last_cycle": int(csv_rows_last_cycle),
                "ws_cache_size": int(ws_cache_size),
                "ws_odds_bookmaker_slots": int(ws_odds_bookmaker_slots),
                "betfair_first_ws_rows": int(betfair_first_ws_rows),
                "briefing_pre_game_distant": int(briefing_pre_game_distant),
                "briefing_within_end_window": int(briefing_within_end_window),
                "briefing_ml_like": int(briefing_ml_like),
                "briefing_io_name_match_cache": int(briefing_io_name_match_cache),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except OSError as e:
            log.debug("[latency_arb_sports] cycle metrics json: %s", e)

    def _write_pending_matches_json(
        self,
        open_games: list[OpenPolymarketGame],
        odds_events_by_key: dict[str, list[OddsEvent]],
        manual_map: dict[str, dict[str, Any]],
    ) -> None:
        try:
            from lab.paths import data_dir

            path = data_dir() / "logs" / "latency_arb_sports_pending_matches.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            body = _build_pending_manual_payload(open_games, odds_events_by_key, manual_map)
            body["updated_at"] = datetime.now(timezone.utc).isoformat()
            path.write_text(json.dumps(body, ensure_ascii=False), encoding="utf-8")
        except OSError as e:
            log.debug("[latency_arb_sports] pending matches json: %s", e)

    def _get_odds_for_sport(self, sport_slug: str) -> list[OddsEvent]:
        poly_key = POLY_SLUG_TO_ODDS_KEY.get(str(sport_slug).strip().lower())
        if not poly_key:
            return []
        return self._odds_client.get_cached_odds(_client_poly_key_for_odds_io(poly_key, sport_slug))

    async def run_once(self) -> None:
        """Compat: no usado si run_loop está sobrescrito; mantener vacío mínimo."""
        return

    def _betfair_gamma_search_ttl_eff(self) -> float:
        if self._is_in_active_window(self._open_games):
            return float(_HARDCODE_BETFAIR_GAMMA_SEARCH_TTL_ACTIVE)
        return self._betfair_gamma_search_ttl

    def _is_in_active_window(self, games: list[OpenPolymarketGame]) -> bool:
        # Si el WS de odds tiene partidos live → ventana activa (Gamma puede ir aún vacío)
        if len(self._odds_client._ws_odds_cache) > 0:
            return True
        now = datetime.now(timezone.utc)
        lo = now - timedelta(hours=self._window_past_hours)
        hi = now + timedelta(hours=self.window_hours_before)
        for g in games:
            if g.end_date is None:
                continue
            if lo <= g.end_date <= hi:
                return True
        return False

    def _discovery_fetch_ttl_sec(self) -> float:
        if self._is_in_active_window(self._open_games):
            return max(5.0, self.discovery_ttl_active)
        return self.discovery_ttl

    async def _paper_equity_snapshot(self, state_manager: Any) -> tuple[float, float]:
        """Devuelve (capital_base, equity_actual) para cablear breaker a paper PnL."""
        try:
            st = await state_manager.get_all()
        except Exception:
            return self._start_capital, self._current_capital
        ent = st.get(self.slug, {}) if isinstance(st, dict) else {}
        try:
            cap = float(ent.get("fict_capital_eur") or self._start_capital)
        except (TypeError, ValueError):
            cap = self._start_capital
        try:
            pnl_cum = float(ent.get("fict_pnl_cumulative_eur") or 0.0)
        except (TypeError, ValueError):
            pnl_cum = 0.0
        return cap, cap + pnl_cum

    def _reference_feed_degraded(self) -> tuple[bool, float]:
        """Kill-switch: WS sin datos + REST en backoff prolongado."""
        ws_cache_size = len(self._odds_client._ws_odds_cache)
        backoff_left_s = float(self._odds_client._rest_backoff_until - time.monotonic())
        degraded = ws_cache_size == 0 and backoff_left_s >= self.ref_backoff_kill_switch_sec
        return degraded, max(0.0, backoff_left_s)

    async def run_loop(self, state_manager: Any) -> None:
        self._state_manager = state_manager
        self._shutdown.clear()
        self._ws_task = asyncio.create_task(self._ws_runner(), name="latency_arb_sports_ws")
        self._install_io_ws_depth_capture_hook()
        if self._odds_client.ws_enabled:
            raw_io_sports = ODDS_API_IO_SPORTS_EMBEDDED
            io_sports = [s.strip() for s in str(raw_io_sports).split(",") if s.strip()]
            self._odds_client.start_ws_stream(sports=io_sports)
        try:
            async with PolyCLOBClient(
                api_key=os.getenv("POLY_API_KEY", ""),
                private_key=os.getenv("POLY_PRIVATE_KEY", ""),
                dry_run=self.dry_run,
            ) as clob:
                while True:
                    try:
                        enabled = await state_manager.is_enabled(self.slug)
                        if not enabled:
                            await asyncio.sleep(self.poll_interval)
                            continue
                        odds_ws = self._odds_client._ws_runner_task
                        if odds_ws is not None and odds_ws.done() and not odds_ws.cancelled():
                            exc = odds_ws.exception()
                            if exc is not None:
                                raise exc
                        await self._poll_cycle(state_manager, clob)
                    except asyncio.CancelledError:
                        raise
                    except OddsApiIoRestQuotaError as e:
                        log.error(
                            "[latency_arb_sports] odds-api.io REST rechazó la petición (p. ej. límite gratuito); "
                            "parando estrategia para no seguir consumiendo requests. %s",
                            e,
                        )
                        raise
                    except Exception as e:
                        log.exception("[latency_arb_sports] poll error")
                        await self.log_signal_async(
                            {
                                "action": "ERROR:INTERNAL",
                                "reason": str(e)[:200],
                                "game_slug": "",
                                "league": "",
                                "home_team": "",
                                "away_team": "",
                                "side": "",
                                "token_id": "",
                                "price_poly": "",
                                "prob_pinnacle": "",
                                "edge": "",
                                "size_usdc": "",
                                "status": "ERROR",
                                "odds_io_updated_at": "",
                                "clob_best_bid": "",
                                "clob_best_bid_size": "",
                                "clob_best_ask": "",
                                "clob_best_ask_size": "",
                                "clob_liquidity_at_price": "",
                                "poly_price_30s": "",
                                "poly_price_60s": "",
                                "poly_price_120s": "",
                                "betfair_depth_home": "",
                                "betfair_depth_away": "",
                                "match_score": "",
                                "signal_anchor_ts": "",
                            }
                        )
                    games = self._open_games
                    interval = self.poll_interval_active if self._is_in_active_window(games) else self.poll_interval
                    await asyncio.sleep(interval)
        finally:
            self._shutdown.set()
            await self._odds_client.stop_ws_stream()
            if self._ws_task:
                self._ws_task.cancel()
                try:
                    await self._ws_task
                except asyncio.CancelledError:
                    pass
                self._ws_task = None

    async def _ws_runner(self) -> None:
        while not self._shutdown.is_set():
            try:
                async with aiohttp.ClientSession(
                    headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=60)
                ) as sess:
                    log.info("[latency_arb_sports] connecting sports WS %s", SPORTS_WS_URL)
                    async with sess.ws_connect(SPORTS_WS_URL, heartbeat=25.0) as ws:
                        async for msg in ws:
                            if self._shutdown.is_set():
                                break
                            if msg.type == WSMsgType.TEXT:
                                if msg.data == "ping":
                                    await ws.send_str("pong")
                                    continue
                                try:
                                    data = json.loads(msg.data)
                                except json.JSONDecodeError:
                                    continue
                                slug = str(data.get("slug") or "").strip()
                                if slug:
                                    self._ws_cache[slug] = data
                            elif msg.type == WSMsgType.PING:
                                await ws.pong(msg.data)
                            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[latency_arb_sports] ws error: %s", e)
            if self._shutdown.is_set():
                break
            await asyncio.sleep(3.0)

    async def _fetch_polymarket_sports_meta(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        now = time.monotonic()
        if self._gamma_sports_meta and now - self._gamma_sports_meta_mono < GAMMA_SPORTS_META_TTL_SEC:
            return self._gamma_sports_meta
        url = f"{GAMMA_API_URL}/sports"
        async with session.get(url, headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            text = await resp.text()
            if resp.status != 200:
                log.warning("[latency_arb_sports] Gamma /sports HTTP %s: %s", resp.status, text[:200])
                return self._gamma_sports_meta
            data = json.loads(text)
        rows = data if isinstance(data, list) else []
        self._gamma_sports_meta = rows
        self._gamma_sports_meta_mono = now
        self._poly_series_by_slug = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            slug = str(row.get("sport") or "").strip().lower()
            sid = row.get("series")
            if slug and sid is not None:
                self._poly_series_by_slug[slug] = str(sid).strip()
        log.info("[latency_arb_sports] Gamma /sports refreshed: n=%s slugs mapped=%s", len(rows), len(self._poly_series_by_slug))
        return self._gamma_sports_meta

    async def _gamma_open_games_for_slug(
        self, session: aiohttp.ClientSession, poly_slug: str
    ) -> list[OpenPolymarketGame]:
        """GET Gamma /events para un poly_slug; sin dedupe global (fusiona el caller)."""
        out: list[OpenPolymarketGame] = []
        odds_key = POLY_SLUG_TO_ODDS_KEY.get(poly_slug)
        if odds_key is None:
            return out
        series_id = self._poly_series_by_slug.get(poly_slug)
        if not series_id:
            return out
        url = f"{GAMMA_API_URL}/events"
        params: dict[str, str | int] = {
            "series_id": int(series_id) if str(series_id).isdigit() else series_id,
            "active": "true",
            "closed": "false",
            "limit": 200,
            "order": "createdAt",
            "ascending": "false",
        }
        try:
            async with session.get(
                url, params=params, headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=45)
            ) as resp:
                text = await resp.text()
                if resp.status != 200:
                    log.warning("[latency_arb_sports] Gamma /events series=%s HTTP %s", series_id, resp.status)
                    return out
                evs = json.loads(text)
        except (json.JSONDecodeError, aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            log.warning("[latency_arb_sports] Gamma /events series=%s error: %s", series_id, e)
            return out
        if not isinstance(evs, list):
            return out
        for ev in evs:
            if not isinstance(ev, dict):
                continue
            if not _gamma_event_usable(ev):
                continue
            og = _poly_event_to_open_game(ev, poly_slug)
            if og is None:
                continue
            raw_title = str(ev.get("title") or "")
            if _latency_sports_skip_doubles() and (
                _doubles_hint_in_text(og.home)
                or _doubles_hint_in_text(og.away)
                or _doubles_hint_in_text(raw_title)
                or _gamma_event_doubles_signal(ev)
            ):
                continue
            out.append(og)
        return out

    async def _fetch_open_polymarket_sports(self, session: aiohttp.ClientSession) -> list[OpenPolymarketGame]:
        now = time.monotonic()
        ttl = self._discovery_fetch_ttl_sec()
        if self._open_games and now - self._open_games_cache_mono < ttl:
            return self._open_games

        await self._fetch_polymarket_sports_meta(session)
        games: list[OpenPolymarketGame] = []
        seen_discovery: set[tuple[str, Any]] = set()

        slug_coros = [self._gamma_open_games_for_slug(session, poly_slug) for poly_slug in self.poly_slugs]
        if slug_coros:
            results = await asyncio.gather(*slug_coros, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    log.warning("[latency_arb_sports] Gamma /events slug task error: %s", res)
                    continue
                for og in res:
                    dedupe_str = (og.slug or og.condition_id or "").strip()
                    if dedupe_str:
                        dk: tuple[str, Any] = ("s", dedupe_str)
                    else:
                        dk = (
                            "s",
                            frozenset(
                                (normalize_name_order(og.home), normalize_name_order(og.away))
                            ),
                        )
                    if dk in seen_discovery:
                        continue
                    seen_discovery.add(dk)
                    games.append(og)

        self._open_games = games
        self._open_games_cache_mono = now
        log.info("[latency_arb_sports] open games discovery: n=%s poly_slugs=%s", len(games), self.poly_slugs)
        return games

    async def _gamma_public_search_fetch(
        self, session: aiohttp.ClientSession, q: str, limit: int
    ) -> tuple[list[dict[str, Any]], list[Any]]:
        self._cycle_public_search_http += 1
        url = f"{GAMMA_API_URL}/public-search"
        params = {"q": q, "limit": str(int(limit))}
        async with session.get(
            url, params=params, headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=45)
        ) as resp:
            text = await resp.text()
            if resp.status != 200:
                log.warning("[latency_arb_sports] Gamma public-search HTTP %s q=%r", resp.status, q[:120])
                return [], []
        return _parse_public_search_response(text)

    async def _resolve_open_game_betfair_public_search(
        self,
        session: aiohttp.ClientSession,
        odds_ev: OddsEvent,
    ) -> Optional[OpenPolymarketGame]:
        eid = str(odds_ev.event_id or "").strip()
        if not eid:
            return None
        now = time.monotonic()
        cached = self._betfair_gamma_search_cache.get(eid)
        if cached is not None and now - cached[0] < self._betfair_gamma_search_ttl_eff():
            self._cycle_public_search_cache_hits += 1
            return cached[1]

        nh = normalize_name_order(str(odds_ev.home or ""))
        na = normalize_name_order(str(odds_ev.away or ""))
        ah = _io_surname_token(nh)
        aa = _io_surname_token(na)
        if not ah:
            self._betfair_gamma_search_cache[eid] = (now, None)
            return None
        # Apellido/token demasiado corto → Gamma puede devolver HTTP 500 (p. ej. q='d'); caché negativa mismo TTL.
        if len(ah) < 2 or (bool(aa) and len(aa) < 2):
            self._betfair_gamma_search_cache[eid] = (now, None)
            return None

        limit = int(_HARDCODE_GAMMA_PUBLIC_SEARCH_LIMIT)
        poly_order = list(self.poly_slugs)

        async def one_round(q: str) -> Optional[OpenPolymarketGame]:
            evs, mks = await self._gamma_public_search_fetch(session, q, limit)
            return _pick_open_game_from_public_search_page(evs, mks, odds_ev, ah, aa, poly_order)

        game: Optional[OpenPolymarketGame] = None
        q1_for_miss = ""
        q2_for_miss = ""
        if aa:
            q1_for_miss = f"{ah} {aa}"
            game = await one_round(q1_for_miss)
            if game is None:
                q2_for_miss = ah
                game = await one_round(q2_for_miss)
        else:
            q1_for_miss = ah
            q2_for_miss = ""
            game = await one_round(q1_for_miss)

        if game is None:
            log.info(
                "[PS_MISS] event_id=%s home=%r away=%r q1=%r q2=%r",
                eid,
                odds_ev.home,
                odds_ev.away,
                q1_for_miss,
                q2_for_miss,
            )

        self._betfair_gamma_search_cache[eid] = (time.monotonic(), game)
        return game

    async def _poll_cycle(self, state_manager: Any, clob: PolyCLOBClient) -> None:
        self._start_capital, self._current_capital = await self._paper_equity_snapshot(state_manager)
        if self._breaker:
            ok = await self._breaker.check(self._current_capital, self._start_capital)
            if not ok:
                await self.log_signal_async(
                    {
                        "action": "SKIP:CIRCUIT_BREAKER",
                        "reason": "max_daily_drawdown exceeded",
                        "game_slug": "",
                        "league": "",
                        "home_team": "",
                        "away_team": "",
                        "side": "",
                        "token_id": "",
                        "price_poly": "",
                        "prob_pinnacle": "",
                        "edge": "",
                        "size_usdc": "",
                        "status": "SKIP",
                        "odds_io_updated_at": "",
                        "clob_best_bid": "",
                        "clob_best_bid_size": "",
                        "clob_best_ask": "",
                        "clob_best_ask_size": "",
                        "clob_liquidity_at_price": "",
                        "poly_price_30s": "",
                        "poly_price_60s": "",
                        "poly_price_120s": "",
                        "betfair_depth_home": "",
                        "betfair_depth_away": "",
                        "match_score": "",
                        "signal_anchor_ts": "",
                    }
                )
                return

        degraded_feed, backoff_left = self._reference_feed_degraded()
        if degraded_feed:
            await self.log_signal_async(
                {
                    "action": "SKIP:REF_DEGRADED",
                    "reason": f"ws_empty_and_rest_backoff_left_s={backoff_left:.1f}",
                    "game_slug": "",
                    "league": "",
                    "home_team": "",
                    "away_team": "",
                    "side": "",
                    "token_id": "",
                    "price_poly": "",
                    "prob_pinnacle": "",
                    "edge": "",
                    "size_usdc": "",
                    "status": "SKIP",
                }
            )
            return

        clear_p = _odds_event_meta_cache_clear_flag_path()
        if clear_p.is_file():
            try:
                self._odds_client._event_meta_cache.clear()
                self._odds_client._event_meta_negative.clear()
                log.info("[latency_arb_sports] odds_event_meta_cache en memoria vaciada (admin clear-meta-cache)")
            finally:
                try:
                    clear_p.unlink(missing_ok=True)
                except OSError:
                    pass

        async with aiohttp.ClientSession(
            headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=45)
        ) as sess:
            self._cycle_seq += 1
            seq = self._cycle_seq
            self._cycle_public_search_http = 0
            self._cycle_public_search_cache_hits = 0
            open_games = await self._fetch_open_polymarket_sports(sess)
            await clob.ensure_market_ws_subscription(_poly_token_ids_from_open_games(open_games))
            await asyncio.sleep(min(float(_HARDCODE_CLOB_WS_WARMUP_SEC), 1.0))
            if seq == 1 and not self._cache_debug_done:
                self._cache_debug_done = True
                odds_key = POLY_SLUG_TO_ODDS_KEY.get("atp", "atp")
                cached = self._odds_client.get_cached_odds(_client_poly_key_for_odds_io(odds_key, "atp"))
                log.info(
                    "[CACHE_DEBUG] odds_key=%s cached_count=%s ws_cache_raw=%s",
                    odds_key,
                    len(cached),
                    list(self._odds_client._ws_odds_cache.keys())[:3],
                )
            odds_events_by_key: dict[str, list[OddsEvent]] = {}
            keys_order: list[str] = []
            pipeline_entered = 0
            reference_aligned = 0
            csv_rows = 0
            for g in open_games:
                ok = POLY_SLUG_TO_ODDS_KEY.get(g.sport_slug)
                if ok and ok not in keys_order:
                    keys_order.append(ok)
            if keys_order:
                pick_idx = self._rest_rr_index % len(keys_order)
                self._rest_rr_index = (self._rest_rr_index + 1) % 10_000
                pick_key = keys_order[pick_idx]
                sample_g = next(gg for gg in open_games if POLY_SLUG_TO_ODDS_KEY.get(gg.sport_slug) == pick_key)
                poly_k = _client_poly_key_for_odds_io(pick_key, sample_g.sport_slug)
                io_s = poly_odds_key_to_io_sport(poly_k)
                ws_nonempty = bool(
                    io_s and self._odds_client.ws_enabled and self._odds_client.has_ws_odds_for_io_sport(io_s)
                )
                if not ws_nonempty:
                    await self._odds_client.refresh_rest_cache(sess, poly_k)
            # Sin ticks WS para un deporte, get_cached_odds queda vacío salvo REST: el round-robin solo
            # rellenaba una clave por ciclo → NBA/EPL podían quedar sin candidatos IO. Forzar REST por
            # cada poly_key distinto con caché vacía y sin WS para ese io_sport.
            distinct_pk: set[str] = set()
            for g2 in open_games:
                ok2 = POLY_SLUG_TO_ODDS_KEY.get(g2.sport_slug)
                if not ok2:
                    continue
                distinct_pk.add(_client_poly_key_for_odds_io(ok2, g2.sport_slug))
            for pk2 in sorted(distinct_pk):
                io_pk = poly_odds_key_to_io_sport(pk2)
                ws_live_pk = bool(
                    io_pk
                    and self._odds_client.ws_enabled
                    and self._odds_client.has_ws_odds_for_io_sport(io_pk)
                )
                if ws_live_pk:
                    continue
                if not self._odds_client.get_cached_odds(pk2):
                    await self._odds_client.refresh_rest_cache(sess, pk2)
            for g in open_games:
                ok = POLY_SLUG_TO_ODDS_KEY.get(g.sport_slug)
                if not ok:
                    continue
                pk = _client_poly_key_for_odds_io(ok, g.sport_slug)
                odds_events_by_key[ok] = self._odds_client.get_cached_odds(pk)
            odds_keys_loaded = {k for k, evs in odds_events_by_key.items() if evs}
            briefing_counts = _compute_briefing_cycle_counts(open_games, odds_events_by_key)
            manual_map = load_manual_matches()
            self._write_pending_matches_json(open_games, odds_events_by_key, manual_map)
            processed_condition_ids: set[str] = set()
            match_sem = asyncio.Semaphore(int(_HARDCODE_MATCH_PARALLELISM))
            processed_lock = asyncio.Lock()

            async def _process_bf_row(odds_ev: OddsEvent) -> tuple[int, int, int]:
                """(pipeline_entered, reference_aligned, csv_rows) por partido."""
                async with match_sem:
                    game_bf = await self._resolve_open_game_betfair_public_search(sess, odds_ev)
                    if game_bf is None:
                        return (0, 0, 0)
                    cid_bf = (game_bf.condition_id or "").strip()
                    odds_key_bf = POLY_SLUG_TO_ODDS_KEY.get(game_bf.sport_slug)
                    if odds_key_bf is None:
                        return (0, 0, 0)
                    async with processed_lock:
                        if not cid_bf or cid_bf in processed_condition_ids:
                            return (0, 0, 0)
                        processed_condition_ids.add(cid_bf)
                    st_bf = await self._process_matched_poly_odds(sess, clob, game_bf, odds_key_bf, odds_ev)
                    return (
                        1,
                        int(st_bf.get("reference_aligned", 0)),
                        int(st_bf.get("csv_rows", 0)),
                    )

            async def _process_og_row(game: OpenPolymarketGame) -> tuple[int, int, int]:
                async with match_sem:
                    cid_g = (game.condition_id or "").strip()
                    async with processed_lock:
                        if cid_g and cid_g in processed_condition_ids:
                            return (0, 0, 0)
                    odds_key = POLY_SLUG_TO_ODDS_KEY.get(game.sport_slug)
                    if odds_key is None:
                        return (0, 0, 0)
                    events_list = odds_events_by_key.get(odds_key) or []
                    odds_ev = find_event_matching_teams(events_list, game.home, game.away)
                    manual_tw: Optional[tuple[str, float, str, float]] = None
                    if odds_ev is None and cid_g:
                        mm = manual_map.get(cid_g)
                        if isinstance(mm, dict):
                            eid = str(mm.get("odds_event_id") or "").strip()
                            swap = bool(mm.get("swap_sides"))
                            if eid:
                                for ev in events_list:
                                    if str(ev.event_id) == eid:
                                        odds_ev = ev
                                        manual_tw = _manual_tw_from_odds_event(ev, swap)
                                        break
                    if odds_ev is None:
                        return (0, 0, 0)
                    async with processed_lock:
                        if cid_g and cid_g in processed_condition_ids:
                            return (0, 0, 0)
                        if cid_g:
                            processed_condition_ids.add(cid_g)
                    st = await self._process_matched_poly_odds(
                        sess, clob, game, odds_key, odds_ev, manual_tw=manual_tw
                    )
                    return (
                        1,
                        int(st.get("reference_aligned", 0)),
                        int(st.get("csv_rows", 0)),
                    )

            bf_events_list: list[OddsEvent] = []
            if self._odds_client.ws_enabled:
                bf_events_list = list(_flatten_ws_odds_best_per_event(self._odds_client))
                if bf_events_list:
                    bf_results = await asyncio.gather(
                        *(_process_bf_row(ev) for ev in bf_events_list),
                        return_exceptions=True,
                    )
                    for r in bf_results:
                        if isinstance(r, Exception):
                            log.warning("[latency_arb_sports] betfair-first row error: %s", r)
                            continue
                        ent_i, ali_i, csv_i = r
                        pipeline_entered += ent_i
                        reference_aligned += ali_i
                        csv_rows += csv_i

            if open_games:
                og_results = await asyncio.gather(
                    *(_process_og_row(g) for g in open_games),
                    return_exceptions=True,
                )
                for r in og_results:
                    if isinstance(r, Exception):
                        log.warning("[latency_arb_sports] open_games row error: %s", r)
                        continue
                    ent_i, ali_i, csv_i = r
                    pipeline_entered += ent_i
                    reference_aligned += ali_i
                    csv_rows += csv_i

            if (
                not self._ref_debug_done
                and seq == 10
                and len(open_games) > 0
                and len(odds_keys_loaded) > 0
            ):
                for game in open_games[:5]:
                    odds_key = POLY_SLUG_TO_ODDS_KEY.get(game.sport_slug, game.sport_slug)
                    odds_events = self._odds_client.get_cached_odds(
                        _client_poly_key_for_odds_io(odds_key, game.sport_slug)
                    )
                    preview = [f"{ev.home} vs {ev.away}" for ev in odds_events[:3]]
                    log.info(
                        "[REF_DEBUG] game='%s vs %s' sport=%s odds_io_events=%s",
                        game.home,
                        game.away,
                        game.sport_slug,
                        preview,
                    )
                self._ref_debug_done = True

            ws_slots = _ws_cache_bookmaker_slots(self._odds_client)
            log.info(
                "[latency_arb_sports] cycle #%s regions=%s min_edge_exec=%.4f max_stake=%.2f "
                "open_poly_games=%s odds_io_keys=%s pipeline_entered=%s reference_aligned=%s csv_rows=%s "
                "discovery_ttl_eff=%.0fs active_window=%s dry_run=%s ws_event_ids=%s ws_bk_slots=%s "
                "bf_first_ws_rows=%s (pipeline cuenta BF+OG antes de stale/CLOB; ver reference_aligned) "
                "public_search_http=%s public_search_cache_hits=%s",
                seq,
                self.regions,
                self.min_edge_exec,
                self.max_stake,
                len(open_games),
                len(odds_keys_loaded),
                pipeline_entered,
                reference_aligned,
                csv_rows,
                self._discovery_fetch_ttl_sec(),
                self._is_in_active_window(open_games),
                self.dry_run,
                len(self._odds_client._ws_odds_cache),
                ws_slots,
                len(bf_events_list),
                self._cycle_public_search_http,
                self._cycle_public_search_cache_hits,
            )
            self._write_cycle_metrics_json(
                len(open_games),
                reference_aligned,
                csv_rows,
                len(self._odds_client._ws_odds_cache),
                pipeline_entered,
                briefing_pre_game_distant=briefing_counts["briefing_pre_game_distant"],
                briefing_within_end_window=briefing_counts["briefing_within_end_window"],
                briefing_ml_like=briefing_counts["briefing_ml_like"],
                briefing_io_name_match_cache=briefing_counts["briefing_io_name_match_cache"],
                ws_odds_bookmaker_slots=ws_slots,
                betfair_first_ws_rows=len(bf_events_list),
            )
            await self._drain_poly_followups(clob)

    async def _enrich_signal_csv_fields(
        self,
        clob: PolyCLOBClient,
        token_id: str,
        mid: float,
        odds_event: OddsEvent,
        match_score_s: str,
        *,
        signal_id: str = "",
        edge_mid: Optional[float] = None,
        edge_exec: Optional[float] = None,
        spread: Optional[float] = None,
        latency_ms_ref_to_clob: Optional[float] = None,
        order_attempted: bool = False,
        fill_price: Optional[float] = None,
        fill_size: Optional[float] = None,
        fee_est: Optional[float] = None,
    ) -> dict[str, str]:
        clob_t = await self._clob_trace_for_token(clob, token_id, float(mid))
        dh, da = self._betfair_depth_for_event(odds_event)
        anchor_iso = datetime.now(timezone.utc).isoformat()
        return {
            "signal_id": signal_id,
            "edge_mid": _fmt_opt_float(edge_mid),
            "edge_exec": _fmt_opt_float(edge_exec),
            "spread": _fmt_opt_float(spread),
            "latency_ms_ref_to_clob": _fmt_opt_float(latency_ms_ref_to_clob, nd=1),
            "order_attempted": "true" if order_attempted else "false",
            "fill_price": _fmt_opt_float(fill_price),
            "fill_size": _fmt_opt_float(fill_size, nd=4),
            "fee_est": _fmt_opt_float(fee_est, nd=6),
            "odds_io_updated_at": str(odds_event.updated_at or ""),
            **clob_t,
            "poly_price_30s": "",
            "poly_price_60s": "",
            "poly_price_120s": "",
            "betfair_depth_home": dh,
            "betfair_depth_away": da,
            "match_score": match_score_s,
            "signal_anchor_ts": anchor_iso,
        }

    def _followup_snap(
        self,
        g: GammaSportMarket,
        odds_home: str,
        odds_away: str,
        side_label: str,
        token_id: str,
        mid: float,
        p_fair: float,
        edge_mid: float,
        edge_exec: Optional[float],
        spread: Optional[float],
        latency_ms_ref_to_clob: Optional[float],
        signal_id: str,
        odds_event: OddsEvent,
        match_score_s: str,
    ) -> dict[str, Any]:
        edge_use = edge_exec if edge_exec is not None else edge_mid
        return {
            "signal_id": signal_id,
            "game_slug": g.slug,
            "league": g.league,
            "home_team": odds_home,
            "away_team": odds_away,
            "side": side_label,
            "token_id": token_id,
            "price_poly": f"{mid:.6f}",
            "prob_pinnacle": f"{p_fair:.6f}",
            "edge_mid": f"{edge_mid:.6f}",
            "edge_exec": f"{edge_use:.6f}",
            "edge": f"{edge_use:.6f}",
            "spread": _fmt_opt_float(spread),
            "latency_ms_ref_to_clob": _fmt_opt_float(latency_ms_ref_to_clob, nd=1),
            "match_score": match_score_s,
            "_odds_event": odds_event,
        }

    async def _process_matched_poly_odds(
        self,
        _session: aiohttp.ClientSession,
        clob: PolyCLOBClient,
        game: OpenPolymarketGame,
        odds_sport_key: str,
        odds_event: OddsEvent,
        *,
        manual_tw: Optional[tuple[str, float, str, float]] = None,
    ) -> dict[str, int]:
        acc: dict[str, int] = {"csv_rows": 0, "reference_aligned": 0}
        if _is_pre_game_listing_game(game):
            return acc
        if _is_reference_lag_over_limit(odds_event) or _is_reference_stale_for_io(odds_event):
            return acc
        tw: Optional[tuple[str, float, str, float]] = manual_tw
        if tw is None:
            tw = _align_odds_io_to_game(odds_event, game)
        if tw is None:
            return acc
        odds_home, dec_h, odds_away, dec_a = tw
        ddraw = odds_event.draw_odds if odds_event.draw_odds and odds_event.draw_odds > 0 else None
        p_h_fair, p_a_fair, _ = remove_vig_decimal(dec_h, dec_a, ddraw)
        g = _open_game_to_gamma_row(game, odds_sport_key)
        tok_h, tok_a = _map_outcomes_to_tokens(g, odds_home, odds_away)
        if not tok_h or not tok_a:
            return acc
        acc["reference_aligned"] = 1
        _mset_snap, mscr_live = self._ws_snapshot_match_fields(game)

        async def _resolve_mid(tid: str) -> Optional[float]:
            cm = clob.get_cached_mid(tid)
            if cm is not None:
                return float(cm)
            m = await clob.get_midpoint(tid)
            if m is not None:
                return float(m)
            ob = await clob.get_orderbook(tid)
            bb, ba = ob.get("best_bid"), ob.get("best_ask")
            if bb is not None and ba is not None:
                return (float(bb) + float(ba)) / 2.0
            return None

        mid_h_val = await _resolve_mid(tok_h)
        mid_a_val = await _resolve_mid(tok_a)
        if mid_h_val is not None and mid_a_val is not None:
            edge_home = float(p_h_fair) - float(mid_h_val)
            edge_away = float(p_a_fair) - float(mid_a_val)
            log.info(
                "[MATCH_OK] poly='%s vs %s' odds_io='%s vs %s' edge_home=%.4f edge_away=%.4f",
                game.home,
                game.away,
                odds_event.home,
                odds_event.away,
                edge_home,
                edge_away,
            )
            snap_ts = datetime.now(timezone.utc).isoformat()
            game_label = (game.raw_title or "").strip() or f"{game.home} vs {game.away}"
            self._append_latency_snapshot_row(
                {
                    "snapshot_at": snap_ts,
                    "ts": snap_ts,
                    "game": game_label,
                    "home": game.home,
                    "away": game.away,
                    "sport": game.sport_slug,
                    "delta_home": f"{edge_home:.10f}".rstrip("0").rstrip("."),
                    "delta_away": f"{edge_away:.10f}".rstrip("0").rstrip("."),
                    "poly_mid_home": f"{float(mid_h_val):.10f}".rstrip("0").rstrip("."),
                    "poly_mid_away": f"{float(mid_a_val):.10f}".rstrip("0").rstrip("."),
                    "betfair_prob_home": f"{float(p_h_fair):.10f}".rstrip("0").rstrip("."),
                    "betfair_prob_away": f"{float(p_a_fair):.10f}".rstrip("0").rstrip("."),
                    "betfair_home_odds": f"{float(dec_h):.10f}".rstrip("0").rstrip("."),
                    "betfair_away_odds": f"{float(dec_a):.10f}".rstrip("0").rstrip("."),
                    "betfair_updated_at": str(odds_event.updated_at or ""),
                    "match_set": _mset_snap,
                    "match_score": mscr_live,
                }
            )

        ref_age_s = _odds_io_updated_age_sec(odds_event)
        latency_ms_ref_to_clob = ref_age_s * 1000.0 if ref_age_s is not None else None
        legs: list[dict[str, Any]] = []
        for side_label, token_id, p_fair in (("YES", tok_h, p_h_fair), ("NO", tok_a, p_a_fair)):
            mid_i: Optional[float] = mid_h_val if token_id == tok_h else mid_a_val
            if mid_i is None:
                mid_i = await _resolve_mid(token_id)
            if mid_i is None:
                continue
            ob = clob.get_cached_orderbook_snapshot(token_id)
            if ob is None:
                ob = await clob.get_orderbook(token_id)
            best_bid: Optional[float] = None
            best_ask: Optional[float] = None
            best_ask_size: Optional[float] = None
            top_ask_notional: Optional[float] = None
            ask_band_notional: Optional[float] = None
            spread: Optional[float] = None
            if isinstance(ob, dict) and not ob.get("error"):
                bb = ob.get("best_bid")
                ba = ob.get("best_ask")
                best_bid = float(bb) if bb is not None else None
                best_ask = float(ba) if ba is not None else None
                try:
                    ba_sz = ob.get("best_ask_size")
                    best_ask_size = float(ba_sz) if ba_sz is not None else None
                except (TypeError, ValueError):
                    best_ask_size = None
                if best_bid is not None and best_ask is not None:
                    spread = best_ask - best_bid
                if best_ask is not None and best_ask_size is not None:
                    top_ask_notional = best_ask * best_ask_size
                ask_center = best_ask if best_ask is not None else float(mid_i)
                ask_band_notional = _asks_notional_usdc_in_band(ob, ask_center, 0.01)
            edge_mid_i = float(p_fair) - float(mid_i)
            edge_exec_i = float(p_fair) - float(best_ask) if best_ask is not None else None
            legs.append(
                {
                    "side_label": side_label,
                    "token_id": token_id,
                    "p_fair": float(p_fair),
                    "mid": float(mid_i),
                    "edge_mid": edge_mid_i,
                    "edge_exec": edge_exec_i,
                    "best_ask": best_ask,
                    "spread": spread,
                    "top_ask_notional": top_ask_notional,
                    "ask_band_notional": ask_band_notional,
                    "latency_ms_ref_to_clob": latency_ms_ref_to_clob,
                }
            )

        if not legs:
            sid0 = str(uuid.uuid4())
            ex0 = await self._enrich_signal_csv_fields(
                clob,
                tok_h,
                0.5,
                odds_event,
                mscr_live,
                signal_id=sid0,
                edge_mid=None,
                spread=None,
                latency_ms_ref_to_clob=latency_ms_ref_to_clob,
            )
            await self.log_signal_async(
                {
                    "action": "SKIP:CLOB_NO_MID",
                    "reason": "poly_odds_aligned_but_no_clob_mid_yes_no_check_ws_subscription",
                    "game_slug": g.slug,
                    "league": g.league,
                    "home_team": odds_home,
                    "away_team": odds_away,
                    "side": "",
                    "token_id": f"{tok_h}|{tok_a}",
                    "price_poly": "",
                    "prob_pinnacle": "",
                    "edge_mid": "",
                    "edge_exec": "",
                    "edge": "",
                    "spread": "",
                    "size_usdc": "0",
                    "status": "SKIP",
                    **ex0,
                }
            )
            acc["csv_rows"] += 1
            return acc

        # Como máximo un BUY por partido/ciclo: el mejor edge ejecutable.
        signal_side: Optional[str] = None
        buy_candidates: list[tuple[float, str]] = []
        for leg in legs:
            edge_exec_i = leg["edge_exec"]
            if edge_exec_i is not None and edge_exec_i > 0 and edge_exec_i >= self.min_edge_exec:
                buy_candidates.append((float(edge_exec_i), str(leg["side_label"])))
        if buy_candidates:
            signal_side = max(buy_candidates, key=lambda x: x[0])[1]

        for leg in legs:
            side_label = str(leg["side_label"])
            token_id = str(leg["token_id"])
            p_fair = float(leg["p_fair"])
            mid = float(leg["mid"])
            edge_mid = float(leg["edge_mid"])
            edge_exec = leg["edge_exec"]
            spread = leg["spread"]
            best_ask = leg["best_ask"]
            top_ask_notional = leg["top_ask_notional"]
            ask_band_notional = leg["ask_band_notional"]
            latency_ms_ref = leg["latency_ms_ref_to_clob"]
            signal_id = str(uuid.uuid4())
            edge_for_log = float(edge_exec) if edge_exec is not None else float(edge_mid)
            clob_read_at = datetime.utcnow().isoformat()
            log.info(
                "[latency_arb_sports] CLOB_READ game='%s vs %s' poly_mid=%s clob_read_at=%s",
                game.home,
                game.away,
                mid,
                clob_read_at,
            )
            log.info(
                "[latency_arb_sports] LATENCY_SNAPSHOT\n   game='%s vs %s'\n"
                "   odds_io_prob=%s poly_mid=%s\n   delta=%.4f\n"
                "   odds_io_updated_at=%s\n   clob_read_at=%s",
                game.home,
                game.away,
                p_fair,
                mid,
                edge_mid,
                odds_event.updated_at,
                clob_read_at,
            )
            edge_mag = abs(edge_for_log)

            if signal_side is not None and side_label != signal_side:
                ex = await self._enrich_signal_csv_fields(
                    clob,
                    token_id,
                    float(mid),
                    odds_event,
                    mscr_live,
                    signal_id=signal_id,
                    edge_mid=edge_mid,
                    edge_exec=edge_exec,
                    spread=spread,
                    latency_ms_ref_to_clob=latency_ms_ref,
                )
                await self.log_signal_async(
                    {
                        "action": "SKIP:BELOW_MIN_EDGE",
                        "reason": (
                            f"not_buy_leg_signal_on_{signal_side}_side={side_label}_"
                            f"edge_exec={_fmt_opt_float(edge_exec)}_edge_mid={edge_mid:.4f}"
                        ),
                        "game_slug": g.slug,
                        "league": g.league,
                        "home_team": odds_home,
                        "away_team": odds_away,
                        "side": side_label,
                        "token_id": token_id,
                        "price_poly": f"{mid:.6f}",
                        "prob_pinnacle": f"{p_fair:.6f}",
                        "edge_mid": f"{edge_mid:.6f}",
                        "edge_exec": _fmt_opt_float(edge_exec),
                        "edge": f"{edge_mag:.6f}",
                        "spread": _fmt_opt_float(spread),
                        "size_usdc": "0",
                        "status": "SKIP",
                        **ex,
                    }
                )
                self._register_poly_followup(
                    self._followup_snap(
                        g,
                        odds_home,
                        odds_away,
                        side_label,
                        token_id,
                        float(mid),
                        float(p_fair),
                        edge_mid,
                        edge_exec,
                        spread,
                        latency_ms_ref,
                        signal_id,
                        odds_event,
                        mscr_live,
                    ),
                    ex["signal_anchor_ts"],
                )
                acc["csv_rows"] += 1
                continue

            if edge_exec is None:
                ex = await self._enrich_signal_csv_fields(
                    clob,
                    token_id,
                    float(mid),
                    odds_event,
                    mscr_live,
                    signal_id=signal_id,
                    edge_mid=edge_mid,
                    spread=spread,
                    latency_ms_ref_to_clob=latency_ms_ref,
                )
                await self.log_signal_async(
                    {
                        "action": "SKIP:BELOW_MIN_EDGE",
                        "reason": "missing_best_ask_for_exec_edge",
                        "game_slug": g.slug,
                        "league": g.league,
                        "home_team": odds_home,
                        "away_team": odds_away,
                        "side": side_label,
                        "token_id": token_id,
                        "price_poly": f"{mid:.6f}",
                        "prob_pinnacle": f"{p_fair:.6f}",
                        "edge_mid": f"{edge_mid:.6f}",
                        "edge_exec": "",
                        "edge": f"{edge_mid:.6f}",
                        "spread": _fmt_opt_float(spread),
                        "size_usdc": "0",
                        "status": "SKIP",
                        **ex,
                    }
                )
                self._register_poly_followup(
                    self._followup_snap(
                        g,
                        odds_home,
                        odds_away,
                        side_label,
                        token_id,
                        float(mid),
                        float(p_fair),
                        edge_mid,
                        edge_exec,
                        spread,
                        latency_ms_ref,
                        signal_id,
                        odds_event,
                        mscr_live,
                    ),
                    ex["signal_anchor_ts"],
                )
                acc["csv_rows"] += 1
                continue

            if edge_exec < self.min_edge_exec:
                ex = await self._enrich_signal_csv_fields(
                    clob,
                    token_id,
                    float(mid),
                    odds_event,
                    mscr_live,
                    signal_id=signal_id,
                    edge_mid=edge_mid,
                    edge_exec=edge_exec,
                    spread=spread,
                    latency_ms_ref_to_clob=latency_ms_ref,
                )
                await self.log_signal_async(
                    {
                        "action": "SKIP:BELOW_MIN_EDGE",
                        "reason": (
                            f"edge_exec={edge_exec:.4f}<min_edge_exec={self.min_edge_exec:.4f}_"
                            f"side={side_label}_edge_mid={edge_mid:.4f}"
                        ),
                        "game_slug": g.slug,
                        "league": g.league,
                        "home_team": odds_home,
                        "away_team": odds_away,
                        "side": side_label,
                        "token_id": token_id,
                        "price_poly": f"{mid:.6f}",
                        "prob_pinnacle": f"{p_fair:.6f}",
                        "edge_mid": f"{edge_mid:.6f}",
                        "edge_exec": f"{edge_exec:.6f}",
                        "edge": f"{edge_exec:.6f}",
                        "spread": _fmt_opt_float(spread),
                        "size_usdc": "0",
                        "status": "SKIP",
                        **ex,
                    }
                )
                self._register_poly_followup(
                    self._followup_snap(
                        g,
                        odds_home,
                        odds_away,
                        side_label,
                        token_id,
                        float(mid),
                        float(p_fair),
                        edge_mid,
                        edge_exec,
                        spread,
                        latency_ms_ref,
                        signal_id,
                        odds_event,
                        mscr_live,
                    ),
                    ex["signal_anchor_ts"],
                )
                acc["csv_rows"] += 1
                continue

            if edge_exec <= 0:
                ex = await self._enrich_signal_csv_fields(
                    clob,
                    token_id,
                    float(mid),
                    odds_event,
                    mscr_live,
                    signal_id=signal_id,
                    edge_mid=edge_mid,
                    edge_exec=edge_exec,
                    spread=spread,
                    latency_ms_ref_to_clob=latency_ms_ref,
                )
                await self.log_signal_async(
                    {
                        "action": "SKIP:BELOW_MIN_EDGE",
                        "reason": (
                            f"edge_exec<=0_not_buy_leg_edge_exec={edge_exec:.4f}_"
                            f"min_edge_exec={self.min_edge_exec:.4f}_side={side_label}"
                        ),
                        "game_slug": g.slug,
                        "league": g.league,
                        "home_team": odds_home,
                        "away_team": odds_away,
                        "side": side_label,
                        "token_id": token_id,
                        "price_poly": f"{mid:.6f}",
                        "prob_pinnacle": f"{p_fair:.6f}",
                        "edge_mid": f"{edge_mid:.6f}",
                        "edge_exec": f"{edge_exec:.6f}",
                        "edge": f"{edge_exec:.6f}",
                        "spread": _fmt_opt_float(spread),
                        "size_usdc": "0",
                        "status": "SKIP",
                        **ex,
                    }
                )
                self._register_poly_followup(
                    self._followup_snap(
                        g,
                        odds_home,
                        odds_away,
                        side_label,
                        token_id,
                        float(mid),
                        float(p_fair),
                        edge_mid,
                        edge_exec,
                        spread,
                        latency_ms_ref,
                        signal_id,
                        odds_event,
                        mscr_live,
                    ),
                    ex["signal_anchor_ts"],
                )
                acc["csv_rows"] += 1
                continue

            if spread is not None and spread > self.max_spread:
                ex = await self._enrich_signal_csv_fields(
                    clob,
                    token_id,
                    float(mid),
                    odds_event,
                    mscr_live,
                    signal_id=signal_id,
                    edge_mid=edge_mid,
                    edge_exec=edge_exec,
                    spread=spread,
                    latency_ms_ref_to_clob=latency_ms_ref,
                )
                await self.log_signal_async(
                    {
                        "action": "SKIP:WIDE_SPREAD",
                        "reason": f"spread={spread:.4f}>max_spread={self.max_spread:.4f}",
                        "game_slug": g.slug,
                        "league": g.league,
                        "home_team": odds_home,
                        "away_team": odds_away,
                        "side": side_label,
                        "token_id": token_id,
                        "price_poly": f"{mid:.6f}",
                        "prob_pinnacle": f"{p_fair:.6f}",
                        "edge_mid": f"{edge_mid:.6f}",
                        "edge_exec": f"{edge_exec:.6f}",
                        "edge": f"{edge_exec:.6f}",
                        "spread": _fmt_opt_float(spread),
                        "size_usdc": "0",
                        "status": "SKIP",
                        **ex,
                    }
                )
                acc["csv_rows"] += 1
                continue

            if top_ask_notional is not None and top_ask_notional < self.min_top_ask_notional:
                ex = await self._enrich_signal_csv_fields(
                    clob,
                    token_id,
                    float(mid),
                    odds_event,
                    mscr_live,
                    signal_id=signal_id,
                    edge_mid=edge_mid,
                    edge_exec=edge_exec,
                    spread=spread,
                    latency_ms_ref_to_clob=latency_ms_ref,
                )
                await self.log_signal_async(
                    {
                        "action": "SKIP:LOW_LIQUIDITY",
                        "reason": (
                            f"top_ask_notional={top_ask_notional:.4f}<min_top_ask_notional="
                            f"{self.min_top_ask_notional:.4f}"
                        ),
                        "game_slug": g.slug,
                        "league": g.league,
                        "home_team": odds_home,
                        "away_team": odds_away,
                        "side": side_label,
                        "token_id": token_id,
                        "price_poly": f"{mid:.6f}",
                        "prob_pinnacle": f"{p_fair:.6f}",
                        "edge_mid": f"{edge_mid:.6f}",
                        "edge_exec": f"{edge_exec:.6f}",
                        "edge": f"{edge_exec:.6f}",
                        "spread": _fmt_opt_float(spread),
                        "size_usdc": "0",
                        "status": "SKIP",
                        **ex,
                    }
                )
                acc["csv_rows"] += 1
                continue

            if ask_band_notional is not None and ask_band_notional < self.min_ask_band_notional:
                ex = await self._enrich_signal_csv_fields(
                    clob,
                    token_id,
                    float(mid),
                    odds_event,
                    mscr_live,
                    signal_id=signal_id,
                    edge_mid=edge_mid,
                    edge_exec=edge_exec,
                    spread=spread,
                    latency_ms_ref_to_clob=latency_ms_ref,
                )
                await self.log_signal_async(
                    {
                        "action": "SKIP:LOW_LIQUIDITY",
                        "reason": (
                            f"ask_band_notional={ask_band_notional:.4f}<min_ask_band_notional="
                            f"{self.min_ask_band_notional:.4f}"
                        ),
                        "game_slug": g.slug,
                        "league": g.league,
                        "home_team": odds_home,
                        "away_team": odds_away,
                        "side": side_label,
                        "token_id": token_id,
                        "price_poly": f"{mid:.6f}",
                        "prob_pinnacle": f"{p_fair:.6f}",
                        "edge_mid": f"{edge_mid:.6f}",
                        "edge_exec": f"{edge_exec:.6f}",
                        "edge": f"{edge_exec:.6f}",
                        "spread": _fmt_opt_float(spread),
                        "size_usdc": "0",
                        "status": "SKIP",
                        **ex,
                    }
                )
                acc["csv_rows"] += 1
                continue

            base_key = (g.slug or game.slug or game.condition_id or "").strip()
            if not base_key:
                base_key = (token_id or "unknown")[:32]
            sig_key = f"{base_key}_{side_label}"
            now_mono = time.monotonic()
            cid_live = (game.condition_id or "").strip()
            if cid_live:
                prev_opp = self._condition_signal_recent.get(cid_live)
                if prev_opp is not None:
                    prev_side, prev_mono = prev_opp
                    if (
                        prev_side != side_label
                        and (now_mono - prev_mono) < float(_HARDCODE_OPPOSITE_SIGNAL_DEDUPE_SEC)
                    ):
                        ex = await self._enrich_signal_csv_fields(
                            clob,
                            token_id,
                            float(mid),
                            odds_event,
                            mscr_live,
                            signal_id=signal_id,
                            edge_mid=edge_mid,
                            edge_exec=edge_exec,
                            spread=spread,
                            latency_ms_ref_to_clob=latency_ms_ref,
                        )
                        await self.log_signal_async(
                            {
                                "action": "SKIP:OPPOSITE_DEDUPE",
                                "reason": (
                                    f"recent_signal_side={prev_side}_this_side={side_label}_"
                                    f"window_s={_HARDCODE_OPPOSITE_SIGNAL_DEDUPE_SEC:.0f}"
                                ),
                                "game_slug": g.slug,
                                "league": g.league,
                                "home_team": odds_home,
                                "away_team": odds_away,
                                "side": side_label,
                                "token_id": token_id,
                                "price_poly": f"{mid:.6f}",
                                "prob_pinnacle": f"{p_fair:.6f}",
                                "edge_mid": f"{edge_mid:.6f}",
                                "edge_exec": f"{edge_exec:.6f}",
                                "edge": f"{edge_exec:.6f}",
                                "spread": _fmt_opt_float(spread),
                                "size_usdc": "0",
                                "status": "SKIP",
                                **ex,
                            }
                        )
                        acc["csv_rows"] += 1
                        continue
            prev_p = self._last_signal_price.get(sig_key)
            prev_t = self._last_signal_mono.get(sig_key)
            if prev_p is not None and prev_t is not None:
                if (
                    abs(float(mid) - float(prev_p)) <= _SIGNAL_COOLDOWN_PRICE_EPS
                    and (now_mono - float(prev_t)) < _SIGNAL_COOLDOWN_SEC
                ):
                    log.debug(
                        "[latency_arb_sports] COOLDOWN_SKIP game=%s vs %s side=%s slug=%s "
                        "price_unchanged mid=%.6f prev_mid=%.6f dt_s=%.2f",
                        game.home,
                        game.away,
                        side_label,
                        g.slug,
                        float(mid),
                        float(prev_p),
                        now_mono - float(prev_t),
                    )
                    continue

            buy_side = "BUY"
            size = float(self.max_stake)
            status = "SIGNAL"
            action = "SIGNAL"
            reason = f"{buy_side}_{side_label}_fair_minus_exec={edge_exec:.4f}_fair_minus_mid={edge_mid:.4f}"
            order_attempted = False
            fill_price = float(best_ask) if best_ask is not None else float(mid)
            fill_size = size
            fee_est: Optional[float] = None
            if not self.dry_run:
                try:
                    order_attempted = True
                    ba = (await clob.get_orderbook(token_id)).get("best_ask")
                    price = float(ba) if ba is not None else float(fill_price)
                    await clob.place_order(
                        token_id,
                        "BUY",
                        price,
                        size,
                        order_type="FOK",
                        post_only=False,
                    )
                    action = "EXECUTED"
                    status = "EXECUTED"
                    reason = f"FOK_BUY_{side_label}@{price:.4f}"
                    fill_price = price
                except Exception as e:
                    action = "ERROR:ORDER_FAIL"
                    status = "SKIP"
                    reason = str(e)[:200]
                    fill_size = 0.0
            ex = await self._enrich_signal_csv_fields(
                clob,
                token_id,
                float(mid),
                odds_event,
                mscr_live,
                signal_id=signal_id,
                edge_mid=edge_mid,
                edge_exec=edge_exec,
                spread=spread,
                latency_ms_ref_to_clob=latency_ms_ref,
                order_attempted=order_attempted,
                fill_price=fill_price,
                fill_size=fill_size,
                fee_est=fee_est,
            )
            await self.log_signal_async(
                {
                    "action": action,
                    "reason": reason,
                    "signal_id": signal_id,
                    "game_slug": g.slug,
                    "league": g.league,
                    "home_team": odds_home,
                    "away_team": odds_away,
                    "side": side_label,
                    "token_id": token_id,
                    "price_poly": f"{mid:.6f}",
                    "prob_pinnacle": f"{p_fair:.6f}",
                    "edge_mid": f"{edge_mid:.6f}",
                    "edge_exec": f"{edge_exec:.6f}",
                    "edge": f"{edge_exec:.6f}",
                    "spread": _fmt_opt_float(spread),
                    "size_usdc": f"{size:.4f}",
                    "status": status,
                    **ex,
                }
            )
            self._register_poly_followup(
                self._followup_snap(
                    g,
                    odds_home,
                    odds_away,
                    side_label,
                    token_id,
                    float(mid),
                    float(p_fair),
                    edge_mid,
                    edge_exec,
                    spread,
                    latency_ms_ref,
                    signal_id,
                    odds_event,
                    mscr_live,
                ),
                ex["signal_anchor_ts"],
            )
            self._last_signal_price[sig_key] = float(mid)
            self._last_signal_mono[sig_key] = now_mono
            if cid_live:
                self._condition_signal_recent[cid_live] = (side_label, now_mono)
                if len(self._condition_signal_recent) > 800:
                    cutoff = now_mono - float(_HARDCODE_OPPOSITE_SIGNAL_DEDUPE_SEC) * 4
                    self._condition_signal_recent = {
                        k: v
                        for k, v in self._condition_signal_recent.items()
                        if v[1] >= cutoff
                    }
            acc["csv_rows"] += 1
        return acc
