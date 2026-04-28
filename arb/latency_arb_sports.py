"""Latency arb sports: Pinnacle (The Odds API) vs precios CLOB Polymarket en mercados deportivos."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiohttp
from aiohttp import WSMsgType

from arb.base import ArbStrategy
from clients.odds_api import odds_team_matches_gamma_blob, teams_match_odds_gamma
from clients.odds_api_io import (
    ODDS_API_IO_SPORTS_EMBEDDED,
    OddsApiIo,
    OddsEvent,
    find_event_matching_teams,
    remove_vig as remove_vig_decimal,
)
from clients.poly_clob import PolyCLOBClient
from clients.poly_markets import GAMMA_API_URL
from clients.poly_parse import api_bool_true, clob_market_tradeable, parse_json_list_maybe, parse_outcomes_list

log = logging.getLogger("latency_arb_sports")

SPORTS_WS_URL = "wss://sports-api.polymarket.com/ws"

_DEFAULT_HEADERS = {
    "User-Agent": "predmarket-arb/latency-arb-sports (aiohttp; +https://github.com)",
    "Accept": "application/json",
}

MIN_DISCOVERY_TTL_SEC = 300

# Defaults embebidos si Railway (u otro) no define env; `os.getenv` sigue pudiendo sobreescribir en local.
_EMBEDDED_LATENCY_SPORTS_POLY_SLUGS = "atp,wta,wttmen,epl,nba,nfl,ucl,uel,nhl"
_EMBEDDED_LATENCY_MIN_EDGE = "0.03"
_EMBEDDED_LATENCY_MAX_STAKE_USDC = "50"
_EMBEDDED_LATENCY_REGIONS = "eu"
_EMBEDDED_LATENCY_POLL_INTERVAL = "5"
_EMBEDDED_LATENCY_POLL_INTERVAL_ACTIVE = "2"
_EMBEDDED_LATENCY_DISCOVERY_TTL = "300"
_EMBEDDED_LATENCY_DISCOVERY_TTL_ACTIVE = "30"
_EMBEDDED_LATENCY_WINDOW_HOURS_BEFORE = "3"

# Gamma /events con ?sport= no filtra de forma fiable; usamos series_id del GET /sports nativo.
GAMMA_SPORTS_META_TTL_SEC = 3600.0

POLY_SLUG_TO_ODDS_KEY: dict[str, str] = {
    "wta": "tennis",
    "atp": "tennis",
    "wttmen": "table-tennis",
    "nba": "basketball_nba",
    "nhl": "icehockey_nhl",
    "mlb": "baseball_mlb",
    "ufc": "mma_mixed_martial_arts",
    "ucl": "soccer_uefa_champs_league",
    "uel": "soccer_uefa_europa_league",
    "epl": "soccer_epl",
    "nfl": "americanfootball_nfl",
}

# Claves que espera clients.odds_api_io.poly_odds_key_to_io_sport (valores POLY arriba = slugs IO).
_ODDS_IO_CLIENT_POLY_KEY: dict[str, str] = {
    "tennis": "tennis_wta",
    "table-tennis": "tabletennis_wtt",
}


def _client_poly_key_for_odds_io(odds_key: str) -> str:
    k = odds_key.strip()
    return _ODDS_IO_CLIENT_POLY_KEY.get(k, k)


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


def _yes_token_from_pairs(pairs: list[tuple[str, str]]) -> Optional[str]:
    for lab, tid in pairs:
        if str(lab).strip().lower() == "yes":
            return tid
    return pairs[0][1] if pairs else None


def _poly_event_to_open_game(ev: dict[str, Any], sport_slug: str) -> Optional[OpenPolymarketGame]:
    title = str(ev.get("title") or "")
    home_p, away_p = _parse_poly_title_teams(title)
    if not home_p or not away_p:
        return None
    end_dt = _parse_iso_utc(ev.get("endDate"))
    end_s = str(ev.get("endDate") or "").strip() or None
    slug_e = str(ev.get("slug") or "")[:240]
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
        return OpenPolymarketGame(
            sport_slug=sport_slug,
            home=home_p,
            away=away_p,
            condition_id=cid,
            token_yes=token_yes,
            end_date=end_dt,
            raw_title=title,
            slug=slug_e,
            outcome_tokens=list(pairs),
            end_date_s=end_s,
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
    blob = ((ev.get("title") or "") + " " + (ev.get("slug") or "")).strip()
    if len(blob) < 3:
        return False
    return odds_team_matches_gamma_blob(home_odds, blob) and odds_team_matches_gamma_blob(away_odds, blob)


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
            "game_slug",
            "league",
            "home_team",
            "away_team",
            "side",
            "token_id",
            "price_poly",
            "prob_pinnacle",
            "edge",
            "size_usdc",
            "status",
        ]

    def __init__(self, config: dict[str, Any], dry_run: bool = True) -> None:
        super().__init__(config, dry_run=dry_run)
        raw_slugs = os.getenv("LATENCY_SPORTS_POLY_SLUGS") or _EMBEDDED_LATENCY_SPORTS_POLY_SLUGS
        self.poly_slugs = [s.strip().lower() for s in str(raw_slugs).split(",") if s.strip()]
        self.min_edge = float(
            config.get("min_edge", os.getenv("LATENCY_SPORTS_MIN_EDGE") or _EMBEDDED_LATENCY_MIN_EDGE)
        )
        self.max_stake = float(
            config.get("max_stake_usdc", os.getenv("LATENCY_SPORTS_MAX_STAKE_USDC") or _EMBEDDED_LATENCY_MAX_STAKE_USDC)
        )
        self.regions = (os.getenv("LATENCY_SPORTS_REGIONS") or _EMBEDDED_LATENCY_REGIONS).strip()
        self.poll_interval = float(
            config.get("poll_interval", os.getenv("LATENCY_SPORTS_POLL_INTERVAL") or _EMBEDDED_LATENCY_POLL_INTERVAL)
        )
        self.poll_interval_active = float(
            os.getenv("LATENCY_SPORTS_POLL_INTERVAL_ACTIVE") or _EMBEDDED_LATENCY_POLL_INTERVAL_ACTIVE
        )
        ttl_raw = float(os.getenv("LATENCY_SPORTS_DISCOVERY_TTL") or _EMBEDDED_LATENCY_DISCOVERY_TTL)
        if ttl_raw < MIN_DISCOVERY_TTL_SEC:
            log.warning(
                "[latency_arb_sports] LATENCY_SPORTS_DISCOVERY_TTL clamped to %ss (was %s)",
                MIN_DISCOVERY_TTL_SEC,
                ttl_raw,
            )
        self.discovery_ttl = max(MIN_DISCOVERY_TTL_SEC, ttl_raw)
        self.discovery_ttl_active = float(
            os.getenv("LATENCY_SPORTS_DISCOVERY_TTL_ACTIVE") or _EMBEDDED_LATENCY_DISCOVERY_TTL_ACTIVE
        )
        self.window_hours_before = float(
            os.getenv("LATENCY_SPORTS_WINDOW_HOURS_BEFORE") or _EMBEDDED_LATENCY_WINDOW_HOURS_BEFORE
        )
        self._window_past_hours = 2.0
        self._breaker = config.get("circuit_breaker")
        self._start_capital = float(config.get("start_capital", os.getenv("ARB_START_CAPITAL", "10000")))
        self._current_capital = float(config.get("current_capital", os.getenv("ARB_CURRENT_CAPITAL", "10000")))
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

    def _get_odds_for_sport(self, sport_slug: str) -> list[OddsEvent]:
        poly_key = POLY_SLUG_TO_ODDS_KEY.get(str(sport_slug).strip().lower())
        if not poly_key:
            return []
        return self._odds_client.get_cached_odds(_client_poly_key_for_odds_io(poly_key))

    async def run_once(self) -> None:
        """Compat: no usado si run_loop está sobrescrito; mantener vacío mínimo."""
        return

    def _is_in_active_window(self, games: list[OpenPolymarketGame]) -> bool:
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

    async def run_loop(self, state_manager: Any) -> None:
        self._state_manager = state_manager
        self._shutdown.clear()
        self._ws_task = asyncio.create_task(self._ws_runner(), name="latency_arb_sports_ws")
        if self._odds_client.ws_enabled:
            raw_io_sports = os.getenv("ODDS_API_IO_SPORTS") or ODDS_API_IO_SPORTS_EMBEDDED
            io_sports = [s.strip() for s in str(raw_io_sports).split(",") if s.strip()]
            self._odds_client.start_ws_stream(sports=io_sports)
        try:
            while True:
                try:
                    enabled = await state_manager.is_enabled(self.slug)
                    if not enabled:
                        await asyncio.sleep(self.poll_interval)
                        continue
                    await self._poll_cycle(state_manager)
                except asyncio.CancelledError:
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

    async def _fetch_open_polymarket_sports(self, session: aiohttp.ClientSession) -> list[OpenPolymarketGame]:
        now = time.monotonic()
        ttl = self._discovery_fetch_ttl_sec()
        if self._open_games and now - self._open_games_cache_mono < ttl:
            return self._open_games

        await self._fetch_polymarket_sports_meta(session)
        games: list[OpenPolymarketGame] = []

        for poly_slug in self.poly_slugs:
            odds_key = POLY_SLUG_TO_ODDS_KEY.get(poly_slug)
            if odds_key is None:
                continue
            series_id = self._poly_series_by_slug.get(poly_slug)
            if not series_id:
                continue
            url = f"{GAMMA_API_URL}/events"
            params: dict[str, str | int] = {
                "series_id": int(series_id) if str(series_id).isdigit() else series_id,
                "active": "true",
                "closed": "false",
                "limit": 100,
            }
            try:
                async with session.get(url, params=params, headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=45)) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        log.warning("[latency_arb_sports] Gamma /events series=%s HTTP %s", series_id, resp.status)
                        continue
                    evs = json.loads(text)
            except (json.JSONDecodeError, aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
                log.warning("[latency_arb_sports] Gamma /events series=%s error: %s", series_id, e)
                continue
            if not isinstance(evs, list):
                continue
            for ev in evs:
                if not isinstance(ev, dict):
                    continue
                if not _gamma_event_usable(ev):
                    continue
                og = _poly_event_to_open_game(ev, poly_slug)
                if og is not None:
                    games.append(og)

        self._open_games = games
        self._open_games_cache_mono = now
        log.info("[latency_arb_sports] open games discovery: n=%s poly_slugs=%s", len(games), self.poly_slugs)
        return games

    async def _poll_cycle(self, state_manager: Any) -> None:
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
                    }
                )
                return

        async with aiohttp.ClientSession(
            headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=45)
        ) as sess, PolyCLOBClient(
            api_key=os.getenv("POLY_API_KEY", ""),
            private_key=os.getenv("POLY_PRIVATE_KEY", ""),
            dry_run=self.dry_run,
        ) as clob:
            self._cycle_seq += 1
            seq = self._cycle_seq
            open_games = await self._fetch_open_polymarket_sports(sess)
            odds_keys_loaded: set[str] = set()
            reference_matched = 0
            csv_rows = 0
            odds_events_by_key: dict[str, list[OddsEvent]] = {}

            for game in open_games:
                odds_key = POLY_SLUG_TO_ODDS_KEY.get(game.sport_slug)
                if odds_key is None:
                    continue
                if odds_key not in odds_events_by_key:
                    if not self._odds_client.ws_enabled:
                        await self._odds_client.refresh_rest_cache(sess, _client_poly_key_for_odds_io(odds_key))
                    odds_events_by_key[odds_key] = self._odds_client.get_cached_odds(
                        _client_poly_key_for_odds_io(odds_key)
                    )
                    odds_keys_loaded.add(odds_key)
                events_list = odds_events_by_key.get(odds_key) or []
                odds_ev = find_event_matching_teams(events_list, game.home, game.away)
                if odds_ev is None:
                    continue
                reference_matched += 1
                st = await self._process_matched_poly_odds(sess, clob, game, odds_key, odds_ev)
                csv_rows += st["csv_rows"]

            if (
                not self._ref_debug_done
                and seq == 10
                and len(open_games) > 0
                and len(odds_keys_loaded) > 0
            ):
                for game in open_games[:5]:
                    odds_key = POLY_SLUG_TO_ODDS_KEY.get(game.sport_slug, game.sport_slug)
                    odds_events = self._odds_client.get_cached_odds(odds_key)
                    preview = [f"{ev.home} vs {ev.away}" for ev in odds_events[:3]]
                    log.info(
                        "[REF_DEBUG] game='%s vs %s' sport=%s odds_io_events=%s",
                        game.home,
                        game.away,
                        game.sport_slug,
                        preview,
                    )
                self._ref_debug_done = True

            log.info(
                "[latency_arb_sports] cycle #%s regions=%s min_edge=%.4f max_stake=%.2f "
                "open_poly_games=%s odds_io_keys=%s reference_matched=%s csv_rows=%s "
                "discovery_ttl_eff=%.0fs active_window=%s dry_run=%s ws_cache_size=%s",
                seq,
                self.regions,
                self.min_edge,
                self.max_stake,
                len(open_games),
                len(odds_keys_loaded),
                reference_matched,
                csv_rows,
                self._discovery_fetch_ttl_sec(),
                self._is_in_active_window(open_games),
                self.dry_run,
                len(self._odds_client._ws_odds_cache),
            )

    async def _process_matched_poly_odds(
        self,
        _session: aiohttp.ClientSession,
        clob: PolyCLOBClient,
        game: OpenPolymarketGame,
        odds_sport_key: str,
        odds_event: OddsEvent,
    ) -> dict[str, int]:
        acc = {"csv_rows": 0}
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
        for side_label, token_id, p_fair in (
            ("YES", tok_h, p_h_fair),
            ("NO", tok_a, p_a_fair),
        ):
            mid = await clob.get_midpoint(token_id)
            if mid is None:
                ob = await clob.get_orderbook(token_id)
                bb, ba = ob.get("best_bid"), ob.get("best_ask")
                if bb is not None and ba is not None:
                    mid = (float(bb) + float(ba)) / 2.0
            if mid is None:
                continue
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
                float(p_fair) - float(mid),
                odds_event.updated_at,
                clob_read_at,
            )
            raw_edge = float(p_fair) - float(mid)
            if abs(raw_edge) < self.min_edge:
                continue
            edge_mag = abs(raw_edge)
            buy_side = "BUY"
            if raw_edge < 0:
                await self.log_signal_async(
                    {
                        "action": "SKIP:LOW_EDGE",
                        "reason": f"negative_edge_side={side_label}_raw={raw_edge:.4f}",
                        "game_slug": g.slug,
                        "league": g.league,
                        "home_team": odds_home,
                        "away_team": odds_away,
                        "side": side_label,
                        "token_id": token_id,
                        "price_poly": f"{mid:.6f}",
                        "prob_pinnacle": f"{p_fair:.6f}",
                        "edge": f"{edge_mag:.6f}",
                        "size_usdc": "0",
                        "status": "SKIP",
                    }
                )
                acc["csv_rows"] += 1
                continue
            size = float(self.max_stake)
            status = "SIGNAL"
            action = "SIGNAL"
            reason = f"{buy_side}_{side_label}_fair_minus_mid={raw_edge:.4f}"
            if not self.dry_run:
                try:
                    ba = (await clob.get_orderbook(token_id)).get("best_ask")
                    price = float(ba) if ba is not None else float(mid)
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
                except Exception as e:
                    action = "ERROR:ORDER_FAIL"
                    status = "SKIP"
                    reason = str(e)[:200]
            await self.log_signal_async(
                {
                    "action": action,
                    "reason": reason,
                    "game_slug": g.slug,
                    "league": g.league,
                    "home_team": odds_home,
                    "away_team": odds_away,
                    "side": side_label,
                    "token_id": token_id,
                    "price_poly": f"{mid:.6f}",
                    "prob_pinnacle": f"{p_fair:.6f}",
                    "edge": f"{edge_mag:.6f}",
                    "size_usdc": f"{size:.4f}",
                    "status": status,
                }
            )
            acc["csv_rows"] += 1
        return acc
