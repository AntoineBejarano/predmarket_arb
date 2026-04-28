"""Latency arb sports: Pinnacle (The Odds API) vs precios CLOB Polymarket en mercados deportivos."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
from aiohttp import WSMsgType

from arb.base import ArbStrategy
from clients.odds_api import (
    get_odds,
    implied_prob,
    odds_api_key,
    odds_team_matches_gamma_blob,
    remove_vig,
    teams_match_odds_gamma,
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


def _odds_event_cache_key(event: dict[str, Any]) -> str:
    oid = str(event.get("id") or "").strip()
    if oid:
        return oid
    h, a = str(event.get("home_team") or ""), str(event.get("away_team") or "")
    return f"{h}|{a}|{event.get('commence_time') or ''}"


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
    # normalize_team_for_match + segmentos vs dentro de odds_team_matches_gamma_blob
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


def _extract_pinnacle_two_way(event: dict[str, Any]) -> Optional[tuple[str, float, str, float]]:
    """(home_name, dec_home, away_name, dec_away) desde mercado h2h Pinnacle."""
    home_team = str(event.get("home_team") or "")
    away_team = str(event.get("away_team") or "")
    for bk in event.get("bookmakers") or []:
        if str(bk.get("key") or "") != "pinnacle":
            continue
        for mk in bk.get("markets") or []:
            if str(mk.get("key") or "") != "h2h":
                continue
            outs = mk.get("outcomes") or []
            by_name: dict[str, float] = {}
            for o in outs:
                if not isinstance(o, dict):
                    continue
                nm = str(o.get("name") or "").strip()
                try:
                    px = float(o.get("price"))
                except (TypeError, ValueError):
                    continue
                if nm:
                    by_name[nm] = px
            # Emparejar nombres Odds API a outcomes por igualdad o substring
            def pick_price(team_full: str) -> Optional[float]:
                if team_full in by_name:
                    return by_name[team_full]
                tl = team_full.lower()
                best_k, best_px = None, None
                for k, v in by_name.items():
                    if tl in k.lower() or k.lower() in tl:
                        best_k, best_px = k, v
                return best_px

            ph, pa = pick_price(home_team), pick_price(away_team)
            if ph is not None and pa is not None:
                return home_team, ph, away_team, pa
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
        raw_sports = os.getenv("LATENCY_SPORTS_SPORTS", "soccer_epl,basketball_nba,americanfootball_nfl")
        self.sports_keys = [s.strip() for s in str(raw_sports).split(",") if s.strip()]
        self.min_edge = float(config.get("min_edge", os.getenv("LATENCY_SPORTS_MIN_EDGE", "0.03")))
        self.max_stake = float(config.get("max_stake_usdc", os.getenv("LATENCY_SPORTS_MAX_STAKE_USDC", "50")))
        self.regions = os.getenv("LATENCY_SPORTS_REGIONS", "eu").strip()
        self.poll_interval = float(config.get("poll_interval", os.getenv("LATENCY_SPORTS_POLL_INTERVAL", "5")))
        ttl_raw = float(os.getenv("LATENCY_SPORTS_DISCOVERY_TTL", "300"))
        if ttl_raw < MIN_DISCOVERY_TTL_SEC:
            log.warning(
                "[latency_arb_sports] LATENCY_SPORTS_DISCOVERY_TTL clamped to %ss (was %s)",
                MIN_DISCOVERY_TTL_SEC,
                ttl_raw,
            )
        self.discovery_ttl = max(MIN_DISCOVERY_TTL_SEC, ttl_raw)
        self.gamma_public_search_limit = int(os.getenv("LATENCY_SPORTS_GAMMA_PUBLIC_SEARCH_LIMIT", "40"))
        self._breaker = config.get("circuit_breaker")
        self._start_capital = float(config.get("start_capital", os.getenv("ARB_START_CAPITAL", "10000")))
        self._current_capital = float(config.get("current_capital", os.getenv("ARB_CURRENT_CAPITAL", "10000")))
        # cache_key_odds_event -> (monotonic_ts, GammaSportMarket|None); TTL por partido
        self._gamma_discovery_cache: dict[str, tuple[float, Optional[GammaSportMarket]]] = {}
        self._ws_cache: dict[str, dict[str, Any]] = {}
        self._ws_task: Optional[asyncio.Task[None]] = None
        self._shutdown = asyncio.Event()
        self._cycle_seq = 0

    async def run_once(self) -> None:
        """Compat: no usado si run_loop está sobrescrito; mantener vacío mínimo."""
        return

    async def run_loop(self, state_manager: Any) -> None:
        self._state_manager = state_manager
        self._shutdown.clear()
        self._ws_task = asyncio.create_task(self._ws_runner(), name="latency_arb_sports_ws")
        try:
            interval = self.poll_interval
            while True:
                try:
                    enabled = await state_manager.is_enabled(self.slug)
                    if not enabled:
                        await asyncio.sleep(interval)
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
                await asyncio.sleep(interval)
        finally:
            self._shutdown.set()
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

    def _prune_gamma_discovery_cache(self, now_mono: float) -> None:
        """Evita crecimiento indefinido del mapa por partidos viejos."""
        cutoff = self.discovery_ttl * 2
        stale = [k for k, (ts, _) in self._gamma_discovery_cache.items() if now_mono - ts > cutoff]
        for k in stale:
            del self._gamma_discovery_cache[k]

    async def _discover_gamma_row_for_odds_event(
        self,
        session: aiohttp.ClientSession,
        sport_key: str,
        event: dict[str, Any],
    ) -> tuple[Optional[GammaSportMarket], Optional[bool]]:
        """
        Gamma vía GET public-search?q=home+away, filtrado por equipos en title/slug.
        Segundo valor: True=HTTP a Gamma, False=acierto de caché TTL, None=sin lookup (métricas).
        """
        odds_commence = _parse_iso_utc(event.get("commence_time"))
        if odds_commence is None:
            return None, None

        key = _odds_event_cache_key(event)
        now_mono = time.monotonic()
        hit = self._gamma_discovery_cache.get(key)
        if hit is not None:
            ts, row = hit
            if now_mono - ts < self.discovery_ttl:
                return row, False

        home = str(event.get("home_team") or "").strip()
        away = str(event.get("away_team") or "").strip()
        q = f"{home} {away}".strip()
        if not q:
            return None, None

        url = f"{GAMMA_API_URL}/public-search"
        params = {"q": q, "limit": str(max(5, self.gamma_public_search_limit))}
        try:
            async with session.get(url, params=params, headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=25)) as resp:
                text = await resp.text()
                if resp.status != 200:
                    log.warning("[latency_arb_sports] public-search HTTP %s q=%r", resp.status, q[:100])
                    self._gamma_discovery_cache[key] = (now_mono, None)
                    return None, True
                data = json.loads(text)
        except (json.JSONDecodeError, aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("[latency_arb_sports] public-search error q=%r: %s", q[:80], e)
            self._gamma_discovery_cache[key] = (now_mono, None)
            return None, True

        evs = data.get("events") if isinstance(data, dict) else None
        if not isinstance(evs, list):
            self._gamma_discovery_cache[key] = (now_mono, None)
            return None, True

        best: Optional[GammaSportMarket] = None
        best_key: Optional[tuple[float, str]] = None
        for ev_gamma in evs:
            if not isinstance(ev_gamma, dict):
                continue
            if not _gamma_event_usable(ev_gamma):
                continue
            if not _event_matches_odds_teams(ev_gamma, home, away):
                continue
            row = _pick_best_market_in_event(ev_gamma, sport_key, odds_commence)
            if row is None:
                continue
            cg = row.commence_for_odds(odds_commence)
            if cg is None:
                continue
            d = abs((cg - odds_commence).total_seconds())
            if "more-markets" in row.slug.lower():
                d += 1e6
            rk = (d, row.condition_id)
            if best is None or rk < (best_key or (1e18, "")):
                best = row
                best_key = rk

        self._gamma_discovery_cache[key] = (now_mono, best)
        if best is None:
            log.debug("[latency_arb_sports] public-search sin match Gamma para q=%r (n=%s eventos)", q, len(evs))
        return best, True

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
            now_mono = time.monotonic()
            self._prune_gamma_discovery_cache(now_mono)
            odds_counts: dict[str, int] = {}
            pinnacle_events = 0
            gamma_matched = 0
            csv_rows = 0
            public_search_http = 0
            public_search_cache = 0
            for sport_key in self.sports_keys:
                try:
                    events = await get_odds(
                        sess,
                        sport_key,
                        regions=self.regions,
                        markets="h2h",
                        bookmakers="pinnacle",
                        api_key=odds_api_key(),
                    )
                except Exception as e:
                    log.warning("[latency_arb_sports] odds fetch %s: %s", sport_key, e)
                    odds_counts[sport_key] = -1
                    continue
                odds_counts[sport_key] = len(events)
                for ev in events:
                    st = await self._process_event(sess, clob, sport_key, ev)
                    pinnacle_events += st["pinnacle"]
                    gamma_matched += st["gamma_match"]
                    csv_rows += st["csv_rows"]
                    public_search_http += st.get("gamma_http", 0)
                    public_search_cache += st.get("gamma_cache", 0)
            log.info(
                "[latency_arb_sports] cycle #%s regions=%s min_edge=%.4f max_stake=%.2f "
                "gamma_cache_entries=%s public_search_http=%s public_search_cache_hit=%s "
                "ws_slugs_cached=%s odds_events=%s pinnacle_h2h=%s gamma_matched_events=%s csv_rows=%s dry_run=%s",
                seq,
                self.regions,
                self.min_edge,
                self.max_stake,
                len(self._gamma_discovery_cache),
                public_search_http,
                public_search_cache,
                len(self._ws_cache),
                odds_counts,
                pinnacle_events,
                gamma_matched,
                csv_rows,
                self.dry_run,
            )

    async def _process_event(
        self,
        session: aiohttp.ClientSession,
        clob: PolyCLOBClient,
        sport_key: str,
        event: dict[str, Any],
    ) -> dict[str, int]:
        acc = {"pinnacle": 0, "gamma_match": 0, "csv_rows": 0, "gamma_http": 0, "gamma_cache": 0}
        tw = _extract_pinnacle_two_way(event)
        if tw is None:
            return acc
        acc["pinnacle"] = 1
        odds_home, dec_h, odds_away, dec_a = tw
        p_h_raw = implied_prob(dec_h)
        p_a_raw = implied_prob(dec_a)
        p_h_fair, p_a_fair, _ = remove_vig(p_h_raw, p_a_raw, None)
        g, gamma_src = await self._discover_gamma_row_for_odds_event(session, sport_key, event)
        if gamma_src is True:
            acc["gamma_http"] += 1
        elif gamma_src is False:
            acc["gamma_cache"] += 1
        if g is None:
            return acc
        acc["gamma_match"] = 1
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
            raw_edge = float(p_fair) - float(mid)
            if abs(raw_edge) < self.min_edge:
                continue
            edge_mag = abs(raw_edge)
            buy_side = "BUY"
            if raw_edge < 0:
                # Poly caro vs fair: no comprar YES/NO en sentido agresivo sin lógica short; skip
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
