"""Cliente odds-api.io: WebSocket (push) o REST con TTL, caché en memoria."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import quote, urlencode

import aiohttp
from aiohttp import WSMsgType

from clients.odds_api import implied_prob as _implied_prob
from clients.odds_api import remove_vig as _remove_vig_probs
from clients.odds_api import teams_match_odds_gamma

log = logging.getLogger("odds_api_io")

# Valores embebidos (p. ej. Railway sin env). `os.getenv("ODDS_API_IO_*")` sigue pudiendo sobreescribir en local.
ODDS_API_IO_KEY_EMBEDDED = "647c2e8972c62f827b54f98b759982ea2b6d891be25b49c2a746a6f1ed4dd360"
ODDS_API_IO_WS_EMBEDDED = True
ODDS_API_IO_CACHE_TTL_EMBEDDED = 60
ODDS_API_IO_BOOKMAKERS_EMBEDDED = "Betfair Exchange,Sharp Exchange"
ODDS_API_IO_MARKETS_EMBEDDED = "ML"
ODDS_API_IO_SPORTS_EMBEDDED = "tennis,table-tennis"

BASE_REST = "https://api.odds-api.io/v3"
WS_BASE = "wss://api.odds-api.io/v3/ws"


def _ws_bookmakers_query_value(bookmakers_csv: str) -> str:
    """Query WS: espacio → '+' dentro de cada nombre; comas entre casas (p. ej. Betfair+Exchange,Sharp+Exchange)."""
    parts = [p.strip() for p in bookmakers_csv.split(",") if p.strip()]
    return ",".join(p.replace(" ", "+") for p in parts)

_DEFAULT_HEADERS = {
    "User-Agent": "predmarket-arb/odds-api-io (aiohttp; +https://github.com)",
    "Accept": "application/json",
}

# Claves Odds / POLY → slug deporte odds-api.io (REST + resolución get_cached_odds)
_POLY_ODDS_KEY_TO_IO_SPORT: dict[str, str] = {
    "tennis_wta": "tennis",
    "tennis_atp": "tennis",
    "tabletennis_wtt": "table-tennis",
    "basketball_nba": "basketball",
    "icehockey_nhl": "ice-hockey",
    "baseball_mlb": "baseball",
    "mma_mixed_martial_arts": "mixed-martial-arts",
    "soccer_uefa_champs_league": "football",
    "soccer_uefa_europa_league": "football",
    "soccer_epl": "football",
    "americanfootball_nfl": "american-football",
}

ODDS_KEY_TO_IO_SPORT: dict[str, str] = {
    "tennis_wta": "tennis",
    "tennis_atp": "tennis",
    "tennis": "tennis",
    "table-tennis": "table-tennis",
    "tabletennis_wtt": "table-tennis",
    "basketball_nba": "basketball",
    "soccer_epl": "football",
    "soccer_uefa_champs_league": "football",
}
for _odds_k, _io_slug in _POLY_ODDS_KEY_TO_IO_SPORT.items():
    ODDS_KEY_TO_IO_SPORT.setdefault(_odds_k, _io_slug)

_WS_BACKOFF_SEC = (2, 4, 8, 16, 30)


@dataclass
class OddsEvent:
    home: str
    away: str
    home_odds: float
    away_odds: float
    draw_odds: Optional[float]
    bookie: str
    updated_at: str
    event_id: str
    sport: str


def remove_vig(
    home_odds: float,
    away_odds: float,
    draw_odds: Optional[float] = None,
) -> tuple[float, float, Optional[float]]:
    """Cuotas decimales → fair probs (misma normalización que remove_vig sobre probs)."""
    ph = _implied_prob(home_odds)
    pa = _implied_prob(away_odds)
    pd: Optional[float] = _implied_prob(draw_odds) if draw_odds is not None and draw_odds > 0 else None
    return _remove_vig_probs(ph, pa, pd)


def normalize_name_order(name: str) -> str:
    """Convierte 'Mensik, Jakub' → 'jakub mensik' (apellido, nombre → nombre apellido)."""
    name = name.strip().lower()
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        if len(parts) == 2 and parts[0] and parts[1]:
            name = f"{parts[1]} {parts[0]}"
    return name


def find_event_matching_teams(
    events: list[OddsEvent],
    poly_home: str,
    poly_away: str,
) -> Optional[OddsEvent]:
    ph, pa = (poly_home or "").strip(), (poly_away or "").strip()
    if not ph or not pa:
        return None
    for ev in events:
        oh_raw, oa_raw = (ev.home or "").strip(), (ev.away or "").strip()
        if not oh_raw or not oa_raw:
            continue
        oh = normalize_name_order(oh_raw)
        oa = normalize_name_order(oa_raw)
        if (teams_match_odds_gamma(ph, oh) and teams_match_odds_gamma(pa, oa)) or (
            teams_match_odds_gamma(ph, oa) and teams_match_odds_gamma(pa, oh)
        ):
            return ev
    return None


def poly_odds_key_to_io_sport(poly_odds_key: str) -> Optional[str]:
    return ODDS_KEY_TO_IO_SPORT.get(poly_odds_key.strip())


def _parse_ml_odds_row(raw: dict[str, Any]) -> tuple[Optional[float], Optional[float], Optional[float]]:
    try:
        ho = float(raw.get("home")) if raw.get("home") not in (None, "") else None
    except (TypeError, ValueError):
        ho = None
    try:
        ao = float(raw.get("away")) if raw.get("away") not in (None, "") else None
    except (TypeError, ValueError):
        ao = None
    draw_o: Optional[float] = None
    if raw.get("draw") not in (None, ""):
        try:
            draw_o = float(raw.get("draw"))
        except (TypeError, ValueError):
            draw_o = None
    return ho, ao, draw_o


def _ml_first_row(markets: Any) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    if not isinstance(markets, list):
        return None, None
    for mk in markets:
        if not isinstance(mk, dict):
            continue
        if str(mk.get("name") or "").upper() != "ML":
            continue
        odds_list = mk.get("odds") or []
        if isinstance(odds_list, list) and odds_list and isinstance(odds_list[0], dict):
            return odds_list[0], str(mk.get("updatedAt") or mk.get("updated_at") or "")
    return None, None


def _bookie_matches_wanted(bk: str, wanted: frozenset[str]) -> bool:
    """Compara con los nombres configurados (identificadores oficiales /v3/bookmakers), case-insensitive."""
    if not wanted:
        return True
    bcf = bk.strip().casefold()
    return any(bcf == w.casefold() for w in wanted)


def _oddsevents_from_odds_payload(
    payload: dict[str, Any],
    sport_slug: str,
    wanted_bookmakers: frozenset[str],
) -> list[OddsEvent]:
    """Parsea cuerpo tipo GET /odds (un evento) a filas OddsEvent por bookie."""
    eid = str(payload.get("id") or "")
    home = str(payload.get("home") or "").strip()
    away = str(payload.get("away") or "").strip()
    out: list[OddsEvent] = []
    bks = payload.get("bookmakers")
    if isinstance(bks, dict):
        items = list(bks.items())
    elif isinstance(bks, list):
        items = []
        for b in bks:
            if not isinstance(b, dict):
                continue
            k = str(b.get("key") or b.get("name") or b.get("title") or "").strip()
            mk = b.get("markets")
            if k and isinstance(mk, list):
                items.append((k, mk))
    else:
        return out
    for bk_name, markets in items:
        bk = str(bk_name).strip()
        if not _bookie_matches_wanted(bk, wanted_bookmakers):
            continue
        mk_list = markets if isinstance(markets, list) else []
        row, upd_at = _ml_first_row(mk_list)
        if row is None:
            continue
        ho, ao, d_o = _parse_ml_odds_row(row)
        if ho is None or ao is None:
            continue
        out.append(
            OddsEvent(
                home=home,
                away=away,
                home_odds=ho,
                away_odds=ao,
                draw_odds=d_o,
                bookie=bk,
                updated_at=upd_at or "",
                event_id=eid,
                sport=sport_slug,
            )
        )
    return out


class OddsApiIo:
    def __init__(self) -> None:
        self.api_key = (os.getenv("ODDS_API_IO_KEY") or ODDS_API_IO_KEY_EMBEDDED).strip()
        _raw_ws = os.getenv("ODDS_API_IO_WS")
        self.ws_enabled = ODDS_API_IO_WS_EMBEDDED if _raw_ws is None else _raw_ws.strip().lower() == "true"
        self.cache_ttl = int(os.getenv("ODDS_API_IO_CACHE_TTL") or ODDS_API_IO_CACHE_TTL_EMBEDDED)
        raw_bk = os.getenv("ODDS_API_IO_BOOKMAKERS") or ODDS_API_IO_BOOKMAKERS_EMBEDDED
        self.bookmakers_csv = ",".join(b.strip() for b in raw_bk.split(",") if b.strip())
        self._wanted_bookmaker_names = frozenset(b.strip() for b in self.bookmakers_csv.split(",") if b.strip())
        self.markets_ws = (os.getenv("ODDS_API_IO_MARKETS") or ODDS_API_IO_MARKETS_EMBEDDED).strip() or "ML"
        self._lock = asyncio.Lock()
        self._ws_odds_cache: dict[str, dict[str, OddsEvent]] = {}
        self._rest_cache: dict[str, tuple[float, list[OddsEvent]]] = {}
        self._event_meta_cache: dict[str, dict[str, str]] = {}
        self._event_meta_inflight: set[str] = set()
        self._ws_runner_task: Optional[asyncio.Task[None]] = None
        self._ws_cancel = asyncio.Event()
        self._raw_logged = False

    def start_ws_stream(self, sports: list[str]) -> None:
        if not self.ws_enabled:
            return
        if self._ws_runner_task and not self._ws_runner_task.done():
            return
        self._ws_cancel.clear()
        clean = [s.strip() for s in sports if s.strip()]
        if not clean:
            clean = [s.strip() for s in ODDS_API_IO_SPORTS_EMBEDDED.split(",") if s.strip()]
        self._ws_runner_task = asyncio.create_task(self._ws_runner_loop(clean), name="odds_api_io_ws")

    async def stop_ws_stream(self) -> None:
        if not self._ws_runner_task:
            return
        self._ws_cancel.set()
        self._ws_runner_task.cancel()
        try:
            await self._ws_runner_task
        except asyncio.CancelledError:
            pass
        self._ws_runner_task = None

    def get_cached_odds(self, sport: str) -> list[OddsEvent]:
        """Solo lectura de caché; en REST la caché se rellena vía refresh_rest_cache."""
        k = sport.strip()
        sport_slug = ODDS_KEY_TO_IO_SPORT.get(k, k).strip().lower()
        if self.ws_enabled:
            return self._flatten_ws_cache_io_sport(sport_slug)
        now = time.monotonic()
        row = self._rest_cache.get(sport_slug)
        if not row:
            return []
        ts, events = row
        if now - ts > self.cache_ttl:
            return []
        return list(events)

    def _flatten_ws_cache_io_sport(self, sport_slug: str) -> list[OddsEvent]:
        sl = sport_slug.strip().casefold()
        acc: list[OddsEvent] = []
        for _eid, by_bk in self._ws_odds_cache.items():
            for ev in by_bk.values():
                if (ev.sport or "").strip().casefold() == sl:
                    acc.append(ev)
        acc.sort(key=lambda e: (self._bookie_priority(e.bookie), e.event_id))
        return acc

    def _bookie_priority(self, bookie: str) -> int:
        bcf = bookie.strip().casefold()
        order = [x.strip() for x in self.bookmakers_csv.split(",") if x.strip()]
        for i, o in enumerate(order):
            if bcf == o.casefold():
                return i
        return len(order)

    async def refresh_rest_cache(self, session: aiohttp.ClientSession, poly_sport: str) -> None:
        if self.ws_enabled:
            return
        io_sport = poly_odds_key_to_io_sport(poly_sport)
        if not io_sport:
            return
        now = time.monotonic()
        async with self._lock:
            row = self._rest_cache.get(io_sport)
            if row and now - row[0] < self.cache_ttl:
                return
        try:
            events_rows = await self._rest_fetch_sport(session, io_sport)
        except Exception as e:
            log.warning("[odds_api_io] REST fetch sport=%s: %s", io_sport, e)
            return
        async with self._lock:
            self._rest_cache[io_sport] = (time.monotonic(), events_rows)

    async def _rest_fetch_sport(self, session: aiohttp.ClientSession, io_sport: str) -> list[OddsEvent]:
        params = {"apiKey": self.api_key, "sport": io_sport}
        url = f"{BASE_REST}/events"
        async with session.get(
            url, params=params, headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=45)
        ) as resp:
            text = await resp.text()
            if resp.status != 200:
                log.warning("[odds_api_io] GET /events sport=%s HTTP %s: %s", io_sport, resp.status, text[:300])
                return []
            data = json.loads(text)
        evs = data if isinstance(data, list) else []
        ids: list[str] = []
        for ev in evs:
            if not isinstance(ev, dict):
                continue
            eid = ev.get("id")
            if eid is not None:
                ids.append(str(eid))
        if not ids:
            return []
        out: list[OddsEvent] = []
        for i in range(0, len(ids), 10):
            chunk = ids[i : i + 10]
            multi = await self._rest_odds_multi(session, chunk, io_sport)
            out.extend(multi)
        return out

    async def _rest_odds_multi(
        self, session: aiohttp.ClientSession, event_ids: list[str], sport_slug: str
    ) -> list[OddsEvent]:
        params = {
            "apiKey": self.api_key,
            "eventIds": ",".join(event_ids),
            "bookmakers": self.bookmakers_csv,
        }
        url = f"{BASE_REST}/odds/multi"
        async with session.get(
            url, params=params, headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=45)
        ) as resp:
            text = await resp.text()
            if resp.status != 200:
                log.warning("[odds_api_io] GET /odds/multi HTTP %s: %s", resp.status, text[:300])
                return []
            data = json.loads(text)
        rows: list[OddsEvent] = []
        if isinstance(data, list):
            arr = data
        elif isinstance(data, dict) and isinstance(data.get("data"), list):
            arr = data["data"]
        else:
            arr = []
        for item in arr:
            if isinstance(item, dict):
                rows.extend(_oddsevents_from_odds_payload(item, sport_slug, self._wanted_bookmaker_names))
        return rows

    async def _fetch_event_meta(self, session: aiohttp.ClientSession, event_id: str) -> dict[str, str]:
        out: dict[str, str] = {}
        if not event_id:
            return out
        try:
            async with self._lock:
                if event_id in self._event_meta_cache:
                    return dict(self._event_meta_cache[event_id])
            url = f"{BASE_REST}/events/{event_id}"
            params = {"apiKey": self.api_key}
            async with session.get(
                url, params=params, headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                text = await resp.text()
                if resp.status != 200:
                    log.debug("[odds_api_io] GET /events/%s HTTP %s", event_id, resp.status)
                    return out
                raw = json.loads(text)
            if not isinstance(raw, dict):
                return out
            home_raw = str(raw.get("home") or raw.get("home_team") or "").strip()
            away_raw = str(raw.get("away") or raw.get("away_team") or "").strip()
            home = normalize_name_order(home_raw) if home_raw else ""
            away = normalize_name_order(away_raw) if away_raw else ""
            sp = raw.get("sport")
            if isinstance(sp, dict):
                sport_slug = str(sp.get("slug") or "").strip().lower()
            else:
                sport_slug = str(sp or raw.get("sport_slug") or "").strip().lower()
            out = {"home": home, "away": away, "sport": sport_slug}
            async with self._lock:
                self._event_meta_cache[event_id] = out
            return dict(out)
        except Exception as e:
            log.warning("[odds_api_io] event meta id=%s: %s", event_id, e)
            return out
        finally:
            async with self._lock:
                self._event_meta_inflight.discard(event_id)

    async def _ws_runner_loop(self, sports: list[str]) -> None:
        attempt = 0
        backoff_i = 0
        sport_param = ",".join(sports)
        while not self._ws_cancel.is_set():
            try:
                await self._ws_once(sport_param)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                attempt += 1
                delay = _WS_BACKOFF_SEC[min(backoff_i, len(_WS_BACKOFF_SEC) - 1)]
                if backoff_i >= len(_WS_BACKOFF_SEC) - 1:
                    delay = 30
                else:
                    backoff_i += 1
                log.warning("[odds_api_io] WS_RECONNECT attempt=%s delay=%ss err=%s", attempt, delay, e)
                try:
                    await asyncio.wait_for(self._ws_cancel.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                continue
            attempt = 0
            backoff_i = 0
            if self._ws_cancel.is_set():
                break
            await asyncio.sleep(2.0)

    async def _ws_once(self, sport_param: str) -> None:
        q = {
            "apiKey": self.api_key,
            "markets": self.markets_ws,
            "sport": sport_param,
            "status": "live",
        }
        bm_ws = _ws_bookmakers_query_value(self.bookmakers_csv)
        url = f"{WS_BASE}?{urlencode(q)}&bookmakers={quote(bm_ws, safe=',+')}"
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=None)
        async with aiohttp.ClientSession(headers=_DEFAULT_HEADERS, timeout=timeout) as session:
            log.info("[odds_api_io] WS connecting sport=%s", sport_param)
            async with session.ws_connect(url, heartbeat=30.0) as ws:
                ws_first_msg_types: set[str] = set()
                welcome_ok = False
                while not self._ws_cancel.is_set():
                    msg = await ws.receive()
                    if msg.type == WSMsgType.CLOSE or msg.type == WSMsgType.CLOSED:
                        break
                    if msg.type == WSMsgType.ERROR:
                        exc = ws.exception()
                        if exc:
                            raise exc
                        break
                    if msg.type != WSMsgType.TEXT:
                        continue
                    try:
                        data = json.loads(msg.data)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(data, dict):
                        continue
                    typ = str(data.get("type") or "").lower()
                    if typ and typ not in ws_first_msg_types:
                        ws_first_msg_types.add(typ)
                        log.info(
                            "[odds_api_io] WS_FIRST_MSG type=%s bookie=%s event_id=%s home=%s away=%s",
                            typ,
                            str(data.get("bookie") or "").strip(),
                            str(data.get("id") or "").strip(),
                            str(data.get("home") or "").strip(),
                            str(data.get("away") or "").strip(),
                        )
                    if typ == "welcome":
                        welcome_ok = True
                        bks = data.get("bookmakers") or []
                        bk_list = list(bks) if isinstance(bks, list) else []
                        log.info("[odds_api_io] WS_CONNECTED bookmakers=%s", bk_list)
                        continue
                    if not welcome_ok:
                        continue
                    if typ in ("created", "updated") and not self._raw_logged:
                        self._raw_logged = True
                        log.info("[odds_api_io] RAW_MSG %s", json.dumps(data)[:500])
                    if typ in ("created", "updated"):
                        eid_w = str(data.get("id") or "").strip()
                        if eid_w:
                            start_meta = False
                            async with self._lock:
                                if eid_w not in self._event_meta_cache and eid_w not in self._event_meta_inflight:
                                    self._event_meta_inflight.add(eid_w)
                                    start_meta = True
                            if start_meta:
                                asyncio.create_task(self._fetch_event_meta(session, eid_w))
                        await self._apply_ws_created_updated(session, data, sport_param, typ)
                    elif typ == "deleted":
                        await self._apply_ws_deleted(data)
                    elif typ == "no_markets":
                        log.debug("[odds_api_io] no_markets id=%s", data.get("id"))

    async def _apply_ws_deleted(self, data: dict[str, Any]) -> None:
        eid = str(data.get("id") or "")
        if not eid:
            return
        bookie = str(data.get("bookie") or "").strip()
        async with self._lock:
            if bookie:
                if eid in self._ws_odds_cache and bookie in self._ws_odds_cache[eid]:
                    del self._ws_odds_cache[eid][bookie]
                if eid in self._ws_odds_cache and not self._ws_odds_cache[eid]:
                    del self._ws_odds_cache[eid]
            else:
                self._ws_odds_cache.pop(eid, None)

    async def _apply_ws_created_updated(
        self, session: aiohttp.ClientSession, data: dict[str, Any], sport_param: str, message_type: str
    ) -> None:
        eid = str(data.get("id") or "")
        bookie = str(data.get("bookie") or "").strip()
        if not eid or not bookie:
            return
        if self._wanted_bookmaker_names and not _bookie_matches_wanted(bookie, self._wanted_bookmaker_names):
            return
        home = str(data.get("home") or "").strip()
        away = str(data.get("away") or "").strip()
        sport_guess = str(data.get("sport") or "").strip().lower()
        meta: dict[str, str] = {}
        async with self._lock:
            if eid in self._event_meta_cache:
                meta = dict(self._event_meta_cache[eid])
        if not home:
            home = str(meta.get("home") or "").strip()
        if not away:
            away = str(meta.get("away") or "").strip()
        if not sport_guess:
            sport_guess = str(meta.get("sport") or "").strip().lower()
        row, upd_at = _ml_first_row(data.get("markets"))
        if row is None:
            return
        ho, ao, d_o = _parse_ml_odds_row(row)
        if ho is None or ao is None:
            return
        if message_type == "updated":
            log.info(
                "[odds_api_io] TICK event_id=%s bookie=%s\n   home_odds=%s away_odds=%s\n   received_at=%s",
                eid,
                bookie,
                ho,
                ao,
                datetime.utcnow().isoformat(),
            )
        if home:
            home = normalize_name_order(home)
        if away:
            away = normalize_name_order(away)
        ev = OddsEvent(
            home=home,
            away=away,
            home_odds=ho,
            away_odds=ao,
            draw_odds=d_o,
            bookie=bookie,
            updated_at=upd_at or "",
            event_id=eid,
            sport=sport_guess or "",
        )
        async with self._lock:
            self._ws_odds_cache.setdefault(eid, {})[bookie] = ev
