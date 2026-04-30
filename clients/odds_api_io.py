"""Cliente odds-api.io: WebSocket (push) o REST con TTL, caché en memoria."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from datetime import datetime
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import quote, urlencode

import aiohttp
from aiohttp import WSMsgType
from typing import Awaitable, Callable

from clients.odds_api import implied_prob as _implied_prob
from clients.odds_api import levenshtein
from clients.odds_api import normalize_team_for_match
from clients.odds_api import remove_vig as _remove_vig_probs

log = logging.getLogger("odds_api_io")

# Tipo de callback de tick Betfair: motor latency_arb_sports lo conecta para reaccionar al instante.
BetfairTickCallback = Callable[["OddsEvent"], Awaitable[None]]

# Valores embebidos (p. ej. Railway sin env). `os.getenv("ODDS_API_IO_*")` sigue pudiendo sobreescribir en local.
ODDS_API_IO_KEY_EMBEDDED = "647c2e8972c62f827b54f98b759982ea2b6d891be25b49c2a746a6f1ed4dd360"
ODDS_API_IO_WS_EMBEDDED = True
ODDS_API_IO_CACHE_TTL_EMBEDDED = 900
# Default hardcodeado para reducir ruido WS sin metadata durante validación: solo Betfair.
ODDS_API_IO_BOOKMAKERS_EMBEDDED = "Betfair Exchange"
ODDS_API_IO_MARKETS_EMBEDDED = "ML"
# Deportes WS: slugs oficiales GET https://api.odds-api.io/v3/sports (p. ej. basketball, football).
ODDS_API_IO_SPORTS_EMBEDDED = "tennis,table-tennis,basketball,football"

BASE_REST = "https://api.odds-api.io/v3"
WS_BASE = "wss://api.odds-api.io/v3/ws"


class OddsApiIoRestQuotaError(RuntimeError):
    """REST odds-api.io devolvió 401/403/429 (u otro fatal acordado); el motor debe parar para no seguir gastando requests."""


def _raise_if_rest_quota_error(status: int, body: str, where: str) -> None:
    """Plan gratuito: ~100 REST/h; el WS es ilimitado. Sin contador: si la API responde error de cuota/auth, abortar."""
    if status not in (401, 403, 429):
        return
    snippet = (body or "").replace("\n", " ")[:400]
    log.error(
        "[odds_api_io] REST odds-api.io HTTP %s %s — deteniendo uso de REST (evitar quemar cuota). Respuesta: %s",
        status,
        where,
        snippet,
    )
    raise OddsApiIoRestQuotaError(f"odds-api.io HTTP {status} {where}: {(body or '')[:200]}")


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

# Claves y valores en minúscula para lookup estable (REST/WS/caché).
ODDS_KEY_TO_IO_SPORT = {
    str(k).strip().lower(): str(v).strip().lower() for k, v in ODDS_KEY_TO_IO_SPORT.items()
}

_WS_BACKOFF_SEC = (2, 4, 8, 16, 30)
# Tope de eventos por deporte en REST (evita decenas de GET /odds/multi por ciclo en tenis/fútbol).
_ODDS_IO_REST_MAX_EVENT_IDS = 100
# Per-sport bulk backoff: 300s default; AUTH errors cubren todos los deportes 1h.
_REST_BULK_BACKOFF_SEC = 300.0
_REST_BULK_BACKOFF_MAX_SEC = 1800.0
_REST_AUTH_BACKOFF_SEC = 3600.0
# Token bucket REST global: máximo 80 reqs/h (margen sobre 100/h del plan gratis).
_REST_QUOTA_PER_HOUR = 80
_REST_QUOTA_WINDOW_SEC = 3600.0
# Watchdog WS: si tras welcome no llega ningún mensaje durante > _WS_IDLE_RESET_SEC, forzar reconexión.
_WS_IDLE_RESET_SEC = 90.0
# Cadencia del watchdog (poll interno).
_WS_WATCHDOG_POLL_SEC = 30.0
# Mínimo entre intentos bulk REST por sport: evita martillear /events cada poll cycle (5s).
_REST_PER_SPORT_MIN_INTERVAL_SEC = 30.0
# Si /events sport=X devuelve lista vacía, no insistir durante esta ventana.
_REST_EMPTY_SPORT_BACKOFF_SEC = 600.0


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


_TEAM_IO_SIMILARITY_MIN = 0.6


def _coerce_io_sport_slug(sport_val: Any) -> str:
    """WS/REST: slug IO (p. ej. tennis) desde string o dict tipo {slug: tennis}."""
    if sport_val is None:
        return ""
    if isinstance(sport_val, dict):
        return str(sport_val.get("slug") or sport_val.get("sport_slug") or "").strip().lower()
    return str(sport_val).strip().lower()


def _levenshtein_similarity_ratio(a: str, b: str) -> float:
    """1.0 = iguales; 0.0 = muy distintos (dist / longitud máxima)."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    d = float(levenshtein(a, b))
    mx = max(len(a), len(b))
    return 1.0 - (d / mx) if mx else 0.0


def _team_similar_for_io_pair(poly_side: str, odds_side: str) -> bool:
    """
    Poly vs odds-api.io: compara cadenas ya en minúscula y sin prefijos/sufijos de club
    (normalize_team_for_match) tras orden nombre/apellido (normalize_name_order).
    Requiere similitud Levenshtein ≥ 60% salvo igualdad exacta.
    """
    a = normalize_team_for_match(normalize_name_order(poly_side))
    b = normalize_team_for_match(normalize_name_order(odds_side))
    if not a or not b:
        return False
    if a == b:
        return True
    return _levenshtein_similarity_ratio(a, b) >= _TEAM_IO_SIMILARITY_MIN


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
        if (_team_similar_for_io_pair(ph, oh) and _team_similar_for_io_pair(pa, oa)) or (
            _team_similar_for_io_pair(ph, oa) and _team_similar_for_io_pair(pa, oh)
        ):
            return ev
    return None


def poly_odds_key_to_io_sport(poly_odds_key: str) -> Optional[str]:
    k = poly_odds_key.strip().lower()
    return ODDS_KEY_TO_IO_SPORT.get(k)


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
        # Hardcode estricto por decisión operativa: no leer ODDS_API_IO_BOOKMAKERS del entorno.
        self.bookmakers_csv = ",".join(
            b.strip() for b in ODDS_API_IO_BOOKMAKERS_EMBEDDED.split(",") if b.strip()
        )
        self._wanted_bookmaker_names = frozenset(b.strip() for b in self.bookmakers_csv.split(",") if b.strip())
        self.markets_ws = (os.getenv("ODDS_API_IO_MARKETS") or ODDS_API_IO_MARKETS_EMBEDDED).strip() or "ML"
        self._lock = asyncio.Lock()
        self._ws_odds_cache: dict[str, dict[str, OddsEvent]] = {}
        self._rest_cache: dict[str, tuple[float, list[OddsEvent]]] = {}
        self._ws_runner_task: Optional[asyncio.Task[None]] = None
        self._ws_watchdog_task: Optional[asyncio.Task[None]] = None
        self._ws_cancel = asyncio.Event()
        # Una conexión activa por sport suscrito.
        self._ws_current_by_sport: dict[str, aiohttp.ClientWebSocketResponse] = {}
        self._ws_tasks_by_sport: dict[str, asyncio.Task[None]] = {}
        self._raw_logged = False
        # Backoff bulk REST: /events + /odds/multi (per-sport para que un 429 en tenis no apague basket)
        self._rest_bulk_backoff_until_per_sport: dict[str, float] = {}
        # Backoff global (auth 401/403): cubre TODOS los deportes hasta vencer.
        self._rest_bulk_backoff_global_until: float = 0.0
        # Token bucket REST global: timestamps monotonic de cada request HTTP REST.
        self._rest_quota_ts: deque[float] = deque(maxlen=_REST_QUOTA_PER_HOUR * 4)
        # Salud WS: edad del último mensaje (cualquier tipo) y del último tick válido (created/updated parseado).
        self._ws_last_msg_mono: float = 0.0
        self._ws_last_tick_mono: float = 0.0
        # Filas WS descartadas por bookie != Betfair (debería ser 0 con bookmakers hardcodeado).
        self._ws_dropped_other_bookie: int = 0
        # Contadores WS para diagnóstico: total de mensajes TEXT y desglose por tipo declarado.
        self._ws_msgs_text_total: int = 0
        self._ws_msgs_by_type: dict[str, int] = {}
        # Callback de tick Betfair (motor latency_arb_sports). Se invoca desde _apply_ws_created_updated.
        self._betfair_tick_callback: Optional[BetfairTickCallback] = None
        # Bootstrap REST: marca True tras preload OK del primer sport con eventos próximos.
        self._bootstrapped: bool = False
        self._bootstrap_task: Optional[asyncio.Task[None]] = None
        # Gating per-sport: monotonic del último intento bulk REST (eventos / odds-multi).
        self._rest_last_attempt_per_sport: dict[str, float] = {}
        # Sports que devolvieron lista vacía: marcados aquí para no insistir en /odds/multi sin meta-cache.
        self._rest_empty_sport_until: dict[str, float] = {}

    def set_betfair_tick_callback(self, cb: Optional[BetfairTickCallback]) -> None:
        """latency_arb_sports lo conecta para reaccionar al instante a cada tick Betfair válido."""
        self._betfair_tick_callback = cb

    def start_ws_stream(self, sports: list[str]) -> None:
        if not self.ws_enabled:
            return
        if self._ws_runner_task and not self._ws_runner_task.done():
            return
        self._ws_cancel.clear()
        clean = [s.strip() for s in sports if s.strip()]
        if not clean:
            clean = [s.strip() for s in ODDS_API_IO_SPORTS_EMBEDDED.split(",") if s.strip()]
        clean = list(dict.fromkeys(clean))
        self._ws_runner_task = asyncio.create_task(self._ws_runner_loop(clean), name="odds_api_io_ws")
        if self._ws_watchdog_task is None or self._ws_watchdog_task.done():
            self._ws_watchdog_task = asyncio.create_task(self._ws_watchdog_loop(), name="odds_api_io_ws_watchdog")
        # Preload bootstrap: primer sport con eventos próximos para poblar cache REST.
        if (self._bootstrap_task is None or self._bootstrap_task.done()) and not self._bootstrapped:
            self._bootstrap_task = asyncio.create_task(
                self._bootstrap_preload(clean), name="odds_api_io_bootstrap_preload"
            )

    def is_bootstrapped(self) -> bool:
        return self._bootstrapped

    async def _bootstrap_preload(self, sports: list[str]) -> None:
        """
        Gasta 1 request bulk en el sport con más eventos próximos (tennis primero) para
        poblar cache REST de respaldo antes del primer ciclo de matching.
        """
        try:
            await asyncio.sleep(0.5)
            if self._bootstrapped:
                return
            preferred = ["tennis", "table-tennis", "basketball", "football"]
            ordered = [s for s in preferred if s in sports] + [s for s in sports if s not in preferred]
            timeout = aiohttp.ClientTimeout(total=45)
            async with aiohttp.ClientSession(headers=_DEFAULT_HEADERS, timeout=timeout) as sess:
                for io_sport in ordered:
                    if self._ws_cancel.is_set():
                        return
                    if self._bulk_backoff_blocked(io_sport):
                        continue
                    if not self._rest_quota_can_consume():
                        log.info(
                            "[odds_api_io] BOOTSTRAP: token bucket %s/%s lleno, salto sport=%s.",
                            len(self._rest_quota_ts),
                            _REST_QUOTA_PER_HOUR,
                            io_sport,
                        )
                        return
                    try:
                        events_rows = await self._rest_fetch_sport(sess, io_sport)
                    except OddsApiIoRestQuotaError as e:
                        log.warning(
                            "[odds_api_io] BOOTSTRAP sport=%s 429 / cuota: per-sport backoff %.0fs. %s",
                            io_sport,
                            _REST_BULK_BACKOFF_SEC,
                            e,
                        )
                        self._set_bulk_backoff_for_sport(io_sport, _REST_BULK_BACKOFF_SEC)
                        continue
                    except Exception as e:
                        log.warning("[odds_api_io] BOOTSTRAP sport=%s error: %s", io_sport, e)
                        continue
                    if events_rows:
                        async with self._lock:
                            self._rest_cache[io_sport] = (time.monotonic(), list(events_rows))
                        self._bootstrapped = True
                        log.info(
                            "[odds_api_io] BOOTSTRAP OK sport=%s events=%s bootstrapped=True",
                            io_sport,
                            len(events_rows),
                        )
                        return
            log.info("[odds_api_io] BOOTSTRAP: ningún sport devolvió eventos; reintentos vía refresh_rest_cache.")
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.warning("[odds_api_io] BOOTSTRAP error inesperado: %s", e)

    async def stop_ws_stream(self) -> None:
        self._ws_cancel.set()
        for t in (self._ws_runner_task, self._ws_watchdog_task, self._bootstrap_task, *self._ws_tasks_by_sport.values()):
            if t is None:
                continue
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._ws_runner_task = None
        self._ws_watchdog_task = None
        self._bootstrap_task = None
        self._ws_tasks_by_sport = {}

    def ws_age_last_msg_sec(self) -> Optional[float]:
        """Edad (s) del último mensaje WS recibido (tipo cualquiera). None si aún no hay conexión."""
        if self._ws_last_msg_mono == 0.0:
            return None
        return max(0.0, time.monotonic() - self._ws_last_msg_mono)

    def ws_age_last_tick_sec(self) -> Optional[float]:
        """Edad (s) del último tick Betfair válido parseado (created/updated con cuotas)."""
        if self._ws_last_tick_mono == 0.0:
            return None
        return max(0.0, time.monotonic() - self._ws_last_tick_mono)

    def bulk_backoff_left_sec(self, io_sport: Optional[str] = None) -> float:
        """Tiempo restante (s) del backoff bulk REST: max(global_auth, per_sport)."""
        now = time.monotonic()
        glob = max(0.0, float(self._rest_bulk_backoff_global_until - now))
        if io_sport is None:
            mx_sport = 0.0
            for v in self._rest_bulk_backoff_until_per_sport.values():
                left = float(v) - now
                if left > mx_sport:
                    mx_sport = left
            return max(glob, max(0.0, mx_sport))
        sport_until = float(self._rest_bulk_backoff_until_per_sport.get(io_sport, 0.0))
        return max(glob, max(0.0, sport_until - now))

    def meta_backoff_left_sec(self) -> float:
        return 0.0

    def capture_state(self) -> str:
        """
        Estado canónico del feed Betfair (plan §2/§6):
          - WS_LIVE_OK: WS conectado, último msg <60s y al menos 1 tick válido <180s.
          - WS_LIVE_NO_TICKS: WS recibe heartbeats / mensajes pero sin ticks válidos en >180s.
          - WS_RECONNECTING: tarea WS viva pero sin conexión activa (entre intentos de _ws_runner_loop).
          - WS_DEAD: tarea WS terminada o sin mensajes en >90s y watchdog aún no recicló.
          - REST_BACKOFF_ACTIVE: WS sin datos y bulk_backoff (auth global o todos sports) > 60s.
        """
        ws_task_alive = self._ws_runner_task is not None and not self._ws_runner_task.done()
        ws_current_alive = len(self._ws_current_by_sport) > 0
        age_msg = self.ws_age_last_msg_sec()
        age_tick = self.ws_age_last_tick_sec()
        bulk_left = self.bulk_backoff_left_sec()
        if not ws_task_alive:
            if bulk_left >= 60.0:
                return "REST_BACKOFF_ACTIVE"
            return "WS_DEAD"
        if not ws_current_alive:
            return "WS_RECONNECTING"
        if age_msg is None:
            return "WS_RECONNECTING"
        if age_msg > _WS_IDLE_RESET_SEC:
            return "WS_DEAD"
        if age_tick is None or age_tick > 180.0:
            return "WS_LIVE_NO_TICKS"
        return "WS_LIVE_OK"

    def rest_quota_used_60min(self) -> int:
        """Reqs REST emitidos en la última hora (token bucket)."""
        self._rest_quota_pop_old()
        return len(self._rest_quota_ts)

    def ws_msgs_summary(self) -> dict[str, Any]:
        """Diagnóstico WS: total de mensajes TEXT recibidos y desglose por tipo."""
        return {
            "ws_msgs_text_total": int(self._ws_msgs_text_total),
            "ws_msgs_by_type": dict(self._ws_msgs_by_type),
        }

    def _rest_quota_pop_old(self) -> None:
        now = time.monotonic()
        while self._rest_quota_ts and now - self._rest_quota_ts[0] > _REST_QUOTA_WINDOW_SEC:
            self._rest_quota_ts.popleft()

    def _rest_quota_can_consume(self) -> bool:
        self._rest_quota_pop_old()
        return len(self._rest_quota_ts) < _REST_QUOTA_PER_HOUR

    def _rest_quota_consume(self) -> None:
        self._rest_quota_ts.append(time.monotonic())

    def _bulk_backoff_blocked(self, io_sport: str) -> bool:
        return self.bulk_backoff_left_sec(io_sport) > 0.0

    def _set_bulk_backoff_for_sport(self, io_sport: str, base_sec: float = _REST_BULK_BACKOFF_SEC) -> None:
        """Backoff per-sport con tope; escala si ya estaba en backoff."""
        now = time.monotonic()
        prev_left = max(0.0, float(self._rest_bulk_backoff_until_per_sport.get(io_sport, 0.0)) - now)
        next_dur = min(_REST_BULK_BACKOFF_MAX_SEC, max(base_sec, prev_left * 2.0))
        self._rest_bulk_backoff_until_per_sport[io_sport] = now + float(next_dur)

    def _set_bulk_backoff_global_auth(self, sec: float = _REST_AUTH_BACKOFF_SEC) -> None:
        self._rest_bulk_backoff_global_until = max(self._rest_bulk_backoff_global_until, time.monotonic() + float(sec))

    async def _ws_watchdog_loop(self) -> None:
        """Cierra la WS activa si lleva > _WS_IDLE_RESET_SEC sin mensajes (la reabrirá _ws_runner_loop)."""
        try:
            while not self._ws_cancel.is_set():
                try:
                    await asyncio.wait_for(self._ws_cancel.wait(), timeout=_WS_WATCHDOG_POLL_SEC)
                    break
                except asyncio.TimeoutError:
                    pass
                sockets = list(self._ws_current_by_sport.items())
                if not sockets:
                    continue
                age_msg = self.ws_age_last_msg_sec()
                if age_msg is None:
                    continue
                if age_msg > _WS_IDLE_RESET_SEC:
                    log.warning(
                        "[odds_api_io] WS_WATCHDOG idle %.0fs > %ss, cerrando %s WS para forzar reconexión.",
                        age_msg,
                        int(_WS_IDLE_RESET_SEC),
                        len(sockets),
                    )
                    for sport_slug, ws in sockets:
                        try:
                            await ws.close()
                        except Exception as e:
                            log.debug("[odds_api_io] WS_WATCHDOG close sport=%s: %s", sport_slug, e)
        except asyncio.CancelledError:
            return

    def get_cached_odds(self, sport: str) -> list[OddsEvent]:
        """Solo lectura de caché; en REST la caché se rellena vía refresh_rest_cache."""
        k = sport.strip().lower()
        sport_slug = str(ODDS_KEY_TO_IO_SPORT.get(k, k)).strip().lower()
        # WS suele estar suscrito solo a un subconjunto (p. ej. tenis). Si no hay ticks,
        # seguir con REST para basketball/football/etc. sin apagar el WS global.
        if self.ws_enabled:
            ws_events = self._flatten_ws_cache_io_sport(sport_slug)
            if ws_events:
                return ws_events
        now = time.monotonic()
        row = self._rest_cache.get(sport_slug)
        if not row:
            return []
        ts, events = row
        if now - ts > self.cache_ttl:
            return []
        return list(events)

    def has_ws_odds_for_io_sport(self, io_sport: str) -> bool:
        """True si hay al menos un tick WS en caché para el slug IO (p. ej. tennis)."""
        return len(self._flatten_ws_cache_io_sport(str(io_sport or "").strip().lower())) > 0

    def ws_health_counts(self) -> dict[str, int]:
        """Diagnóstico WS: filas utilizables vs filas incompletas (home/away)."""
        total = 0
        usable = 0
        missing_meta = 0
        for by_bk in self._ws_odds_cache.values():
            for ev in by_bk.values():
                total += 1
                has_meta = bool((ev.home or "").strip() and (ev.away or "").strip())
                if has_meta:
                    usable += 1
                else:
                    missing_meta += 1
        return {
            "ws_rows_total": int(total),
            "ws_rows_usable": int(usable),
            "ws_rows_missing_meta": int(missing_meta),
        }

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
        io_sport = poly_odds_key_to_io_sport(poly_sport)
        if not io_sport:
            return
        if self._bulk_backoff_blocked(io_sport):
            return
        now = time.monotonic()
        # Gating per-sport: aunque el TTL de cache haya vencido, no martillear /events cada 5s.
        last_attempt = float(self._rest_last_attempt_per_sport.get(io_sport, 0.0))
        if last_attempt > 0 and (now - last_attempt) < _REST_PER_SPORT_MIN_INTERVAL_SEC:
            return
        # Sport marcado como "vacío" recientemente: no insistir hasta que venza la ventana.
        empty_until = float(self._rest_empty_sport_until.get(io_sport, 0.0))
        if empty_until > now:
            return
        if not self._rest_quota_can_consume():
            log.debug(
                "[odds_api_io] REST quota %s/%s en última hora; salto refresh sport=%s.",
                len(self._rest_quota_ts),
                _REST_QUOTA_PER_HOUR,
                io_sport,
            )
            return
        async with self._lock:
            row = self._rest_cache.get(io_sport)
            if row and now - row[0] < self.cache_ttl:
                return
        self._rest_last_attempt_per_sport[io_sport] = now
        try:
            events_rows = await self._rest_fetch_sport(session, io_sport)
        except OddsApiIoRestQuotaError as e:
            log.warning(
                "[odds_api_io] REST sport=%s en pausa (cuota/límite); per-sport backoff %.0fs. %s",
                io_sport,
                _REST_BULK_BACKOFF_SEC,
                e,
            )
            self._set_bulk_backoff_for_sport(io_sport, _REST_BULK_BACKOFF_SEC)
            return
        except Exception as e:
            log.warning("[odds_api_io] REST fetch sport=%s: %s", io_sport, e)
            return
        if not events_rows:
            # Sport sin eventos próximos en odds-api.io: no llamar a /odds/multi y posponer reintentos.
            self._rest_empty_sport_until[io_sport] = now + _REST_EMPTY_SPORT_BACKOFF_SEC
            log.debug(
                "[odds_api_io] REST sport=%s sin eventos; backoff %.0fs antes de reintentar /events.",
                io_sport,
                _REST_EMPTY_SPORT_BACKOFF_SEC,
            )
            async with self._lock:
                self._rest_cache[io_sport] = (time.monotonic(), [])
            return
        async with self._lock:
            self._rest_cache[io_sport] = (time.monotonic(), events_rows)

    async def _rest_fetch_sport(self, session: aiohttp.ClientSession, io_sport: str) -> list[OddsEvent]:
        params = {"apiKey": self.api_key, "sport": io_sport}
        url = f"{BASE_REST}/events"
        self._rest_quota_consume()
        async with session.get(
            url, params=params, headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=45)
        ) as resp:
            text = await resp.text()
            if resp.status != 200:
                if resp.status in (401, 403):
                    self._set_bulk_backoff_global_auth(_REST_AUTH_BACKOFF_SEC)
                _raise_if_rest_quota_error(resp.status, text, f"GET /events sport={io_sport}")
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
        if len(ids) > _ODDS_IO_REST_MAX_EVENT_IDS:
            ids = ids[:_ODDS_IO_REST_MAX_EVENT_IDS]
        if not ids:
            return []
        out: list[OddsEvent] = []
        for i in range(0, len(ids), 10):
            if not self._rest_quota_can_consume():
                log.info(
                    "[odds_api_io] REST quota agotada en chunks /odds/multi sport=%s; cortamos batch.",
                    io_sport,
                )
                break
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
        self._rest_quota_consume()
        async with session.get(
            url, params=params, headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=45)
        ) as resp:
            text = await resp.text()
            if resp.status != 200:
                if resp.status in (401, 403):
                    self._set_bulk_backoff_global_auth(_REST_AUTH_BACKOFF_SEC)
                _raise_if_rest_quota_error(resp.status, text, "GET /odds/multi")
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

    async def _ws_runner_loop(self, sports: list[str]) -> None:
        # Un loop independiente por sport para que cada mensaje quede etiquetado por canal.
        self._ws_tasks_by_sport = {
            sport_slug: asyncio.create_task(
                self._ws_sport_loop(sport_slug),
                name=f"odds_api_io_ws_{sport_slug}",
            )
            for sport_slug in sports
        }
        try:
            await self._ws_cancel.wait()
        finally:
            for t in self._ws_tasks_by_sport.values():
                t.cancel()
            await asyncio.gather(*self._ws_tasks_by_sport.values(), return_exceptions=True)
            self._ws_tasks_by_sport = {}

    async def _ws_sport_loop(self, bound_sport: str) -> None:
        attempt = 0
        backoff_i = 0
        while not self._ws_cancel.is_set():
            try:
                await self._ws_once(bound_sport)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                attempt += 1
                delay = _WS_BACKOFF_SEC[min(backoff_i, len(_WS_BACKOFF_SEC) - 1)]
                if backoff_i >= len(_WS_BACKOFF_SEC) - 1:
                    delay = 30
                else:
                    backoff_i += 1
                log.warning(
                    "[odds_api_io] WS_RECONNECT sport=%s attempt=%s delay=%ss err=%s",
                    bound_sport,
                    attempt,
                    delay,
                    e,
                )
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

    async def _ws_once(self, bound_sport: str) -> None:
        q = {
            "apiKey": self.api_key,
            "markets": self.markets_ws,
            "sport": bound_sport,
            "status": "live",
        }
        bm_ws = _ws_bookmakers_query_value(self.bookmakers_csv)
        url = f"{WS_BASE}?{urlencode(q)}&bookmakers={quote(bm_ws, safe=',+')}"
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=None)
        async with aiohttp.ClientSession(headers=_DEFAULT_HEADERS, timeout=timeout) as session:
            log.info("[odds_api_io] WS connecting sport=%s", bound_sport)
            async with session.ws_connect(url, heartbeat=30.0) as ws:
                self._ws_current_by_sport[bound_sport] = ws
                # En cuanto se conecta, marcar last_msg para que el watchdog no dispare durante el handshake.
                self._ws_last_msg_mono = time.monotonic()
                try:
                    ws_first_msg_types: set[str] = set()
                    welcome_ok = False
                    while not self._ws_cancel.is_set():
                        msg = await ws.receive()
                        # Cualquier mensaje del WS (incluso heartbeats / no-text) cuenta como "vivo".
                        self._ws_last_msg_mono = time.monotonic()
                        if msg.type == WSMsgType.CLOSE or msg.type == WSMsgType.CLOSED:
                            break
                        if msg.type == WSMsgType.ERROR:
                            exc = ws.exception()
                            if exc:
                                raise exc
                            break
                        if msg.type != WSMsgType.TEXT:
                            continue
                        # Cuenta de mensajes TEXT (incluso si luego no se parsea bien o no es dict).
                        self._ws_msgs_text_total += 1
                        try:
                            data = json.loads(msg.data)
                        except json.JSONDecodeError:
                            self._ws_msgs_by_type["__json_error__"] = (
                                self._ws_msgs_by_type.get("__json_error__", 0) + 1
                            )
                            continue
                        if not isinstance(data, dict):
                            self._ws_msgs_by_type["__non_dict__"] = (
                                self._ws_msgs_by_type.get("__non_dict__", 0) + 1
                            )
                            continue
                        typ = str(data.get("type") or "").lower()
                        type_key = typ or "__no_type__"
                        self._ws_msgs_by_type[type_key] = self._ws_msgs_by_type.get(type_key, 0) + 1
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
                            await self._apply_ws_created_updated(data, bound_sport, typ)
                        elif typ == "deleted":
                            await self._apply_ws_deleted(data)
                        elif typ == "no_markets":
                            log.debug("[odds_api_io] no_markets id=%s", data.get("id"))
                finally:
                    self._ws_current_by_sport.pop(bound_sport, None)

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

    async def _apply_ws_created_updated(self, data: dict[str, Any], bound_sport: str, message_type: str) -> None:
        eid = str(data.get("id") or "")
        bookie = str(data.get("bookie") or "").strip()
        if not eid or not bookie:
            return
        if self._wanted_bookmaker_names and not _bookie_matches_wanted(bookie, self._wanted_bookmaker_names):
            self._ws_dropped_other_bookie += 1
            log.debug(
                "[odds_api_io] WS_DROP_OTHER_BOOKIE event_id=%s bookie=%s wanted=%s",
                eid,
                bookie,
                sorted(self._wanted_bookmaker_names),
            )
            return
        home = str(data.get("home") or "").strip()
        away = str(data.get("away") or "").strip()
        sport_guess = str(bound_sport or "").strip().lower()
        incoming_sport = _coerce_io_sport_slug(data.get("sport"))
        if incoming_sport and incoming_sport != sport_guess:
            log.debug(
                "[odds_api_io] WS_SPORT_MISMATCH event_id=%s payload=%s bound=%s",
                eid,
                incoming_sport,
                sport_guess,
            )
        async with self._lock:
            prev = self._ws_odds_cache.get(eid, {}).get(bookie)
        if prev is not None:
            if not home:
                home = str(prev.home or "").strip()
            if not away:
                away = str(prev.away or "").strip()
        if not home or not away:
            log.debug(
                "[odds_api_io] WS_INCOMPLETE event_id=%s bookie=%s home=%r away=%r sport=%r",
                eid,
                bookie,
                home,
                away,
                sport_guess,
            )
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
        # Marca tick válido y dispara callback (motor latency_arb_sports reacciona al instante).
        self._ws_last_tick_mono = time.monotonic()
        cb = self._betfair_tick_callback
        if cb is not None and home and away and sport_guess:
            try:
                await cb(ev)
            except Exception as e:
                log.warning("[odds_api_io] tick callback err event_id=%s: %s", eid, e)
