"""Descubrimiento de mercados para bundle_arb: Gamma, eventos negRisk y CLOB simplified."""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Optional, Union

import aiohttp

from clients.poly_parse import (
    api_bool_true,
    gamma_condition_id,
    gamma_market_child_eligible,
    gamma_market_token_ids,
    gamma_yes_token_id,
)

if TYPE_CHECKING:
    from clients.poly_clob import PolyCLOBClient

BundleDiscoveryRow = Union["BundleCandidate", "NegRiskBundleCandidate"]

GAMMA_API_URL = os.getenv("GAMMA_API_URL", "https://gamma-api.polymarket.com").rstrip("/")

_DEFAULT_HEADERS = {
    "User-Agent": "predmarket-arb/arb-engine (aiohttp; +https://github.com)",
    "Accept": "application/json",
}


@dataclass
class BundleCandidate:
    """Mercado candidato para escanear libros CLOB (tras descubrimiento)."""

    condition_id: str
    token_ids: list[str]
    source: str
    question: str = ""


@dataclass
class NegRiskLeg:
    """Un hijo binario del evento: token YES + condition CLOB."""

    condition_id: str
    yes_token_id: str
    question: str = ""
    fees_enabled: Optional[bool] = None


@dataclass
class NegRiskBundleCandidate:
    """Evento Gamma negRisk: piernas = token YES por market hijo apto."""

    event_id: str
    slug: str
    end_date_iso: str
    legs: list[NegRiskLeg] = field(default_factory=list)
    score: float = 0.0


def _float_field(m: dict[str, Any], *keys: str) -> Optional[float]:
    for k in keys:
        v = m.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def _event_id(ev: dict[str, Any]) -> str:
    for k in ("id", "eventId", "event_id"):
        v = ev.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _event_slug(ev: dict[str, Any]) -> str:
    s = ev.get("slug") or ev.get("ticker") or ""
    return str(s).strip()[:200]


def _score_negrisk_event(ev: dict[str, Any], n_legs: int, now: datetime) -> float:
    """Score v1: liquidez/volumen del evento + ventana de días + 1/n."""
    w_liq = float(os.getenv("BUNDLE_SCORE_LIQ_WEIGHT", "0.4"))
    w_vol = float(os.getenv("BUNDLE_SCORE_VOL_WEIGHT", "0.4"))
    w_days = float(os.getenv("BUNDLE_SCORE_DAYS_WEIGHT", "0.1"))
    w_inv = float(os.getenv("BUNDLE_SCORE_INVN_WEIGHT", "0.1"))
    liq = _float_field(ev, "liquidityClob", "liquidityNum", "liquidity") or 0.0
    vol = _float_field(ev, "volume24hr", "volume24Hr", "volume24HR") or 0.0
    end_dt = _parse_end_date(ev)
    dscore = 0.5
    if end_dt is not None:
        days = (end_dt - now).total_seconds() / 86400.0
        dscore = max(0.0, min(1.0, days / 60.0))
    inv = 1.0 / max(n_legs, 1)
    npen = 1.0 / math.sqrt(max(n_legs, 1))
    base = (
        w_liq * math.log1p(max(liq, 0.0))
        + w_vol * math.log1p(max(vol, 0.0))
        + w_days * dscore
        + w_inv * inv
    )
    w_n = float(os.getenv("BUNDLE_SCORE_OUTCOME_COUNT_WEIGHT", "0.15"))
    return base + w_n * npen


def _parse_end_date(m: dict[str, Any]) -> Optional[datetime]:
    raw = m.get("endDate") or m.get("end_date")
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class MarketsRegistry:
    """
    Universo de mercados para bundle_arb con caché TTL corto.
    Modos: ``gamma`` (recomendado), ``clob_simplified``.
    """

    def __init__(
        self,
        *,
        min_outcomes: int = 2,
        max_outcomes: int = 5,
        gamma_max_pages: int = 30,
        gamma_limit: int = 100,
        gamma_events_max_pages: int = 20,
        gamma_events_limit: int = 50,
        min_liquidity_usd: float = 0.0,
        min_volume_24h: float = 0.0,
        min_hours_to_resolution: float = 0.0,
        simplified_max_pages: int = 5,
        cache_ttl_sec: float = 45.0,
    ) -> None:
        self.min_outcomes = min_outcomes
        self.max_outcomes = max_outcomes
        self.gamma_max_pages = gamma_max_pages
        self.gamma_limit = gamma_limit
        self.gamma_events_max_pages = gamma_events_max_pages
        self.gamma_events_limit = gamma_events_limit
        self.min_liquidity_usd = min_liquidity_usd
        self.min_volume_24h = min_volume_24h
        self.min_hours_to_resolution = min_hours_to_resolution
        self.simplified_max_pages = simplified_max_pages
        self.cache_ttl_sec = cache_ttl_sec
        self._cached_at: float = 0.0
        self._cached_mode: str = ""
        self._cached: list[BundleDiscoveryRow] = []
        self._last_diag: dict[str, Any] = {}

    @classmethod
    def from_env(cls, min_outcomes: int, max_outcomes: int) -> "MarketsRegistry":
        return cls(
            min_outcomes=min_outcomes,
            max_outcomes=max_outcomes,
            gamma_max_pages=int(os.getenv("BUNDLE_GAMMA_MAX_PAGES", os.getenv("GAMMA_MAX_PAGES", "30"))),
            gamma_limit=int(os.getenv("BUNDLE_GAMMA_LIMIT", "100")),
            gamma_events_max_pages=int(os.getenv("BUNDLE_GAMMA_EVENTS_MAX_PAGES", "20")),
            gamma_events_limit=int(os.getenv("BUNDLE_GAMMA_EVENTS_LIMIT", "50")),
            min_liquidity_usd=float(os.getenv("BUNDLE_MIN_LIQUIDITY_USD", "0")),
            min_volume_24h=float(os.getenv("BUNDLE_MIN_VOLUME_24H", "0")),
            min_hours_to_resolution=float(os.getenv("BUNDLE_MIN_HOURS_TO_RES", "0")),
            simplified_max_pages=int(os.getenv("BUNDLE_SIMPLIFIED_MAX_PAGES", "5")),
            cache_ttl_sec=float(os.getenv("MARKETS_CACHE_TTL_SEC", "45")),
        )

    def _passes_gamma_filters(self, m: dict[str, Any], n_tokens: int) -> tuple[bool, str]:
        if n_tokens < self.min_outcomes or n_tokens > self.max_outcomes:
            return False, "outcomes_range"
        liq = _float_field(m, "liquidityNum", "liquidity")
        if liq is not None and self.min_liquidity_usd > 0 and liq < self.min_liquidity_usd:
            return False, "low_liquidity"
        v24 = _float_field(m, "volume24hr", "volume24Hr", "volume24HR")
        if v24 is not None and self.min_volume_24h > 0 and v24 < self.min_volume_24h:
            return False, "low_volume24h"
        if self.min_hours_to_resolution > 0:
            end_dt = _parse_end_date(m)
            if end_dt is not None:
                min_end = datetime.now(timezone.utc) + timedelta(hours=self.min_hours_to_resolution)
                if end_dt < min_end:
                    return False, "resolution_too_soon"
        return True, ""

    async def discover_gamma(self, session: aiohttp.ClientSession) -> tuple[list[BundleCandidate], dict[str, Any]]:
        """GET Gamma ``/markets`` con active/closed/archived."""
        diag: dict[str, Any] = {
            "gamma_pages": 0,
            "gamma_rows_total": 0,
            "gamma_after_outcomes": 0,
            "gamma_skip_filters": 0,
            "gamma_skip_no_cid": 0,
            "gamma_skip_no_tokens": 0,
        }
        out: list[BundleCandidate] = []
        offset = 0
        base = f"{GAMMA_API_URL}/markets"
        for page in range(self.gamma_max_pages):
            params: dict[str, str | int] = {
                "active": "true",
                "closed": "false",
                "archived": "false",
                "limit": self.gamma_limit,
                "offset": offset,
            }
            async with session.get(base, params=params, headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                text = await resp.text()
                if resp.status != 200:
                    diag["gamma_error"] = f"HTTP {resp.status}: {text[:200]}"
                    break
                chunk = json.loads(text)
            if not isinstance(chunk, list):
                diag["gamma_error"] = "response_not_list"
                break
            diag["gamma_pages"] += 1
            diag["gamma_rows_total"] += len(chunk)
            if not chunk:
                break
            for m in chunk:
                if not isinstance(m, dict):
                    continue
                cid = gamma_condition_id(m)
                if not cid:
                    diag["gamma_skip_no_cid"] += 1
                    continue
                tids = gamma_market_token_ids(m)
                n = len(tids)
                if n < self.min_outcomes:
                    diag["gamma_skip_no_tokens"] += 1
                    continue
                ok, _why = self._passes_gamma_filters(m, n)
                if not ok:
                    diag["gamma_skip_filters"] += 1
                    continue
                diag["gamma_after_outcomes"] += 1
                q = str(m.get("question") or m.get("title") or "")[:200]
                out.append(BundleCandidate(condition_id=cid, token_ids=tids, source="gamma", question=q))
            if len(chunk) < self.gamma_limit:
                break
            offset += self.gamma_limit
        self._last_diag = diag
        return out, diag

    async def discover_gamma_events_keyset(
        self, session: aiohttp.ClientSession
    ) -> tuple[list[NegRiskBundleCandidate], dict[str, Any]]:
        """
        GET Gamma ``/events/keyset`` (active, closed=false) con cursores.
        Filtra negRisk, fechas sobre ``endDate`` del evento, hijos tradeables con token YES.
        """
        now = datetime.now(timezone.utc)
        diag: dict[str, Any] = {
            "events_pages": 0,
            "events_raw": 0,
            "skip_not_negrisk": 0,
            "skip_augmented": 0,
            "skip_date": 0,
            "skip_event_liquidity": 0,
            "skip_event_volume": 0,
            "skip_child_tradability": 0,
            "skip_yes_token": 0,
            "skip_outcomes_range": 0,
            "skip_no_event_id": 0,
            "candidates_built": 0,
            "candidates_after_cap": 0,
        }
        require_negrisk = api_bool_true(os.getenv("BUNDLE_REQUIRE_NEGRISK", "true"))
        allow_augmented = api_bool_true(os.getenv("BUNDLE_ALLOW_AUGMENTED_NEGRISK", "false"))
        min_days = float(os.getenv("BUNDLE_MIN_DAYS_TO_EXPIRY", "14"))
        max_days = float(os.getenv("BUNDLE_MAX_DAYS_TO_EXPIRY", "365"))
        max_cap = int(os.getenv("BUNDLE_MAX_CANDIDATES_PER_CYCLE", "120"))
        min_liq_clob = float(os.getenv("BUNDLE_MIN_LIQUIDITY_CLOB", "0"))
        min_vol24 = float(os.getenv("BUNDLE_MIN_VOLUME_24H", "0"))

        built: list[NegRiskBundleCandidate] = []
        cursor: str = ""
        base = f"{GAMMA_API_URL}/events/keyset"

        for _page in range(self.gamma_events_max_pages):
            params: dict[str, str] = {
                "active": "true",
                "closed": "false",
                "limit": str(self.gamma_events_limit),
            }
            if cursor:
                params["next_cursor"] = cursor
            async with session.get(
                base, params=params, headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                text = await resp.text()
                if resp.status != 200:
                    diag["events_keyset_error"] = f"HTTP {resp.status}: {text[:200]}"
                    break
                payload = json.loads(text)
            if not isinstance(payload, dict):
                diag["events_keyset_error"] = "response_not_object"
                break
            diag["events_pages"] += 1
            events = payload.get("events") or []
            if not isinstance(events, list):
                diag["events_keyset_error"] = "events_not_list"
                break
            for ev in events:
                if not isinstance(ev, dict):
                    continue
                diag["events_raw"] += 1
                if require_negrisk and not api_bool_true(ev.get("negRisk")):
                    diag["skip_not_negrisk"] += 1
                    continue
                if not allow_augmented and api_bool_true(ev.get("negRiskAugmented")):
                    diag["skip_augmented"] += 1
                    continue
                end_dt = _parse_end_date(ev)
                if end_dt is None:
                    diag["skip_date"] += 1
                    continue
                days = (end_dt - now).total_seconds() / 86400.0
                if days < min_days or days > max_days:
                    diag["skip_date"] += 1
                    continue

                if min_liq_clob > 0:
                    eliq = _float_field(ev, "liquidityClob", "liquidityNum", "liquidity")
                    if eliq is None or eliq < min_liq_clob:
                        diag["skip_event_liquidity"] += 1
                        continue
                if min_vol24 > 0:
                    ev24 = _float_field(ev, "volume24hr", "volume24Hr", "volume24HR")
                    if ev24 is None or ev24 < min_vol24:
                        diag["skip_event_volume"] += 1
                        continue

                legs: list[NegRiskLeg] = []
                markets = ev.get("markets") or []
                if not isinstance(markets, list):
                    markets = []
                for m in markets:
                    if not isinstance(m, dict):
                        continue
                    ok_child, _reason = gamma_market_child_eligible(m)
                    if not ok_child:
                        diag["skip_child_tradability"] += 1
                        continue
                    cid = gamma_condition_id(m)
                    yid = gamma_yes_token_id(m)
                    if not cid or not yid:
                        diag["skip_yes_token"] += 1
                        continue
                    q = str(m.get("question") or m.get("title") or "")[:200]
                    fe = m.get("feesEnabled")
                    if fe is None:
                        fe = m.get("fees_enabled")
                    fees_en: Optional[bool] = None
                    if isinstance(fe, bool):
                        fees_en = fe
                    elif isinstance(fe, str):
                        fees_en = api_bool_true(fe)
                    legs.append(
                        NegRiskLeg(condition_id=cid, yes_token_id=yid, question=q, fees_enabled=fees_en)
                    )

                n = len(legs)
                if n < self.min_outcomes or n > self.max_outcomes:
                    diag["skip_outcomes_range"] += 1
                    continue

                eid = _event_id(ev)
                if not eid:
                    diag["skip_no_event_id"] += 1
                    continue
                end_raw = str(ev.get("endDate") or ev.get("end_date") or "")[:40]
                sc = _score_negrisk_event(ev, n, now)
                built.append(
                    NegRiskBundleCandidate(
                        event_id=eid,
                        slug=_event_slug(ev),
                        end_date_iso=end_raw,
                        legs=legs,
                        score=sc,
                    )
                )
                diag["candidates_built"] += 1

            cursor = str(payload.get("next_cursor") or "").strip()
            if not cursor:
                break

        built.sort(key=lambda c: c.score, reverse=True)
        capped = built[: max(0, max_cap)]
        diag["candidates_after_cap"] = len(capped)
        self._last_diag = diag
        return capped, diag

    async def discover_simplified(self, poly: "PolyCLOBClient") -> tuple[list[BundleCandidate], dict[str, Any]]:
        """GET CLOB ``/simplified-markets`` paginado vía ``PolyCLOBClient`` (throttle + retries)."""
        diag: dict[str, Any] = {
            "simplified_pages": 0,
            "simplified_rows_total": 0,
            "simplified_candidates": 0,
        }
        out: list[BundleCandidate] = []
        cursor: str = ""
        for _ in range(self.simplified_max_pages):
            try:
                page = await poly.get_simplified_markets(next_cursor=cursor)
            except Exception as e:
                diag["simplified_error"] = str(e)[:200]
                break
            rows = list(page.get("data") or [])
            diag["simplified_pages"] += 1
            diag["simplified_rows_total"] += len(rows)
            for m in rows:
                if not isinstance(m, dict):
                    continue
                cid = str(m.get("condition_id") or "").strip()
                tokens = m.get("tokens") or []
                tids: list[str] = []
                for t in tokens:
                    if isinstance(t, dict):
                        tid = t.get("token_id") or t.get("tokenId")
                        if tid:
                            tids.append(str(tid))
                n = len(tids)
                if not cid or n < self.min_outcomes or n > self.max_outcomes:
                    continue
                q = str(m.get("question") or m.get("title") or "")[:200]
                out.append(BundleCandidate(condition_id=cid, token_ids=tids, source="clob_simplified", question=q))
                diag["simplified_candidates"] += 1
            cursor = str(page.get("next_cursor") or "").strip()
            if not cursor:
                break
        self._last_diag = diag
        return out, diag

    async def get_candidates(
        self,
        mode: str,
        poly: "PolyCLOBClient",
        *,
        force_refresh: bool = False,
    ) -> tuple[list[BundleDiscoveryRow], dict[str, Any]]:
        """Devuelve candidatos y diagnóstico; usa caché TTL salvo ``force_refresh``."""
        now = time.monotonic()
        if (
            not force_refresh
            and self._cached_mode == mode
            and self._cached
            and (now - self._cached_at) < self.cache_ttl_sec
        ):
            d = {**self._last_diag, "cache_hit": True, "registry_fetched_at": self._cached_at}
            return list(self._cached), d

        if mode == "gamma":
            cands, diag = await self.discover_gamma(poly.http_session)
        elif mode == "gamma_events":
            cands_n, diag = await self.discover_gamma_events_keyset(poly.http_session)
            cands = list(cands_n)
        elif mode == "clob_simplified":
            cands, diag = await self.discover_simplified(poly)
        else:
            cands, diag = [], {"error": f"unknown_discovery_mode:{mode}"}

        self._cached = cands
        self._cached_at = now
        self._cached_mode = mode
        diag["cache_hit"] = False
        diag["registry_fetched_at"] = now
        self._last_diag = diag
        return cands, diag
