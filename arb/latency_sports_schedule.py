"""
Planificación latency_arb_sports usando solo Gamma (Polymarket).

No llama a odds-api.io: ahorra cuota REST/ruido. ``should_run`` y la tabla UI usan
``OpenPolymarketGame.kickoff_utc`` (inferido en ``_poly_event_to_open_game``).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import aiohttp

from arb.latency_arb_sports import (
    LatencyArbSportsStrategy,
    OpenPolymarketGame,
    _HARDCODE_POLY_SLUGS,
    _is_ml_like_open_game,
)

log = logging.getLogger("latency_sports_schedule")
_TZ_ES = ZoneInfo("Europe/Madrid")


def default_poly_slugs() -> list[str]:
    return [s.strip().lower() for s in _HARDCODE_POLY_SLUGS.split(",") if s.strip()]


def compute_active_windows_from_games(
    games: list[OpenPolymarketGame],
    now: datetime,
    *,
    pre_start_sec: float,
    post_end_sec: float,
) -> list[tuple[datetime, datetime, str]]:
    windows: list[tuple[datetime, datetime, str]] = []
    for g in games:
        dt = g.kickoff_utc
        if dt is None:
            continue
        sid = (g.slug or g.condition_id or "").strip() or "?"
        start = dt - timedelta(seconds=float(pre_start_sec))
        end = dt + timedelta(seconds=float(post_end_sec))
        windows.append((start, end, sid))
    return windows


def compute_should_run_from_games(
    games: list[OpenPolymarketGame],
    now: datetime,
    *,
    pre_start_sec: float,
    post_end_sec: float,
) -> tuple[bool, str, int]:
    wins = compute_active_windows_from_games(games, now, pre_start_sec=pre_start_sec, post_end_sec=post_end_sec)
    for s, e, sid in wins:
        if s <= now <= e:
            return True, f"inside_window slug_or_cid={sid[:64]}", len(wins)
    return False, "no_active_window", len(wins)


def pick_gamma_upcoming(
    games: list[OpenPolymarketGame],
    now: datetime,
    *,
    limit: int = 10,
    kickoff_grace_after_min: int = 45,
) -> list[OpenPolymarketGame]:
    """Orden por kickoff_utc ascendente; excluye sin kickoff o kickoff hace más de grace."""
    grace = timedelta(minutes=int(kickoff_grace_after_min))
    rows: list[tuple[datetime, OpenPolymarketGame]] = []
    for g in games:
        dt = g.kickoff_utc
        if dt is None:
            continue
        if dt < (now - grace):
            continue
        if dt > now + timedelta(days=14):
            continue
        if not _is_ml_like_open_game(g):
            continue
        rows.append((dt, g))
    rows.sort(key=lambda x: x[0])
    return [r[1] for r in rows[:limit]]


def format_gamma_row_for_api(g: OpenPolymarketGame, *, now: datetime) -> dict[str, Any]:
    dt = g.kickoff_utc
    if dt is None:
        return {
            "event_id": str(g.condition_id or g.slug or ""),
            "home": g.home,
            "away": g.away,
            "status": "gamma_open",
            "league": str(g.sport_slug or ""),
            "gamma_slug": (g.slug or "")[:120],
            "kickoff_utc": None,
            "kickoff_es_label": None,
            "minutes_to_kickoff": None,
            "kickoff_delta_label": "sin kickoff estimado",
            "polymarket_slug": g.sport_slug,
            "source": "polymarket_gamma",
        }
    loc = dt.astimezone(_TZ_ES)
    mins = round((dt - now).total_seconds() / 60.0, 1)
    out: dict[str, Any] = {
        "event_id": str(g.condition_id or g.slug or ""),
        "home": g.home,
        "away": g.away,
        "status": "gamma_open",
        "league": str(g.sport_slug or ""),
        "gamma_slug": (g.slug or "")[:120],
        "kickoff_utc": dt.isoformat().replace("+00:00", "Z"),
        "kickoff_es_label": loc.strftime("%Y-%m-%d %H:%M") + f" {loc.tzname() or 'Europe/Madrid'}",
        "minutes_to_kickoff": mins,
        "polymarket_slug": g.sport_slug,
        "source": "polymarket_gamma",
    }
    if mins < 0:
        out["kickoff_delta_label"] = f"hace {abs(int(round(mins)))} min"
    elif mins == 0:
        out["kickoff_delta_label"] = "ahora"
    else:
        out["kickoff_delta_label"] = f"en {int(round(mins))} min"
    return out


async def build_schedule_payload(
    session: aiohttp.ClientSession,
    *,
    poly_slugs: Optional[list[str]] = None,
    upcoming_limit: int = 10,
    pre_start_sec: Optional[float] = None,
    post_end_sec: Optional[float] = None,
) -> dict[str, Any]:
    slugs = poly_slugs if poly_slugs is not None else default_poly_slugs()
    pre = float(pre_start_sec if pre_start_sec is not None else os.getenv("LATENCY_SPORTS_SCHEDULER_PRE_START_SEC", "2700"))
    post = float(post_end_sec if post_end_sec is not None else os.getenv("LATENCY_SPORTS_SCHEDULER_POST_END_SEC", "18000"))

    now = datetime.now(timezone.utc)
    err: Optional[str] = None
    games: list[OpenPolymarketGame] = []
    try:
        cfg: dict[str, Any] = {"start_capital": 10_000.0, "current_capital": 10_000.0}
        strat = LatencyArbSportsStrategy(cfg, dry_run=True)
        strat.poly_slugs = slugs
        games = await strat._fetch_open_polymarket_sports(session)
    except Exception as e:
        err = str(e)
        log.warning("[latency_sports_schedule] Gamma fetch: %s", e)

    should, reason, n_win = compute_should_run_from_games(games, now, pre_start_sec=pre, post_end_sec=post)
    upcoming_g = pick_gamma_upcoming(games, now, limit=int(upcoming_limit))
    upcoming = [format_gamma_row_for_api(g, now=now) for g in upcoming_g]

    n_kick = sum(1 for g in games if g.kickoff_utc is not None)

    return {
        "updated_at": now.isoformat().replace("+00:00", "Z"),
        "timezone_display": "Europe/Madrid",
        "poly_slugs": slugs,
        "source": "polymarket_gamma",
        "gamma_open_games": len(games),
        "gamma_games_with_kickoff": n_kick,
        "active_windows_count": n_win,
        "upcoming": upcoming,
        "scheduler_enabled": scheduler_env_enabled(),
        "scheduler_pre_start_sec": pre,
        "scheduler_post_end_sec": post,
        "should_run_latency_sports": should,
        "should_run_reason": reason,
        "error": err,
    }


def scheduler_env_enabled() -> bool:
    v = os.getenv("LATENCY_SPORTS_SCHEDULER", "").strip().lower()
    return v in ("1", "true", "yes")


def scheduler_poll_sec() -> float:
    try:
        return max(30.0, float(os.getenv("LATENCY_SPORTS_SCHEDULER_POLL_SEC", "120")))
    except (TypeError, ValueError):
        return 120.0


def write_schedule_cache(path: Path, payload: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except OSError as e:
        log.warning("[latency_sports_schedule] no se pudo escribir %s: %s", path, e)
