"""Cliente HTTP async para The Odds API v4 (Pinnacle u otros bookmakers)."""

from __future__ import annotations

import os
import unicodedata
from typing import Any, Optional

import aiohttp

# LAB PRIVADO — reemplazar por env var en producción; preferir ODDS_API_KEY en .env
_ODDS_API_KEY_LAB = "81b787a998338c0c36fb6751bbf53a04"

ODDS_API_BASE = "https://api.the-odds-api.com/v4"

_DEFAULT_HEADERS = {
    "User-Agent": "predmarket-arb/odds-api (aiohttp; +https://github.com)",
    "Accept": "application/json",
}


def odds_api_key() -> str:
    return (os.getenv("ODDS_API_KEY") or _ODDS_API_KEY_LAB).strip()


def implied_prob(decimal_odd: float) -> float:
    if decimal_odd <= 0:
        return 0.0
    return 1.0 / float(decimal_odd)


def remove_vig(
    home_prob: float,
    away_prob: float,
    draw_prob: Optional[float] = None,
) -> tuple[float, float, Optional[float]]:
    """
    Devuelve probabilidades fair normalizadas (suman 1 en el subconjunto de mercados).
    Si draw_prob es None, solo 2 vías home/away.
    """
    if draw_prob is None:
        s = home_prob + away_prob
        if s <= 0:
            return 0.0, 0.0, None
        return home_prob / s, away_prob / s, None
    s = home_prob + away_prob + draw_prob
    if s <= 0:
        return 0.0, 0.0, 0.0
    return home_prob / s, away_prob / s, draw_prob / s


def normalize_team_label(s: str) -> str:
    s = unicodedata.normalize("NFKD", (s or "").strip().lower())
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = " ".join(s.split())
    return s


def teams_match_odds_gamma(odds_team: str, gamma_label: str) -> bool:
    """
    True si el equipo Odds API y el label Gamma (outcome o texto) son el mismo partido.
    Incluye Levenshtein < 3 y reglas de contención (p. ej. 'Celtics' vs 'Boston Celtics').
    """
    a = normalize_team_label(odds_team)
    b = normalize_team_label(gamma_label)
    if not a or not b:
        return False
    if levenshtein(a, b) < 3:
        return True
    if a in b or b in a:
        return True
    aw = a.split()[-1] if a.split() else a
    bw = b.split()[-1] if b.split() else b
    if aw == bw or aw in bw or bw in aw:
        return True
    return levenshtein(aw, bw) < 3


def levenshtein(a: str, b: str) -> int:
    """Distancia de Levenshtein (DP O(nm)); strings cortos de equipos."""
    if len(a) < len(b):
        a, b = b, a
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            ins, delete, sub = cur[j - 1] + 1, prev[j] + 1, prev[j - 1] + (ca != cb)
            cur.append(min(ins, delete, sub))
        prev = cur
    return prev[-1]


async def get_sports(session: aiohttp.ClientSession, *, api_key: Optional[str] = None) -> list[dict[str, Any]]:
    key = api_key or odds_api_key()
    url = f"{ODDS_API_BASE}/sports/"
    params = {"apiKey": key}
    async with session.get(url, params=params, headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as resp:
        text = await resp.text()
        if resp.status != 200:
            raise RuntimeError(f"The Odds API GET /sports HTTP {resp.status}: {text[:400]}")
        data = await resp.json()
    return data if isinstance(data, list) else []


async def get_odds(
    session: aiohttp.ClientSession,
    sport: str,
    *,
    regions: str = "eu",
    markets: str = "h2h",
    bookmakers: str = "pinnacle",
    odds_format: str = "decimal",
    api_key: Optional[str] = None,
) -> list[dict[str, Any]]:
    """
    GET /sports/{sport}/odds — eventos con bookmakers filtrados.
    `sport` es la sport_key (p. ej. basketball_nba).
    """
    key = api_key or odds_api_key()
    url = f"{ODDS_API_BASE}/sports/{sport}/odds"
    params: dict[str, str] = {
        "apiKey": key,
        "regions": regions,
        "markets": markets,
        "bookmakers": bookmakers,
        "oddsFormat": odds_format,
    }
    async with session.get(url, params=params, headers=_DEFAULT_HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        text = await resp.text()
        if resp.status != 200:
            raise RuntimeError(f"The Odds API odds HTTP {resp.status} sport={sport}: {text[:400]}")
        data = await resp.json()
    return data if isinstance(data, list) else []
