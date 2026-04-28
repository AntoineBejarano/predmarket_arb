"""Cliente HTTP async para The Odds API v4 (Pinnacle u otros bookmakers)."""

from __future__ import annotations

import os
import re
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


# Odds API nombres largos → nicknames tipo Gamma (expandir según necesidad).
_TEAM_ALIAS_MAP: dict[str, str] = {
    "los angeles lakers": "lakers",
    "los angeles clippers": "clippers",
    "golden state warriors": "warriors",
    "boston celtics": "celtics",
    "new york knicks": "knicks",
    "miami heat": "heat",
    "milwaukee bucks": "bucks",
    "denver nuggets": "nuggets",
    "phoenix suns": "suns",
    "dallas mavericks": "mavericks",
    "oklahoma city thunder": "thunder",
    "minnesota timberwolves": "timberwolves",
    "arsenal fc": "arsenal",
    "chelsea fc": "chelsea",
    "manchester city": "man city",
    "manchester united": "man united",
    "tottenham hotspur": "tottenham",
    "newcastle united": "newcastle",
    "west ham united": "west ham",
    "nottingham forest": "nottm forest",
    "aston villa": "aston villa",
}

_CLUB_SUFFIXES = (" fc", " cf", " sc", " ac", " bc")


def _strip_club_suffixes(t: str) -> str:
    out = (t or "").strip()
    changed = True
    while changed:
        changed = False
        for suf in _CLUB_SUFFIXES:
            if out.endswith(suf):
                out = out[: -len(suf)].rstrip()
                changed = True
    return out


def normalize_team_for_match(s: str) -> str:
    """
    Normaliza nombre de equipo Odds o Gamma para comparar: alias NBA/EPL,
    luego sufijos de club (FC, SC, …).
    """
    t = normalize_team_label(s)
    if not t:
        return ""
    if t in _TEAM_ALIAS_MAP:
        return _TEAM_ALIAS_MAP[t]
    t = _strip_club_suffixes(t)
    if t in _TEAM_ALIAS_MAP:
        return _TEAM_ALIAS_MAP[t]
    return t


def normalized_strings_match(a: str, b: str) -> bool:
    """Match tras normalize_team_for_match: igualdad, substring en cualquier sentido, o Levenshtein < 3."""
    if not a or not b:
        return False
    if a == b or a in b or b in a:
        return True
    return levenshtein(a, b) < 3


_BLOB_VS_SPLIT = re.compile(r"\s+(?:vs\.?|v|@)\s+|\s+[-–]\s+", re.IGNORECASE)


def _gamma_blob_team_chunks(blob: str) -> list[str]:
    """Parte título/slug en nombres de equipo (separador vs)."""
    n = normalize_team_label(blob)
    if not n:
        return []
    parts: list[str] = []
    for part in _BLOB_VS_SPLIT.split(n):
        p = part.strip()
        if not p:
            continue
        p = re.sub(r"^[a-z0-9]{2,5}:\s*", "", p).strip()
        if p:
            parts.append(p)
    return parts


def odds_team_matches_gamma_blob(odds_team: str, blob: str) -> bool:
    """
    True si el nombre Odds coincide con el texto Gamma (título+slug con varios equipos).
    Prueba match contra el blob completo y contra cada tramo separado por 'vs'.
    """
    n_o = normalize_team_for_match(odds_team)
    if not n_o:
        return False
    n_full = normalize_team_label(blob)
    if n_full and normalized_strings_match(n_o, n_full):
        return True
    chunks = _gamma_blob_team_chunks(blob)
    if len(chunks) >= 2:
        for chunk in chunks:
            n_chunk = normalize_team_for_match(chunk)
            if normalized_strings_match(n_o, n_chunk):
                return True
        return False
    n_single = normalize_team_for_match(blob)
    return normalized_strings_match(n_o, n_single)


def teams_match_odds_gamma(odds_team: str, gamma_label: str) -> bool:
    """
    True si el equipo Odds API y el label Gamma (outcome corto o texto largo) coinciden.
    Textos con varios equipos (p. ej. título con \"vs\") comparan por segmentos.
    """
    chunks = _gamma_blob_team_chunks(gamma_label)
    if len(chunks) >= 2:
        return odds_team_matches_gamma_blob(odds_team, gamma_label)
    a = normalize_team_for_match(odds_team)
    b = normalize_team_for_match(gamma_label)
    return normalized_strings_match(a, b)


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
