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


# Odds API nombres largos → nicknames tipo Gamma (Odds largo / Gamma corto).
_ALIAS_MAP: dict[str, str] = {
    # NBA
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
    "san antonio spurs": "spurs",
    "new orleans pelicans": "pelicans",
    "memphis grizzlies": "grizzlies",
    "sacramento kings": "kings",
    "portland trail blazers": "blazers",
    "utah jazz": "jazz",
    "indiana pacers": "pacers",
    "chicago bulls": "bulls",
    "cleveland cavaliers": "cavaliers",
    "detroit pistons": "pistons",
    "toronto raptors": "raptors",
    "charlotte hornets": "hornets",
    "washington wizards": "wizards",
    "atlanta hawks": "hawks",
    "orlando magic": "magic",
    "brooklyn nets": "nets",
    "philadelphia 76ers": "76ers",
    # EPL
    "arsenal fc": "arsenal",
    "chelsea fc": "chelsea",
    "manchester city": "man city",
    "manchester united": "man united",
    "tottenham hotspur": "tottenham",
    "newcastle united": "newcastle",
    "west ham united": "west ham",
    "nottingham forest": "nottm forest",
    "wolverhampton wanderers": "wolves",
    "brighton & hove albion": "brighton",
    "brighton and hove albion": "brighton",
    "brentford fc": "brentford",
    "fulham fc": "fulham",
    "everton fc": "everton",
    "leicester city": "leicester",
    "ipswich town": "ipswich",
    "southampton fc": "southampton",
    "crystal palace": "crystal palace",
    "afc bournemouth": "bournemouth",
    "bournemouth": "bournemouth",
    "bournemouth fc": "bournemouth",
    "aston villa": "aston villa",
    "liverpool fc": "liverpool",
}

_CLUB_SUFFIXES: tuple[str, ...] = tuple(
    sorted(
        (" fc", " cf", " sc", " ac", " bc", " afc", " fk", " if", " bk"),
        key=len,
        reverse=True,
    )
)


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


_CLUB_PREFIXES: tuple[str, ...] = tuple(
    sorted(
        ("afc ", "fc ", "cf ", "as ", "ac ", "ss ", "us "),
        key=len,
        reverse=True,
    )
)


def _strip_club_prefixes(t: str) -> str:
    out = (t or "").strip()
    changed = True
    while changed:
        changed = False
        for pre in _CLUB_PREFIXES:
            if out.startswith(pre):
                out = out[len(pre) :].lstrip()
                changed = True
    return out


def normalize_team_for_match(s: str) -> str:
    """
    Normaliza para matching Odds ↔ Gamma:
    1) lowercase + espacios (normalize_team_label),
    2) strip sufijos de club al final,
    3) strip prefijos de club al inicio,
    4) lookup en _ALIAS_MAP,
    5) fallback última palabra.
    """
    t = normalize_team_label(s)
    if not t:
        return ""
    t = _strip_club_suffixes(t)
    t = _strip_club_prefixes(t)
    if t in _ALIAS_MAP:
        return _ALIAS_MAP[t]
    parts = t.split()
    return parts[-1] if parts else ""


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


def _team_match_normalized_tokens(a: str, b: str) -> bool:
    if not a or not b:
        return False
    aw = a.split()[-1] if a.split() else a
    bw = b.split()[-1] if b.split() else b
    return (
        a == b
        or a in b
        or b in a
        or levenshtein(a, b) < 3
        or aw == bw
    )


def normalized_strings_match(a: str, b: str) -> bool:
    """Misma lógica que teams_match_odds_gamma sobre tokens ya normalizados (p. ej. blobs)."""
    return _team_match_normalized_tokens(a, b)


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


def _teams_match_odds_gamma_impl(odds_name: str, gamma_name: str) -> bool:
    """Implementación base: dos nombres ya acotados a un equipo."""
    a = normalize_team_for_match(odds_name)
    b = normalize_team_for_match(gamma_name)
    return _team_match_normalized_tokens(a, b)


def teams_match_odds_gamma(odds_name: str, gamma_name: str) -> bool:
    """
    True si el equipo Odds API y el label Gamma (outcome corto o texto largo) coinciden.
    Textos con varios equipos (p. ej. título con \"vs\") comparan por segmentos.
    """
    chunks = _gamma_blob_team_chunks(gamma_name)
    if len(chunks) >= 2:
        return odds_team_matches_gamma_blob(odds_name, gamma_name)
    return _teams_match_odds_gamma_impl(odds_name, gamma_name)


def find_odds_event_matching_teams(
    events: list[dict[str, Any]], poly_home: str, poly_away: str
) -> Optional[dict[str, Any]]:
    """Primer evento Odds cuyo home/away matchea (orden o cruzado) con nombres Polymarket."""
    ph, pa = (poly_home or "").strip(), (poly_away or "").strip()
    if not ph or not pa:
        return None
    for ev in events:
        if not isinstance(ev, dict):
            continue
        oh = str(ev.get("home_team") or "").strip()
        oa = str(ev.get("away_team") or "").strip()
        if not oh or not oa:
            continue
        if (teams_match_odds_gamma(ph, oh) and teams_match_odds_gamma(pa, oa)) or (
            teams_match_odds_gamma(ph, oa) and teams_match_odds_gamma(pa, oh)
        ):
            return ev
    return None


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
