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
    s = re.sub(r"[^\w\s]+", " ", s, flags=re.UNICODE)
    s = " ".join(s.split())
    return s


def sport_tags_from_hint(sport_slug: Optional[str]) -> frozenset[str]:
    """Etiquetas gruesas para desambiguar alias (p. ej. Spurs NBA vs EPL)."""
    if not sport_slug:
        return frozenset()
    sl = str(sport_slug).strip().lower()
    tags: set[str] = set()
    if "nba" in sl or sl in ("basketball", "basketball_nba"):
        tags.add("nba")
    if "epl" in sl or "premier" in sl or sl == "soccer_epl":
        tags.add("epl")
        tags.add("soccer")
    if sl.startswith("soccer_") or sl in ("football", "soccer", "soccer_uefa_champs_league", "soccer_uefa_europa_league"):
        tags.add("soccer")
    if sl in ("ice-hockey", "icehockey_nhl"):
        tags.add("nhl")
    return frozenset(tags)


# Grupos de tokens sinónimos: (tokens, tags_requeridos) tags_requeridos=None → siempre si hay intersección.
# tags_requeridos no vacío → se aplica solo si sport_tags_from_hint ∩ tags_requeridos ≠ ∅.
_TOKEN_SYNONYM_GROUPS: list[tuple[frozenset[str], Optional[frozenset[str]]]] = [
    (frozenset({"cleveland", "cavaliers", "cavs", "cle", "clevelandcavaliers"}), None),
    (frozenset({"toronto", "raptors", "tor", "torontoraptors"}), None),
    (frozenset({"manchester", "united", "man", "utd", "manutd", "manchesterunited", "manunited"}), frozenset({"epl", "soccer"})),
    (frozenset({"tottenham", "spurs", "hotspur", "tottenhamhotspur"}), frozenset({"epl", "soccer"})),
    (frozenset({"san", "antonio", "spurs", "sanantonio"}), frozenset({"nba"})),
    (frozenset({"manchester", "city", "mancity", "manc", "manchestercity"}), frozenset({"epl", "soccer"})),
    (frozenset({"west", "ham", "westham", "whu"}), frozenset({"epl", "soccer"})),
    (frozenset({"nottingham", "forest", "nottm", "nffc"}), frozenset({"epl", "soccer"})),
    (frozenset({"wolverhampton", "wanderers", "wolves", "wwfc"}), frozenset({"epl", "soccer"})),
    (frozenset({"brighton", "hove", "albion", "bha"}), frozenset({"epl", "soccer"})),
    (frozenset({"newcastle", "nufc"}), frozenset({"epl", "soccer"})),
]

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
    "man utd": "man united",
    "man utd.": "man united",
}

_CLUB_SUFFIXES: tuple[str, ...] = tuple(
    sorted(
        (" fc", " cf", " sc", " ac", " bc", " afc", " fk", " if", " bk", " club"),
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
        ("afc ", "fc ", "cf ", "as ", "ac ", "ss ", "us ", "club "),
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
    Normaliza para matching Odds ↔ Gamma (forma compacta legada):
    1) lowercase + espacios (normalize_team_label),
    2) strip sufijos/prefijos de club,
    3) lookup en _ALIAS_MAP,
    4) fallback última palabra (compat.).
    """
    t = normalize_team_label(s)
    if not t:
        return ""
    t = _strip_club_suffixes(t)
    t = _strip_club_prefixes(t)
    if t in _ALIAS_MAP:
        return _ALIAS_MAP[t]
    parts = t.split()
    if len(parts) >= 2 and parts[0] in ("man", "los", "new", "golden", "san", "oklahoma", "portland"):
        joined = "".join(parts)
        if joined in _ALIAS_MAP:
            return _ALIAS_MAP[joined]
    return parts[-1] if parts else ""


def _base_tokens_from_label(t: str) -> set[str]:
    if not t:
        return set()
    out: set[str] = set()
    for w in t.split():
        if w:
            out.add(w)
    collapsed = "".join(t.split())
    if collapsed and collapsed != t.replace(" ", ""):
        pass
    if collapsed:
        out.add(collapsed)
    return out


def expand_team_tokens(name: str, sport_slug: Optional[str] = None) -> frozenset[str]:
    """
    Tokens para matching robusto: palabras normalizadas + expansión por grupos de sinónimos.
    """
    t = normalize_team_label(name)
    if not t:
        return frozenset()
    t2 = _strip_club_suffixes(_strip_club_prefixes(t))
    tokens: set[str] = set(_base_tokens_from_label(t2))
    if t2 in _ALIAS_MAP:
        alias_val = normalize_team_label(_ALIAS_MAP[t2])
        tokens |= _base_tokens_from_label(alias_val)
        tokens.add("".join(alias_val.split()))
    compact = normalize_team_for_match(name)
    if compact:
        tokens.add(compact)
        tokens |= _base_tokens_from_label(compact)
    sport_tags = sport_tags_from_hint(sport_slug)
    for group, req in _TOKEN_SYNONYM_GROUPS:
        if req is not None and not (sport_tags & req):
            continue
        if tokens & group:
            tokens |= group
    return frozenset(x for x in tokens if x)


# Tokens geográficos compartidos por varios clubes distintos (no bastan solos para match 1.0).
_GEO_SHARED_TOKENS = frozenset({"manchester", "los", "new", "san", "st"})


def _token_pair_score(ta: str, tb: str) -> float:
    if not ta or not tb:
        return 0.0
    if ta == tb:
        if ta in _GEO_SHARED_TOKENS:
            return 0.35
        return 1.0
    if ta in tb or tb in ta:
        return 0.95
    mx = max(len(ta), len(tb))
    if mx == 0:
        return 0.0
    d = float(levenshtein(ta, tb))
    sim = 1.0 - d / mx
    if d < 3 and min(len(ta), len(tb)) <= 5:
        sim = max(sim, 0.82)
    return max(0.0, min(1.0, sim))


def team_token_sets_match_score(tokens_a: frozenset[str], tokens_b: frozenset[str]) -> float:
    """Score 0–1 entre dos conjuntos de tokens (mismo equipo bajo distintas formas)."""
    if not tokens_a or not tokens_b:
        return 0.0
    inter = tokens_a & tokens_b
    if inter:
        core = inter - _GEO_SHARED_TOKENS
        if core:
            return 1.0
    best = 0.0
    for ta in tokens_a:
        for tb in tokens_b:
            best = max(best, _token_pair_score(ta, tb))
    union = tokens_a | tokens_b
    if union:
        jacc_like = len(inter) / len(union) if union else 0.0
        best = max(best, jacc_like)
    return best


def _manchester_city_vs_united_false_pair(a: str, b: str) -> bool:
    """Evita confundir Manchester City con Manchester United (mismo prefijo geográfico)."""
    na, nb = normalize_team_label(a), normalize_team_label(b)
    if "manchester" not in na or "manchester" not in nb:
        return False

    def _is_city_side(s: str) -> bool:
        return bool(re.search(r"\bcity\b", s)) or "mancity" in s.replace(" ", "") or "man city" in s

    def _is_united_side(s: str) -> bool:
        return bool(re.search(r"\bunited\b", s)) or bool(re.search(r"\butd\b", s)) or "manutd" in s.replace(
            " ", ""
        )

    return (_is_city_side(na) and _is_united_side(nb)) or (_is_city_side(nb) and _is_united_side(na))


def single_side_match_score(a: str, b: str, *, sport_slug: Optional[str] = None) -> float:
    """Score 0–1 de que dos nombres de equipo se refieran al mismo club."""
    if not (a or "").strip() or not (b or "").strip():
        return 0.0
    if _manchester_city_vs_united_false_pair(a, b):
        return 0.05
    sa = expand_team_tokens(a, sport_slug)
    sb = expand_team_tokens(b, sport_slug)
    sc = team_token_sets_match_score(sa, sb)
    if sc >= 0.55:
        return sc
    ca, cb = normalize_team_for_match(a), normalize_team_for_match(b)
    if _team_match_normalized_tokens(ca, cb):
        return max(sc, 0.88)
    mx = max(len(ca), len(cb))
    if mx and ca and cb:
        d = float(levenshtein(ca, cb))
        sc = max(sc, 1.0 - d / mx)
    return sc


TEAM_PAIR_MATCH_THRESHOLD = 0.72


def team_pair_match_score(
    poly_home: str,
    poly_away: str,
    odds_home: str,
    odds_away: str,
    *,
    sport_slug: Optional[str] = None,
) -> tuple[float, str]:
    """
    Score del par Poly vs Odds (directo o home/away cruzado). Retorna (score, direct|swap|none).
    """
    ph, pa = (poly_home or "").strip(), (poly_away or "").strip()
    oh, oa = (odds_home or "").strip(), (odds_away or "").strip()
    if not ph or not pa or not oh or not oa:
        return 0.0, "none"
    d_h = single_side_match_score(ph, oh, sport_slug=sport_slug)
    d_a = single_side_match_score(pa, oa, sport_slug=sport_slug)
    direct = (d_h + d_a) / 2.0
    if direct >= TEAM_PAIR_MATCH_THRESHOLD and d_h >= 0.5 and d_a >= 0.5:
        return direct, "direct"
    s_h = single_side_match_score(ph, oa, sport_slug=sport_slug)
    s_a = single_side_match_score(pa, oh, sport_slug=sport_slug)
    swap = (s_h + s_a) / 2.0
    if swap >= TEAM_PAIR_MATCH_THRESHOLD and s_h >= 0.5 and s_a >= 0.5:
        return swap, "swap"
    best = max(direct, swap)
    tag = "none"
    if best == direct and direct > 0:
        tag = "partial_direct"
    elif best == swap and swap > 0:
        tag = "partial_swap"
    return best, tag


def normalized_poly_odds_token_lists(
    poly_home: str,
    poly_away: str,
    odds_home: str,
    odds_away: str,
    *,
    sport_slug: Optional[str] = None,
) -> tuple[list[str], list[str]]:
    """Listas ordenadas de tokens para diagnóstico JSON."""
    ph, pa = (poly_home or "").strip(), (poly_away or "").strip()
    oh, oa = (odds_home or "").strip(), (odds_away or "").strip()
    poly_t = expand_team_tokens(ph, sport_slug) | expand_team_tokens(pa, sport_slug)
    odds_t = expand_team_tokens(oh, sport_slug) | expand_team_tokens(oa, sport_slug)
    return sorted(poly_t), sorted(odds_t)


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
    """Compat.: matching sobre strings ya normalizados."""
    return single_side_match_score(a, b, sport_slug=None) >= 0.55 or _team_match_normalized_tokens(a, b)


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


def _blob_context_tags(blob: str) -> frozenset[str]:
    """Heurística para desambiguar Spurs sin sport_slug en blobs Gamma."""
    b = (blob or "").lower()
    tags: set[str] = set()
    if "nba" in b or "san antonio" in b or "basketball" in b:
        tags.add("nba")
    if "epl" in b or "premier" in b or "tottenham" in b or "hotspur" in b:
        tags.add("epl")
        tags.add("soccer")
    return frozenset(tags)


def odds_team_matches_gamma_blob(
    odds_team: str,
    blob: str,
    *,
    sport_slug: Optional[str] = None,
) -> bool:
    """
    True si el nombre Odds coincide con el texto Gamma (título+slug con varios equipos).
    """
    blob_tags = _blob_context_tags(blob)
    effective_sport = sport_slug
    if not sport_tags_from_hint(sport_slug) and blob_tags:
        effective_sport = "soccer_epl" if blob_tags & {"epl", "soccer"} and not (blob_tags & {"nba"}) else "basketball_nba"
    n_o_tokens = expand_team_tokens(odds_team, effective_sport)
    if not n_o_tokens:
        return False
    n_full = normalize_team_label(blob)
    if n_full:
        full_tokens = expand_team_tokens(n_full, effective_sport)
        if team_token_sets_match_score(n_o_tokens, full_tokens) >= 0.55:
            return True
    chunks = _gamma_blob_team_chunks(blob)
    if len(chunks) >= 2:
        for chunk in chunks:
            ct = expand_team_tokens(chunk, effective_sport)
            if team_token_sets_match_score(n_o_tokens, ct) >= 0.55:
                return True
        return False
    n_single = expand_team_tokens(blob, effective_sport)
    return team_token_sets_match_score(n_o_tokens, n_single) >= 0.55


def _teams_match_odds_gamma_impl(odds_name: str, gamma_name: str, *, sport_slug: Optional[str] = None) -> bool:
    return single_side_match_score(odds_name, gamma_name, sport_slug=sport_slug) >= 0.55


def teams_match_odds_gamma(
    odds_name: str,
    gamma_name: str,
    *,
    sport_slug: Optional[str] = None,
) -> bool:
    """
    True si el equipo Odds API y el label Gamma (outcome corto o texto largo) coinciden.
    Textos con varios equipos (p. ej. título con \"vs\") comparan por segmentos.
    """
    chunks = _gamma_blob_team_chunks(gamma_name)
    if len(chunks) >= 2:
        return odds_team_matches_gamma_blob(odds_name, gamma_name, sport_slug=sport_slug)
    return _teams_match_odds_gamma_impl(odds_name, gamma_name, sport_slug=sport_slug)


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
        sc, _ = team_pair_match_score(ph, pa, oh, oa, sport_slug=None)
        if sc >= TEAM_PAIR_MATCH_THRESHOLD:
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
