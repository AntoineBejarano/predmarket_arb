"""Descarga mercados Polymarket 5m crypto Up/Down resueltos (Gamma GET /events + CLOB opcional); guarda parquet en data/raw."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, track
from rich.table import Table

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
OUT_PATH = RAW_DIR / "poly_markets_resolved.parquet"

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_PRICES = "https://clob.polymarket.com/prices-history"

MIN_END = pd.Timestamp("2023-01-01", tz="UTC")

# Prefijo de búsqueda legacy (primer campo); el descubrimiento usa GET /events + slug regex.
# Gamma suele 422 si offset > ~100k; pedimos hasta 100k y cortamos si la página cruza MIN_END (startDate).
GAMMA_EVENTS_MAX_OFFSET = 100_000

SEARCH_SPECS: list[tuple[str, re.Pattern[str], str]] = [
    ("bitcoin up or down", re.compile(r"^btc-updown-5m-\d+$"), "BTCUSDT"),
    ("ethereum up or down", re.compile(r"^eth-updown-5m-\d+$"), "ETHUSDT"),
    ("solana up or down", re.compile(r"^sol-updown-5m-\d+$"), "SOLUSDT"),
    ("xrp up or down", re.compile(r"^xrp-updown-5m-\d+$"), "XRPUSDT"),
    ("bnb up or down", re.compile(r"^bnb-updown-5m-\d+$"), "BNBUSDT"),
]

ALLOWED_ASSETS = frozenset(s[2] for s in SEARCH_SPECS)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "predmarket-arb-poly-history/1.0"})
console = Console()


def _parse_ts(val: object) -> pd.Timestamp | None:
    if val is None:
        return None
    try:
        t = pd.Timestamp(val)
        if t.tzinfo is None:
            t = t.tz_localize("UTC")
        else:
            t = t.tz_convert("UTC")
        return t
    except (ValueError, TypeError):
        return None


def _parse_clob_token_ids(raw: object) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if str(x).strip()]
    return []


def _merge_event_market(ev: dict) -> dict | None:
    """Une event Gamma con su primer mercado anidado (misma slug típica)."""
    mkts = ev.get("markets")
    if isinstance(mkts, list) and mkts and isinstance(mkts[0], dict):
        merged = {**ev, **mkts[0]}
        return merged
    return dict(ev) if isinstance(ev, dict) else None


def _up_outcome_won(m: dict) -> bool | None:
    """True si resolvió Up (primer outcome = Up). Acepta outcomePrices casi 1/0 o claramente sesgados."""
    raw = m.get("outcomePrices")
    prices: list[float] | None = None
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            try:
                prices = [float(x) for x in parsed]
            except (TypeError, ValueError):
                prices = None
    elif isinstance(raw, list):
        try:
            prices = [float(x) for x in raw]
        except (TypeError, ValueError):
            prices = None
    if prices and len(prices) >= 2:
        p_up, p_dn = float(prices[0]), float(prices[1])
        mx, mn = max(p_up, p_dn), min(p_up, p_dn)
        # Resuelto “claro”: casi 1 vs ~0 (Gamma suele usar 1/0 o strings "1"/"0").
        if mx >= 0.95 or mn <= 0.05:
            return p_up > p_dn
        # Aún decible con margen (evita tirar filas con 0.52/0.48).
        if mx >= 0.55 and abs(p_up - p_dn) >= 0.08:
            return p_up > p_dn
    wo = m.get("winningOutcome") or m.get("winning_outcome")
    if isinstance(wo, str):
        u = wo.strip().upper()
        if u == "UP":
            return True
        if u == "DOWN":
            return False
    r = m.get("resolution")
    if isinstance(r, str):
        u = r.strip().upper()
        if u in ("YES", "Y"):
            return True
        if u in ("NO", "N"):
            return False
    return None


def _open_price_ref(m: dict) -> float | None:
    v = m.get("openPrice") if "openPrice" in m else m.get("open_price")
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def fetch_yes_price_at_close(token_id: str, close_ts: pd.Timestamp) -> float | None:
    try:
        end_ts = int(close_ts.timestamp())
        start_ts = end_ts - 300
        r = SESSION.get(
            CLOB_PRICES,
            params={
                "market": token_id,
                "startTs": start_ts,
                "endTs": end_ts,
                "fidelity": 1,
            },
            timeout=30,
        )
        r.raise_for_status()
        j = r.json()
        hist = j.get("history", []) if isinstance(j, dict) else []
        if not hist:
            return None
        prices: list[float] = []
        for row in hist:
            if not isinstance(row, dict):
                continue
            p = row.get("p")
            if p is not None:
                try:
                    prices.append(float(p))
                except (TypeError, ValueError):
                    continue
        if not prices:
            return None
        return float(sum(prices) / len(prices))
    except Exception as e:  # noqa: BLE001
        log.debug("prices-history %s: %s", token_id[:16], e)
        return None


def _oldest_start_on_page(events: list) -> pd.Timestamp | None:
    """Menor startDate/start_date parseable de los eventos de una página (UTC)."""
    ts_list: list[pd.Timestamp] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        raw = ev.get("startDate") or ev.get("start_date")
        if raw is None:
            continue
        t = _parse_ts(raw)
        if t is not None:
            ts_list.append(t)
    return min(ts_list) if ts_list else None


def fetch_updown_markets_via_events(assets: frozenset[str] | None) -> list[dict]:
    """Pagina GET /events con closed=true y filtra por slug *-updown-5m-* (offset real)."""
    specs = SEARCH_SPECS
    if assets is not None:
        specs = [s for s in SEARCH_SPECS if s[2] in assets]
        if not specs:
            return []
    rows: list[dict] = []
    seen_ids: set[str] = set()
    limit = 100

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as prog:
        task = prog.add_task("Gamma /events …", total=None)
        for _q_text, slug_pat, asset in specs:
            offset = 0
            while True:
                if offset > GAMMA_EVENTS_MAX_OFFSET:
                    log.info(
                        "events %s: offset %s > máx %s; no se pide más a Gamma.",
                        asset,
                        offset,
                        GAMMA_EVENTS_MAX_OFFSET,
                    )
                    break
                try:
                    r = SESSION.get(
                        f"{GAMMA_BASE}/events",
                        params={
                            "closed": "true",
                            "limit": limit,
                            "offset": offset,
                            "order": "startDate",
                            "ascending": "false",
                        },
                        timeout=90,
                    )
                    r.raise_for_status()
                    events = r.json()
                except requests.HTTPError as e:
                    code = e.response.status_code if e.response is not None else None
                    if code == 422:
                        log.warning(
                            "events %s offset=%s: Gamma 422 (offset fuera de rango; típ. límite ~%s). Fin paginación.",
                            asset,
                            offset,
                            GAMMA_EVENTS_MAX_OFFSET,
                        )
                    else:
                        log.warning("events %s offset=%s: %s", asset, offset, e)
                    break
                except Exception as e:  # noqa: BLE001
                    log.warning("events %s offset=%s: %s", asset, offset, e)
                    break
                if not isinstance(events, list) or len(events) == 0:
                    break

                n_new = 0
                for ev in events:
                    if not isinstance(ev, dict):
                        continue
                    if not ev.get("closed"):
                        continue
                    slug = str(ev.get("slug") or "")
                    if not slug_pat.match(slug):
                        continue
                    m = _merge_event_market(ev)
                    if not m:
                        continue
                    mid = str(m.get("id") or "")
                    if not mid or mid in seen_ids:
                        continue
                    seen_ids.add(mid)
                    m["_asset"] = asset
                    rows.append(m)
                    n_new += 1

                oldest_start = _oldest_start_on_page(events)
                if oldest_start is not None and oldest_start < MIN_END:
                    log.info(
                        "events %s off=%s: startDate mín. de página %s < MIN_END (%s); fin.",
                        asset,
                        offset,
                        oldest_start,
                        MIN_END,
                    )
                    break

                prog.update(
                    task,
                    description=f"{asset} off={offset} +{n_new} pág (únicos {len(rows)})",
                )
                offset += limit
                time.sleep(0.15)

                if len(events) < limit:
                    break

    return rows


def process_markets(raw: list[dict], *, skip_clob_prices: bool = False) -> list[dict]:
    out: list[dict] = []
    n_skip_res = 0
    n_skip_end = 0
    n_skip_start = 0
    n_dup = 0
    seen_ids: set[str] = set()
    n_raw = len(raw)
    if n_raw:
        if skip_clob_prices:
            console.print(f"[dim]Filtros sin CLOB: {n_raw} mercados (rápido; yes_price_at_close quedará vacío).[/dim]")
        else:
            est_min = max(0.11 * n_raw / 60.0, 0.2)
            console.print(
                f"[dim]Filtros + CLOB: {n_raw} mercados (~{est_min:.0f} min por sleep 0.1s + red; ver barra)[/dim]",
            )
    for i, m in enumerate(
        track(
            raw,
            description="Filtros (+ CLOB)" if not skip_clob_prices else "Filtros",
            console=console,
            refresh_per_second=2,
        ),
    ):
        try:
            asset = str(m.get("_asset") or "")
            if not asset:
                continue
            q = str(m.get("question") or m.get("title") or "")

            end_raw = m.get("endDate") or m.get("end_date") or m.get("endDateIso")
            close_ts = _parse_ts(end_raw)
            if close_ts is None or close_ts < MIN_END:
                n_skip_end += 1
                continue

            ry = _up_outcome_won(m)
            if ry is None:
                n_skip_res += 1
                continue

            start_raw = m.get("startDate") or m.get("start_date")
            open_ts = _parse_ts(start_raw)
            if open_ts is None:
                n_skip_start += 1
                continue

            mid = m.get("id")
            if mid is None:
                continue
            market_id = str(mid)
            if market_id in seen_ids:
                n_dup += 1
                continue
            seen_ids.add(market_id)

            toks = _parse_clob_token_ids(m.get("clobTokenIds") or m.get("clob_token_ids"))
            token_yes_id = toks[0] if toks else None

            yes_price: float | None = None
            if token_yes_id and not skip_clob_prices:
                yes_price = fetch_yes_price_at_close(token_yes_id, close_ts)
                time.sleep(0.1)

            out.append(
                {
                    "market_id": market_id,
                    "question": q,
                    "asset": asset,
                    "direction": "both",
                    "open_ts": open_ts,
                    "close_ts": close_ts,
                    "resolved_yes": bool(ry),
                    "open_price_ref": _open_price_ref(m),
                    "token_yes_id": token_yes_id,
                    "yes_price_at_close": yes_price,
                }
            )
        except Exception as e:  # noqa: BLE001
            log.warning("mercado índice %s: %s", i, e)
            continue

    console.print(
        f"[dim]Filtros: sin_fecha_fin={n_skip_end}, sin_resolución={n_skip_res}, "
        f"sin_fecha_inicio={n_skip_start}, duplicados_id={n_dup}[/dim]"
    )
    return out


def _parse_assets_arg(names: list[str] | None) -> frozenset[str] | None:
    """None = todos los activos soportados; si no vacío, solo esos *USDT."""
    if not names:
        return None
    out: set[str] = set()
    for raw in names:
        u = raw.strip().upper()
        if not u.endswith("USDT"):
            u = f"{u}USDT"
        if u not in ALLOWED_ASSETS:
            console.print(f"[red]Activo no soportado para Poly 5m: {raw} (usa BTC, ETH, …)[/red]")
            sys.exit(1)
        out.add(u)
    return frozenset(out)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Descarga mercados Polymarket 5m Up/Down resueltos (Gamma + CLOB opcional).",
    )
    p.add_argument(
        "--assets",
        nargs="+",
        metavar="SYM",
        default=None,
        help="Solo estos activos (ej. BTC o BTCUSDT). Por defecto: todos los soportados.",
    )
    p.add_argument(
        "--skip-clob-prices",
        action="store_true",
        help="No llamar a CLOB prices-history por mercado (mucho más rápido; yes_price_at_close = null).",
    )
    args = p.parse_args()
    asset_filter = _parse_assets_arg(args.assets)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if asset_filter:
        console.print(
            f"[bold]Descarga Polymarket 5m Up/Down — solo: {', '.join(sorted(asset_filter))}[/bold]"
        )
    else:
        console.print("[bold]Descarga mercados Polymarket 5m Up/Down (Gamma /events)[/bold]")
    raw = fetch_updown_markets_via_events(asset_filter)
    console.print(f"Total mercados candidatos (tras filtro slug): {len(raw)}")
    rows = process_markets(raw, skip_clob_prices=bool(args.skip_clob_prices))
    if not rows:
        console.print("[red]Sin filas tras filtros. Abortando sin escribir parquet.[/red]")
        sys.exit(1)
    df = pd.DataFrame(rows)
    df.to_parquet(OUT_PATH, index=False, engine="pyarrow")
    console.print(f"[green]OK[/green] {OUT_PATH.relative_to(REPO_ROOT)} ({len(df)} filas)")

    t = Table(title="Resumen por asset")
    t.add_column("asset")
    t.add_column("n", justify="right")
    t.add_column("% Up", justify="right")
    for asset in sorted(df["asset"].unique()):
        sub = df[df["asset"] == asset]
        n = len(sub)
        pct = 100.0 * float(sub["resolved_yes"].mean()) if n else 0.0
        t.add_row(asset, str(n), f"{pct:.1f}%")
    console.print(t)


if __name__ == "__main__":
    main()
